import json
import os
import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock, call, patch
from urllib.error import HTTPError

from mbox import parse_mbox
from sync import (
    CHECKPOINT_INDEX,
    MAILING_LISTS,
    bulk_index,
    decode_header_value,
    get_checkpoint,
    get_latest_date,
    lambda_handler,
    month_range,
    parse_date,
    put_checkpoint,
    resolve_start,
    sync_list,
    strip_angle_brackets,
    transform_message,
)

MBOX_TWO_MESSAGES = b"""\
From sender@example.com Mon Jan  1 00:00:00 2024
From: Alice <alice@example.com>
Subject: First
Message-ID: <msg-1@example.com>
Date: Mon, 01 Jan 2024 00:00:00 +0000

Hello from Alice.

From sender@example.com Tue Jan  2 00:00:00 2024
From: Bob <bob@example.com>
Subject: Second
Message-ID: <msg-2@example.com>
Date: Tue, 02 Jan 2024 00:00:00 +0000
In-Reply-To: <msg-1@example.com>

Reply from Bob.
"""

MBOX_RFC2047_AUTHOR = b"""\
From sender@example.com Mon Jan  1 00:00:00 2024
From: Alice =?utf-8?q?M=C3=BCller?= <amuller@example.com>
Subject: =?utf-8?q?Caf=C3=A9_menu?=
Message-ID: <rfc2047@example.com>
Date: Mon, 01 Jan 2024 00:00:00 +0000

Body text.
"""

MBOX_FOLDED_SUBJECT = b"""\
From sender@example.com Mon Jan  1 00:00:00 2024
From: Alice <alice@example.com>
Subject: Re: RFR: 8372353: API to compute the byte length of a String encoded
 in a given Charset [v21]
Message-ID: <folded@example.com>
Date: Mon, 01 Jan 2024 00:00:00 +0000

Body text.
"""


def mock_response(body_bytes):
    resp = MagicMock()
    resp.read.return_value = body_bytes
    resp.__enter__ = lambda s: s
    resp.__exit__ = MagicMock(return_value=False)
    return resp


# --- Pure function tests ---


class TestStripAngleBrackets(unittest.TestCase):
    def test_with_brackets(self):
        self.assertEqual(strip_angle_brackets("<foo@bar>"), "foo@bar")

    def test_without_brackets(self):
        self.assertEqual(strip_angle_brackets("foo@bar"), "foo@bar")

    def test_none(self):
        self.assertIsNone(strip_angle_brackets(None))

    def test_empty(self):
        self.assertIsNone(strip_angle_brackets(""))

    def test_whitespace(self):
        self.assertEqual(strip_angle_brackets("  <id@host>  "), "id@host")


class TestParseDate(unittest.TestCase):
    def test_valid(self):
        msg = {"Date": "Mon, 01 Jan 2024 00:00:00 +0000"}
        self.assertEqual(parse_date(msg), "2024-01-01T00:00:00+0000")

    def test_missing(self):
        self.assertIsNone(parse_date({}))

    def test_malformed(self):
        msg = {"Date": "not-a-date"}
        self.assertEqual(parse_date(msg), "not-a-date")


class TestTransformMessage(unittest.TestCase):
    def setUp(self):
        self.messages = parse_mbox(MBOX_TWO_MESSAGES)

    def test_basic_fields(self):
        doc = transform_message(self.messages[0], "test-list")
        self.assertEqual(doc["_id"], "msg-1@example.com")
        self.assertEqual(doc["list"], "test-list")
        self.assertEqual(doc["message_id"], "msg-1@example.com")
        self.assertIsNone(doc["in_reply_to"])
        self.assertEqual(doc["subject"], "First")
        self.assertEqual(doc["author"], "Alice")
        self.assertEqual(doc["email"], "alice@example.com")
        self.assertIn("2024", doc["date"])
        self.assertIn("Hello from Alice", doc["body"])

    def test_reply(self):
        doc = transform_message(self.messages[1], "test-list")
        self.assertEqual(doc["in_reply_to"], "msg-1@example.com")
        self.assertEqual(doc["author"], "Bob")
        self.assertEqual(doc["email"], "bob@example.com")

    def test_no_message_id(self):
        msg = MagicMock()
        msg.get.return_value = None
        self.assertIsNone(transform_message(msg, "test-list"))

    def test_rfc2047_author(self):
        messages = parse_mbox(MBOX_RFC2047_AUTHOR)
        doc = transform_message(messages[0], "test-list")
        self.assertEqual(doc["author"], "Alice Müller")
        self.assertEqual(doc["email"], "amuller@example.com")
        self.assertEqual(doc["subject"], "Café menu")

    def test_folded_subject(self):
        messages = parse_mbox(MBOX_FOLDED_SUBJECT)
        doc = transform_message(messages[0], "test-list")
        self.assertNotIn("\n", doc["subject"])
        self.assertIn("String encoded in a given Charset", doc["subject"])


class TestDecodeHeaderValue(unittest.TestCase):
    def test_plain_text(self):
        self.assertEqual(decode_header_value("Hello World"), "Hello World")

    def test_empty(self):
        self.assertEqual(decode_header_value(""), "")

    def test_none(self):
        self.assertIsNone(decode_header_value(None))

    def test_q_encoding_utf8(self):
        self.assertEqual(
            decode_header_value("=?utf-8?q?M=C3=BCller?="),
            "Müller",
        )

    def test_mixed_plain_and_encoded(self):
        self.assertEqual(
            decode_header_value("Alice =?utf-8?q?M=C3=BCller?="),
            "Alice Müller",
        )

    def test_b_encoding(self):
        self.assertEqual(
            decode_header_value("=?utf-8?b?TcO8bGxlcg==?="),
            "Müller",
        )

    def test_folded_header(self):
        self.assertEqual(
            decode_header_value("long subject\n continued here"),
            "long subject continued here",
        )

    def test_leading_fold(self):
        self.assertEqual(
            decode_header_value("\n Re: RFR: subject text"),
            "Re: RFR: subject text",
        )


# --- HTTP-dependent tests (mocked) ---


class TestMonthRange(unittest.TestCase):
    def test_single_month(self):
        result = month_range(2026, 3)
        self.assertIn((2026, 3), result)
        self.assertEqual(result[0], (2026, 3))

    def test_spans_year_boundary(self):
        result = month_range(2024, 11)
        self.assertIn((2024, 11), result)
        self.assertIn((2024, 12), result)
        self.assertIn((2025, 1), result)

    def test_ordered(self):
        result = month_range(2024, 1)
        self.assertEqual(result, sorted(result))


class TestGetLatestDate(unittest.TestCase):
    @patch("sync.urlopen")
    def test_with_results(self, mock_urlopen):
        body = json.dumps({
            "aggregations": {
                "latest": {
                    "value": 1704067200000,
                    "value_as_string": "2024-01-01T00:00:00.000Z",
                }
            }
        }).encode()
        mock_urlopen.return_value = mock_response(body)
        result = get_latest_date("http://fake:9200", "test-index", "test-list")
        self.assertEqual(result, datetime(2024, 1, 1, tzinfo=timezone.utc))

    @patch("sync.urlopen")
    def test_no_index_404(self, mock_urlopen):
        mock_urlopen.side_effect = HTTPError(
            url="http://fake:9200/test-index/_search",
            code=404, msg="Not Found", hdrs={}, fp=None,
        )
        result = get_latest_date("http://fake:9200", "test-index", "test-list")
        self.assertIsNone(result)

    @patch("sync.urlopen")
    def test_no_records(self, mock_urlopen):
        body = json.dumps({
            "aggregations": {"latest": {"value": None, "value_as_string": None}}
        }).encode()
        mock_urlopen.return_value = mock_response(body)
        result = get_latest_date("http://fake:9200", "test-index", "test-list")
        self.assertIsNone(result)


class TestResolveStart(unittest.TestCase):
    @patch("sync.get_latest_date")
    def test_with_date(self, mock_latest):
        mock_latest.return_value = datetime(2024, 6, 15, tzinfo=timezone.utc)
        self.assertEqual(resolve_start("test", "http://fake", "idx"), (2024, 6, 15))

    @patch("sync.get_latest_date")
    def test_no_records(self, mock_latest):
        mock_latest.return_value = None
        self.assertIsNone(resolve_start("test", "http://fake", "idx"))


class TestBulkIndex(unittest.TestCase):
    def test_empty(self):
        self.assertEqual(bulk_index("http://fake:9200", "idx", []), (0, 0))

    @patch("sync.urlopen")
    def test_success(self, mock_urlopen):
        body = json.dumps({
            "errors": False,
            "items": [
                {"index": {"_id": "a", "status": 201}},
                {"index": {"_id": "b", "status": 201}},
            ],
        }).encode()
        mock_urlopen.return_value = mock_response(body)
        docs = [
            {"_id": "a", "subject": "A"},
            {"_id": "b", "subject": "B"},
        ]
        self.assertEqual(bulk_index("http://fake:9200", "idx", docs), (2, 0))

    @patch("sync.urlopen")
    def test_partial_error(self, mock_urlopen):
        body = json.dumps({
            "errors": True,
            "items": [
                {"index": {"_id": "a", "status": 201}},
                {"index": {"_id": "b", "status": 400, "error": {"reason": "bad field"}}},
            ],
        }).encode()
        mock_urlopen.return_value = mock_response(body)
        docs = [
            {"_id": "a", "subject": "A"},
            {"_id": "b", "subject": "B"},
        ]
        self.assertEqual(bulk_index("http://fake:9200", "idx", docs), (1, 1))

    @patch("sync.BULK_BATCH_SIZE", 2)
    @patch("sync.urlopen")
    def test_batching(self, mock_urlopen):
        body = json.dumps({"errors": False, "items": []}).encode()
        mock_urlopen.return_value = mock_response(body)
        docs = [
            {"_id": "a", "x": 1},
            {"_id": "b", "x": 2},
            {"_id": "c", "x": 3},
        ]
        bulk_index("http://fake:9200", "idx", docs)
        self.assertEqual(mock_urlopen.call_count, 2)

        first_call = mock_urlopen.call_args_list[0]
        first_req = first_call[0][0]
        first_lines = first_req.data.decode().strip().split("\n")
        self.assertEqual(len(first_lines), 4)

        second_call = mock_urlopen.call_args_list[1]
        second_req = second_call[0][0]
        second_lines = second_req.data.decode().strip().split("\n")
        self.assertEqual(len(second_lines), 2)


class TestGetCheckpoint(unittest.TestCase):
    @patch("sync.urlopen")
    def test_found(self, mock_urlopen):
        body = json.dumps({
            "_source": {"list": "amber-dev", "synced_at": "2026-03-06T23:46:05Z"}
        }).encode()
        mock_urlopen.return_value = mock_response(body)
        result = get_checkpoint("http://fake:9200", "cp-index", "amber-dev")
        self.assertEqual(result, (2026, 3, 6))

    @patch("sync.urlopen")
    def test_not_found(self, mock_urlopen):
        mock_urlopen.side_effect = HTTPError(
            url="", code=404, msg="Not Found", hdrs={}, fp=None,
        )
        result = get_checkpoint("http://fake:9200", "cp-index", "amber-dev")
        self.assertIsNone(result)

    @patch("sync.urlopen")
    def test_parses_month_correctly(self, mock_urlopen):
        body = json.dumps({
            "_source": {"list": "test", "synced_at": "2025-11-15T10:30:00Z"}
        }).encode()
        mock_urlopen.return_value = mock_response(body)
        result = get_checkpoint("http://fake:9200", "cp-index", "test")
        self.assertEqual(result, (2025, 11, 15))


class TestPutCheckpoint(unittest.TestCase):
    @patch("sync.urlopen")
    def test_writes_checkpoint(self, mock_urlopen):
        mock_urlopen.return_value = mock_response(b'{"result": "created"}')
        put_checkpoint("http://fake:9200", "cp-index", "amber-dev")
        req = mock_urlopen.call_args[0][0]
        self.assertEqual(req.get_method(), "PUT")
        self.assertIn("cp-index/_doc/amber-dev", req.full_url)
        body = json.loads(req.data)
        self.assertEqual(body["list"], "amber-dev")
        self.assertIn("synced_at", body)


class TestSeedListCheckpoints(unittest.TestCase):
    @patch("sync.put_checkpoint")
    @patch("sync.download_mbox", return_value=(b"", 0, 0.0))
    @patch("sync.get_checkpoint", return_value=(2026, 3, 5))
    def test_uses_checkpoint_over_resolve_start(self, mock_get_cp, mock_dl, mock_put_cp):
        sync_list("test-list", "http://fake:9200", "idx", None, "cp-index")
        mock_get_cp.assert_called_once_with("http://fake:9200", "cp-index", "test-list")
        mock_put_cp.assert_called_once_with("http://fake:9200", "cp-index", "test-list")

    @patch("sync.put_checkpoint")
    @patch("sync.download_mbox", return_value=(b"", 0, 0.0))
    @patch("sync.resolve_start", return_value=(2026, 3, 1))
    @patch("sync.get_checkpoint", return_value=None)
    def test_falls_back_to_resolve_start(self, mock_get_cp, mock_resolve, mock_dl, mock_put_cp):
        sync_list("test-list", "http://fake:9200", "idx", None, "cp-index")
        mock_get_cp.assert_called_once()
        mock_resolve.assert_called_once()
        mock_put_cp.assert_called_once()

    @patch("sync.put_checkpoint")
    @patch("sync.download_mbox", return_value=(b"", 0, 0.0))
    @patch("sync.get_checkpoint")
    def test_start_ym_bypasses_checkpoint(self, mock_get_cp, mock_dl, mock_put_cp):
        sync_list("test-list", "http://fake:9200", "idx", (2026, 3), "cp-index")
        mock_get_cp.assert_not_called()
        mock_put_cp.assert_called_once()


class TestLambdaHandler(unittest.TestCase):
    @patch("sync.sync_list")
    @patch.dict(os.environ, {"ES_URL": "http://es:9200"})
    def test_calls_sync_list_for_each_mailing_list(self, mock_seed):
        lambda_handler({}, None)
        self.assertEqual(mock_seed.call_count, len(MAILING_LISTS))
        for list_name in MAILING_LISTS:
            mock_seed.assert_any_call(
                list_name, "http://es:9200", "openjdk-mail",
                start_ym=None, checkpoint_index=CHECKPOINT_INDEX,
            )

    @patch("sync.sync_list")
    @patch.dict(os.environ, {"ES_URL": "http://es:9200", "INDEX_NAME": "custom-index"})
    def test_custom_index_name(self, mock_seed):
        lambda_handler({}, None)
        for c in mock_seed.call_args_list:
            self.assertEqual(c[0][2], "custom-index")

    @patch("sync.sync_list")
    @patch.dict(os.environ, {}, clear=True)
    def test_missing_es_url_raises(self, mock_seed):
        with self.assertRaises(KeyError):
            lambda_handler({}, None)
        mock_seed.assert_not_called()

    @patch("sync.sync_list")
    @patch.dict(os.environ, {"ES_URL": "http://es:9200"})
    def test_default_checkpoint_index(self, mock_seed):
        lambda_handler({}, None)
        for c in mock_seed.call_args_list:
            self.assertEqual(c[1]["checkpoint_index"], CHECKPOINT_INDEX)

    @patch("sync.sync_list")
    @patch.dict(os.environ, {"ES_URL": "http://es:9200", "CHECKPOINT_INDEX": "custom-cp"})
    def test_custom_checkpoint_index(self, mock_seed):
        lambda_handler({}, None)
        for c in mock_seed.call_args_list:
            self.assertEqual(c[1]["checkpoint_index"], "custom-cp")


if __name__ == "__main__":
    unittest.main()
