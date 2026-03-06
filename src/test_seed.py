import json
import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch
from urllib.error import HTTPError

from mbox import parse_mbox
from seed import (
    bulk_index,
    get_latest_date,
    month_range,
    parse_date,
    resolve_start,
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
    @patch("seed.urlopen")
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

    @patch("seed.urlopen")
    def test_no_index_404(self, mock_urlopen):
        mock_urlopen.side_effect = HTTPError(
            url="http://fake:9200/test-index/_search",
            code=404, msg="Not Found", hdrs={}, fp=None,
        )
        result = get_latest_date("http://fake:9200", "test-index", "test-list")
        self.assertIsNone(result)

    @patch("seed.urlopen")
    def test_no_records(self, mock_urlopen):
        body = json.dumps({
            "aggregations": {"latest": {"value": None, "value_as_string": None}}
        }).encode()
        mock_urlopen.return_value = mock_response(body)
        result = get_latest_date("http://fake:9200", "test-index", "test-list")
        self.assertIsNone(result)


class TestResolveStart(unittest.TestCase):
    @patch("seed.get_latest_date")
    def test_with_date(self, mock_latest):
        mock_latest.return_value = datetime(2024, 6, 15, tzinfo=timezone.utc)
        self.assertEqual(resolve_start("test", "http://fake", "idx"), (2024, 6, 15))

    @patch("seed.get_latest_date")
    def test_no_records(self, mock_latest):
        mock_latest.return_value = None
        self.assertIsNone(resolve_start("test", "http://fake", "idx"))


class TestBulkIndex(unittest.TestCase):
    def test_empty(self):
        self.assertEqual(bulk_index("http://fake:9200", "idx", []), (0, 0))

    @patch("seed.urlopen")
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

    @patch("seed.urlopen")
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

    @patch("seed.BULK_BATCH_SIZE", 2)
    @patch("seed.urlopen")
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


if __name__ == "__main__":
    unittest.main()
