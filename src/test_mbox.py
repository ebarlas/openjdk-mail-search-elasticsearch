import mailbox
import unittest

from mbox import body_text, build_export_url, parse_mbox

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


class TestBuildExportUrl(unittest.TestCase):
    def test_normal_month(self):
        url = build_export_url("amber-dev", 2024, 3)
        self.assertIn("/amber-dev@openjdk.org/export/", url)
        self.assertIn("amber-dev@openjdk.org-2024-03.mbox.gz", url)
        self.assertIn("start=2024-03-01", url)
        self.assertIn("end=2024-04-01", url)

    def test_december_rollover(self):
        url = build_export_url("discuss", 2024, 12)
        self.assertIn("start=2024-12-01", url)
        self.assertIn("end=2025-01-01", url)


class TestParseMbox(unittest.TestCase):
    def test_two_messages(self):
        messages = parse_mbox(MBOX_TWO_MESSAGES)
        self.assertEqual(len(messages), 2)
        self.assertEqual(messages[0]["Subject"], "First")
        self.assertEqual(messages[0]["From"], "Alice <alice@example.com>")
        self.assertEqual(messages[0]["Message-ID"], "<msg-1@example.com>")
        self.assertEqual(messages[1]["Subject"], "Second")
        self.assertEqual(messages[1]["In-Reply-To"], "<msg-1@example.com>")

    def test_empty(self):
        messages = parse_mbox(b"")
        self.assertEqual(len(messages), 0)


class TestBodyText(unittest.TestCase):
    def test_plain(self):
        msg = mailbox.mboxMessage()
        msg.set_payload("Hello, world!", charset="utf-8")
        self.assertEqual(body_text(msg), "Hello, world!")

    def test_multipart(self):
        from email.mime.multipart import MIMEMultipart
        from email.mime.text import MIMEText

        outer = MIMEMultipart("alternative")
        outer.attach(MIMEText("<p>HTML</p>", "html"))
        outer.attach(MIMEText("Plain text body", "plain"))
        msg = mailbox.mboxMessage(outer)
        self.assertEqual(body_text(msg), "Plain text body")

    def test_empty_payload(self):
        msg = mailbox.mboxMessage()
        self.assertEqual(body_text(msg), "")


if __name__ == "__main__":
    unittest.main()
