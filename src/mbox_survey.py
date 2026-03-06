"""
Download and parse a HyperkItty mbox archive export, printing a summary of each message.

Usage:
    python mbox_survey.py amber-dev 2026 2
    python mbox_survey.py amber-dev 2026 2 --raw
"""

import argparse
import sys

from mbox import body_text, build_export_url, download_mbox, parse_mbox


def summarize(msg, index):
    subject = msg.get("Subject", "(no subject)")
    from_hdr = msg.get("From", "(unknown)")
    date = msg.get("Date", "(no date)")
    message_id = msg.get("Message-ID", "(no id)")
    in_reply_to = msg.get("In-Reply-To")
    body = body_text(msg)
    body_len = len(body)
    body_lines = body.count("\n") + 1 if body else 0
    preview = body[:200].replace("\n", "\\n") if body else "(empty)"

    print(f"--- #{index + 1} ---")
    print(f"  Subject:     {subject}")
    print(f"  From:        {from_hdr}")
    print(f"  Date:        {date}")
    print(f"  Message-ID:  {message_id}")
    if in_reply_to:
        print(f"  In-Reply-To: {in_reply_to}")
    print(f"  Body:        {body_len} chars, {body_lines} lines")
    print(f"  Preview:     {preview}")
    print()


def main():
    parser = argparse.ArgumentParser(description="Survey an OpenJDK mailing list mbox archive")
    parser.add_argument("list_name", help="Mailing list name, e.g. amber-dev")
    parser.add_argument("year", type=int)
    parser.add_argument("month", type=int)
    parser.add_argument("--raw", action="store_true", help="Print full message bodies instead of summaries")
    args = parser.parse_args()

    url = build_export_url(args.list_name, args.year, args.month)
    print(f"Downloading: {url}")
    print()

    try:
        raw, compressed_size, elapsed = download_mbox(url)
    except Exception as e:
        print(f"Download failed: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"Downloaded {compressed_size:,} bytes compressed -> {len(raw):,} bytes raw in {elapsed:.1f}s")
    print()

    if not raw:
        print("Archive is empty (no messages for this period).")
        sys.exit(0)

    messages = parse_mbox(raw)
    print(f"Parsed {len(messages)} messages")
    print("=" * 72)
    print()

    for i, msg in enumerate(messages):
        if args.raw:
            print(f"--- #{i + 1} ---")
            print(f"  Subject: {msg.get('Subject', '')}")
            print(f"  From:    {msg.get('From', '')}")
            print()
            print(body_text(msg))
            print()
        else:
            summarize(msg, i)

    subjects = set()
    senders = set()
    for msg in messages:
        subjects.add(msg.get("Subject", ""))
        senders.add(msg.get("From", ""))

    print("=" * 72)
    print(f"Total:    {len(messages)} messages")
    print(f"Threads:  ~{len(subjects)} unique subjects")
    print(f"Senders:  {len(senders)} unique senders")


if __name__ == "__main__":
    main()
