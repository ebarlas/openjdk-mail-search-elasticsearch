"""
Seed and synchronize a mailing list's ES index from HyperkItty mbox archives.

On first run (no existing records), downloads all available months.
On subsequent runs, re-downloads from the month of the latest indexed
record onward, filling any gaps idempotently.

Usage:
    python seed.py amber-dev
    python seed.py amber-dev --es-url http://localhost:9200 --index openjdk-mail
    python seed.py amber-dev --start 2024-01
"""

import argparse
import json
import re
import sys
import time
from datetime import datetime, timezone
from email.utils import parseaddr, parsedate_to_datetime
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from mbox import ARCHIVES_BASE, body_text, build_export_url, download_mbox, parse_mbox

BULK_BATCH_SIZE = 500


def discover_months(list_name):
    """Fetch the archive page and extract all available (year, month) pairs."""
    fqlist = f"{list_name}@openjdk.org"
    url = f"{ARCHIVES_BASE}/{fqlist}/"
    req = Request(url, headers={"User-Agent": "openjdk-mail-search"})
    with urlopen(req, timeout=30) as resp:
        html = resp.read().decode("utf-8", errors="replace")
    pattern = re.compile(rf"/archives/list/{re.escape(fqlist)}/(\d{{4}})/(\d{{1,2}})/")
    months = sorted({(int(m.group(1)), int(m.group(2))) for m in pattern.finditer(html)})
    return months


def get_latest_date(es_url, index_name, list_name):
    """Query ES for the most recent date in the index for this list."""
    query = {
        "size": 0,
        "query": {"term": {"list": list_name}},
        "aggs": {"latest": {"max": {"field": "date"}}},
    }
    req = Request(
        f"{es_url}/{index_name}/_search",
        data=json.dumps(query).encode(),
        headers={"Content-Type": "application/json", "User-Agent": "openjdk-mail-search"},
    )
    try:
        with urlopen(req, timeout=15) as resp:
            result = json.loads(resp.read())
    except HTTPError as e:
        if e.code == 404:
            return None
        raise
    value = result["aggregations"]["latest"]["value"]
    if value is None:
        return None
    epoch_ms = value
    return datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc)


def strip_angle_brackets(s):
    if not s:
        return None
    s = s.strip()
    if s.startswith("<") and s.endswith(">"):
        return s[1:-1]
    return s


def parse_date(msg):
    raw = msg.get("Date")
    if not raw:
        return None
    try:
        dt = parsedate_to_datetime(raw)
        return dt.strftime("%Y-%m-%dT%H:%M:%S%z")
    except Exception:
        return raw


def transform_message(msg, list_name):
    message_id = strip_angle_brackets(msg.get("Message-ID"))
    if not message_id:
        return None

    from_hdr = msg.get("From", "")
    author_name, author_email = parseaddr(from_hdr)

    return {
        "_id": message_id,
        "list": list_name,
        "message_id": message_id,
        "in_reply_to": strip_angle_brackets(msg.get("In-Reply-To")),
        "subject": msg.get("Subject", ""),
        "author": author_name or author_email,
        "email": author_email,
        "date": parse_date(msg),
        "body": body_text(msg),
    }


def bulk_index(es_url, index_name, docs):
    """Send documents to ES via the _bulk API. Returns (success_count, error_count)."""
    if not docs:
        return 0, 0

    total_ok = 0
    total_err = 0

    for i in range(0, len(docs), BULK_BATCH_SIZE):
        batch = docs[i:i + BULK_BATCH_SIZE]
        lines = []
        for doc in batch:
            doc_id = doc.pop("_id")
            action = {"index": {"_index": index_name, "_id": doc_id}}
            lines.append(json.dumps(action, separators=(",", ":")))
            lines.append(json.dumps(doc, separators=(",", ":")))
        body = "\n".join(lines) + "\n"

        req = Request(
            f"{es_url}/_bulk",
            data=body.encode("utf-8"),
            headers={
                "Content-Type": "application/x-ndjson",
                "User-Agent": "openjdk-mail-search",
            },
            method="POST",
        )
        with urlopen(req, timeout=120) as resp:
            result = json.loads(resp.read())

        if result.get("errors"):
            for item in result["items"]:
                action_result = item.get("index", {})
                if action_result.get("error"):
                    total_err += 1
                    print(f"  error: {action_result['error']['reason']}", file=sys.stderr)
                else:
                    total_ok += 1
        else:
            total_ok += len(batch)

    return total_ok, total_err


def resolve_start(list_name, es_url, index_name):
    """Determine the starting (year, month) based on the latest indexed record."""
    latest = get_latest_date(es_url, index_name, list_name)
    if latest is None:
        return None
    return (latest.year, latest.month)


def seed_list(list_name, es_url, index_name, start_ym):
    print(f"Discovering months for {list_name}...")
    months = discover_months(list_name)
    print(f"Found {len(months)} months ({months[0][0]}-{months[0][1]:02d} to {months[-1][0]}-{months[-1][1]:02d})")

    if start_ym is None:
        start_ym = resolve_start(list_name, es_url, index_name)

    if start_ym:
        full_count = len(months)
        months = [(y, m) for y, m in months if (y, m) >= start_ym]
        print(f"Starting from {start_ym[0]}-{start_ym[1]:02d}, {len(months)} of {full_count} months")
    else:
        print("No existing records, full seed")

    cumulative = 0
    t_start = time.monotonic()

    for year, month in months:
        url = build_export_url(list_name, year, month)
        try:
            raw, compressed_size, dl_elapsed = download_mbox(url)
        except Exception as e:
            print(f"  {year}-{month:02d}: download failed: {e}", file=sys.stderr)
            continue

        if not raw:
            print(f"  {year}-{month:02d}: empty")
            continue

        messages = parse_mbox(raw)
        docs = []
        skipped = 0
        for msg in messages:
            doc = transform_message(msg, list_name)
            if doc:
                docs.append(doc)
            else:
                skipped += 1

        ok, err = bulk_index(es_url, index_name, docs)
        cumulative += ok
        elapsed = time.monotonic() - t_start

        print(
            f"  {year}-{month:02d}: "
            f"{len(messages)} parsed, {ok} indexed, {err} errors, {skipped} skipped | "
            f"cumulative: {cumulative} | {elapsed:.0f}s"
        )

    total_elapsed = time.monotonic() - t_start
    print(f"\nDone. {cumulative} documents indexed in {total_elapsed:.0f}s")


def main():
    parser = argparse.ArgumentParser(description="Seed/sync ES index from HyperkItty mbox archives")
    parser.add_argument("list_name", help="Mailing list name, e.g. amber-dev")
    parser.add_argument("--es-url", default="http://localhost:9200", help="Elasticsearch URL")
    parser.add_argument("--index", default="openjdk-mail", help="Target index name")
    parser.add_argument("--start", help="Start from YYYY-MM (overrides auto-detection)")
    args = parser.parse_args()

    start_ym = None
    if args.start:
        parts = args.start.split("-")
        start_ym = (int(parts[0]), int(parts[1]))

    seed_list(args.list_name, args.es_url, args.index, start_ym)


if __name__ == "__main__":
    main()
