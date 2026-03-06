"""
Seed and synchronize a mailing list's ES index from HyperkItty mbox archives.

On first run (no existing records), downloads all available months.
On subsequent runs, re-downloads from the month of the latest indexed
record onward, filling any gaps idempotently.

CLI usage:
    python sync.py amber-dev
    python sync.py amber-dev --es-url http://localhost:9200 --index openjdk-mail
    python sync.py amber-dev --start 2024-01

Lambda usage:
    Handler: sync.lambda_handler
    Environment: ES_URL (required), INDEX_NAME (optional, default: openjdk-mail)
"""

import argparse
import json
import logging
import os
import time
from datetime import datetime, timezone
from email.utils import parseaddr, parsedate_to_datetime
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from mbox import body_text, build_export_url, download_mbox, parse_mbox

logger = logging.getLogger(__name__)

BULK_BATCH_SIZE = 500
ORIGIN_YEAR, ORIGIN_MONTH = 2007, 1


def month_range(start_year, start_month):
    """Generate (year, month) pairs from a start point to the current month."""
    now = datetime.now(timezone.utc)
    end_year, end_month = now.year, now.month
    y, m = start_year, start_month
    result = []
    while (y, m) <= (end_year, end_month):
        result.append((y, m))
        m += 1
        if m > 12:
            y += 1
            m = 1
    return result


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
                    logger.error("bulk index error: %s", action_result['error']['reason'])
                else:
                    total_ok += 1
        else:
            total_ok += len(batch)

    return total_ok, total_err


def resolve_start(list_name, es_url, index_name):
    """Determine the starting (year, month, day) based on the latest indexed record."""
    latest = get_latest_date(es_url, index_name, list_name)
    if latest is None:
        return None
    return (latest.year, latest.month, latest.day)


def seed_list(list_name, es_url, index_name, start_ym):
    start_day = 1
    if start_ym is None:
        resolved = resolve_start(list_name, es_url, index_name)
        if resolved:
            start_ym = (resolved[0], resolved[1])
            start_day = resolved[2]
    elif len(start_ym) == 3:
        start_day = start_ym[2]
        start_ym = (start_ym[0], start_ym[1])

    if start_ym:
        months = month_range(start_ym[0], start_ym[1])
        if start_day > 1:
            logger.info("Syncing %s from %d-%02d-%02d, %d months",
                        list_name, start_ym[0], start_ym[1], start_day, len(months))
        else:
            logger.info("Syncing %s from %d-%02d, %d months",
                        list_name, start_ym[0], start_ym[1], len(months))
    else:
        months = month_range(ORIGIN_YEAR, ORIGIN_MONTH)
        logger.info("Full seed for %s, %d months", list_name, len(months))

    cumulative = 0
    t_start = time.monotonic()
    is_first = True

    for year, month in months:
        day = start_day if is_first else 1
        is_first = False
        url = build_export_url(list_name, year, month, start_day=day)
        try:
            raw, compressed_size, dl_elapsed = download_mbox(url)
        except Exception as e:
            logger.error("%d-%02d: download failed: %s", year, month, e)
            continue

        if not raw:
            logger.info("%d-%02d: empty", year, month)
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

        logger.info(
            "%d-%02d: %d parsed, %d indexed, %d errors, %d skipped | cumulative: %d | %.0fs",
            year, month, len(messages), ok, err, skipped, cumulative, elapsed,
        )

    total_elapsed = time.monotonic() - t_start
    logger.info("Done %s. %d documents indexed in %.0fs", list_name, cumulative, total_elapsed)


MAILING_LISTS = [
    'amber-dev',
    'amber-spec-experts',
    'babylon-dev',
    'classfile-api-dev',
    'client-libs-dev',
    'compiler-dev',
    'core-libs-dev',
    'crac-dev',
    'discuss',
    'graal-dev',
    'javadoc-dev',
    'jdk-dev',
    'jextract-dev',
    'jigsaw-dev',
    'jmh-dev',
    'leyden-dev',
    'lilliput-dev',
    'loom-dev',
    'mobile-dev',
    'net-dev',
    'nio-dev',
    'openjfx-dev',
    'panama-dev',
    'quality-discuss',
    'valhalla-dev',
    'valhalla-spec-comments',
    'valhalla-spec-experts',
]


def lambda_handler(event, context):
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] %(levelname)s %(name)s - %(message)s',
    )
    es_url = os.environ['ES_URL']
    index_name = os.environ.get('INDEX_NAME', 'openjdk-mail')
    for list_name in MAILING_LISTS:
        seed_list(list_name, es_url, index_name, start_ym=None)


def main():
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] %(levelname)s %(name)s - %(message)s',
    )
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
