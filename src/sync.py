"""
Seed and synchronize a mailing list's ES index from HyperkItty mbox archives.

On first run (no existing records), downloads all available months.
On subsequent runs, re-downloads from the month of the latest indexed
record onward, filling any gaps idempotently.

CLI usage:
    python sync.py amber-dev
    python sync.py amber-dev --es-url https://elastic:pass@host:9200 --index openjdk-mail
    python sync.py amber-dev --start 2024-01

Lambda usage:
    Handler: sync.lambda_handler
    Environment: ES_URL (required, e.g. https://elastic:pass@host:9200),
                 INDEX_NAME (optional, default: openjdk-mail)
"""

import argparse
import base64
import json
import logging
import os
import re
import ssl
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from email.header import decode_header, make_header
from email.utils import parseaddr, parsedate_to_datetime
from urllib.error import HTTPError
from urllib.parse import urlparse, urlunparse
from urllib.request import Request, urlopen

from mbox import body_text, build_export_url, download_mbox, parse_mbox

logger = logging.getLogger(__name__)

BULK_BATCH_SIZE = 500
DOWNLOAD_RETRIES = 3
DOWNLOAD_RETRY_DELAY = 5
ORIGIN_YEAR, ORIGIN_MONTH = 2007, 1
CHECKPOINT_INDEX = 'openjdk-mail-checkpoints'
SAFETY_BUFFER_DAYS = 1

_es_ssl_ctx = ssl.create_default_context()
_es_ssl_ctx.check_hostname = False
_es_ssl_ctx.verify_mode = ssl.CERT_NONE

_es_auth_header = None


def _init_es_auth(es_url):
    """Extract credentials from ES URL, set auth header, return clean URL."""
    global _es_auth_header
    parsed = urlparse(es_url)
    if parsed.username:
        credentials = base64.b64encode(
            f"{parsed.username}:{parsed.password or ''}".encode()
        ).decode()
        _es_auth_header = f"Basic {credentials}"
        netloc = parsed.hostname
        if parsed.port:
            netloc = f"{netloc}:{parsed.port}"
        return urlunparse(parsed._replace(netloc=netloc))
    return es_url


def _es_urlopen(req, **kwargs):
    """urlopen wrapper that adds ES auth and skips TLS verification."""
    if _es_auth_header:
        req.add_header("Authorization", _es_auth_header)
    return urlopen(req, context=_es_ssl_ctx, **kwargs)


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
        with _es_urlopen(req, timeout=15) as resp:
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


def decode_header_value(value):
    """Decode RFC 2047 encoded-words and unfold RFC 5322 continuation lines."""
    if not value:
        return value
    value = re.sub(r'\r?\n[ \t]+', ' ', value).strip()
    return str(make_header(decode_header(value)))


def transform_message(msg, list_name):
    message_id = strip_angle_brackets(msg.get("Message-ID"))
    if not message_id:
        return None

    from_hdr = decode_header_value(msg.get("From", ""))
    author_name, author_email = parseaddr(from_hdr)

    return {
        "_id": message_id,
        "list": list_name,
        "message_id": message_id,
        "in_reply_to": strip_angle_brackets(msg.get("In-Reply-To")),
        "subject": decode_header_value(msg.get("Subject", "")),
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
        with _es_urlopen(req, timeout=120) as resp:
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


def filter_existing(es_url, index_name, docs):
    """Remove docs that already exist in ES. Returns (new_docs, existing_count)."""
    if not docs:
        return [], 0

    existing_ids = set()
    ids = [doc["_id"] for doc in docs]

    for i in range(0, len(ids), BULK_BATCH_SIZE):
        batch_ids = ids[i:i + BULK_BATCH_SIZE]
        req = Request(
            f"{es_url}/{index_name}/_mget?_source=false",
            data=json.dumps({"ids": batch_ids}).encode(),
            headers={"Content-Type": "application/json", "User-Agent": "openjdk-mail-search"},
        )
        try:
            with _es_urlopen(req, timeout=15) as resp:
                result = json.loads(resp.read())
        except HTTPError as e:
            if e.code == 404:
                return docs, 0
            raise
        for doc in result["docs"]:
            if doc.get("found"):
                existing_ids.add(doc["_id"])

    new_docs = [doc for doc in docs if doc["_id"] not in existing_ids]
    return new_docs, len(existing_ids)


def resolve_start(list_name, es_url, index_name):
    """Determine the starting (year, month, day) based on the latest indexed record."""
    latest = get_latest_date(es_url, index_name, list_name)
    if latest is None:
        return None
    return (latest.year, latest.month, latest.day)


def get_checkpoint(es_url, checkpoint_index, list_name):
    """Read the checkpoint for a list. Returns (year, month) or None."""
    req = Request(
        f"{es_url}/{checkpoint_index}/_doc/{list_name}",
        headers={"User-Agent": "openjdk-mail-search"},
    )
    try:
        with _es_urlopen(req, timeout=15) as resp:
            result = json.loads(resp.read())
    except HTTPError as e:
        if e.code == 404:
            return None
        raise
    synced_at = result["_source"]["synced_at"]
    dt = datetime.fromisoformat(synced_at.replace("Z", "+00:00"))
    return (dt.year, dt.month, dt.day)


def put_checkpoint(es_url, checkpoint_index, list_name, had_updates):
    """Write a checkpoint for a list with the current UTC timestamp.

    Always sets synced_at. Only overwrites updated_at when had_updates is True;
    otherwise the existing updated_at value is retained via partial update.
    """
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    doc = {"list": list_name, "synced_at": now}
    if had_updates:
        doc["updated_at"] = now
    body = {"doc": doc, "doc_as_upsert": True}
    req = Request(
        f"{es_url}/{checkpoint_index}/_update/{list_name}",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json", "User-Agent": "openjdk-mail-search"},
        method="POST",
    )
    with _es_urlopen(req, timeout=15):
        pass
    logger.info("Checkpoint %s: %s (updated=%s)", list_name, now, had_updates)


def sync_list(list_name, es_url, index_name, start_ym, checkpoint_index,
              full=False):
    start_day = 1
    if start_ym is None and not full:
        cp = get_checkpoint(es_url, checkpoint_index, list_name)
        if cp:
            start_ym = (cp[0], cp[1])
            start_day = cp[2]
        else:
            resolved = resolve_start(list_name, es_url, index_name)
            if resolved:
                start_ym = (resolved[0], resolved[1])
                start_day = resolved[2]
        if start_ym:
            buffered = datetime(start_ym[0], start_ym[1], start_day,
                                tzinfo=timezone.utc) - timedelta(days=SAFETY_BUFFER_DAYS)
            start_ym = (buffered.year, buffered.month)
            start_day = buffered.day
    elif start_ym and len(start_ym) == 3:
        start_day = start_ym[2]
        start_ym = (start_ym[0], start_ym[1])

    label = "Full sync" if full else "Syncing"
    if start_ym:
        months = month_range(start_ym[0], start_ym[1])
        if start_day > 1:
            logger.info("%s %s from %d-%02d-%02d, %d months",
                        label, list_name, start_ym[0], start_ym[1], start_day, len(months))
        else:
            logger.info("%s %s from %d-%02d, %d months",
                        label, list_name, start_ym[0], start_ym[1], len(months))
    else:
        months = month_range(ORIGIN_YEAR, ORIGIN_MONTH)
        logger.info("%s %s, %d months", label, list_name, len(months))

    cumulative_indexed = 0
    cumulative_existing = 0
    t_start = time.monotonic()
    is_first = True

    for year, month in months:
        day = start_day if is_first else 1
        is_first = False
        url = build_export_url(list_name, year, month, start_day=day)
        for attempt in range(1, DOWNLOAD_RETRIES + 1):
            try:
                raw, compressed_size, dl_elapsed = download_mbox(url)
                break
            except Exception:
                if attempt == DOWNLOAD_RETRIES:
                    raise
                logger.warning("%d-%02d: download attempt %d/%d failed, retrying in %ds",
                               year, month, attempt, DOWNLOAD_RETRIES, DOWNLOAD_RETRY_DELAY)
                time.sleep(DOWNLOAD_RETRY_DELAY)

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

        if full:
            new_docs, existing = docs, 0
        else:
            new_docs, existing = filter_existing(es_url, index_name, docs)
        ok, err = bulk_index(es_url, index_name, new_docs)
        cumulative_indexed += ok
        cumulative_existing += existing
        elapsed = time.monotonic() - t_start

        logger.info(
            "%d-%02d: %d parsed, %d indexed, %d existing, %d skipped, %d errors"
            " | cumulative: %d indexed, %d existing | %dms",
            year, month, len(messages), ok, existing, skipped, err,
            cumulative_indexed, cumulative_existing, elapsed * 1000,
        )

    total_elapsed = time.monotonic() - t_start
    logger.info("Done %s. %d indexed, %d existing in %dms",
                list_name, cumulative_indexed, cumulative_existing, total_elapsed * 1000)

    put_checkpoint(es_url, checkpoint_index, list_name,
                   had_updates=(cumulative_indexed > 0))


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
    logging.getLogger().setLevel(logging.INFO)
    es_url = _init_es_auth(os.environ['ES_URL'])
    index_name = os.environ.get('INDEX_NAME', 'openjdk-mail')
    checkpoint_index = os.environ.get('CHECKPOINT_INDEX', CHECKPOINT_INDEX)
    for list_name in MAILING_LISTS:
        sync_list(list_name, es_url, index_name, start_ym=None,
                  checkpoint_index=checkpoint_index)


def main():
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] %(levelname)s %(name)s - %(message)s',
    )
    parser = argparse.ArgumentParser(description="Seed/sync ES index from HyperkItty mbox archives")
    parser.add_argument("list_name", nargs="?",
                        help="Mailing list name, e.g. amber-dev (omit when using --all)")
    parser.add_argument("--all", action="store_true", dest="sync_all",
                        help="Sync all known mailing lists")
    parser.add_argument("--full", action="store_true",
                        help="Full sync: re-download and re-index all messages, "
                             "overwriting existing documents")
    parser.add_argument("--workers", type=int, default=1,
                        help="Number of lists to sync concurrently (default: 1)")
    parser.add_argument("--es-url", default="http://localhost:9200", help="Elasticsearch URL")
    parser.add_argument("--index", default="openjdk-mail", help="Target index name")
    parser.add_argument("--start", help="Start from YYYY-MM (overrides auto-detection)")
    parser.add_argument("--checkpoint-index", default=CHECKPOINT_INDEX,
                        help="ES index for sync checkpoints (default: %(default)s)")
    args = parser.parse_args()

    if not args.list_name and not args.sync_all:
        parser.error("provide a list name or use --all")

    es_url = _init_es_auth(args.es_url)

    start_ym = None
    if args.start:
        parts = args.start.split("-")
        start_ym = (int(parts[0]), int(parts[1]))

    lists = MAILING_LISTS if args.sync_all else [args.list_name]
    if args.workers > 1 and len(lists) > 1:
        logger.info("Syncing %d lists with %d workers", len(lists), args.workers)
        failed = []
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            futures = {
                pool.submit(sync_list, name, es_url, args.index, start_ym,
                            args.checkpoint_index, full=args.full): name
                for name in lists
            }
            for future in as_completed(futures):
                name = futures[future]
                try:
                    future.result()
                except Exception:
                    logger.exception("Failed to sync %s", name)
                    failed.append(name)
        if failed:
            logger.error("Failed lists: %s", ", ".join(failed))
            raise SystemExit(1)
    else:
        for list_name in lists:
            sync_list(list_name, es_url, args.index, start_ym,
                      checkpoint_index=args.checkpoint_index, full=args.full)


if __name__ == "__main__":
    main()
