"""Shared utilities for downloading and parsing HyperkItty mbox archives."""

import email
import email.policy
import gzip
import mailbox
import os
import tempfile
import time
from urllib.request import Request, urlopen

ARCHIVES_BASE = "https://mail.openjdk.org/archives/list"


def build_export_url(list_name, year, month):
    fqlist = f"{list_name}@openjdk.org"
    next_month = month + 1 if month < 12 else 1
    next_year = year if month < 12 else year + 1
    return (
        f"{ARCHIVES_BASE}/{fqlist}/export/"
        f"{fqlist}-{year}-{month:02d}.mbox.gz"
        f"?start={year}-{month:02d}-01&end={next_year}-{next_month:02d}-01"
    )


def download_mbox(url):
    req = Request(url, headers={"User-Agent": "openjdk-mail-search"})
    t0 = time.monotonic()
    with urlopen(req, timeout=60) as resp:
        compressed = resp.read()
    elapsed = time.monotonic() - t0
    raw = gzip.decompress(compressed)
    return raw, len(compressed), elapsed


def parse_mbox(raw_bytes):
    fd, path = tempfile.mkstemp(suffix=".mbox")
    try:
        os.write(fd, raw_bytes)
        os.close(fd)
        mbox = mailbox.mbox(path)
        messages = list(mbox)
        mbox.close()
    finally:
        os.unlink(path)
    return messages


def body_text(msg):
    if msg.is_multipart():
        for part in msg.walk():
            ct = part.get_content_type()
            if ct == "text/plain":
                payload = part.get_payload(decode=True)
                if payload:
                    charset = part.get_content_charset() or "utf-8"
                    return payload.decode(charset, errors="replace")
    else:
        payload = msg.get_payload(decode=True)
        if payload:
            charset = msg.get_content_charset() or "utf-8"
            return payload.decode(charset, errors="replace")
    return ""
