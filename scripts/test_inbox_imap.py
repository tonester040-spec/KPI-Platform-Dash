#!/usr/bin/env python3
"""
scripts/test_inbox_imap.py
KPI Platform — local smoke test for the KPI inbox IMAP credentials.

Connects to imap.gmail.com using KPI_INBOX_EMAIL + KPI_INBOX_APP_PASSWORD,
selects INBOX, prints how many messages are there, and runs the same search
query the real watcher uses against today's whitelist. No writes, no labels,
no archive — purely read-only.

Use this to validate a freshly generated App Password before pushing to
GitHub Secrets. Set the env vars however you like; this script also reads
a local .env if python-dotenv is available.

    python scripts/test_inbox_imap.py

Exit codes:
    0 — connect + login + select succeeded
    1 — anything failed (auth, network, config)
"""

from __future__ import annotations

import imaplib
import json
import os
import sys
from pathlib import Path

REPO_ROOT   = Path(__file__).resolve().parent.parent
CONFIG_PATH = REPO_ROOT / "config" / "inbox_config.json"

IMAP_HOST = "imap.gmail.com"
IMAP_PORT = 993


def main() -> int:
    # Optional .env loading — silent if python-dotenv isn't installed
    try:
        from dotenv import load_dotenv  # type: ignore
        load_dotenv(REPO_ROOT / ".env")
    except ImportError:
        pass

    email_addr   = os.environ.get("KPI_INBOX_EMAIL", "").strip()
    app_password = os.environ.get("KPI_INBOX_APP_PASSWORD", "").strip()

    if not email_addr or not app_password:
        print("ERROR: KPI_INBOX_EMAIL and KPI_INBOX_APP_PASSWORD must be set.")
        print("       Either export them in your shell or add them to .env.")
        return 1

    print(f"Connecting to {IMAP_HOST}:{IMAP_PORT} as {email_addr}...")
    try:
        conn = imaplib.IMAP4_SSL(IMAP_HOST, IMAP_PORT, timeout=30)
    except OSError as exc:
        print(f"FAIL — network error reaching IMAP host: {exc}")
        return 1

    try:
        conn.login(email_addr, app_password)
    except imaplib.IMAP4.error as exc:
        print(f"FAIL — login rejected: {exc}")
        print("       Common causes: wrong App Password, 2-Step Verification disabled,")
        print("       or IMAP access disabled in Gmail settings.")
        return 1

    print("OK    — IMAP login succeeded.")

    try:
        status, data = conn.select("INBOX", readonly=True)
        if status != "OK":
            print(f"FAIL — could not select INBOX: status={status} data={data!r}")
            return 1
        total = int(data[0].decode()) if data and data[0] else 0
        print(f"OK    — INBOX selected. Total messages: {total}")

        # Run the same query the real watcher would run, against this inbox's config.
        if CONFIG_PATH.exists():
            cfg = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
            senders = cfg.get("whitelisted_senders", [])
            window  = cfg.get("search_window_days", 2)
            if senders:
                sender_query = " OR ".join(f"from:{s}" for s in senders)
                query = f"({sender_query}) has:attachment newer_than:{window}d"
                quoted = '"' + query.replace("\\", "\\\\").replace('"', '\\"') + '"'
                status, data = conn.uid("SEARCH", "X-GM-RAW", quoted)
                if status == "OK":
                    uids = data[0].split() if data and data[0] else []
                    print(f"OK    — X-GM-RAW search would match {len(uids)} message(s):")
                    print(f"        query: {query}")
                else:
                    print(f"WARN  — X-GM-RAW search returned status={status} data={data!r}")
            else:
                print("INFO  — No whitelisted_senders configured; skipping search test.")
        else:
            print(f"INFO  — {CONFIG_PATH.relative_to(REPO_ROOT)} not found; skipping search test.")
    finally:
        try:
            conn.close()
        except Exception:
            pass
        try:
            conn.logout()
        except Exception:
            pass

    print("All checks passed. App Password is good.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
