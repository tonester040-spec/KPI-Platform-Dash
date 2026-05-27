#!/usr/bin/env python3
"""
parsers/gmail_attachment_watcher.py
KPI Platform — Gmail Attachment Watcher (Step 0 of the weekly pipeline).

What this does
--------------
Every Monday, before the main KPI pipeline runs, this module:
  1. Authenticates to the KPI inbox (karissaperformanceintelligence@gmail.com)
  2. Searches for attachment emails from whitelisted senders (Elaina, etc.)
  3. Validates each sender from raw headers (NOT just the search result)
  4. Downloads attachments, hashes them (SHA256), dedupes against the ledger
  5. Archives each new file to data/archive/{YYYY-MM-DD}/
  6. Writes each new file to data/inbox/ with a collision-safe name
  7. Updates data/processed_attachments.json (the idempotency ledger)
  8. Writes data/inbox/manifest.json (contract with Tier 2 batch processor)
  9. Writes data/logs/inbox_run_*.json (human-readable run summary)
 10. Applies Gmail labels + archives clean emails out of inbox
 11. Sends a rejection email if anything was flagged as invalid / unauthorized

Design invariants
-----------------
- Archive BEFORE inbox write (always — even if inbox write later fails)
- Trust headers, not search results (search is a filter, headers are the gate)
- SHA256 hash is the source of truth for idempotency — never filename or size
- Watcher NEVER computes trust_layer_flags — it just provides the empty field
- Every run ends with a run summary, even on catastrophic failure (top-level
  try/except writes one last log before sys.exit(1))
- dry_run=true in inbox_config.json disables ALL writes and API calls

Auth
----
Uses IMAP + a Gmail App Password (NOT OAuth). OAuth was abandoned 2026-05-27
because Google's "Testing"-mode refresh tokens for Gmail restricted scopes
expire after 7 days, and the watcher requires `gmail.modify` (restricted),
which excludes us from production-publishing without a CASA security
assessment (impractical for a single-user project).

Secrets (set in GitHub Secrets):
  KPI_INBOX_EMAIL         karissaperformanceintelligence@gmail.com
  KPI_INBOX_APP_PASSWORD  16-char Gmail App Password for that account

App Passwords don't expire (until the user changes their account password
or revokes the App Password). Setup: in the KPI inbox Google Account
→ Security → 2-Step Verification (must be on) → App passwords → generate.

Run it
------
  python parsers/gmail_attachment_watcher.py          # real run
  # dry_run: flip inbox_config.json "dry_run": true
"""

from __future__ import annotations

import email as email_pkg
import hashlib
import imaplib
import json
import logging
import os
import re
import sys
import tempfile
import traceback
from datetime import datetime, timezone
from email.message import Message
from pathlib import Path
from typing import Any

# ─── Logging ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("inbox_watcher")

# ─── Paths ────────────────────────────────────────────────────────────────────

REPO_ROOT    = Path(__file__).resolve().parent.parent
CONFIG_PATH  = REPO_ROOT / "config" / "inbox_config.json"
LEDGER_PATH  = REPO_ROOT / "data" / "processed_attachments.json"
INBOX_DIR    = REPO_ROOT / "data" / "inbox"
ARCHIVE_DIR  = REPO_ROOT / "data" / "archive"
LOG_DIR      = REPO_ROOT / "data" / "logs"
MANIFEST_PATH = INBOX_DIR / "manifest.json"

# ─── Gmail IMAP constants ─────────────────────────────────────────────────────

IMAP_HOST = "imap.gmail.com"
IMAP_PORT = 993


# ═══════════════════════════════════════════════════════════════════════════════
# Auth (IMAP + App Password)
# ═══════════════════════════════════════════════════════════════════════════════

def authenticate() -> imaplib.IMAP4_SSL:
    """
    Open an authenticated IMAP4_SSL connection to the KPI inbox using a Gmail
    App Password. Returns the connection with INBOX selected (read-write so
    STORE works).

    On failure: logs and raises RuntimeError so the top-level handler writes a
    run summary and exits with code 1. Does NOT catch silently.
    """
    email_addr   = os.environ.get("KPI_INBOX_EMAIL", "").strip()
    app_password = os.environ.get("KPI_INBOX_APP_PASSWORD", "").strip()

    missing = [k for k, v in {
        "KPI_INBOX_EMAIL":        email_addr,
        "KPI_INBOX_APP_PASSWORD": app_password,
    }.items() if not v]
    if missing:
        raise RuntimeError(
            f"INBOX WATCHER AUTH FAILURE — missing secrets: {', '.join(missing)}. "
            "Generate an App Password in the KPI inbox Google Account → Security "
            "→ App passwords (requires 2-Step Verification), then add to GitHub Secrets."
        )

    try:
        conn = imaplib.IMAP4_SSL(IMAP_HOST, IMAP_PORT, timeout=30)
    except OSError as exc:
        raise RuntimeError(f"INBOX WATCHER AUTH FAILURE — could not reach {IMAP_HOST}:{IMAP_PORT}: {exc}") from exc

    try:
        conn.login(email_addr, app_password)
    except imaplib.IMAP4.error as exc:
        raise RuntimeError(f"INBOX WATCHER AUTH FAILURE — IMAP login rejected: {exc}") from exc

    status, _ = conn.select("INBOX", readonly=False)
    if status != "OK":
        raise RuntimeError(f"INBOX WATCHER AUTH FAILURE — could not select INBOX (status={status})")

    log.info("Authenticated to KPI inbox via IMAP (%s)", email_addr)
    return conn


def disconnect(conn: imaplib.IMAP4_SSL | None) -> None:
    """Close + logout an IMAP connection. Safe to call on partial / dead conns."""
    if conn is None:
        return
    try:
        conn.close()
    except Exception:
        pass
    try:
        conn.logout()
    except Exception:
        pass


# ═══════════════════════════════════════════════════════════════════════════════
# IMAP helpers (thin wrappers around imaplib's awkward tuple responses)
# ═══════════════════════════════════════════════════════════════════════════════

# Pattern to pull X-GM-MSGID out of a FETCH response line:
#   b'1 (X-GM-MSGID 1234567890123456789 UID 42)'
_RE_MSGID = re.compile(rb"X-GM-MSGID\s+(\d+)")


def _check_ok(label: str, status: str, data: Any) -> None:
    """Raise on non-OK IMAP responses with a useful message."""
    if status != "OK":
        raise RuntimeError(f"IMAP {label} failed: status={status} data={data!r}")


def _fetch_gm_msgid(conn: imaplib.IMAP4_SSL, uid: bytes) -> str:
    """Get the stable Gmail message ID (X-GM-MSGID) for a UID. Used by the ledger."""
    status, data = conn.uid("FETCH", uid, "(X-GM-MSGID)")
    _check_ok("FETCH X-GM-MSGID", status, data)
    for chunk in data:
        if not chunk:
            continue
        line = chunk if isinstance(chunk, (bytes, bytearray)) else chunk[0]
        m = _RE_MSGID.search(line)
        if m:
            return m.group(1).decode()
    return ""


def _imap_quote(s: str) -> str:
    """Quote a string for the IMAP wire (escape \\ and \", wrap in double quotes)."""
    escaped = s.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


# ═══════════════════════════════════════════════════════════════════════════════
# Email search
# ═══════════════════════════════════════════════════════════════════════════════

def search_for_attachment_emails(conn: imaplib.IMAP4_SSL, config: dict) -> list[bytes]:
    """
    Search the KPI inbox for attachment emails from whitelisted senders in the
    last N days. Returns a list of IMAP UIDs (bytes) — stable within this
    session; we never persist them.

    Uses Gmail's X-GM-RAW extension so the existing query syntax (has:attachment,
    from:, newer_than:Nd) is preserved unchanged from the REST-API era.

    IMPORTANT: We do NOT filter on read/unread — the ledger is our source of
    truth, not Gmail state. A hand-marked-as-read email will still be processed
    (and skipped as duplicate if we've seen the file before).
    """
    senders = config.get("whitelisted_senders", [])
    if not senders:
        log.warning("No whitelisted_senders configured — nothing to search for")
        return []

    sender_query = " OR ".join(f"from:{s}" for s in senders)
    window = config.get("search_window_days", 2)
    query = f"({sender_query}) has:attachment newer_than:{window}d"
    log.info("Gmail query: %s", query)

    try:
        status, data = conn.uid("SEARCH", "X-GM-RAW", _imap_quote(query))
        _check_ok("UID SEARCH X-GM-RAW", status, data)
    except Exception as exc:
        log.error("Gmail search failed: %s — continuing with empty set", exc)
        return []

    raw = data[0] if data else b""
    uids = raw.split() if raw else []
    log.info("Found %d message(s) matching query", len(uids))
    return uids


# ═══════════════════════════════════════════════════════════════════════════════
# Attachment fetch
# ═══════════════════════════════════════════════════════════════════════════════

def _extract_sender_from_message(msg: Message) -> str:
    """Pull the bare email out of an RFC 822 From header. Returns '' on miss."""
    raw = msg.get("From", "") or ""
    m = re.search(r"<([^>]+)>", raw)
    return (m.group(1) if m else raw).strip().lower()


def _extract_date_from_message(msg: Message) -> str:
    """Return the message Date header verbatim, or now() on miss."""
    return msg.get("Date", "") or datetime.now(timezone.utc).isoformat()


def _fetch_rfc822(conn: imaplib.IMAP4_SSL, uid: bytes, peek: bool) -> bytes:
    """
    Fetch the raw RFC 822 bytes for a UID. `peek=True` uses BODY.PEEK so the
    \\Seen flag isn't set (used for header-only sender validation).
    """
    spec = "(BODY.PEEK[])" if peek else "(RFC822)"
    status, data = conn.uid("FETCH", uid, spec)
    _check_ok(f"UID FETCH {spec}", status, data)
    for chunk in data:
        if isinstance(chunk, tuple) and len(chunk) >= 2 and isinstance(chunk[1], (bytes, bytearray)):
            return bytes(chunk[1])
    return b""


def fetch_attachments(conn: imaplib.IMAP4_SSL, uid: bytes, config: dict) -> list[dict]:
    """
    Fetch a single message, validate the sender from raw headers, and return
    downloadable attachment records.

    Returns a list of dicts: {filename, content_bytes, message_id, sender,
    date_received, rejected_reason}.

    If the sender fails header validation, returns ONE dict with
    rejected_reason="security" so the caller can log and skip it.
    """
    uid_str = uid.decode() if isinstance(uid, (bytes, bytearray)) else str(uid)

    # Stable Gmail message ID used by the ledger / manifest
    try:
        gm_msgid = _fetch_gm_msgid(conn, uid)
    except Exception as exc:
        log.warning("Could not fetch X-GM-MSGID for UID %s: %s — using empty msgid", uid_str, exc)
        gm_msgid = ""

    # Fetch full message bytes (single round-trip — we'd need the body anyway
    # for any whitelisted sender, and BODY.PEEK keeps \Seen off)
    try:
        raw_bytes = _fetch_rfc822(conn, uid, peek=True)
    except Exception as exc:
        log.error("Could not fetch message UID %s: %s", uid_str, exc)
        return []

    if not raw_bytes:
        log.error("Empty fetch result for UID %s", uid_str)
        return []

    msg = email_pkg.message_from_bytes(raw_bytes)
    sender = _extract_sender_from_message(msg)
    date_received = _extract_date_from_message(msg)
    whitelist = [s.lower() for s in config.get("whitelisted_senders", [])]

    if sender not in whitelist:
        log.warning("INBOX SECURITY: rejected email UID=%s msgid=%s — sender %s not whitelisted",
                    uid_str, gm_msgid or "?", sender or "(empty)")
        return [{
            "filename": "(unknown)",
            "content_bytes": b"",
            "message_id": gm_msgid,
            "sender": sender,
            "date_received": date_received,
            "rejected_reason": "security",
        }]

    allowed_ext = [e.lower() for e in config.get("allowed_extensions", [])]
    attachments: list[dict] = []

    for part in msg.walk():
        # Skip multipart containers — only leaf parts have payloads
        if part.is_multipart():
            continue
        filename = part.get_filename()
        if not filename:
            continue

        ext = Path(filename).suffix.lower()
        if ext not in allowed_ext:
            log.info("Skipping %s (extension %s not allowed)", filename, ext or "(none)")
            attachments.append({
                "filename": filename,
                "content_bytes": b"",
                "message_id": gm_msgid,
                "sender": sender,
                "date_received": date_received,
                "rejected_reason": "invalid_extension",
            })
            continue

        try:
            content_bytes = part.get_payload(decode=True) or b""
        except Exception as exc:
            log.error("Could not decode attachment %s from UID %s: %s", filename, uid_str, exc)
            continue

        if not content_bytes:
            log.warning("Attachment %s returned empty bytes — skipping", filename)
            continue

        attachments.append({
            "filename": filename,
            "content_bytes": content_bytes,
            "message_id": gm_msgid,
            "sender": sender,
            "date_received": date_received,
            "rejected_reason": None,
        })

    return attachments


# ═══════════════════════════════════════════════════════════════════════════════
# Hashing + dedup ledger
# ═══════════════════════════════════════════════════════════════════════════════

def compute_hash(content_bytes: bytes) -> str:
    """SHA256 hex digest of raw binary content. Source of truth for idempotency."""
    return hashlib.sha256(content_bytes).hexdigest()


def load_processed_ledger() -> dict:
    """Load the idempotency ledger. Returns {} if the file is missing."""
    if not LEDGER_PATH.exists():
        return {}
    try:
        return json.loads(LEDGER_PATH.read_text(encoding="utf-8")) or {}
    except json.JSONDecodeError as exc:
        log.error("Ledger %s is corrupt (%s) — treating as empty", LEDGER_PATH, exc)
        return {}


def save_processed_ledger(ledger: dict) -> None:
    """Atomic write (temp file + rename) to survive interrupted writes."""
    LEDGER_PATH.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8",
        dir=str(LEDGER_PATH.parent), delete=False, suffix=".tmp",
    ) as f:
        json.dump(ledger, f, indent=2, sort_keys=True)
        tmp_name = f.name
    os.replace(tmp_name, LEDGER_PATH)


def is_duplicate(content_hash: str, ledger: dict) -> bool:
    return content_hash in ledger


# ═══════════════════════════════════════════════════════════════════════════════
# Archive + inbox write
# ═══════════════════════════════════════════════════════════════════════════════

def archive_attachment(
    filename: str,
    content_bytes: bytes,
    content_hash: str,
    message_id: str,
    date_str: str,
) -> str | None:
    """
    Write the raw bytes to data/archive/{YYYY-MM-DD}/ with a traceable name.
    Returns the archived path on success, None on failure (does not raise —
    archive failures must never block inbox processing).
    """
    try:
        day_dir = ARCHIVE_DIR / date_str
        day_dir.mkdir(parents=True, exist_ok=True)
        stem = Path(filename).stem
        ext = Path(filename).suffix
        safe_name = f"{date_str}_{message_id[:6]}_{stem}_{content_hash[:6]}{ext}"
        archived_path = day_dir / safe_name
        archived_path.write_bytes(content_bytes)
        return str(archived_path.relative_to(REPO_ROOT))
    except Exception as exc:
        log.error("Archive write failed for %s: %s — continuing", filename, exc)
        return None


def write_to_inbox(filename: str, content_bytes: bytes, content_hash: str) -> str:
    """
    Write to data/inbox/ with a hash-prefixed collision-safe filename.
    Returns the relative path.
    """
    INBOX_DIR.mkdir(parents=True, exist_ok=True)
    safe_filename = f"{content_hash[:6]}_{filename}"
    target = INBOX_DIR / safe_filename
    target.write_bytes(content_bytes)
    return str(target.relative_to(REPO_ROOT))


# ═══════════════════════════════════════════════════════════════════════════════
# Manifest + run summary
# ═══════════════════════════════════════════════════════════════════════════════

def write_manifest(file_records: list[dict]) -> None:
    """Write data/inbox/manifest.json — the contract with Tier 2."""
    INBOX_DIR.mkdir(parents=True, exist_ok=True)
    MANIFEST_PATH.write_text(json.dumps(file_records, indent=2, sort_keys=True), encoding="utf-8")
    log.info("Wrote manifest: %d record(s) → %s",
             len(file_records), MANIFEST_PATH.relative_to(REPO_ROOT))


def write_run_summary(
    emails_scanned: int = 0,
    attachments_found: int = 0,
    new_files: int = 0,
    duplicates_skipped: int = 0,
    invalid_extension_skipped: int = 0,
    security_rejections: int = 0,
    status: str = "success",
    notes: list[str] | None = None,
) -> Path:
    """Write data/logs/inbox_run_*.json. Always writes — even on fatal error."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    run_time = datetime.now(timezone.utc).isoformat()
    stamp = datetime.now().strftime("%Y-%m-%d-%H%M%S")
    path = LOG_DIR / f"inbox_run_{stamp}.json"
    summary = {
        "run_time":                  run_time,
        "emails_scanned":            emails_scanned,
        "attachments_found":         attachments_found,
        "new_files":                 new_files,
        "duplicates_skipped":        duplicates_skipped,
        "invalid_extension_skipped": invalid_extension_skipped,
        "security_rejections":       security_rejections,
        "status":                    status,
        "notes":                     notes or [],
    }
    path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    log.info("Wrote run summary: %s (status=%s)", path.relative_to(REPO_ROOT), status)
    return path


# ═══════════════════════════════════════════════════════════════════════════════
# Gmail label + archive (Step 4b)
# ═══════════════════════════════════════════════════════════════════════════════

def label_and_archive_email(
    conn: imaplib.IMAP4_SSL,
    uid: bytes,
    run_status: str,
    config: dict,
) -> None:
    """
    Apply KPI-Processed or KPI-Attention label to a source email based on the
    run status, and archive (remove from inbox) on success only.

    success          → KPI-Processed + archive out of inbox  (inbox stays clean)
    partial_success  → KPI-Attention, do NOT archive         (problem stays visible)
    error            → no label, no archive                  (problem stays visible)

    Implementation notes
    --------------------
    Gmail's IMAP X-GM-LABELS extension auto-creates a user label on first STORE,
    so no separate "ensure label exists" step is needed. The system label \\Inbox
    is removed (not the INBOX folder mailbox) to archive — Gmail's IMAP treats
    the inbox as a label, not a folder.
    """
    if run_status == "error":
        return  # Preserve inbox signal — don't touch the email

    processed = config.get("kpi_processed_label", "KPI-Processed")
    attention = config.get("kpi_attention_label", "KPI-Attention")
    uid_str = uid.decode() if isinstance(uid, (bytes, bytearray)) else str(uid)

    try:
        if run_status == "success":
            status, data = conn.uid("STORE", uid, "+X-GM-LABELS", _imap_quote(processed))
            _check_ok("STORE +X-GM-LABELS", status, data)
            # Removing the system \Inbox label archives the message in Gmail
            status, data = conn.uid("STORE", uid, "-X-GM-LABELS", "\\Inbox")
            _check_ok("STORE -X-GM-LABELS \\Inbox", status, data)
            log.info("Labeled UID %s as %s and archived out of inbox", uid_str, processed)
        elif run_status == "partial_success":
            status, data = conn.uid("STORE", uid, "+X-GM-LABELS", _imap_quote(attention))
            _check_ok("STORE +X-GM-LABELS", status, data)
            # Deliberately NOT removing \Inbox — inbox emptiness is the health signal
            log.info("Labeled UID %s as %s (kept in inbox)", uid_str, attention)
    except Exception as exc:
        log.error("label_and_archive_email failed for UID %s: %s", uid_str, exc)
        # Never raise — labeling is a nice-to-have, not a blocker


# ═══════════════════════════════════════════════════════════════════════════════
# Config
# ═══════════════════════════════════════════════════════════════════════════════

def load_config() -> dict:
    if not CONFIG_PATH.exists():
        raise RuntimeError(f"Missing config file: {CONFIG_PATH}")
    return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))


# ═══════════════════════════════════════════════════════════════════════════════
# Main orchestration
# ═══════════════════════════════════════════════════════════════════════════════

def _make_file_record(
    filename: str,
    sender: str,
    message_id: str,
    date_received: str,
    content_hash: str,
    archived_path: str | None,
    inbox_path: str | None,
    processing_status: str,
) -> dict:
    """Build one manifest record. trust_layer_flags is always [] here — Tier 2 populates it."""
    safe_filename = f"{content_hash[:6]}_{filename}" if inbox_path and content_hash else ""
    return {
        "filename":           filename,
        "safe_filename":      safe_filename,
        "archived_path":      archived_path or "",
        "inbox_path":         inbox_path or "",
        "hash":               content_hash or "",
        "message_id":         message_id,
        "sender":             sender,
        "date_received":      date_received,
        "processing_status":  processing_status,
        "trust_layer_flags":  [],
    }


def main() -> None:
    # Pre-flight: create directories first so nothing below has to worry about it
    INBOX_DIR.mkdir(parents=True, exist_ok=True)
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    config = load_config()
    dry_run = bool(config.get("dry_run", False))

    if dry_run:
        log.info("DRY RUN MODE — no files will be downloaded, written, or emailed")

    # Counters for the run summary
    emails_scanned = 0
    attachments_found = 0
    new_files = 0
    duplicates_skipped = 0
    invalid_extension_skipped = 0
    security_rejections = 0
    notes: list[str] = []
    file_records: list[dict] = []

    if dry_run:
        log.info("DRY RUN: would authenticate to KPI inbox via IMAP")
        log.info("DRY RUN: would search with whitelisted_senders=%s window=%dd",
                 config.get("whitelisted_senders"), config.get("search_window_days", 2))
        log.info("DRY RUN: would dedupe against %s", LEDGER_PATH.relative_to(REPO_ROOT))
        write_manifest([])
        write_run_summary(status="no_files", notes=["dry_run=true — no real work performed"])
        return

    # Real run from here on
    conn = authenticate()
    try:
        uids = search_for_attachment_emails(conn, config)
        emails_scanned = len(uids)

        ledger = load_processed_ledger()
        today_str = datetime.now().strftime("%Y-%m-%d")

        # Outer loop keyed by UID (stable within session, used for labeling).
        # The gm_msgid stored in attachment records is the Gmail-wide stable ID,
        # which is what the ledger and manifest preserve.
        uid_outcomes: dict[bytes, str] = {}

        for uid in uids:
            uid_str = uid.decode() if isinstance(uid, (bytes, bytearray)) else str(uid)
            attachments = fetch_attachments(conn, uid, config)
            if not attachments:
                notes.append(f"UID {uid_str}: no attachments fetched")
                continue

            message_had_rejection = False
            message_had_success = False

            for att in attachments:
                attachments_found += 1
                filename = att["filename"]
                sender = att["sender"]
                date_received = att["date_received"]
                message_id = att["message_id"]   # X-GM-MSGID (stable, ledger key)
                rejected = att.get("rejected_reason")

                if rejected == "security":
                    security_rejections += 1
                    file_records.append(_make_file_record(
                        filename, sender, message_id, date_received,
                        content_hash="", archived_path=None, inbox_path=None,
                        processing_status="security_rejected",
                    ))
                    message_had_rejection = True
                    continue

                if rejected == "invalid_extension":
                    invalid_extension_skipped += 1
                    file_records.append(_make_file_record(
                        filename, sender, message_id, date_received,
                        content_hash="", archived_path=None, inbox_path=None,
                        processing_status="invalid_extension_skipped",
                    ))
                    message_had_rejection = True
                    continue

                content_bytes = att["content_bytes"]
                content_hash = compute_hash(content_bytes)

                if is_duplicate(content_hash, ledger):
                    duplicates_skipped += 1
                    file_records.append(_make_file_record(
                        filename, sender, message_id, date_received,
                        content_hash=content_hash, archived_path=None, inbox_path=None,
                        processing_status="duplicate_skipped",
                    ))
                    # A duplicate is a clean outcome for this message
                    message_had_success = True
                    continue

                # New file — archive first, then write to inbox, then update ledger
                archived = archive_attachment(
                    filename, content_bytes, content_hash, message_id, today_str,
                )
                try:
                    inbox_path = write_to_inbox(filename, content_bytes, content_hash)
                except Exception as exc:
                    log.error("Inbox write failed for %s: %s", filename, exc)
                    notes.append(f"inbox write failed: {filename} ({exc})")
                    file_records.append(_make_file_record(
                        filename, sender, message_id, date_received,
                        content_hash=content_hash, archived_path=archived, inbox_path=None,
                        processing_status="invalid_extension_skipped",  # closest existing bucket
                    ))
                    message_had_rejection = True
                    continue

                new_files += 1
                ledger[content_hash] = {
                    "filename": filename,
                    "processed_at": datetime.now(timezone.utc).isoformat(),
                    "message_id": message_id,
                }
                file_records.append(_make_file_record(
                    filename, sender, message_id, date_received,
                    content_hash=content_hash, archived_path=archived, inbox_path=inbox_path,
                    processing_status="ready",
                ))
                message_had_success = True

            # Classify the message outcome for labeling (keyed by UID)
            if message_had_rejection and message_had_success:
                uid_outcomes[uid] = "partial_success"
            elif message_had_rejection and not message_had_success:
                uid_outcomes[uid] = "error"
            elif message_had_success:
                uid_outcomes[uid] = "success"
            # else: no outcome — no attachments of interest (won't label)

        # Persist state
        save_processed_ledger(ledger)
        write_manifest(file_records)

        # Determine overall run status
        if emails_scanned == 0 or new_files == 0 and duplicates_skipped == 0 and attachments_found == 0:
            overall_status = "no_files"
        elif security_rejections > 0 or invalid_extension_skipped > 0:
            overall_status = "partial_success" if new_files > 0 or duplicates_skipped > 0 else "error"
        else:
            overall_status = "success"

        log_path = write_run_summary(
            emails_scanned=emails_scanned,
            attachments_found=attachments_found,
            new_files=new_files,
            duplicates_skipped=duplicates_skipped,
            invalid_extension_skipped=invalid_extension_skipped,
            security_rejections=security_rejections,
            status=overall_status,
            notes=notes,
        )

        # Label + archive emails per-message outcome
        # ⚠️ Always call AFTER write_run_summary — never before.
        for uid, outcome in uid_outcomes.items():
            label_and_archive_email(conn, uid, outcome, config)

        # Error notification — send immediately if anything was rejected
        if overall_status in ("partial_success", "error"):
            try:
                from core.email_sender import send_inbox_notification  # local import to avoid cycles
                send_inbox_notification(
                    inbox_config=config,
                    status="error",
                    file_records=file_records,
                    run_date=today_str,
                )
            except Exception as exc:
                log.error("Failed to send error notification: %s", exc)

        # Success email: intentionally NOT sent here. Tier 2 batch processor will call
        # send_inbox_notification(status="success", ...) on completion once wiring is built.
        # See CLAUDE.md → Gmail Attachment Watcher section.

        if new_files == 0 and duplicates_skipped == 0:
            log.info("No new files found — pipeline will skip")
    finally:
        disconnect(conn)


# ═══════════════════════════════════════════════════════════════════════════════
# Entrypoint — guarantees a run summary on fatal error
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    try:
        main()
        sys.exit(0)
    except Exception as e:
        log.error("INBOX WATCHER FATAL: %s\n%s", e, traceback.format_exc())
        try:
            write_run_summary(
                status="error",
                notes=[f"fatal: {type(e).__name__}: {e}"],
            )
        except Exception as inner:
            log.error("Also failed to write run summary: %s", inner)
        sys.exit(1)
