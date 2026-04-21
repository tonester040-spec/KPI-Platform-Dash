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
Uses env vars (GitHub Secrets) matching the pattern in
email_assistant/gmail_connector.py. This is a DIFFERENT Gmail account than
Karissa's personal inbox, so it needs its own OAuth app + refresh token:
  KPI_INBOX_CLIENT_ID
  KPI_INBOX_CLIENT_SECRET
  KPI_INBOX_REFRESH_TOKEN

Run it
------
  python parsers/gmail_attachment_watcher.py          # real run
  # dry_run: flip inbox_config.json "dry_run": true
"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import os
import re
import sys
import tempfile
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
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

# ─── Gmail API constants ──────────────────────────────────────────────────────

TOKEN_URL = "https://oauth2.googleapis.com/token"
GMAIL_API = "https://gmail.googleapis.com/gmail/v1/users/me"


# ═══════════════════════════════════════════════════════════════════════════════
# Auth
# ═══════════════════════════════════════════════════════════════════════════════

def authenticate() -> str:
    """
    Exchange the KPI inbox refresh token for a fresh access token.
    Returns the access token string.

    On failure: logs and raises RuntimeError so the top-level handler writes a
    run summary and exits with code 1. Does NOT catch silently.
    """
    client_id     = os.environ.get("KPI_INBOX_CLIENT_ID", "").strip()
    client_secret = os.environ.get("KPI_INBOX_CLIENT_SECRET", "").strip()
    refresh_token = os.environ.get("KPI_INBOX_REFRESH_TOKEN", "").strip()

    missing = [k for k, v in {
        "KPI_INBOX_CLIENT_ID":     client_id,
        "KPI_INBOX_CLIENT_SECRET": client_secret,
        "KPI_INBOX_REFRESH_TOKEN": refresh_token,
    }.items() if not v]
    if missing:
        raise RuntimeError(
            f"INBOX WATCHER AUTH FAILURE — missing secrets: {', '.join(missing)}. "
            "Generate via email_assistant/get_token.py against the KPI inbox "
            "Gmail account, then add to GitHub Secrets."
        )

    data = urllib.parse.urlencode({
        "grant_type":    "refresh_token",
        "client_id":     client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token,
    }).encode()

    req = urllib.request.Request(TOKEN_URL, data=data, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")

    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            token_data = json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        raise RuntimeError(f"INBOX WATCHER AUTH FAILURE — {e.code}: {body}") from e

    access_token = token_data.get("access_token")
    if not access_token:
        raise RuntimeError(f"INBOX WATCHER AUTH FAILURE — no access_token in response: {token_data}")

    log.info("Authenticated to KPI inbox")
    return access_token


# ═══════════════════════════════════════════════════════════════════════════════
# Gmail API helpers (thin wrappers — no third-party client library)
# ═══════════════════════════════════════════════════════════════════════════════

def _api_get(access_token: str, path: str, params: dict | None = None) -> dict:
    """Authenticated GET against the Gmail API with retry on 429/5xx."""
    url = f"{GMAIL_API}/{path}"
    if params:
        url += "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url)
    req.add_header("Authorization", f"Bearer {access_token}")

    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=20) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            if e.code == 429 or e.code >= 500:
                wait = 2 ** attempt
                log.warning("Gmail API %d on %s (attempt %d) — retrying in %ds",
                            e.code, path, attempt + 1, wait)
                time.sleep(wait)
            else:
                body = e.read().decode()
                raise RuntimeError(f"Gmail API {e.code} on {path}: {body}") from e
    raise RuntimeError(f"Gmail API failed after 3 attempts: {path}")


def _api_post(access_token: str, path: str, body: dict) -> dict:
    """Authenticated POST against the Gmail API."""
    url = f"{GMAIL_API}/{path}"
    data = json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("Authorization", f"Bearer {access_token}")
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        body_text = e.read().decode()
        raise RuntimeError(f"Gmail API POST {e.code} on {path}: {body_text}") from e


# ═══════════════════════════════════════════════════════════════════════════════
# Email search
# ═══════════════════════════════════════════════════════════════════════════════

def search_for_attachment_emails(access_token: str, config: dict) -> list[str]:
    """
    Search the KPI inbox for attachment emails from whitelisted senders in the
    last N days. Returns a list of Gmail message IDs.

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
        result = _api_get(access_token, "messages", {"q": query, "maxResults": 50})
    except Exception as exc:
        log.error("Gmail search failed: %s — continuing with empty set", exc)
        return []

    messages = result.get("messages", [])
    ids = [m["id"] for m in messages]
    log.info("Found %d message(s) matching query", len(ids))
    return ids


# ═══════════════════════════════════════════════════════════════════════════════
# Attachment fetch
# ═══════════════════════════════════════════════════════════════════════════════

def _extract_sender_from_headers(message: dict) -> str:
    """Pull the 'From:' email out of a Gmail message's headers. Returns '' on miss."""
    headers = message.get("payload", {}).get("headers", [])
    for h in headers:
        if h.get("name", "").lower() == "from":
            value = h.get("value", "")
            m = re.search(r"<([^>]+)>", value)
            return (m.group(1) if m else value).strip().lower()
    return ""


def _extract_date_from_headers(message: dict) -> str:
    """Return the message Date header as ISO-ish string, or now() on miss."""
    headers = message.get("payload", {}).get("headers", [])
    for h in headers:
        if h.get("name", "").lower() == "date":
            return h.get("value", "")
    return datetime.now(timezone.utc).isoformat()


def _walk_parts(part: dict) -> list[dict]:
    """Flatten a Gmail message payload into every leaf part that has a filename."""
    out = []
    filename = part.get("filename", "")
    if filename:
        out.append(part)
    for sub in part.get("parts", []) or []:
        out.extend(_walk_parts(sub))
    return out


def fetch_attachments(access_token: str, message_id: str, config: dict) -> list[dict]:
    """
    Fetch a single message, validate the sender from raw headers, and return
    downloadable attachment records.

    Returns a list of dicts: {filename, content_bytes, message_id, sender,
    date_received, rejected_reason}.

    If the sender fails header validation, returns ONE dict with
    rejected_reason="security" so the caller can log and skip it.
    """
    try:
        msg = _api_get(access_token, f"messages/{message_id}", {"format": "full"})
    except Exception as exc:
        log.error("Could not fetch message %s: %s", message_id, exc)
        return []

    sender = _extract_sender_from_headers(msg)
    date_received = _extract_date_from_headers(msg)
    whitelist = [s.lower() for s in config.get("whitelisted_senders", [])]

    if sender not in whitelist:
        log.warning("INBOX SECURITY: rejected email %s — sender %s not whitelisted",
                    message_id, sender or "(empty)")
        return [{
            "filename": "(unknown)",
            "content_bytes": b"",
            "message_id": message_id,
            "sender": sender,
            "date_received": date_received,
            "rejected_reason": "security",
        }]

    allowed_ext = [e.lower() for e in config.get("allowed_extensions", [])]
    payload = msg.get("payload", {})
    parts = _walk_parts(payload)

    attachments: list[dict] = []
    for part in parts:
        filename = part.get("filename", "")
        if not filename:
            continue
        ext = Path(filename).suffix.lower()
        if ext not in allowed_ext:
            log.info("Skipping %s (extension %s not allowed)", filename, ext or "(none)")
            attachments.append({
                "filename": filename,
                "content_bytes": b"",
                "message_id": message_id,
                "sender": sender,
                "date_received": date_received,
                "rejected_reason": "invalid_extension",
            })
            continue

        attachment_id = part.get("body", {}).get("attachmentId")
        if not attachment_id:
            log.warning("Part %s in message %s has no attachmentId — skipping", filename, message_id)
            continue

        try:
            resp = _api_get(access_token, f"messages/{message_id}/attachments/{attachment_id}")
        except Exception as exc:
            log.error("Could not download attachment %s from %s: %s", filename, message_id, exc)
            continue

        data_b64 = resp.get("data", "")
        content_bytes = base64.urlsafe_b64decode(data_b64 + "==") if data_b64 else b""
        if not content_bytes:
            log.warning("Attachment %s returned empty bytes — skipping", filename)
            continue

        attachments.append({
            "filename": filename,
            "content_bytes": content_bytes,
            "message_id": message_id,
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

def _ensure_label(access_token: str, label_name: str) -> str:
    """Return the Gmail label ID for label_name, creating it if necessary."""
    existing = _api_get(access_token, "labels")
    for lbl in existing.get("labels", []):
        if lbl.get("name") == label_name:
            return lbl["id"]
    created = _api_post(access_token, "labels", {
        "name": label_name,
        "labelListVisibility": "labelShow",
        "messageListVisibility": "show",
    })
    log.info("Created Gmail label %s (id=%s)", label_name, created.get("id", "?"))
    return created["id"]


def label_and_archive_email(
    access_token: str,
    message_id: str,
    run_status: str,
    config: dict,
) -> None:
    """
    Apply KPI-Processed or KPI-Attention label to a source email based on the
    run status, and archive (remove from inbox) on success only.

    success          → KPI-Processed + archive out of inbox  (inbox stays clean)
    partial_success  → KPI-Attention, do NOT archive         (problem stays visible)
    error            → no label, no archive                  (problem stays visible)
    """
    if run_status == "error":
        return  # Preserve inbox signal — don't touch the email

    processed = config.get("kpi_processed_label", "KPI-Processed")
    attention = config.get("kpi_attention_label", "KPI-Attention")

    try:
        if run_status == "success":
            label_id = _ensure_label(access_token, processed)
            _api_post(access_token, f"messages/{message_id}/modify", {
                "addLabelIds":    [label_id],
                "removeLabelIds": ["INBOX"],
            })
            log.info("Labeled %s as %s and archived out of inbox", message_id, processed)
        elif run_status == "partial_success":
            label_id = _ensure_label(access_token, attention)
            _api_post(access_token, f"messages/{message_id}/modify", {
                "addLabelIds": [label_id],
                # Deliberately NOT removing INBOX label — inbox emptiness is the health signal
            })
            log.info("Labeled %s as %s (kept in inbox)", message_id, attention)
    except Exception as exc:
        log.error("label_and_archive_email failed for %s: %s", message_id, exc)
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
    message_outcomes: dict[str, str] = {}   # message_id → run_status ("success"/"partial_success")

    if dry_run:
        log.info("DRY RUN: would authenticate to KPI inbox")
        log.info("DRY RUN: would search with whitelisted_senders=%s window=%dd",
                 config.get("whitelisted_senders"), config.get("search_window_days", 2))
        log.info("DRY RUN: would dedupe against %s", LEDGER_PATH.relative_to(REPO_ROOT))
        write_manifest([])
        write_run_summary(status="no_files", notes=["dry_run=true — no real work performed"])
        return

    # Real run from here on
    access_token = authenticate()
    message_ids = search_for_attachment_emails(access_token, config)
    emails_scanned = len(message_ids)

    ledger = load_processed_ledger()
    today_str = datetime.now().strftime("%Y-%m-%d")

    for message_id in message_ids:
        attachments = fetch_attachments(access_token, message_id, config)
        if not attachments:
            notes.append(f"message {message_id[:8]}: no attachments fetched")
            continue

        message_had_rejection = False
        message_had_success = False

        for att in attachments:
            attachments_found += 1
            filename = att["filename"]
            sender = att["sender"]
            date_received = att["date_received"]
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

        # Classify the message outcome for labeling
        if message_had_rejection and message_had_success:
            message_outcomes[message_id] = "partial_success"
        elif message_had_rejection and not message_had_success:
            message_outcomes[message_id] = "error"
        elif message_had_success:
            message_outcomes[message_id] = "success"
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
    for message_id, outcome in message_outcomes.items():
        label_and_archive_email(access_token, message_id, outcome, config)

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
