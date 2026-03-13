#!/usr/bin/env python3
"""
email/gmail_connector.py
KPI Platform — Gmail API connector.

Uses OAuth 2.0 with refresh token stored in GitHub Secrets.
Required environment variables (set as GitHub Secrets):
  GMAIL_CLIENT_ID      — OAuth app client ID
  GMAIL_CLIENT_SECRET  — OAuth app client secret
  GMAIL_REFRESH_TOKEN  — refresh token from one-time setup (get_token.py)

If any of these are missing, raises NotConfiguredError — caught by
run_assistant.py which writes a graceful "not yet configured" debrief.
"""

import os
import json
import logging
import base64
import re
import time
import urllib.request
import urllib.parse
import urllib.error
from datetime import datetime, timezone, timedelta
from email.mime.text import MIMEText
from typing import Optional

log = logging.getLogger(__name__)

TOKEN_URL = "https://oauth2.googleapis.com/token"
GMAIL_API = "https://gmail.googleapis.com/gmail/v1/users/me"


class NotConfiguredError(Exception):
    """Raised when Gmail credentials are not yet set up."""
    pass


# ─── Auth ─────────────────────────────────────────────────────────────────────

def _get_credentials() -> dict:
    """Load OAuth credentials from environment variables (GitHub Secrets)."""
    client_id     = os.environ.get("GMAIL_CLIENT_ID", "").strip()
    client_secret = os.environ.get("GMAIL_CLIENT_SECRET", "").strip()
    refresh_token = os.environ.get("GMAIL_REFRESH_TOKEN", "").strip()

    missing = [k for k, v in {
        "GMAIL_CLIENT_ID": client_id,
        "GMAIL_CLIENT_SECRET": client_secret,
        "GMAIL_REFRESH_TOKEN": refresh_token,
    }.items() if not v]

    if missing:
        raise NotConfiguredError(
            f"Gmail not yet configured. Missing secrets: {', '.join(missing)}. "
            f"Run email/get_token.py to complete setup."
        )

    return {
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token,
    }


def authenticate() -> str:
    """
    Exchange refresh token for a fresh access token.
    Returns the access token string.
    """
    creds = _get_credentials()

    data = urllib.parse.urlencode({
        "grant_type":    "refresh_token",
        "client_id":     creds["client_id"],
        "client_secret": creds["client_secret"],
        "refresh_token": creds["refresh_token"],
    }).encode()

    req = urllib.request.Request(TOKEN_URL, data=data, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")

    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            token_data = json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        raise RuntimeError(f"Token refresh failed ({e.code}): {body}") from e

    access_token = token_data.get("access_token")
    if not access_token:
        raise RuntimeError(f"No access_token in response: {token_data}")

    log.info("Gmail authenticated — access token acquired")
    return access_token


# ─── API helpers ──────────────────────────────────────────────────────────────

def _api_get(access_token: str, path: str, params: dict = None) -> dict:
    """Make an authenticated GET request to the Gmail API."""
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
                log.warning("API rate limit / server error (attempt %d) — retrying in %ds", attempt + 1, wait)
                time.sleep(wait)
            else:
                body = e.read().decode()
                raise RuntimeError(f"Gmail API error ({e.code}) {path}: {body}") from e
    raise RuntimeError(f"Gmail API failed after 3 attempts: {path}")


def _api_post(access_token: str, path: str, body: dict) -> dict:
    """Make an authenticated POST request to the Gmail API."""
    url = f"{GMAIL_API}/{path}"
    data = json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("Authorization", f"Bearer {access_token}")
    req.add_header("Content-Type", "application/json")

    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        raise RuntimeError(f"Gmail API POST error ({e.code}) {path}: {body}") from e


# ─── Email parsing ────────────────────────────────────────────────────────────

def _decode_body(payload: dict) -> str:
    """Extract plain text body from a Gmail message payload."""
    body = ""

    def _walk(part):
        nonlocal body
        mime = part.get("mimeType", "")
        if mime == "text/plain" and "data" in part.get("body", {}):
            raw = base64.urlsafe_b64decode(part["body"]["data"] + "==").decode("utf-8", errors="replace")
            body += raw
        elif mime.startswith("multipart/"):
            for sub in part.get("parts", []):
                _walk(sub)

    _walk(payload)

    # Strip quoted replies (lines starting with > or "On [date]...wrote:")
    lines = body.split("\n")
    clean = []
    for line in lines:
        stripped = line.strip()
        if stripped.startswith(">"):
            continue
        if re.match(r"^On .{10,80} wrote:$", stripped):
            break  # Everything after this is quoted
        clean.append(line)

    # Remove footer noise
    text = "\n".join(clean)
    # Common footer markers
    footer_markers = [
        "unsubscribe", "opt out", "view in browser",
        "sent from my iphone", "sent from my samsung",
        "get outlook for", "confidentiality notice",
    ]
    for marker in footer_markers:
        idx = text.lower().find(marker)
        if idx > 100:  # Only strip if it's not the whole email
            text = text[:idx]

    return text.strip()


def _parse_message(msg: dict) -> dict:
    """Parse a raw Gmail API message into a clean dict."""
    headers = {h["name"].lower(): h["value"] for h in msg.get("payload", {}).get("headers", [])}
    payload = msg.get("payload", {})

    sender_full = headers.get("from", "Unknown")
    # Extract name and email from "Name <email@domain>" format
    email_match = re.search(r"<([^>]+)>", sender_full)
    sender_email = email_match.group(1) if email_match else sender_full
    sender_name  = re.sub(r"\s*<[^>]+>", "", sender_full).strip().strip('"') or sender_email

    # Check for noise indicators in headers
    has_list_id      = "list-id" in headers
    has_unsubscribe  = "list-unsubscribe" in headers
    labels           = msg.get("labelIds", [])

    body = _decode_body(payload)

    return {
        "id":             msg["id"],
        "thread_id":      msg.get("threadId", ""),
        "subject":        headers.get("subject", "(no subject)"),
        "sender":         sender_name,
        "sender_email":   sender_email,
        "date":           headers.get("date", ""),
        "body_plain":     body,
        "has_attachment": any(
            p.get("filename") for p in payload.get("parts", [])
        ),
        "labels":         labels,
        "has_list_id":    has_list_id,
        "has_unsubscribe": has_unsubscribe,
    }


# ─── Public API ───────────────────────────────────────────────────────────────

def fetch_recent_emails(access_token: str, hours: int = 24) -> list[dict]:
    """
    Fetch all emails from the last N hours.
    Returns list of parsed email dicts.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    # Gmail query: after:unix_timestamp
    after_ts = int(cutoff.timestamp())
    query    = f"after:{after_ts} in:inbox"

    log.info("Fetching emails from last %d hours (after %s)", hours, cutoff.strftime("%Y-%m-%d %H:%M UTC"))

    # Get message IDs
    result   = _api_get(access_token, "messages", {"q": query, "maxResults": 200})
    messages = result.get("messages", [])
    log.info("Found %d messages — fetching full content", len(messages))

    emails = []
    for i, msg_ref in enumerate(messages):
        try:
            full_msg = _api_get(access_token, f"messages/{msg_ref['id']}", {"format": "full"})
            parsed   = _parse_message(full_msg)
            emails.append(parsed)
            if i > 0 and i % 10 == 0:
                log.info("  Fetched %d/%d messages...", i + 1, len(messages))
        except Exception as exc:
            log.warning("  Skipping message %s — error: %s", msg_ref["id"], exc)

    log.info("Fetched %d emails successfully", len(emails))
    return emails


def fetch_thread_context(access_token: str, thread_id: str, max_messages: int = 3) -> str:
    """
    Fetch the last N messages in a thread for reply context.
    Returns plain text thread summary.
    """
    try:
        thread = _api_get(access_token, f"threads/{thread_id}", {"format": "full"})
        msgs   = thread.get("messages", [])[-max_messages:]

        parts = []
        for msg in msgs:
            parsed = _parse_message(msg)
            date   = parsed.get("date", "")
            sender = parsed.get("sender", "Unknown")
            body   = parsed.get("body_plain", "")[:500]  # Truncate for context
            parts.append(f"From: {sender} ({date})\n{body}")

        return "\n\n---\n\n".join(parts)
    except Exception as exc:
        log.warning("Could not fetch thread context for %s: %s", thread_id, exc)
        return ""


def create_draft(
    access_token: str,
    to: str,
    subject: str,
    body: str,
    thread_id: Optional[str] = None,
) -> str:
    """
    Create a draft in Gmail Drafts folder.
    Returns the draft ID.
    """
    mime = MIMEText(body, "plain", "utf-8")
    mime["to"]      = to
    mime["subject"] = subject

    raw = base64.urlsafe_b64encode(mime.as_bytes()).decode()

    draft_body: dict = {"message": {"raw": raw}}
    if thread_id:
        draft_body["message"]["threadId"] = thread_id

    result = _api_post(access_token, "drafts", draft_body)
    draft_id = result.get("id", "unknown")
    log.info("Draft created: '%s' → %s (id: %s)", subject, to, draft_id)
    return draft_id
