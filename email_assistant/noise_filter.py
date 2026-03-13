#!/usr/bin/env python3
"""
email/noise_filter.py
KPI Platform — Email noise filter.

Removes newsletters, automated emails, and other noise that doesn't
need Karissa's attention. Real emails pass through for categorization.

Config loaded from config/email_config.json.
First-run noise senders saved to config/noise_senders_found.json.
"""

import json
import logging
import re
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)

REPO_ROOT    = Path(__file__).resolve().parent.parent
CONFIG_PATH  = REPO_ROOT / "config" / "email_config.json"
NOISE_LOG    = REPO_ROOT / "config" / "noise_senders_found.json"
DISMISS_FLAG = REPO_ROOT / "config" / "noise_dismissed.json"

# Known newsletter/automated domains — extended list
NOISE_DOMAINS = {
    "mailchimp.com", "sendgrid.net", "constantcontact.com", "klaviyo.com",
    "hubspot.com", "marketo.com", "mailgun.org", "amazonses.com",
    "bounce.com", "email.com", "noreply.com", "notifications.com",
    "no-reply.com", "automated.com", "mailer.com", "newsletter.com",
    "campaign-archive.com", "list-manage.com", "groupon.com",
    "livingsocial.com", "yelp.com", "tripadvisor.com", "linkedin.com",
    "facebookmail.com", "twitter.com", "instagram.com",
}

# Subject keywords that almost always indicate noise
NOISE_SUBJECT_PATTERNS = [
    r"\bunsubscribe\b", r"\bnewsletter\b", r"\bpromo\b", r"\bsale\b",
    r"\d+%\s*off\b", r"\bdeals?\b", r"\bspecial offer\b", r"\blimited time\b",
    r"\bexclusive offer\b", r"\bblack friday\b", r"\bcyber monday\b",
    r"\byou.ve been selected\b", r"\bcongratulations\b", r"\bwinner\b",
    r"\bclaim your\b", r"\bact now\b", r"\bdon.t miss\b", r"\blast chance\b",
    r"\bfree shipping\b", r"\bfree trial\b", r"\bno obligation\b",
    r"\bmonthly digest\b", r"\bweekly digest\b", r"\bweekly roundup\b",
    r"\bnews & updates\b", r"\bwhat.s new\b",
]

# Sender name patterns that indicate automated senders
NOISE_SENDER_PATTERNS = [
    r"\bno.?reply\b", r"\bnoreply\b", r"\bdo.not.reply\b",
    r"\bnotification\b", r"\balert\b", r"\bmailer\b", r"\bnewsletter\b",
    r"\bautomated\b", r"\bsupport@\b", r"\binfo@\b", r"\bmarketing@\b",
    r"\bhelp@\b", r"\bteam@\b",
]

_noise_patterns_compiled = [re.compile(p, re.IGNORECASE) for p in NOISE_SUBJECT_PATTERNS]
_sender_patterns_compiled = [re.compile(p, re.IGNORECASE) for p in NOISE_SENDER_PATTERNS]


def _load_config() -> dict:
    """Load email config, return empty config if file doesn't exist yet."""
    if CONFIG_PATH.exists():
        try:
            return json.loads(CONFIG_PATH.read_text())
        except Exception:
            pass
    return {"blocked_domains": [], "blocked_senders": [], "blocked_subject_patterns": []}


def _is_noise(email: dict, config: dict) -> tuple[bool, str]:
    """
    Determine if an email is noise.
    Returns (is_noise: bool, reason: str)
    """
    sender_email = email.get("sender_email", "").lower()
    subject      = email.get("subject", "").lower()
    sender_name  = email.get("sender", "").lower()

    # Gmail-header-based signals (most reliable)
    if email.get("has_unsubscribe"):
        return True, "has List-Unsubscribe header"
    if email.get("has_list_id"):
        return True, "has List-ID header (mailing list)"

    # Config: explicitly blocked senders
    blocked_senders = [s.lower() for s in config.get("blocked_senders", [])]
    if sender_email in blocked_senders:
        return True, f"blocked sender: {sender_email}"

    # Config: explicitly blocked domains
    blocked_domains = config.get("blocked_domains", [])
    sender_domain = sender_email.split("@")[-1] if "@" in sender_email else ""
    if sender_domain in blocked_domains or sender_domain in NOISE_DOMAINS:
        return True, f"blocked domain: {sender_domain}"

    # Config: custom subject patterns
    for pattern in config.get("blocked_subject_patterns", []):
        if re.search(pattern, subject, re.IGNORECASE):
            return True, f"subject matches pattern: {pattern}"

    # Built-in subject patterns
    for pattern in _noise_patterns_compiled:
        if pattern.search(subject):
            return True, f"subject contains noise keyword"

    # Sender name patterns
    for pattern in _sender_patterns_compiled:
        if pattern.search(sender_email) or pattern.search(sender_name):
            return True, f"sender looks automated"

    return False, ""


def _record_noise_sender(sender_email: str, reason: str):
    """Append a new noise sender to the first-run log."""
    if DISMISS_FLAG.exists():
        return  # User has dismissed the noise report

    existing = {}
    if NOISE_LOG.exists():
        try:
            existing = json.loads(NOISE_LOG.read_text())
        except Exception:
            pass

    if sender_email not in existing:
        existing[sender_email] = reason
        NOISE_LOG.write_text(json.dumps(existing, indent=2))


def apply(emails: list[dict]) -> dict:
    """
    Filter emails into real vs noise.

    Returns:
      {
        "real":   [list of real emails],
        "noise":  [list of noise emails],
        "noise_report": {sender_email: reason, ...},
      }
    """
    config      = _load_config()
    real        = []
    noise       = []
    noise_report = {}

    for email in emails:
        is_noise_flag, reason = _is_noise(email, config)
        if is_noise_flag:
            noise.append(email)
            sender_email = email.get("sender_email", "unknown")
            noise_report[sender_email] = reason
            _record_noise_sender(sender_email, reason)
            log.debug("NOISE [%s]: %s — %s", reason, sender_email, email.get("subject", ""))
        else:
            real.append(email)

    log.info(
        "Noise filter: %d real, %d noise (%.0f%% filtered) from %d total",
        len(real), len(noise),
        100 * len(noise) / max(len(emails), 1),
        len(emails),
    )

    return {
        "real":         real,
        "noise":        noise,
        "noise_report": noise_report,
    }


def noise_dismissed() -> bool:
    """Returns True if the user has dismissed the first-run noise report."""
    return DISMISS_FLAG.exists()


def dismiss_noise_report():
    """Mark the noise report as dismissed — won't show again."""
    DISMISS_FLAG.write_text(json.dumps({"dismissed": True}))
    log.info("Noise report dismissed.")
