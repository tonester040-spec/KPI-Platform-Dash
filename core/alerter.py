#!/usr/bin/env python3
"""
core/alerter.py
KPI Platform — CRITICAL/HIGH failure alerting.

Any pipeline failure classified as CRITICAL or HIGH must call
alerter.send() BEFORE the pipeline exits. Silent failures are
not acceptable.

Alert routing (in order of preference):
  1. Email via Gmail SMTP (uses existing GMAIL_APP_PASSWORD / GMAIL_SENDER)
  2. GitHub Actions Job Summary (always available — written to $GITHUB_STEP_SUMMARY)

Every alert includes:
  - Timestamp of failure
  - Module or function that failed
  - Location ID affected (if applicable)
  - Error message in plain English
  - Suggested first diagnostic step

Usage:
  from core import alerter
  alerter.send(
      severity="CRITICAL",
      module="sheets_writer.write_current",
      error_message="Google Sheets write failed after 3 retries",
      location_id="z001",
      diagnostic="Check GOOGLE_SERVICE_ACCOUNT_JSON secret and Sheets API quota."
  )
"""

import os
import smtplib
import logging
import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pathlib import Path

log = logging.getLogger(__name__)

SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587

# Severities that trigger an immediate alert
ALERT_SEVERITIES = {"CRITICAL", "HIGH"}


def send(
    severity: str,
    module: str,
    error_message: str,
    location_id: str = "",
    diagnostic: str = "",
) -> None:
    """
    Fire an alert for a CRITICAL or HIGH pipeline failure.

    Always writes to GitHub Actions Job Summary (if $GITHUB_STEP_SUMMARY exists).
    Also sends email if GMAIL_APP_PASSWORD and GMAIL_SENDER are configured.

    Args:
        severity:      "CRITICAL" or "HIGH"
        module:        The module/function that failed (e.g., "sheets_writer.write_current")
        error_message: Human-readable description of what went wrong
        location_id:   Location ID affected, if applicable (e.g., "z001")
        diagnostic:    Suggested first diagnostic step for whoever receives the alert
    """
    if severity not in ALERT_SEVERITIES:
        log.debug("Alert severity %s below threshold — not firing alert", severity)
        return

    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC")
    loc_str   = f" (location: {location_id})" if location_id else ""
    diag_str  = f"\n\nFirst diagnostic step: {diagnostic}" if diagnostic else ""

    body = (
        f"⚠️  KPI Platform — {severity} Alert\n"
        f"{'=' * 50}\n\n"
        f"Timestamp : {timestamp}\n"
        f"Severity  : {severity}\n"
        f"Module    : {module}{loc_str}\n\n"
        f"Error: {error_message}"
        f"{diag_str}\n\n"
        f"Pipeline run: https://github.com/tonester040-spec/KPI-Platform-Dash/actions"
    )

    log.error("🚨 %s ALERT | %s | %s", severity, module, error_message)

    # ── Path 1: GitHub Actions Job Summary (always available in CI) ────────────
    _write_github_summary(severity, module, error_message, location_id, diagnostic, timestamp)

    # ── Path 2: Email via Gmail SMTP (best effort — never blocks pipeline exit) ─
    _send_email_alert(severity, module, body)


def _write_github_summary(
    severity: str,
    module: str,
    error_message: str,
    location_id: str,
    diagnostic: str,
    timestamp: str,
) -> None:
    """Write alert to GitHub Actions Job Summary ($GITHUB_STEP_SUMMARY)."""
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY", "")
    if not summary_path:
        return  # Not running in GitHub Actions — skip

    try:
        loc_line = f"\n- **Location**: `{location_id}`" if location_id else ""
        diag_line = f"\n- **First step**: {diagnostic}" if diagnostic else ""

        summary = (
            f"\n## 🚨 {severity} Alert — KPI Pipeline\n\n"
            f"- **Timestamp**: {timestamp}\n"
            f"- **Module**: `{module}`{loc_line}\n"
            f"- **Error**: {error_message}{diag_line}\n\n"
            f"> Alert fired before pipeline exit — check logs above for full trace.\n"
        )

        with open(summary_path, "a") as f:
            f.write(summary)

        log.info("Alert written to GitHub Actions Job Summary")
    except Exception as exc:
        log.warning("Could not write to GITHUB_STEP_SUMMARY: %s", exc)


def _send_email_alert(severity: str, module: str, body: str) -> None:
    """Send alert email via Gmail SMTP (best effort)."""
    app_password  = os.environ.get("GMAIL_APP_PASSWORD", "").strip()
    sender_email  = os.environ.get("GMAIL_SENDER", "").strip()
    alert_recipients_raw = os.environ.get("KPI_ALERT_EMAIL", "").strip()

    if not app_password or not sender_email:
        log.debug("Email alerting not configured (GMAIL_APP_PASSWORD/GMAIL_SENDER not set) — skipping")
        return

    # Default alert recipient is the sender itself (Tony's account) unless overridden
    recipients = [r.strip() for r in alert_recipients_raw.split(",") if r.strip()] if alert_recipients_raw else [sender_email]

    subject = f"[KPI {severity}] Pipeline failure — {module}"

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = sender_email
    msg["To"]      = ", ".join(recipients)
    msg.attach(MIMEText(body, "plain"))

    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as smtp:
            smtp.ehlo()
            smtp.starttls()
            smtp.login(sender_email, app_password)
            smtp.sendmail(sender_email, recipients, msg.as_string())
        log.info("Alert email sent to %d recipient(s)", len(recipients))
    except smtplib.SMTPAuthenticationError:
        log.warning("Alert email failed — Gmail auth error. Alert still written to Job Summary.")
    except Exception as exc:
        log.warning("Alert email failed: %s. Alert still written to Job Summary.", exc)


def test_alert() -> None:
    """
    Fire a deliberate test alert.
    Used during sandbox validation (Section 12) to confirm alert path works.
    """
    send(
        severity="HIGH",
        module="alerter.test_alert",
        error_message="This is a deliberate test alert fired during sandbox validation. No action required.",
        location_id="",
        diagnostic="Confirm this alert appears in GitHub Actions Job Summary and (if configured) email.",
    )
