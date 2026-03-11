#!/usr/bin/env python3
"""
core/email_sender.py
KPI Platform — sends weekly report email via Gmail SMTP.

Requires GitHub Secrets:
  GMAIL_APP_PASSWORD → 16-char Google App Password (2FA must be enabled)
  GMAIL_SENDER       → sender email address (must match the Google account)

Graceful degradation:
  If GMAIL_APP_PASSWORD is not set, logs a warning and skips email.
  This prevents a missing credential from crashing the whole pipeline.

DRY_RUN=true:
  Prints the email HTML to console instead of sending.
"""

import os
import smtplib
import logging
import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from pathlib import Path

log = logging.getLogger(__name__)

SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587


# ─── Email HTML ───────────────────────────────────────────────────────────────

def _build_html(data: dict, ai_cards: dict, report_path: Path | None) -> str:
    net = data["network"]
    week = net.get("week_ending", "")
    briefing = ai_cards.get("coach_briefing", "No briefing generated.")
    locs = data["locations"]

    # Location rows for the table
    loc_rows = ""
    for loc in sorted(locs, key=lambda x: x.get("rank_pph", 99)):
        flag = loc.get("flag", "solid")
        badge = (
            "⭐" if flag == "star" else
            "⚠️" if flag == "watch" else
            "✓"
        )
        pph_delta = loc.get("pph_delta")
        delta_str = f'<span style="color:{"green" if pph_delta and pph_delta >= 0 else "red"}">{pph_delta:+.2f}</span>' if pph_delta is not None else "—"
        loc_rows += f"""
        <tr>
          <td>{badge} {loc['loc_name']}</td>
          <td style="text-align:right;">${loc['pph']:.2f}</td>
          <td style="text-align:right;">{delta_str}</td>
          <td style="text-align:right;">{loc['product_pct']:.1f}%</td>
          <td style="text-align:right;">{loc['guests']:,}</td>
          <td style="text-align:right;">${loc['total_sales']:,.0f}</td>
        </tr>"""

    briefing_html = briefing.replace("\n\n", "</p><p>").replace("\n", "<br>")

    attachment_note = (
        f'<p>📎 Full report attached: <strong>{report_path.name}</strong></p>'
        if report_path and report_path.exists()
        else "<p>(No attachment — report build failed or was skipped.)</p>"
    )

    return f"""
<!DOCTYPE html>
<html>
<head>
<style>
  body {{ font-family: Arial, sans-serif; color: #333; max-width: 800px; margin: 0 auto; }}
  h1 {{ color: #0F1117; font-size: 22px; }}
  h2 {{ color: #1E3A5F; font-size: 16px; border-bottom: 2px solid #4A90D9; padding-bottom: 6px; }}
  table {{ border-collapse: collapse; width: 100%; margin: 16px 0; }}
  th {{ background: #0F1117; color: #fff; padding: 8px 12px; text-align: left; font-size: 13px; }}
  td {{ padding: 8px 12px; border-bottom: 1px solid #E8ECF0; font-size: 13px; }}
  tr:nth-child(even) td {{ background: #F8F9FA; }}
  .briefing {{ background: #F0F9FF; border-left: 4px solid #4A90D9; padding: 16px; margin: 16px 0; }}
  .footer {{ color: #999; font-size: 12px; margin-top: 32px; }}
</style>
</head>
<body>
<h1>📊 KPI Weekly Report — {week}</h1>
<p><strong>Network Summary:</strong>
{net.get('total_locations', 0)} locations ·
{net.get('total_guests', 0):,} guests ·
${net.get('total_sales', 0):,.0f} total sales ·
Avg PPH ${net.get('avg_pph', 0):.2f}
</p>

{attachment_note}

<h2>Coach Briefing</h2>
<div class="briefing"><p>{briefing_html}</p></div>

<h2>Location Performance</h2>
<table>
  <tr>
    <th>Location</th>
    <th>PPH</th>
    <th>vs Last Wk</th>
    <th>Product %</th>
    <th>Guests</th>
    <th>Total Sales</th>
  </tr>
  {loc_rows}
</table>

<div class="footer">
  KPI Platform · Karissa's Salon Network · Generated {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')} CT
</div>
</body>
</html>"""


# ─── Send ─────────────────────────────────────────────────────────────────────

def send_report(
    config: dict,
    data: dict,
    ai_cards: dict,
    report_path: Path | None,
    dry_run: bool = False,
):
    """
    Send the weekly KPI email to recipients in karissa_001.json.
    Gracefully skips if GMAIL_APP_PASSWORD is not set.
    """
    app_password = os.environ.get("GMAIL_APP_PASSWORD", "").strip()
    sender_email = os.environ.get("GMAIL_SENDER", "").strip()

    recipients = config.get("email_recipients", [])
    if not recipients:
        log.warning("No email_recipients in config — skipping email")
        return

    html_body = _build_html(data, ai_cards, report_path)
    week = data["network"].get("week_ending", "")
    subject = f"KPI Weekly Report — {week}"

    if dry_run:
        log.info("DRY RUN: Would send email to: %s", recipients)
        log.info("DRY RUN: Subject: %s", subject)
        print("\n" + "="*60)
        print("DRY RUN EMAIL OUTPUT:")
        print("="*60)
        print(f"To: {', '.join(recipients)}")
        print(f"Subject: {subject}")
        print("-"*60)
        # Print a text version
        net = data["network"]
        print(f"Network: {net.get('total_locations')} locs, ${net.get('total_sales',0):,.0f} sales, avg PPH ${net.get('avg_pph',0):.2f}")
        print("\nCoach Briefing:")
        print(ai_cards.get("coach_briefing", "(no briefing)"))
        print("="*60 + "\n")
        return

    if not app_password:
        log.warning(
            "GMAIL_APP_PASSWORD not set — skipping email send. "
            "Set this secret in GitHub → Settings → Secrets → Actions."
        )
        return

    if not sender_email:
        log.warning("GMAIL_SENDER not set — skipping email send.")
        return

    # Build message
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = sender_email
    msg["To"]      = ", ".join(recipients)

    msg.attach(MIMEText(html_body, "html"))

    # Attach Excel report if it exists
    if report_path and report_path.exists():
        with open(report_path, "rb") as f:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(f.read())
        encoders.encode_base64(part)
        part.add_header(
            "Content-Disposition",
            f'attachment; filename="{report_path.name}"'
        )
        msg.attach(part)
        log.info("Attached report: %s (%.1f KB)", report_path.name, report_path.stat().st_size / 1024)
    else:
        log.warning("Report file not found — sending email without attachment")

    # Send
    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as smtp:
            smtp.ehlo()
            smtp.starttls()
            smtp.login(sender_email, app_password)
            smtp.sendmail(sender_email, recipients, msg.as_string())
        log.info("Email sent to: %s", recipients)
    except smtplib.SMTPAuthenticationError:
        log.error(
            "Gmail authentication failed. Check GMAIL_APP_PASSWORD and GMAIL_SENDER. "
            "Ensure 2FA is enabled and App Password is generated correctly."
        )
    except Exception as exc:
        log.error("Email send failed: %s", exc)
