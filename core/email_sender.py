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
        log.info("Email sent to %d recipient(s)", len(recipients))
    except smtplib.SMTPAuthenticationError:
        log.error(
            "Gmail authentication failed. Check GMAIL_APP_PASSWORD and GMAIL_SENDER. "
            "Ensure 2FA is enabled and App Password is generated correctly."
        )
    except Exception as exc:
        log.error("Email send failed: %s", exc)


# ─── Manager coach card email ──────────────────────────────────────────────────

def _build_coach_card_html(manager_name: str, card: dict, week: str, dashboard_url: str) -> str:
    """Build the HTML email body for a manager's weekly coach card."""

    def esc(text: str) -> str:
        """Minimal HTML escaping."""
        return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    headline = esc(card.get("territory_headline", ""))
    star = card.get("star_of_week", {})
    priority = card.get("priority_call", {})
    watch = card.get("one_to_watch", {})
    loc_cards = card.get("location_cards", [])
    spotlight = card.get("stylist_spotlight", {})
    pph_comp = card.get("pph_comparison", [])

    # Flag badge helpers
    def flag_badge(flag: str) -> str:
        flag = flag.upper()
        if flag == "STAR":
            return '<span style="background:#FEF3C7;color:#B45309;padding:2px 8px;border-radius:12px;font-size:11px;font-weight:700;">⭐ STAR</span>'
        if flag == "WATCH":
            return '<span style="background:#FEE2E2;color:#B91C1C;padding:2px 8px;border-radius:12px;font-size:11px;font-weight:700;">⚠ WATCH</span>'
        return '<span style="background:#F0F9FF;color:#1E3A5F;padding:2px 8px;border-radius:12px;font-size:11px;font-weight:700;">✓ SOLID</span>'

    # Location card rows
    loc_rows_html = ""
    for lc in loc_cards:
        pts = lc.get("talking_points", [])
        pts_html = "".join(f'<li style="margin-bottom:8px;">{esc(p)}</li>' for p in pts)
        pph_vs = lc.get("pph_vs_network", 0)
        pph_color = "#16A34A" if pph_vs >= 0 else "#DC2626"
        pph_sign = "+" if pph_vs >= 0 else ""
        rev_pct = lc.get("revenue_to_goal_pct", 0)
        rev_color = "#16A34A" if rev_pct >= 95 else ("#F59E0B" if rev_pct >= 80 else "#DC2626")

        loc_rows_html += f"""
        <div style="background:#fff;border:1px solid #E8ECF0;border-radius:8px;padding:16px;margin-bottom:12px;">
          <div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:10px;">
            <div>
              <span style="font-size:15px;font-weight:700;color:#1E3A5F;">{esc(lc.get('name',''))}</span>
              &nbsp;&nbsp;{flag_badge(lc.get('flag','SOLID'))}
            </div>
            <div style="text-align:right;font-size:12px;color:#7A8BA0;">
              Rank #{lc.get('rank_pph', lc.get('network_rank','?'))} network
            </div>
          </div>
          <div style="display:flex;gap:16px;flex-wrap:wrap;margin-bottom:12px;">
            <div style="text-align:center;">
              <div style="font-size:11px;color:#7A8BA0;text-transform:uppercase;font-weight:600;">PPH</div>
              <div style="font-size:20px;font-weight:800;color:#1E3A5F;">${lc.get('pph',0):.2f}</div>
              <div style="font-size:12px;color:{pph_color};font-weight:600;">{pph_sign}{pph_vs:.2f} vs avg</div>
            </div>
            <div style="text-align:center;">
              <div style="font-size:11px;color:#7A8BA0;text-transform:uppercase;font-weight:600;">Avg Ticket</div>
              <div style="font-size:20px;font-weight:800;color:#1E3A5F;">${lc.get('avg_ticket',0):.2f}</div>
            </div>
            <div style="text-align:center;">
              <div style="font-size:11px;color:#7A8BA0;text-transform:uppercase;font-weight:600;">Guests</div>
              <div style="font-size:20px;font-weight:800;color:#1E3A5F;">{lc.get('guests',0)}</div>
            </div>
            <div style="text-align:center;">
              <div style="font-size:11px;color:#7A8BA0;text-transform:uppercase;font-weight:600;">Product %</div>
              <div style="font-size:20px;font-weight:800;color:#1E3A5F;">{lc.get('product_pct',0):.1f}%</div>
            </div>
            <div style="text-align:center;">
              <div style="font-size:11px;color:#7A8BA0;text-transform:uppercase;font-weight:600;">vs Goal</div>
              <div style="font-size:20px;font-weight:800;color:{rev_color};">{rev_pct:.0f}%</div>
            </div>
          </div>
          <ul style="margin:0;padding-left:18px;color:#374151;font-size:13px;line-height:1.6;">{pts_html}</ul>
        </div>"""

    # PPH comparison table rows
    pph_rows = ""
    for row in pph_comp:
        delta = row.get("delta", 0)
        delta_color = "#16A34A" if delta >= 0 else "#DC2626"
        delta_str = f'<span style="color:{delta_color};font-weight:600;">{delta:+.2f}</span>'
        pph_rows += f"""
        <tr>
          <td style="padding:8px 12px;border-bottom:1px solid #F0F3F6;font-weight:600;color:#1E3A5F;">{esc(row.get('location',''))}</td>
          <td style="padding:8px 12px;border-bottom:1px solid #F0F3F6;text-align:right;">${row.get('this_week',0):.2f}</td>
          <td style="padding:8px 12px;border-bottom:1px solid #F0F3F6;text-align:right;">${row.get('last_week',0):.2f}</td>
          <td style="padding:8px 12px;border-bottom:1px solid #F0F3F6;text-align:right;">{delta_str}</td>
        </tr>"""

    # Stylist spotlight
    recog = spotlight.get("recognition", {})
    concern = spotlight.get("concern")
    spotlight_html = ""
    if recog and recog.get("name") and recog["name"] != "N/A":
        spotlight_html += f"""
        <div style="background:#ECFDF5;border-left:4px solid #16A34A;padding:12px 16px;margin-bottom:10px;border-radius:0 6px 6px 0;">
          <div style="font-size:12px;font-weight:700;color:#16A34A;text-transform:uppercase;margin-bottom:4px;">⭐ Recognition</div>
          <div style="font-size:14px;font-weight:700;color:#1E3A5F;">{esc(recog.get('name',''))} — {esc(recog.get('location',''))}</div>
          <div style="font-size:13px;color:#374151;margin-top:4px;">{esc(recog.get('note',''))}</div>
          <div style="font-size:12px;color:#7A8BA0;margin-top:4px;">PPH ${recog.get('pph',0):.2f} · Rebook {recog.get('rebook_rate',0):.1f}%</div>
        </div>"""
    if concern and concern.get("name"):
        spotlight_html += f"""
        <div style="background:#FFF7ED;border-left:4px solid #F59E0B;padding:12px 16px;border-radius:0 6px 6px 0;">
          <div style="font-size:12px;font-weight:700;color:#B45309;text-transform:uppercase;margin-bottom:4px;">⚑ Concern</div>
          <div style="font-size:14px;font-weight:700;color:#1E3A5F;">{esc(concern.get('name',''))} — {esc(concern.get('location',''))}</div>
          <div style="font-size:13px;color:#374151;margin-top:4px;">{esc(concern.get('note',''))}</div>
          <div style="font-size:12px;color:#7A8BA0;margin-top:4px;">PPH ${concern.get('pph',0):.2f} · Rebook {concern.get('rebook_rate',0):.1f}%</div>
        </div>"""

    return f"""<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1.0"/>
<style>
  body {{font-family:Arial,sans-serif;color:#333;max-width:680px;margin:0 auto;background:#F5F7FA;padding:0;}}
  .wrap {{max-width:680px;margin:0 auto;background:#fff;}}
  h2 {{color:#1E3A5F;font-size:15px;border-bottom:2px solid #4A90D9;padding-bottom:6px;margin:20px 0 12px;}}
</style>
</head>
<body>
<div class="wrap">

<!-- Header -->
<div style="background:#0F1117;padding:20px 24px;border-radius:0;">
  <div style="font-size:22px;font-weight:800;color:#fff;letter-spacing:-.5px;">
    KPI <span style="color:#4A90D9;">·</span> Weekly Coach Card
  </div>
  <div style="font-size:13px;color:#7A8BA0;margin-top:4px;">
    {esc(manager_name)}'s Territory &nbsp;·&nbsp; Week ending {esc(week)}
  </div>
</div>

<!-- Territory Headline -->
<div style="background:#F0F9FF;border-left:4px solid #4A90D9;padding:16px 20px;margin:0;">
  <div style="font-size:13px;font-weight:700;color:#4A90D9;text-transform:uppercase;margin-bottom:6px;">Territory Headline</div>
  <div style="font-size:15px;color:#1E3A5F;line-height:1.6;">{headline}</div>
</div>

<div style="padding:0 20px 20px;">

<!-- Star of the Week -->
<h2>⭐ Star of the Week</h2>
<div style="background:#FFFBEB;border:1px solid #FDE68A;border-radius:8px;padding:14px 16px;">
  <div style="font-size:14px;font-weight:700;color:#B45309;">{esc(star.get('location',''))} — ${star.get('pph',0):.2f} PPH</div>
  <div style="font-size:14px;color:#374151;margin-top:6px;line-height:1.6;">{esc(star.get('recognition_line',''))}</div>
</div>

<!-- Priority Call -->
<h2>📞 Priority Call — {esc(priority.get('location',''))}</h2>
<div style="background:#FFF5F5;border:1px solid #FCA5A5;border-radius:8px;padding:14px 16px;">
  <div style="margin-bottom:8px;">
    <span style="font-size:11px;font-weight:700;color:#DC2626;text-transform:uppercase;">Issue</span><br/>
    <span style="font-size:13px;color:#374151;">{esc(priority.get('issue',''))}</span>
  </div>
  <div style="margin-bottom:8px;">
    <span style="font-size:11px;font-weight:700;color:#DC2626;text-transform:uppercase;">Probable Cause</span><br/>
    <span style="font-size:13px;color:#374151;">{esc(priority.get('probable_cause',''))}</span>
  </div>
  <div style="background:#fff;border-left:3px solid #DC2626;padding:10px 14px;margin-top:10px;border-radius:0 6px 6px 0;">
    <span style="font-size:11px;font-weight:700;color:#DC2626;text-transform:uppercase;">Open With</span><br/>
    <span style="font-size:14px;font-style:italic;color:#1E3A5F;line-height:1.6;">"{esc(priority.get('coaching_question',''))}"</span>
  </div>
</div>

<!-- One to Watch -->
<h2>👀 One to Watch — {esc(watch.get('location',''))}</h2>
<div style="background:#FFFBEB;border:1px solid #FDE68A;border-radius:8px;padding:14px 16px;">
  <div style="margin-bottom:6px;font-size:13px;color:#374151;"><strong>Trend:</strong> {esc(watch.get('trend',''))}</div>
  <div style="margin-bottom:6px;font-size:13px;color:#374151;"><strong>Threshold:</strong> {esc(watch.get('threshold',''))}</div>
  <div style="font-size:13px;color:#374151;"><strong>Estimated weeks until critical:</strong> {watch.get('weeks_until_critical','?')}</div>
</div>

<!-- Location Cards -->
<h2>📍 Location Breakdown</h2>
{loc_rows_html}

<!-- Stylist Spotlight -->
<h2>💇 Stylist Spotlight</h2>
{spotlight_html if spotlight_html else '<div style="color:#7A8BA0;font-size:13px;">No stylist spotlight data available.</div>'}

<!-- PPH Comparison -->
<h2>📊 PPH Week-over-Week</h2>
<table style="width:100%;border-collapse:collapse;font-size:13px;">
  <thead>
    <tr style="background:#0F1117;">
      <th style="padding:8px 12px;text-align:left;color:#fff;font-weight:600;">Location</th>
      <th style="padding:8px 12px;text-align:right;color:#fff;font-weight:600;">This Week</th>
      <th style="padding:8px 12px;text-align:right;color:#fff;font-weight:600;">Last Week</th>
      <th style="padding:8px 12px;text-align:right;color:#fff;font-weight:600;">Δ</th>
    </tr>
  </thead>
  <tbody>{pph_rows}</tbody>
</table>

<!-- Dashboard link -->
<div style="margin-top:24px;text-align:center;">
  <a href="{dashboard_url}" style="display:inline-block;background:#4A90D9;color:#fff;padding:12px 28px;border-radius:8px;text-decoration:none;font-weight:700;font-size:14px;">
    Open My Dashboard →
  </a>
</div>

<div style="color:#999;font-size:11px;margin-top:24px;text-align:center;padding-bottom:20px;">
  KPI Platform · Karissa's Salon Network · {esc(week)} · Generated by KPI AI
</div>

</div>
</div>
</body>
</html>"""


# ─── Inbox watcher notification ───────────────────────────────────────────────

def _build_inbox_error_html(file_records: list[dict], run_date: str) -> str:
    """Build the HTML body for an inbox rejection / error email."""
    rejected = [r for r in file_records if r.get("processing_status") in (
        "security_rejected", "invalid_extension_skipped"
    )]

    rows = ""
    for r in rejected:
        status = r.get("processing_status", "")
        reason = {
            "security_rejected": "Sender not on whitelist (header validation failed)",
            "invalid_extension_skipped": "File type not in allowed extensions",
        }.get(status, "Unknown reason")
        rows += f"""
        <tr>
          <td style="padding:8px 12px;border-bottom:1px solid #E8ECF0;">{r.get('filename','(unknown)')}</td>
          <td style="padding:8px 12px;border-bottom:1px solid #E8ECF0;">{r.get('sender','(unknown)')}</td>
          <td style="padding:8px 12px;border-bottom:1px solid #E8ECF0;color:#B91C1C;">{reason}</td>
        </tr>"""

    if not rows:
        rows = (
            '<tr><td colspan="3" style="padding:12px;color:#7A8BA0;">'
            'No file-level details available — see inbox run log for specifics.'
            '</td></tr>'
        )

    return f"""<!DOCTYPE html>
<html><body style="font-family:Arial,sans-serif;color:#333;max-width:680px;margin:0 auto;">
  <div style="background:#B91C1C;color:#fff;padding:16px 20px;border-radius:6px 6px 0 0;">
    <div style="font-size:18px;font-weight:700;">⚠️ KPI Inbox — Files Rejected</div>
    <div style="font-size:13px;opacity:0.9;margin-top:4px;">Run date: {run_date}</div>
  </div>
  <div style="border:1px solid #FCA5A5;border-top:none;padding:20px;border-radius:0 0 6px 6px;">
    <p style="margin-top:0;">Some files from this morning's email batch were not processed. Details below.</p>
    <table style="width:100%;border-collapse:collapse;font-size:13px;">
      <thead>
        <tr style="background:#F9FAFB;">
          <th style="padding:8px 12px;text-align:left;border-bottom:2px solid #E8ECF0;">Filename</th>
          <th style="padding:8px 12px;text-align:left;border-bottom:2px solid #E8ECF0;">Sender</th>
          <th style="padding:8px 12px;text-align:left;border-bottom:2px solid #E8ECF0;">Reason</th>
        </tr>
      </thead>
      <tbody>{rows}</tbody>
    </table>
    <p style="margin-top:20px;font-size:13px;color:#374151;">
      <strong>What to do:</strong> If a file was rejected in error, verify the sender address is on the
      whitelist in <code>config/inbox_config.json</code> and that the extension is one of .xlsx, .xls, .pdf.
      Then re-send or re-run the workflow manually from the GitHub Actions tab.
    </p>
  </div>
</body></html>"""


def _build_inbox_success_html(
    karissa_email: str,
    file_records: list[dict],
    locations_processed: list[str],
    dashboard_url: str,
    trust_layer_flags: list | None,
) -> str:
    """Build the HTML body for a successful inbox ingestion email."""
    ready = [r for r in file_records if r.get("processing_status") == "ready"]
    file_list = "".join(
        f'<li style="margin-bottom:4px;">{r.get("filename","(unknown)")}</li>'
        for r in ready
    )
    loc_list = ", ".join(locations_processed) if locations_processed else "(none detected)"

    flags_block = ""
    if trust_layer_flags:
        flag_items = "".join(f'<li style="margin-bottom:4px;">{f}</li>' for f in trust_layer_flags)
        flags_block = f"""
        <div style="background:#FFFBEB;border-left:4px solid #F59E0B;padding:12px 16px;margin-top:16px;border-radius:0 6px 6px 0;">
          <div style="font-weight:700;color:#B45309;margin-bottom:6px;">⚑ Issues flagged during processing</div>
          <ul style="margin:0;padding-left:20px;font-size:13px;color:#374151;">{flag_items}</ul>
        </div>"""

    salutation = "Hey Karissa,"
    if karissa_email and not karissa_email.startswith("karissa@[REPLACE"):
        salutation = f"Hey Karissa ({karissa_email}),"

    return f"""<!DOCTYPE html>
<html><body style="font-family:Arial,sans-serif;color:#333;max-width:680px;margin:0 auto;">
  <div style="background:#0F1117;color:#fff;padding:16px 20px;border-radius:6px 6px 0 0;">
    <div style="font-size:18px;font-weight:700;">✅ KPI Weekly Data Processed</div>
  </div>
  <div style="border:1px solid #E8ECF0;border-top:none;padding:20px;border-radius:0 0 6px 6px;">
    <p style="margin-top:0;">{salutation}</p>
    <p>Your weekly data is in and the pipeline has run. Quick summary:</p>
    <p><strong>Files received ({len(ready)}):</strong></p>
    <ul style="margin:4px 0 16px;padding-left:20px;">{file_list or '<li>(no files)</li>'}</ul>
    <p><strong>Locations processed:</strong> {loc_list}</p>
    {flags_block}
    <div style="margin-top:24px;text-align:center;">
      <a href="{dashboard_url}" style="display:inline-block;background:#4A90D9;color:#fff;padding:12px 28px;border-radius:8px;text-decoration:none;font-weight:700;font-size:14px;">
        Open Dashboards →
      </a>
    </div>
  </div>
</body></html>"""


def send_inbox_notification(
    inbox_config: dict,
    status: str,
    file_records: list[dict],
    run_date: str,
    trust_layer_flags: list | None = None,
    locations_processed: list[str] | None = None,
    dashboard_url: str = "https://tonester040-spec.github.io/KPI-Platform-Dash/",
    dry_run: bool = False,
) -> None:
    """
    Send a notification email for the Gmail Attachment Watcher.

    Reuses the existing SMTP plumbing (GMAIL_APP_PASSWORD + GMAIL_SENDER).

    status == "error"   → sends rejection/error email to notification_recipients
    status == "success" → sends success email to notification_recipients (addresses Karissa personally)
    other statuses     → logs and skips (no email)

    Silently skips on missing GMAIL_APP_PASSWORD / GMAIL_SENDER so a missing
    credential does not crash the watcher.
    """
    recipients = inbox_config.get("notification_recipients", [])
    # Filter out the placeholder so we don't try to email "[REPLACE_BEFORE_GO_LIVE]"
    recipients = [r for r in recipients if "[REPLACE" not in r]

    if not recipients:
        log.info("No notification_recipients in inbox_config — skipping notification email")
        return

    if status == "error":
        subject = f"⚠️ KPI Inbox — Files Rejected ({run_date})"
        html_body = _build_inbox_error_html(file_records, run_date)
    elif status == "success":
        subject = f"✅ KPI Weekly Data Processed — {run_date}"
        html_body = _build_inbox_success_html(
            karissa_email=inbox_config.get("karissa_email", ""),
            file_records=file_records,
            locations_processed=locations_processed or [],
            dashboard_url=dashboard_url,
            trust_layer_flags=trust_layer_flags,
        )
    else:
        log.info("send_inbox_notification called with status=%r — no email to send", status)
        return

    if dry_run:
        log.info("DRY RUN: Would send %s inbox notification to: %s", status, recipients)
        log.info("DRY RUN: Subject: %s", subject)
        return

    app_password = os.environ.get("GMAIL_APP_PASSWORD", "").strip()
    sender_email = os.environ.get("GMAIL_SENDER", "").strip()

    if not app_password or not sender_email:
        log.warning(
            "GMAIL_APP_PASSWORD or GMAIL_SENDER not set — skipping inbox notification email. "
            "Watcher run summary still written to data/logs/."
        )
        return

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = sender_email
    msg["To"]      = ", ".join(recipients)
    msg.attach(MIMEText(html_body, "html"))

    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as smtp:
            smtp.ehlo()
            smtp.starttls()
            smtp.login(sender_email, app_password)
            smtp.sendmail(sender_email, recipients, msg.as_string())
        log.info("Inbox notification (%s) sent to %d recipient(s)", status, len(recipients))
    except smtplib.SMTPAuthenticationError:
        log.error("Gmail authentication failed sending inbox notification.")
    except Exception as exc:
        log.error("Inbox notification email failed: %s", exc)


def send_manager_coach_cards(
    config: dict,
    coach_cards: dict,
    week: str,
    dry_run: bool = False,
):
    """
    Send weekly coach card emails to Jess and Jenn.

    Reads manager email addresses from config["managers"][n]["email"].
    Gracefully skips any manager with no email configured.
    Gracefully skips if GMAIL_APP_PASSWORD is not set.

    coach_cards: {manager_name: coach_card_dict}
    """
    app_password = os.environ.get("GMAIL_APP_PASSWORD", "").strip()
    sender_email = os.environ.get("GMAIL_SENDER", "").strip()
    managers = config.get("managers", [])

    # Build manager email lookup from config
    mgr_emails = {m["name"]: m.get("email", "").strip() for m in managers}

    base_url = "https://tonester040-spec.github.io/KPI-Platform-Dash"

    for manager_name, card in coach_cards.items():
        recipient = mgr_emails.get(manager_name, "")

        if not recipient:
            log.info(
                "No email configured for manager %s — skipping coach card email. "
                "Add 'email' field to config/customers/karissa_001.json managers entry.",
                manager_name
            )
            continue

        # Dashboard URL for this manager's file
        mgr_config = next((m for m in managers if m.get("name") == manager_name), {})
        filename = mgr_config.get("filename", "")
        dashboard_url = f"{base_url}/{filename}" if filename else base_url

        html_body = _build_coach_card_html(manager_name, card, week, dashboard_url)
        subject = f"Your KPI Coach Card — Week ending {week}"

        if dry_run:
            log.info("DRY RUN: Would send coach card email to %s (%s)", manager_name, recipient)
            log.info("DRY RUN: Subject: %s", subject)
            headline = card.get("territory_headline", "(no headline)")[:120]
            print(f"\n{'='*55}")
            print(f"DRY RUN COACH CARD EMAIL — {manager_name}")
            print(f"To: {recipient}")
            print(f"Subject: {subject}")
            print(f"Headline: {headline}...")
            print(f"Priority Call: {card.get('priority_call',{}).get('location','?')}")
            print(f"{'='*55}\n")
            continue

        if not app_password:
            log.warning(
                "GMAIL_APP_PASSWORD not set — skipping coach card email for %s.", manager_name
            )
            return

        if not sender_email:
            log.warning("GMAIL_SENDER not set — skipping coach card email for %s.", manager_name)
            return

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = sender_email
        msg["To"] = recipient
        msg.attach(MIMEText(html_body, "html"))

        try:
            with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as smtp:
                smtp.ehlo()
                smtp.starttls()
                smtp.login(sender_email, app_password)
                smtp.sendmail(sender_email, [recipient], msg.as_string())
            log.info("Coach card email sent to %s (%s)", manager_name, recipient)
        except smtplib.SMTPAuthenticationError:
            log.error("Gmail authentication failed sending coach card to %s.", manager_name)
        except Exception as exc:
            log.error("Coach card email failed for %s: %s", manager_name, exc)
