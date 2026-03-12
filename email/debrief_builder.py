#!/usr/bin/env python3
"""
email/debrief_builder.py
KPI Platform — Morning debrief HTML builder.

Builds docs/karissa-debrief.html from processed email data.
On Fridays, Zones 2 and 2.5 are replaced with a weekly recap
(handled by friday_recap.py).
"""

import json
import logging
from datetime import datetime
from pathlib import Path

from email import friday_recap as fr

log = logging.getLogger(__name__)

REPO_ROOT  = Path(__file__).resolve().parent.parent
DOCS_DIR   = REPO_ROOT / "docs"
NOISE_LOG  = REPO_ROOT / "config" / "noise_senders_found.json"
NOISE_DISM = REPO_ROOT / "config" / "noise_dismissed.json"

URGENCY_DOT = {
    1: '<span class="urgency-dot dot-red" title="Needs attention today"></span>',
    2: '<span class="urgency-dot dot-amber" title="Reply within 48hrs"></span>',
    3: '<span class="urgency-dot dot-gray" title="FYI / low priority"></span>',
}

CATEGORY_ICONS = {
    "Location Issue": "📍",
    "Vendor":         "🏢",
    "Coach":          "🎯",
    "Personal":       "👤",
    "Admin":          "📋",
    "FYI Only":       "📌",
}


# ─── Zone renderers ───────────────────────────────────────────────────────────

def _render_zone1(result: dict) -> str:
    now          = datetime.now()
    date_str     = now.strftime("%A, %B %-d")
    total        = result.get("total_emails", 0)
    noise        = result.get("noise_count", 0)
    real         = total - noise
    drafts       = result.get("drafts_created", 0)
    loc_mentions = result.get("location_mentions", {})

    loc_alerts = ""
    for loc, subjects in loc_mentions.items():
        count = len(subjects)
        loc_alerts += (
            f'<div class="loc-alert">'
            f'<span class="loc-alert-pin">📍</span>'
            f'<strong>{loc}</strong> mentioned in {count} email{"s" if count > 1 else ""} today'
            f'</div>'
        )

    return f"""
    <div class="zone zone-1">
      <div class="debrief-header">
        <div>
          <div class="debrief-date">{date_str}</div>
          <div class="debrief-meta">{real} emails · {noise} filtered as noise · {drafts} draft{"s" if drafts != 1 else ""} ready</div>
        </div>
        <a href="https://mail.google.com" target="_blank" class="gmail-link">Open Gmail →</a>
      </div>
      {loc_alerts}
    </div>"""


def _render_zone2(urgency_1: list, urgency_2_all: list) -> str:
    """Top 3 fires. Hard cap. Pulls from urgency=2 to fill if needed."""
    top3 = urgency_1[:3]
    if len(top3) < 3:
        fill = [e for e in urgency_2_all if e not in top3]
        top3 += fill[:3 - len(top3)]

    if not top3:
        return """
    <div class="zone zone-2">
      <div class="zone-header"><span class="zone-label">🔥 Top 3 Today</span></div>
      <div class="empty-zone">Clean inbox — nothing urgent today.</div>
    </div>"""

    items_html = ""
    for i, email in enumerate(top3[:3], 1):
        urg  = email.get("urgency", 2)
        dot  = URGENCY_DOT.get(urg, URGENCY_DOT[2])
        items_html += f"""
        <div class="top3-item">
          <span class="top3-num">{i}</span>
          {dot}
          <div class="top3-content">
            <div class="top3-who">{email.get('sender','Unknown')} · {email.get('subject','')[:60]}</div>
            <div class="top3-what">{email.get('action_summary','')}</div>
          </div>
        </div>"""

    return f"""
    <div class="zone zone-2">
      <div class="zone-header"><span class="zone-label">🔥 Top 3 Today</span><span class="zone-meta">Fires only</span></div>
      {items_html}
    </div>"""


def _render_zone25(urgency_2: list) -> str:
    """On Your Radar — 4-7 urgency=2 items. Omit if empty."""
    if not urgency_2:
        return ""

    items = urgency_2[:7]
    items_html = ""
    for email in items:
        items_html += f"""
        <div class="radar-item">
          {URGENCY_DOT[2]}
          <div class="radar-content">
            <span class="radar-who">{email.get('sender','Unknown')}</span>
            <span class="radar-sep">·</span>
            <span class="radar-subject">{email.get('subject','')[:55]}</span>
            <div class="radar-action">{email.get('action_summary','')}</div>
          </div>
        </div>"""

    return f"""
    <div class="zone zone-25">
      <div class="zone-header"><span class="zone-label">👁 On Your Radar Today</span><span class="zone-meta">Reply within 48hrs</span></div>
      {items_html}
    </div>"""


def _render_zone3(drafts_count: int) -> str:
    return f"""
    <div class="zone zone-3">
      <div class="zone-header"><span class="zone-label">✍️ Drafts Ready</span></div>
      <div class="drafts-content">
        <span class="drafts-count">{drafts_count}</span>
        <span class="drafts-label">draft{"s" if drafts_count != 1 else ""} waiting in Gmail</span>
        <a href="https://mail.google.com/mail/#drafts" target="_blank" class="drafts-btn">Open Drafts →</a>
      </div>
    </div>"""


def _render_zone4(categorized: list) -> str:
    """Email summary — collapsible by category."""
    from collections import defaultdict
    by_cat: dict = defaultdict(list)
    for email in categorized:
        by_cat[email.get("category", "FYI Only")].append(email)

    cat_order = ["Location Issue", "Coach", "Vendor", "Admin", "Personal", "FYI Only"]
    sections_html = ""

    for cat in cat_order:
        emails = by_cat.get(cat, [])
        if not emails:
            continue

        icon  = CATEGORY_ICONS.get(cat, "📧")
        count = len(emails)
        rows  = ""
        for email in emails:
            urg = email.get("urgency", 3)
            rows += f"""
            <div class="email-row">
              {URGENCY_DOT[urg]}
              <div class="email-row-content">
                <span class="email-from">{email.get('sender','Unknown')}</span>
                <span class="email-subj">{email.get('subject','')[:60]}</span>
                <div class="email-action">{email.get('action_summary','')}</div>
              </div>
            </div>"""

        sections_html += f"""
        <details class="cat-section">
          <summary class="cat-summary">
            <span class="cat-icon">{icon}</span>
            <span class="cat-name">{cat}</span>
            <span class="cat-count">{count}</span>
          </summary>
          <div class="cat-emails">{rows}</div>
        </details>"""

    if not sections_html:
        sections_html = '<div class="empty-zone">No emails to display.</div>'

    return f"""
    <div class="zone zone-4">
      <div class="zone-header"><span class="zone-label">📥 Email Summary</span></div>
      {sections_html}
    </div>"""


def _render_zone5(tasks: list, categorized: list) -> str:
    """Task list — all non-null tasks rendered as checkboxes."""
    all_tasks = []
    for email in categorized:
        if email.get("task"):
            all_tasks.append({
                "id":   email["id"],
                "text": email["task"],
                "from": email.get("sender", ""),
            })

    if not all_tasks:
        return ""

    items_html = ""
    for task in all_tasks:
        safe_id = task["id"].replace("-", "_")
        items_html += f"""
        <label class="task-item" id="task_{safe_id}">
          <input type="checkbox" class="task-cb" data-id="{task['id']}"
                 onchange="saveTask(this)"/>
          <span class="task-text">{task['text']}</span>
          <span class="task-from">· {task['from']}</span>
        </label>"""

    return f"""
    <div class="zone zone-5">
      <div class="zone-header"><span class="zone-label">✅ Tasks</span><span class="zone-meta">Reset tomorrow</span></div>
      <div class="task-list">{items_html}</div>
    </div>"""


def _render_zone6_noise(noise_report: dict) -> str:
    """First-run noise cleanup — only shows if not dismissed."""
    if not noise_report or NOISE_DISM.exists():
        return ""

    rows = ""
    for sender, reason in list(noise_report.items())[:20]:
        domain  = sender.split("@")[-1] if "@" in sender else sender
        search  = f"https://mail.google.com/mail/#search/from:{sender}"
        rows += f"""
        <div class="noise-row">
          <span class="noise-sender">{sender}</span>
          <span class="noise-reason">{reason}</span>
          <a href="{search}" target="_blank" class="noise-link">Find & Unsubscribe →</a>
        </div>"""

    count = len(noise_report)
    return f"""
    <div class="zone zone-6" id="noise-zone">
      <div class="zone-header">
        <span class="zone-label">🧹 Inbox Noise Cleanup</span>
        <span class="zone-meta">First-run only</span>
        <button class="dismiss-btn" onclick="dismissNoise()">Dismiss</button>
      </div>
      <p class="noise-intro">{count} noise senders detected. Unsubscribe to clean up your inbox permanently.</p>
      <div class="noise-list">{rows}</div>
    </div>"""


# ─── Main builder ─────────────────────────────────────────────────────────────

def build_debrief(result: dict, output_path: Path = None, dry_run: bool = False) -> Path:
    """
    Build the morning debrief HTML page.

    result dict keys:
      total_emails, noise_count, drafts_created,
      location_mentions, categorized, urgency_1, urgency_2,
      tasks, noise_report, draft_results
    """
    if output_path is None:
        output_path = DOCS_DIR / "karissa-debrief.html"

    today   = datetime.now()
    is_fri  = today.weekday() == 4

    # Build zones
    zone1 = _render_zone1(result)

    if is_fri and result.get("week_emails"):
        recap     = fr.generate_week_summary(result["week_emails"], dry_run=dry_run)
        zones_2_25 = fr.render_friday_zone_html(recap)
    else:
        u1 = result.get("urgency_1", [])
        u2 = result.get("urgency_2", [])
        # Zone 2.5 should not repeat Zone 2 items
        u2_filtered = [e for e in u2 if e not in u1[:3]]
        zones_2_25  = _render_zone2(u1, u2) + _render_zone25(u2_filtered[3:10])

    zone3 = _render_zone3(result.get("drafts_created", 0))
    zone4 = _render_zone4(result.get("categorized", []))
    zone5 = _render_zone5(result.get("tasks", []), result.get("categorized", []))
    zone6 = _render_zone6_noise(result.get("noise_report", {}))

    generated_at = today.strftime("%Y-%m-%d %H:%M")
    html = _build_html(zone1, zones_2_25, zone3, zone4, zone5, zone6, generated_at)

    if dry_run:
        log.info("DRY RUN: Would write debrief to %s (%d bytes)", output_path, len(html))
    else:
        output_path.parent.mkdir(exist_ok=True)
        output_path.write_text(html, encoding="utf-8")
        log.info("Debrief written to %s (%d bytes)", output_path.name, len(html))

    return output_path


def build_error_debrief(error_message: str, output_path: Path = None) -> Path:
    """Minimal fallback debrief when the pipeline fails."""
    if output_path is None:
        output_path = DOCS_DIR / "karissa-debrief.html"

    today    = datetime.now().strftime("%A, %B %-d")
    safe_msg = error_message.replace("<", "&lt;").replace(">", "&gt;")
    html = _build_html(
        zone1=f'<div class="zone zone-1"><div class="debrief-header"><div class="debrief-date">{today}</div></div></div>',
        zones_2_25=f'<div class="zone zone-2"><div class="error-msg">⚠️ Email assistant encountered an error today.<br/><code>{safe_msg}</code><br/>Check GitHub Actions logs for details.</div></div>',
        zone3="", zone4="", zone5="", zone6="",
        generated_at=datetime.now().strftime("%Y-%m-%d %H:%M"),
    )
    output_path.parent.mkdir(exist_ok=True)
    output_path.write_text(html, encoding="utf-8")
    log.info("Error debrief written to %s", output_path.name)
    return output_path


def _build_html(zone1, zones_2_25, zone3, zone4, zone5, zone6, generated_at) -> str:
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1.0"/>
<title>KPI — Morning Debrief</title>
<link rel="manifest" href="/KPI-Platform-Dash/manifest.json"/>
<meta name="apple-mobile-web-app-capable" content="yes"/>
<meta name="apple-mobile-web-app-status-bar-style" content="black-translucent"/>
<meta name="apple-mobile-web-app-title" content="KPI"/>
<link rel="apple-touch-icon" href="/KPI-Platform-Dash/icons/icon-192.png"/>
<meta name="theme-color" content="#0F1117"/>
<link rel="preconnect" href="https://fonts.googleapis.com"/>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap" rel="stylesheet"/>
<style>
*,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:'Inter',-apple-system,sans-serif;background:#F5F7FA;color:#1A1A2E;font-size:14px;line-height:1.5;padding-bottom:40px}}

/* ── PIN Gate ── */
#pin-gate{{position:fixed;inset:0;background:#0F1117;z-index:9999;display:flex;align-items:center;justify-content:center;flex-direction:column}}
#pin-gate.unlocked{{display:none}}
.pin-box{{background:#1a2035;border-radius:16px;padding:48px 40px;text-align:center;box-shadow:0 24px 64px rgba(0,0,0,.6);width:min(360px,90vw)}}
.pin-box h2{{color:#fff;font-size:22px;margin:0 0 8px}}
.pin-box p{{color:#8899BB;margin:0 0 28px;font-size:14px}}
.pin-inputs{{display:flex;gap:12px;justify-content:center;margin-bottom:24px}}
.pin-input{{width:64px;height:68px;border-radius:12px;border:2px solid #334;background:#243050;color:#fff;font-size:26px;font-weight:700;text-align:center;outline:none;caret-color:transparent;transition:border-color .15s;-webkit-tap-highlight-color:transparent;-webkit-appearance:none}}
.pin-input:focus{{border-color:#C8A97E;background:#1e2d4a}}
@keyframes pin-shake{{0%,100%{{transform:translateX(0)}}20%{{transform:translateX(-8px)}}40%{{transform:translateX(8px)}}60%{{transform:translateX(-6px)}}80%{{transform:translateX(6px)}}}}
.pin-shake{{animation:pin-shake .4s ease}}
.pin-error{{color:#f87171;font-size:13px;min-height:20px;margin-top:4px}}

/* ── Page header ── */
.page-header{{background:#1E3A5F;padding:16px 20px;display:flex;align-items:center;justify-content:space-between}}
.page-logo{{font-size:18px;font-weight:800;color:#fff;letter-spacing:-.3px}}
.page-logo span{{color:#C8A97E}}
.page-gen{{font-size:11px;color:#7A9BBE}}

/* ── Zones ── */
.zone{{background:#fff;margin:12px 12px 0;border-radius:10px;border:1px solid #E8ECF0;box-shadow:0 1px 3px rgba(0,0,0,.04);overflow:hidden}}
.zone-header{{display:flex;align-items:center;gap:10px;padding:14px 16px;border-bottom:1px solid #F0F3F6;background:#FAFBFC}}
.zone-label{{font-size:13px;font-weight:700;color:#1E3A5F;flex:1}}
.zone-meta{{font-size:11px;color:#9AAABB;font-weight:500}}
.empty-zone{{padding:20px 16px;color:#9AAABB;font-size:13px;text-align:center}}

/* ── Zone 1 — Header ── */
.debrief-header{{display:flex;align-items:center;justify-content:space-between;padding:16px;flex-wrap:wrap;gap:10px}}
.debrief-date{{font-size:20px;font-weight:800;color:#1E3A5F}}
.debrief-meta{{font-size:12px;color:#7A8BA0;margin-top:2px}}
.gmail-link{{font-size:12px;font-weight:600;color:#4A90D9;text-decoration:none;border:1px solid #C8D8EA;border-radius:6px;padding:6px 12px;white-space:nowrap}}
.gmail-link:hover{{background:#EEF4FB}}
.loc-alert{{display:flex;align-items:center;gap:8px;padding:8px 16px;background:#FFF7ED;border-top:1px solid #FED7AA;font-size:13px;color:#92400E}}
.loc-alert-pin{{flex-shrink:0}}

/* ── Urgency dots ── */
.urgency-dot{{display:inline-block;width:9px;height:9px;border-radius:50%;flex-shrink:0;margin-top:2px}}
.dot-red{{background:#EF4444}}
.dot-amber{{background:#F59E0B}}
.dot-gray{{background:#CBD5E1}}

/* ── Zone 2 — Top 3 ── */
.top3-item{{display:flex;align-items:flex-start;gap:10px;padding:14px 16px;border-bottom:1px solid #F0F3F6}}
.top3-item:last-child{{border-bottom:none}}
.top3-num{{font-size:18px;font-weight:800;color:#CBD5E1;width:20px;flex-shrink:0;text-align:center;line-height:1.3}}
.top3-content{{flex:1;min-width:0}}
.top3-who{{font-size:13px;font-weight:700;color:#1E3A5F;margin-bottom:3px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}}
.top3-what{{font-size:13px;color:#4A5568;line-height:1.4}}

/* ── Zone 2.5 — Radar ── */
.zone-25 .zone-header{{background:#F8FAFC}}
.zone-25 .zone-label{{color:#64748B}}
.radar-item{{display:flex;align-items:flex-start;gap:8px;padding:10px 16px;border-bottom:1px solid #F0F3F6}}
.radar-item:last-child{{border-bottom:none}}
.radar-content{{flex:1;min-width:0}}
.radar-who{{font-size:12.5px;font-weight:600;color:#374151}}
.radar-sep{{color:#D1D5DB;margin:0 4px}}
.radar-subject{{font-size:12.5px;color:#6B7280;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}}
.radar-action{{font-size:12px;color:#9AAABB;margin-top:2px}}

/* ── Zone 3 — Drafts ── */
.drafts-content{{display:flex;align-items:center;gap:14px;padding:16px;flex-wrap:wrap}}
.drafts-count{{font-size:32px;font-weight:800;color:#1E3A5F}}
.drafts-label{{font-size:14px;color:#7A8BA0;flex:1}}
.drafts-btn{{background:#C8A97E;color:#fff;border:none;border-radius:8px;padding:10px 18px;font-size:13px;font-weight:700;cursor:pointer;text-decoration:none;white-space:nowrap;transition:background .15s}}
.drafts-btn:hover{{background:#B8956A}}

/* ── Zone 4 — Email Summary ── */
.cat-section{{border-bottom:1px solid #F0F3F6}}
.cat-section:last-child{{border-bottom:none}}
.cat-summary{{display:flex;align-items:center;gap:8px;padding:12px 16px;cursor:pointer;list-style:none;user-select:none}}
.cat-summary::-webkit-details-marker{{display:none}}
.cat-summary:hover{{background:#FAFBFC}}
.cat-icon{{font-size:15px;flex-shrink:0}}
.cat-name{{font-size:13px;font-weight:600;color:#1E3A5F;flex:1}}
.cat-count{{background:#E8ECF0;color:#64748B;font-size:11px;font-weight:700;border-radius:10px;padding:1px 8px}}
.cat-emails{{border-top:1px solid #F0F3F6;background:#FAFBFC}}
.email-row{{display:flex;align-items:flex-start;gap:8px;padding:10px 16px 10px 28px;border-bottom:1px solid #F0F3F6}}
.email-row:last-child{{border-bottom:none}}
.email-row-content{{flex:1;min-width:0}}
.email-from{{font-size:12.5px;font-weight:600;color:#374151;margin-right:6px}}
.email-subj{{font-size:12.5px;color:#6B7280}}
.email-action{{font-size:12px;color:#9AAABB;margin-top:2px}}

/* ── Zone 5 — Tasks ── */
.task-list{{padding:8px 0}}
.task-item{{display:flex;align-items:flex-start;gap:10px;padding:10px 16px;cursor:pointer;transition:background .1s}}
.task-item:hover{{background:#FAFBFC}}
.task-cb{{width:17px;height:17px;margin-top:1px;flex-shrink:0;accent-color:#1E3A5F;cursor:pointer}}
.task-text{{font-size:13px;color:#2D3748;flex:1}}
.task-from{{font-size:11px;color:#9AAABB}}
input[type=checkbox]:checked + .task-text{{text-decoration:line-through;color:#9AAABB}}

/* ── Zone 6 — Noise ── */
.noise-intro{{padding:10px 16px;font-size:13px;color:#7A8BA0}}
.noise-list{{padding:0 0 8px}}
.noise-row{{display:flex;align-items:center;gap:8px;padding:8px 16px;border-bottom:1px solid #F0F3F6;flex-wrap:wrap}}
.noise-sender{{font-size:12.5px;font-weight:600;color:#374151;flex:1;min-width:180px}}
.noise-reason{{font-size:11px;color:#9AAABB}}
.noise-link{{font-size:12px;color:#4A90D9;text-decoration:none;white-space:nowrap}}
.dismiss-btn{{background:none;border:1px solid #E8ECF0;color:#9AAABB;border-radius:6px;padding:4px 10px;font-size:11px;cursor:pointer;font-family:inherit}}
.dismiss-btn:hover{{background:#F0F3F6}}

/* ── Friday recap ── */
.zone-friday{{}}
.friday-summary{{padding:14px 16px;font-size:13.5px;color:#374151;line-height:1.6;border-bottom:1px solid #F0F3F6}}
.friday-grid{{display:grid;grid-template-columns:repeat(3,1fr);gap:0;border-top:1px solid #F0F3F6}}
@media(max-width:600px){{.friday-grid{{grid-template-columns:1fr}}}}
.friday-col{{padding:14px 16px;border-right:1px solid #F0F3F6}}
.friday-col:last-child{{border-right:none}}
.friday-col-title{{font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.06em;color:#7A8BA0;margin-bottom:8px}}
.friday-list{{list-style:none;padding:0}}
.friday-list li{{font-size:12.5px;color:#4A5568;padding:4px 0;border-bottom:1px solid #F8F9FA;line-height:1.4}}
.friday-list li:last-child{{border-bottom:none}}

/* ── Error state ── */
.error-msg{{padding:20px 16px;font-size:13px;color:#7A8BA0;text-align:center;line-height:1.8}}
.error-msg code{{background:#F0F3F6;border-radius:4px;padding:2px 6px;font-size:12px;color:#DC2626;display:block;margin-top:8px;word-break:break-all}}

/* ── Mobile ── */
@media(max-width:480px){{
  .zone{{margin:8px 8px 0}}
  .debrief-date{{font-size:17px}}
  .top3-num{{font-size:15px}}
}}
</style>
</head>
<body>

<div id="pin-gate">
  <div class="pin-box">
    <h2>🔒 Morning Debrief</h2>
    <p>Enter your PIN to continue</p>
    <div class="pin-inputs" id="pin-inputs">
      <input class="pin-input" type="text" inputmode="numeric" maxlength="1" pattern="[0-9]" autocomplete="off"/>
      <input class="pin-input" type="text" inputmode="numeric" maxlength="1" pattern="[0-9]" autocomplete="off"/>
      <input class="pin-input" type="text" inputmode="numeric" maxlength="1" pattern="[0-9]" autocomplete="off"/>
      <input class="pin-input" type="text" inputmode="numeric" maxlength="1" pattern="[0-9]" autocomplete="off"/>
    </div>
    <div id="pin-error" class="pin-error"></div>
  </div>
</div>

<div class="page-header">
  <div class="page-logo">KPI <span>·</span> Morning Debrief</div>
  <div class="page-gen">Generated {generated_at}</div>
</div>

{zone1}
{zones_2_25}
{zone3}
{zone4}
{zone5}
{zone6}

<script>
/* ── PIN Gate ── */
(function() {{
  var CORRECT  = '1489';
  var SESS_KEY = 'kpi_unlocked_debrief';
  var gate     = document.getElementById('pin-gate');
  var inputs   = Array.from(document.querySelectorAll('.pin-input'));
  var errEl    = document.getElementById('pin-error');

  function unlock() {{
    gate.classList.add('unlocked');
    sessionStorage.setItem(SESS_KEY, '1');
  }}

  if (sessionStorage.getItem(SESS_KEY) === '1') {{ unlock(); return; }}

  function shake() {{
    var box = document.getElementById('pin-inputs');
    box.classList.add('pin-shake');
    box.addEventListener('animationend', function() {{ box.classList.remove('pin-shake'); }}, {{once:true}});
  }}

  inputs.forEach(function(inp, idx) {{
    inp.addEventListener('input', function() {{
      var val = inp.value.replace(/\D/g,'').slice(-1);
      inp.value = val;
      if (val && idx < inputs.length - 1) inputs[idx+1].focus();
      if (idx === inputs.length - 1 && val) checkPin();
    }});
    inp.addEventListener('keydown', function(e) {{
      if (e.key === 'Backspace' && !inp.value && idx > 0) {{
        inputs[idx-1].value = '';
        inputs[idx-1].focus();
      }}
    }});
    inp.addEventListener('paste', function(e) {{
      e.preventDefault();
      var text = (e.clipboardData||window.clipboardData).getData('text').replace(/\D/g,'').slice(0,4);
      text.split('').forEach(function(ch,i){{ if(inputs[i]) inputs[i].value=ch; }});
      if (text.length===4) checkPin();
      else if (inputs[text.length]) inputs[text.length].focus();
    }});
  }});

  function clearInputs() {{ inputs.forEach(function(i){{ i.value=''; }}); inputs[0].focus(); }}

  function checkPin() {{
    var entered = inputs.map(function(i){{ return i.value; }}).join('');
    if (entered.length < 4) return;
    if (entered === CORRECT) {{
      unlock();
    }} else {{
      shake();
      errEl.textContent = 'Incorrect PIN — try again';
      setTimeout(function() {{ errEl.textContent=''; clearInputs(); }}, 1500);
    }}
  }}

  inputs[0].focus();
}})();

/* ── Task persistence (sessionStorage) ── */
function saveTask(cb) {{
  var key = 'task_' + cb.dataset.id + '_' + new Date().toDateString();
  sessionStorage.setItem(key, cb.checked ? '1' : '0');
}}

// Restore task state on load
document.querySelectorAll('.task-cb').forEach(function(cb) {{
  var key = 'task_' + cb.dataset.id + '_' + new Date().toDateString();
  if (sessionStorage.getItem(key) === '1') cb.checked = true;
}});

/* ── Noise dismiss ── */
window.dismissNoise = function() {{
  var zone = document.getElementById('noise-zone');
  if (zone) zone.style.display = 'none';
  // Signal to pipeline on next run (flag file set server-side)
  fetch('/api/dismiss-noise', {{method:'POST'}}).catch(function(){{}});
}};
</script>
</body>
</html>"""
