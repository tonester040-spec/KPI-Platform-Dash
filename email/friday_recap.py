#!/usr/bin/env python3
"""
email/friday_recap.py
KPI Platform — Friday weekly email recap.

Replaces Zones 2 and 2.5 in the debrief with a "Week in Review" summary
when today is Friday. Called by debrief_builder.py.
"""

import logging
import os
from datetime import datetime

log = logging.getLogger(__name__)


def is_friday() -> bool:
    return datetime.now().weekday() == 4  # Monday=0, Friday=4


def _client():
    try:
        import anthropic
        api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        if not api_key:
            raise EnvironmentError("ANTHROPIC_API_KEY not set")
        return anthropic.Anthropic(api_key=api_key)
    except ImportError:
        raise ImportError("anthropic package not installed.")


def generate_week_summary(all_week_emails: list[dict], dry_run: bool = False) -> dict:
    """
    Summarize a full week of emails for the Friday recap.

    Returns:
      {
        "week_range":    "Mon Mar 10 – Fri Mar 14",
        "summary_html":  "<p>...</p>",
        "open_items":    ["item 1", "item 2", ...],
        "resolved":      ["item 1", ...],
        "recurring":     ["pattern 1", ...],
      }
    """
    from datetime import datetime, timedelta
    today     = datetime.now()
    monday    = today - timedelta(days=today.weekday())
    week_range = f"{monday.strftime('%a %b %-d')} – {today.strftime('%a %b %-d')}"

    if dry_run or not all_week_emails:
        return {
            "week_range":   week_range,
            "summary_html": (
                "<p>This was a steady week across the network. "
                "Most location managers were responsive, vendor communications "
                "were routine, and no major escalations hit your inbox. "
                "A few action items from earlier in the week still need follow-up.</p>"
            ),
            "open_items":   ["Follow up with Andover manager re: scheduling"],
            "resolved":     ["Vendor invoice approved", "Coach call rescheduled"],
            "recurring":    ["Scheduling questions from location managers"],
        }

    # Build summary prompt
    email_summaries = []
    for email in all_week_emails[:60]:  # Cap at 60 for token budget
        email_summaries.append(
            f"- [{email.get('category','?')}] From: {email['sender']} | "
            f"Subject: {email.get('subject','')} | "
            f"Action: {email.get('action_summary','')}"
        )

    prompt = (
        f"Summarize this week's email activity for Karissa, "
        f"owner of a 13-location salon network (week: {week_range}).\n\n"
        f"Email list:\n" + "\n".join(email_summaries) + "\n\n"
        f"Return a JSON object with these exact keys:\n"
        f"- summary_html: one paragraph (plain English, wrapped in <p> tags) summarizing the week's themes\n"
        f"- open_items: array of strings — things still needing follow-up\n"
        f"- resolved: array of strings — things that got handled this week\n"
        f"- recurring: array of strings — patterns or recurring issues noticed\n\n"
        f"Be specific. Use names and details from the emails. Sound like a chief of staff briefing their boss."
    )

    try:
        client = _client()
        import anthropic
        msg = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=800,
            messages=[{"role": "user", "content": prompt}],
        )
        import json
        text  = msg.content[0].text.strip()
        start = text.find("{")
        end   = text.rfind("}") + 1
        data  = json.loads(text[start:end])
        data["week_range"] = week_range
        log.info("Friday recap generated successfully")
        return data
    except Exception as exc:
        log.error("Friday recap generation failed: %s — using fallback", exc)
        return {
            "week_range":   week_range,
            "summary_html": "<p>Weekly summary could not be generated. Check GitHub Actions logs.</p>",
            "open_items":   [],
            "resolved":     [],
            "recurring":    [],
        }


def render_friday_zone_html(recap: dict) -> str:
    """
    Renders the Friday 'Week in Review' zone HTML.
    Replaces Zones 2 and 2.5 in the debrief layout.
    """
    week_range   = recap.get("week_range", "This week")
    summary_html = recap.get("summary_html", "")
    open_items   = recap.get("open_items", [])
    resolved     = recap.get("resolved", [])
    recurring    = recap.get("recurring", [])

    open_li   = "".join(f"<li>{item}</li>" for item in open_items) or "<li>None identified</li>"
    res_li    = "".join(f"<li>{item}</li>" for item in resolved) or "<li>None recorded</li>"
    recur_li  = "".join(f"<li>{item}</li>" for item in recurring) or "<li>None identified</li>"

    return f"""
    <div class="zone zone-friday">
      <div class="zone-header">
        <span class="zone-label">📅 Week in Review</span>
        <span class="zone-meta">{week_range}</span>
      </div>
      <div class="friday-summary">
        {summary_html}
      </div>
      <div class="friday-grid">
        <div class="friday-col">
          <div class="friday-col-title">⚠️ Still Open</div>
          <ul class="friday-list open-list">{open_li}</ul>
        </div>
        <div class="friday-col">
          <div class="friday-col-title">✅ Resolved</div>
          <ul class="friday-list resolved-list">{res_li}</ul>
        </div>
        <div class="friday-col">
          <div class="friday-col-title">🔄 Recurring Themes</div>
          <ul class="friday-list recurring-list">{recur_li}</ul>
        </div>
      </div>
    </div>
"""
