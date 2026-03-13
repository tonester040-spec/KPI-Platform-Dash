#!/usr/bin/env python3
"""
email/categorizer.py
KPI Platform — Claude-powered email categorization.

Processes all real emails in a single API call to minimize cost.
Returns structured triage data per email.
"""

import json
import logging
import os
import time
from typing import Any

log = logging.getLogger(__name__)

# Location names for Karissa's network — used for location mention detection
LOCATION_NAMES = [
    "Andover", "Blaine", "Crystal", "Elk River", "Forest Lake",
    "Prior Lake", "Hudson", "New Richmond", "Roseville",
    "Apple Valley", "Lakeville", "Farmington",
]

CATEGORIES     = ["Location Issue", "Vendor", "Coach", "Personal", "Admin", "FYI Only"]
URGENCY_DESC   = {1: "needs attention today", 2: "respond within 48hrs", 3: "FYI or low priority"}
ACTION_TYPES   = ["Reply Needed", "Task Created", "No Action", "Information Only"]

SYSTEM_PROMPT = """You are an email categorization assistant for Karissa, owner of a 13-location salon network in Minneapolis, MN.

Your job: categorize each email and extract what action (if any) is needed.

Categories:
- Location Issue: anything from/about a specific salon location — manager questions, staffing, operations, complaints
- Vendor: suppliers, software vendors, product companies, service providers
- Coach: Karissa's business coaches, consultants, advisors
- Personal: family, friends, personal appointments, non-business
- Admin: banking, legal, HR, insurance, licensing, taxes
- FYI Only: informational updates, confirmations, receipts that need no action

Urgency:
- 1 = needs attention today (problem, escalation, time-sensitive decision)
- 2 = respond within 48 hours (normal business reply needed)
- 3 = FYI or low priority (no real action needed)

Action types:
- Reply Needed: Karissa needs to send a response
- Task Created: creates a to-do (schedule something, review something, call someone)
- No Action: read and done
- Information Only: useful info to keep, no action

draft_needed: set to true ONLY for Location Issue, Vendor, or Coach emails where action_type is "Reply Needed". NEVER true for Personal emails.

Respond with a JSON array — one object per email, in the same order as the input. Each object:
{
  "id": "<email id>",
  "category": "<category>",
  "urgency": <1|2|3>,
  "action_type": "<action type>",
  "action_summary": "<one sentence: what needs to happen and why>",
  "task": "<task description if action_type is Task Created, otherwise null>",
  "draft_needed": <true|false>,
  "reason": "<one phrase explaining the categorization>"
}

Be concise. action_summary should be one clear sentence a busy person can read in 3 seconds."""


def _client():
    """Lazy-load Anthropic client."""
    try:
        import anthropic
        api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        if not api_key:
            raise EnvironmentError("ANTHROPIC_API_KEY not set")
        return anthropic.Anthropic(api_key=api_key)
    except ImportError:
        raise ImportError("anthropic package not installed. Run: pip install anthropic")


def _detect_location_mentions(emails: list[dict]) -> dict[str, list[str]]:
    """
    Scan email subjects and bodies for location name mentions.
    Returns {location_name: [email_subjects]} for the header zone.
    """
    mentions: dict[str, list[str]] = {}
    for email in emails:
        text = f"{email.get('subject','')} {email.get('body_plain','')[:500]}".lower()
        for loc in LOCATION_NAMES:
            if loc.lower() in text:
                mentions.setdefault(loc, []).append(email.get("subject", "(no subject)"))
    return mentions


def _build_batch_prompt(emails: list[dict]) -> str:
    """Build the prompt for batch categorization."""
    lines = []
    for i, email in enumerate(emails):
        body_preview = email.get("body_plain", "")[:300].replace("\n", " ").strip()
        lines.append(
            f"Email {i+1}:\n"
            f"ID: {email['id']}\n"
            f"From: {email['sender']} <{email['sender_email']}>\n"
            f"Subject: {email.get('subject','(no subject)')}\n"
            f"Body preview: {body_preview}"
        )
    return "\n\n".join(lines)


def _parse_response(response_text: str, emails: list[dict]) -> list[dict]:
    """Parse Claude's JSON response and merge back with email data."""
    # Extract JSON array from response (Claude sometimes adds preamble)
    text = response_text.strip()
    start = text.find("[")
    end   = text.rfind("]") + 1
    if start == -1 or end == 0:
        raise ValueError(f"No JSON array found in categorization response: {text[:200]}")

    categories = json.loads(text[start:end])

    # Build lookup by email id
    cat_by_id = {c["id"]: c for c in categories if "id" in c}

    results = []
    for email in emails:
        cat = cat_by_id.get(email["id"], {})
        results.append({
            **email,
            "category":       cat.get("category", "FYI Only"),
            "urgency":        cat.get("urgency", 3),
            "action_type":    cat.get("action_type", "No Action"),
            "action_summary": cat.get("action_summary", ""),
            "task":           cat.get("task"),
            "draft_needed":   cat.get("draft_needed", False),
            "reason":         cat.get("reason", ""),
        })

    return results


def categorize(emails: list[dict], dry_run: bool = False) -> dict[str, Any]:
    """
    Categorize all emails using Claude (single API call).

    Returns:
      {
        "categorized":         [email dicts with category fields added],
        "location_mentions":   {loc_name: [subjects]},
        "urgency_1":           [emails needing attention today],
        "urgency_2":           [emails for radar zone],
        "drafts_needed":       [emails where draft should be created],
        "tasks":               [non-null task strings],
      }
    """
    if not emails:
        log.info("No emails to categorize.")
        return _empty_result()

    location_mentions = _detect_location_mentions(emails)

    if dry_run:
        log.info("DRY RUN: Generating placeholder categorizations for %d emails", len(emails))
        categorized = _dry_run_categorize(emails)
    else:
        log.info("Categorizing %d emails with Claude...", len(emails))
        client = _client()
        prompt = _build_batch_prompt(emails)

        for attempt in range(3):
            try:
                msg = client.messages.create(
                    model="claude-haiku-4-5-20251001",
                    max_tokens=len(emails) * 150 + 500,
                    system=SYSTEM_PROMPT,
                    messages=[{"role": "user", "content": prompt}],
                )
                response_text = msg.content[0].text.strip()
                categorized   = _parse_response(response_text, emails)
                break
            except Exception as exc:
                if attempt < 2:
                    log.warning("Categorization attempt %d failed: %s — retrying", attempt + 1, exc)
                    time.sleep(5)
                else:
                    log.error("Categorization failed after 3 attempts: %s", exc)
                    categorized = _dry_run_categorize(emails)  # Fallback

    log.info("Categorization complete: %d emails processed", len(categorized))

    # Organize results
    urgency_1     = [e for e in categorized if e.get("urgency") == 1]
    urgency_2     = [e for e in categorized if e.get("urgency") == 2]
    drafts_needed = [e for e in categorized if e.get("draft_needed")]
    tasks         = [e["task"] for e in categorized if e.get("task")]

    log.info(
        "  Urgency 1: %d  |  Urgency 2: %d  |  Drafts needed: %d  |  Tasks: %d",
        len(urgency_1), len(urgency_2), len(drafts_needed), len(tasks),
    )

    return {
        "categorized":       categorized,
        "location_mentions": location_mentions,
        "urgency_1":         urgency_1,
        "urgency_2":         urgency_2,
        "drafts_needed":     drafts_needed,
        "tasks":             tasks,
    }


def _empty_result() -> dict:
    return {
        "categorized":       [],
        "location_mentions": {},
        "urgency_1":         [],
        "urgency_2":         [],
        "drafts_needed":     [],
        "tasks":             [],
    }


def _dry_run_categorize(emails: list[dict]) -> list[dict]:
    """Generate deterministic placeholder categorizations for DRY RUN mode."""
    import itertools
    cycle_cats = itertools.cycle(CATEGORIES)
    cycle_urg  = itertools.cycle([1, 2, 3, 3, 2])

    results = []
    for email in emails:
        cat = next(cycle_cats)
        urg = next(cycle_urg)
        results.append({
            **email,
            "category":       cat,
            "urgency":        urg,
            "action_type":    "Reply Needed" if urg == 1 else "No Action",
            "action_summary": f"[DRY RUN] {cat} email from {email['sender']} — review and respond.",
            "task":           f"Follow up with {email['sender']}" if urg == 1 else None,
            "draft_needed":   (urg == 1 and cat in ["Location Issue", "Vendor", "Coach"]),
            "reason":         "dry run placeholder",
        })
    return results
