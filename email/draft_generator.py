#!/usr/bin/env python3
"""
email/draft_generator.py
KPI Platform — AI draft reply generator.

For each email where draft_needed=True, generates a reply in Karissa's
voice and deposits it into Gmail Drafts (properly threaded).
"""

import logging
import os
import time
from typing import Optional

from email import voice_profile as vp
from email import gmail_connector as gc

log = logging.getLogger(__name__)


def _client():
    """Lazy-load Anthropic client."""
    try:
        import anthropic
        api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        if not api_key:
            raise EnvironmentError("ANTHROPIC_API_KEY not set")
        return anthropic.Anthropic(api_key=api_key)
    except ImportError:
        raise ImportError("anthropic package not installed.")


def _generate_draft_text(
    email: dict,
    thread_context: str,
    profile: dict,
    client,
) -> str:
    """Call Claude to generate a draft reply in Karissa's voice."""
    recipient_type = vp.infer_recipient_type(email)
    system_prompt  = vp.get_draft_system_prompt(recipient_type, profile)

    thread_section = ""
    if thread_context:
        thread_section = f"Thread context (for your awareness only — do not quote it):\n{thread_context}\n\n"

    user_prompt = (
        f"{thread_section}"
        f"Email that needs a reply:\n"
        f"From: {email['sender']} <{email['sender_email']}>\n"
        f"Subject: {email.get('subject','')}\n\n"
        f"{email.get('body_plain','')[:800]}\n\n"
        f"Context about why this email matters: {email.get('action_summary','')}\n\n"
        f"Write a reply in Karissa's voice. Reply only with the email body."
    )

    msg = client.messages.create(
        model="claude-sonnet-4-6",  # Better model for voice-matched drafts
        max_tokens=400,
        system=system_prompt,
        messages=[{"role": "user", "content": user_prompt}],
    )
    return msg.content[0].text.strip()


def generate_drafts(
    emails_needing_drafts: list[dict],
    access_token: str,
    dry_run: bool = False,
) -> list[dict]:
    """
    Generate and deposit draft replies for all emails where draft_needed=True.

    Returns list of result dicts:
      {email_id, subject, sender, draft_id, success, error}
    """
    if not emails_needing_drafts:
        log.info("No drafts needed.")
        return []

    profile = vp.load_profile()
    results = []

    if dry_run:
        log.info("DRY RUN: Would generate %d drafts", len(emails_needing_drafts))
        for email in emails_needing_drafts:
            log.info("  [DRY RUN] Draft for: '%s' from %s", email.get("subject"), email.get("sender"))
            results.append({
                "email_id": email["id"],
                "subject":  email.get("subject", ""),
                "sender":   email.get("sender", ""),
                "draft_id": "dry-run-placeholder",
                "success":  True,
                "error":    None,
            })
        return results

    client = _client()
    log.info("Generating %d draft replies...", len(emails_needing_drafts))

    for i, email in enumerate(emails_needing_drafts):
        subject    = email.get("subject", "(no subject)")
        sender     = email.get("sender", "Unknown")
        sender_email = email.get("sender_email", "")
        thread_id  = email.get("thread_id", "")

        log.info("  Draft %d/%d: '%s' from %s", i + 1, len(emails_needing_drafts), subject, sender)

        try:
            # Fetch thread context for richer reply
            thread_context = ""
            if thread_id:
                thread_context = gc.fetch_thread_context(access_token, thread_id, max_messages=3)

            # Generate draft text
            draft_text = _generate_draft_text(email, thread_context, profile, client)

            # Deposit into Gmail Drafts
            reply_subject = subject if subject.lower().startswith("re:") else f"Re: {subject}"
            draft_id = gc.create_draft(
                access_token,
                to=sender_email,
                subject=reply_subject,
                body=draft_text,
                thread_id=thread_id or None,
            )

            results.append({
                "email_id": email["id"],
                "subject":  subject,
                "sender":   sender,
                "draft_id": draft_id,
                "success":  True,
                "error":    None,
            })

        except Exception as exc:
            log.error("  Failed to generate draft for '%s': %s", subject, exc)
            results.append({
                "email_id": email["id"],
                "subject":  subject,
                "sender":   sender,
                "draft_id": None,
                "success":  False,
                "error":    str(exc),
            })

        # Rate limiting between drafts
        if i < len(emails_needing_drafts) - 1:
            time.sleep(1.0)

    successful = sum(1 for r in results if r["success"])
    log.info("Drafts complete: %d/%d successful", successful, len(results))
    return results
