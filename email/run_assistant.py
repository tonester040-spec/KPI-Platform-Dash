#!/usr/bin/env python3
"""
email/run_assistant.py
KPI Platform — Email Assistant main orchestrator.

Called by .github/workflows/email_assistant.yml every morning at 7:30am CT.
Runs the full pipeline and writes docs/karissa-debrief.html.

Steps:
  1. Authenticate with Gmail
  2. Fetch recent emails (last 24hrs)
  3. Apply noise filter
  4. Categorize real emails via Claude
  5. Generate draft replies via Claude
  6. Build debrief HTML
  7. Commit docs/ (workflow pushes)

If Gmail is not yet configured (secrets not set):
  → Writes a "not yet configured" placeholder page and exits cleanly.
  This lets the file exist at the URL without crashing.

Environment variables (GitHub Secrets):
  GMAIL_CLIENT_ID        — OAuth client ID
  GMAIL_CLIENT_SECRET    — OAuth client secret
  GMAIL_REFRESH_TOKEN    — OAuth refresh token (from get_token.py)
  ANTHROPIC_API_KEY      — Claude API key
  DRY_RUN                — 'true' to skip Gmail + writes

Usage:
  python email/run_assistant.py
  DRY_RUN=true python email/run_assistant.py
"""

import os
import sys
import time
import logging
import datetime
from pathlib import Path

# ─── Logging ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("email_assistant")

# ─── Paths ────────────────────────────────────────────────────────────────────

REPO_ROOT   = Path(__file__).resolve().parent.parent
DOCS_DIR    = REPO_ROOT / "docs"
DEBRIEF_OUT = DOCS_DIR / "karissa-debrief.html"

DRY_RUN = os.environ.get("DRY_RUN", "").strip().lower() in ("true", "1", "yes")


def _step(num: int, label: str):
    log.info("─" * 50)
    log.info("Step %d: %s", num, label)


def _ok(label: str, detail: str = ""):
    msg = f"  ✓ {label}"
    if detail:
        msg += f" — {detail}"
    log.info(msg)


def _skip(label: str, reason: str = ""):
    log.info("  ⏭  %s skipped%s", label, f" — {reason}" if reason else "")


def main():
    start = time.time()

    print()
    print("╔══════════════════════════════════════════════════════════╗")
    print("║   KPI Email Assistant                                    ║")
    print(f"║   DRY RUN  : {'YES' if DRY_RUN else 'NO — live run':<44}║")
    print(f"║   Started  : {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'):<44}║")
    print("╚══════════════════════════════════════════════════════════╝")
    print()

    from email import gmail_connector as gc
    from email import noise_filter, categorizer, draft_generator, debrief_builder

    result = {
        "total_emails":      0,
        "noise_count":       0,
        "drafts_created":    0,
        "location_mentions": {},
        "categorized":       [],
        "urgency_1":         [],
        "urgency_2":         [],
        "tasks":             [],
        "noise_report":      {},
        "draft_results":     [],
        "week_emails":       [],
    }

    # ── Step 1: Authenticate ──────────────────────────────────────────────────
    _step(1, "Gmail authentication")
    access_token = None

    if DRY_RUN:
        _skip("Gmail auth", "DRY RUN mode")
    else:
        try:
            access_token = gc.authenticate()
            _ok("Gmail auth", "access token acquired")
        except gc.NotConfiguredError as e:
            log.warning("Gmail not configured yet: %s", e)
            log.info("Writing placeholder debrief page — setup required.")
            _write_not_configured_page()
            log.info("Done — placeholder page written. Configure Gmail to go live.")
            return
        except Exception as e:
            log.error("Gmail auth failed: %s", e)
            debrief_builder.build_error_debrief(str(e), DEBRIEF_OUT)
            sys.exit(1)

    # ── Step 2: Fetch emails ──────────────────────────────────────────────────
    _step(2, "Fetching emails (last 24 hours)")
    emails = []

    if DRY_RUN:
        emails = _mock_emails()
        _ok("Fetch", f"{len(emails)} mock emails loaded")
    else:
        try:
            emails = gc.fetch_recent_emails(access_token, hours=24)
            result["total_emails"] = len(emails)
            _ok("Fetch", f"{len(emails)} emails retrieved")
        except Exception as e:
            log.error("Email fetch failed: %s", e)
            debrief_builder.build_error_debrief(f"Email fetch failed: {e}", DEBRIEF_OUT)
            sys.exit(1)

    # ── Step 3: Noise filter ──────────────────────────────────────────────────
    _step(3, "Noise filter")
    try:
        filter_result = noise_filter.apply(emails)
        real_emails   = filter_result["real"]
        result["noise_count"]  = len(filter_result["noise"])
        result["total_emails"] = len(emails)
        result["noise_report"] = filter_result["noise_report"]
        _ok("Noise filter", f"{len(real_emails)} real, {result['noise_count']} noise")
    except Exception as e:
        log.error("Noise filter failed: %s", e)
        real_emails = emails  # Fall through with all emails

    # ── Step 4: Categorize ────────────────────────────────────────────────────
    _step(4, "Categorizing emails with Claude")
    try:
        cat_result = categorizer.categorize(real_emails, dry_run=DRY_RUN)
        result.update({
            "categorized":       cat_result["categorized"],
            "location_mentions": cat_result["location_mentions"],
            "urgency_1":         cat_result["urgency_1"],
            "urgency_2":         cat_result["urgency_2"],
            "tasks":             cat_result["tasks"],
        })
        _ok("Categorize", (
            f"{len(cat_result['categorized'])} emails — "
            f"{len(cat_result['urgency_1'])} urgent, "
            f"{len(cat_result['drafts_needed'])} need drafts"
        ))
    except Exception as e:
        log.error("Categorization failed: %s", e)
        debrief_builder.build_error_debrief(f"Categorization failed: {e}", DEBRIEF_OUT)
        sys.exit(1)

    # ── Step 5: Generate drafts ───────────────────────────────────────────────
    _step(5, "Generating draft replies")
    drafts_needed = cat_result.get("drafts_needed", [])

    if not drafts_needed:
        _skip("Draft generation", "no drafts needed")
    elif not access_token and not DRY_RUN:
        _skip("Draft generation", "no access token")
    else:
        try:
            draft_results = draft_generator.generate_drafts(
                drafts_needed,
                access_token or "dry-run",
                dry_run=DRY_RUN,
            )
            result["draft_results"] = draft_results
            result["drafts_created"] = sum(1 for r in draft_results if r["success"])
            _ok("Drafts", f"{result['drafts_created']}/{len(drafts_needed)} created successfully")
        except Exception as e:
            log.error("Draft generation failed: %s", e)
            # Non-fatal — continue to build debrief

    # ── Step 6: Friday recap (if needed) ─────────────────────────────────────
    from email import friday_recap as fr
    if fr.is_friday():
        _step(6, "Friday — fetching week's emails for recap")
        if DRY_RUN:
            result["week_emails"] = _mock_emails()  # Reuse mock
            _ok("Friday recap", "DRY RUN — using mock week data")
        elif access_token:
            try:
                week_emails = gc.fetch_recent_emails(access_token, hours=120)
                # Categorize week emails for the recap
                week_cat = categorizer.categorize(week_emails, dry_run=DRY_RUN)
                result["week_emails"] = week_cat["categorized"]
                _ok("Friday recap", f"{len(week_emails)} emails from this week")
            except Exception as e:
                log.warning("Friday recap fetch failed: %s — skipping", e)
    else:
        _step(6, "Friday recap")
        _skip("Friday recap", "not Friday")

    # ── Step 7: Build debrief HTML ────────────────────────────────────────────
    _step(7, "Building morning debrief HTML")
    try:
        debrief_path = debrief_builder.build_debrief(result, DEBRIEF_OUT, dry_run=DRY_RUN)
        size_kb = DEBRIEF_OUT.stat().st_size // 1024 if DEBRIEF_OUT.exists() else 0
        _ok("Debrief built", f"{debrief_path.name} ({size_kb} KB)")
    except Exception as e:
        log.error("Debrief build failed: %s", e)
        debrief_builder.build_error_debrief(str(e), DEBRIEF_OUT)

    # ── Done ─────────────────────────────────────────────────────────────────
    elapsed = time.time() - start
    print()
    print("╔══════════════════════════════════════════════════════════╗")
    print("║   Email Assistant complete ✓                             ║")
    print(f"║   Total runtime : {elapsed:.1f}s{' ' * (41 - len(f'{elapsed:.1f}s'))}║")
    print(f"║   Emails total  : {result['total_emails']:<42}║")
    print(f"║   Noise filtered: {result['noise_count']:<42}║")
    print(f"║   Drafts created: {result['drafts_created']:<42}║")
    print("╚══════════════════════════════════════════════════════════╝")
    print()
    print("Debrief URL: https://tonester040-spec.github.io/KPI-Platform-Dash/karissa-debrief.html")
    print()


def _write_not_configured_page():
    """Write a placeholder page shown before Gmail OAuth is configured."""
    DOCS_DIR.mkdir(exist_ok=True)
    html = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1.0"/>
<title>KPI — Morning Debrief</title>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600;800&display=swap" rel="stylesheet"/>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:'Inter',sans-serif;background:#0F1117;color:#fff;display:flex;align-items:center;justify-content:center;min-height:100vh;padding:24px}
.card{background:#1a2035;border-radius:16px;padding:48px 40px;text-align:center;max-width:400px;width:100%}
.icon{font-size:48px;margin-bottom:16px}
h2{font-size:22px;font-weight:800;color:#fff;margin-bottom:8px}
p{color:#8899BB;font-size:14px;line-height:1.6;margin-bottom:0}
.tag{display:inline-block;background:#243050;color:#C8A97E;border-radius:8px;padding:4px 12px;font-size:12px;font-weight:700;margin-top:16px}
</style>
</head>
<body>
<div class="card">
  <div class="icon">⏳</div>
  <h2>Coming Soon</h2>
  <p>The Email Assistant is built and ready. Gmail authorization will be completed before it goes live.</p>
  <div class="tag">Setup in progress</div>
</div>
</body>
</html>"""
    DEBRIEF_OUT.write_text(html, encoding="utf-8")
    log.info("Placeholder page written to %s", DEBRIEF_OUT)


def _mock_emails() -> list:
    """Realistic mock emails for DRY_RUN testing."""
    return [
        {
            "id": "mock001", "thread_id": "thread001",
            "subject": "Scheduling issue at Andover — need help",
            "sender": "Andover Manager", "sender_email": "andover@example.com",
            "date": "Thu, 12 Mar 2026 08:15:00", "body_plain":
            "Hi Karissa, we're short two stylists on Saturday and I'm not sure how to handle the bookings. "
            "Can you advise? We have 14 clients scheduled.",
            "has_attachment": False, "labels": ["INBOX"], "has_list_id": False, "has_unsubscribe": False,
        },
        {
            "id": "mock002", "thread_id": "thread002",
            "subject": "Invoice #4892 — Redken Product Order",
            "sender": "Redken Accounts", "sender_email": "invoices@redken.com",
            "date": "Thu, 12 Mar 2026 07:30:00", "body_plain":
            "Please find attached invoice #4892 for $2,847.50 due March 26. Contact us with any questions.",
            "has_attachment": True, "labels": ["INBOX"], "has_list_id": False, "has_unsubscribe": False,
        },
        {
            "id": "mock003", "thread_id": "thread003",
            "subject": "This week's coaching session recap",
            "sender": "Business Coach", "sender_email": "coach@businesscoach.com",
            "date": "Thu, 12 Mar 2026 09:00:00", "body_plain":
            "Great session yesterday. Action items: 1) Review PPH trend at Farmington, "
            "2) Set Q2 revenue target by Friday, 3) Follow up on Jess's manager training.",
            "has_attachment": False, "labels": ["INBOX"], "has_list_id": False, "has_unsubscribe": False,
        },
        {
            "id": "mock004", "thread_id": "thread004",
            "subject": "Monthly newsletter — Salon Business Today",
            "sender": "Salon Business Today", "sender_email": "news@salonbusiness.com",
            "date": "Thu, 12 Mar 2026 06:00:00", "body_plain": "This month in the industry...",
            "has_attachment": False, "labels": ["INBOX"], "has_list_id": True, "has_unsubscribe": True,
        },
        {
            "id": "mock005", "thread_id": "thread005",
            "subject": "Prior Lake rebook rate question",
            "sender": "Jess", "sender_email": "jess@example.com",
            "date": "Thu, 12 Mar 2026 08:45:00", "body_plain":
            "Hey K — noticed rebook is down at Prior Lake. Should I address it in the team meeting Friday?",
            "has_attachment": False, "labels": ["INBOX"], "has_list_id": False, "has_unsubscribe": False,
        },
    ]


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.info("Interrupted by user.")
        sys.exit(0)
    except Exception as exc:
        log.exception("Email assistant failed with unhandled error: %s", exc)
        # Last-resort error page
        try:
            from email import debrief_builder
            debrief_builder.build_error_debrief(str(exc))
        except Exception:
            pass
        sys.exit(1)
