#!/usr/bin/env python3
"""
main.py
KPI Platform — weekly pipeline orchestrator.

Pipeline sequence:
  1. Load config + authenticate
  2. Read current week data from Google Sheets (DATA, STYLISTS_DATA)
  3. Enrich data (ranks, deltas, flags)
  4. Generate AI cards (location summaries, stylist notes, coach briefing)
  5. Write back to Sheets (CURRENT, STYLISTS_CURRENT, ALERTS)
  6. Build Excel report
  7. Send email to Karissa with Excel attached
  8. Rebuild dashboard HTML (index.html, jess.html, jenn.html)
  9. Git commit + push docs/ to main

Environment variables:
  GOOGLE_SERVICE_ACCOUNT_JSON  → base64-encoded service account JSON (required)
  ANTHROPIC_API_KEY            → Claude API key (required)
  GMAIL_APP_PASSWORD           → Gmail App Password (optional — email skipped if missing)
  GMAIL_SENDER                 → Gmail sender address (optional)
  ACTIVE_CUSTOMER_ID           → customer config to load (default: karissa_001)
  DRY_RUN                      → set to 'true' to skip writes, prints, and pushes

Usage:
  python main.py
  DRY_RUN=true python main.py
"""

import os
import sys
import time
import logging
import datetime
from pathlib import Path

# ─── Logging setup ────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("main")

# ─── Paths ────────────────────────────────────────────────────────────────────

REPO_ROOT   = Path(__file__).resolve().parent
CUSTOMER_ID = os.environ.get("ACTIVE_CUSTOMER_ID", "karissa_001")
CONFIG_PATH = REPO_ROOT / "config" / "customers" / f"{CUSTOMER_ID}.json"
DOCS_DIR    = REPO_ROOT / "docs"
OUTPUT_DIR  = REPO_ROOT / "data" / "output"
LOG_DIR     = REPO_ROOT / "data" / "logs"

DRY_RUN = os.environ.get("DRY_RUN", "").strip().lower() in ("true", "1", "yes")


# ─── Step helpers ─────────────────────────────────────────────────────────────

def _step(num: int, label: str):
    log.info("─" * 55)
    log.info("Step %d: %s", num, label)


def _ok(label: str, detail: str = ""):
    if detail:
        log.info("  ✓ %s — %s", label, detail)
    else:
        log.info("  ✓ %s", label)


def _skip(label: str, reason: str = ""):
    if reason:
        log.info("  ⏭  %s skipped — %s", label, reason)
    else:
        log.info("  ⏭  %s skipped", label)


def _fail(label: str, exc: Exception):
    log.error("  ✗ %s FAILED: %s", label, exc)


# ─── Main pipeline ────────────────────────────────────────────────────────────

def main():
    start_time = time.time()

    print()
    print("╔══════════════════════════════════════════════════════════╗")
    print("║   KPI Platform — Weekly Pipeline                         ║")
    print(f"║   Customer : {CUSTOMER_ID:<44}║")
    print(f"║   DRY RUN  : {'YES — no writes will occur' if DRY_RUN else 'NO — live run':<44}║")
    print(f"║   Started  : {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'):<44}║")
    print("╚══════════════════════════════════════════════════════════╝")
    print()

    if DRY_RUN:
        log.info("⚠️  DRY RUN MODE — Sheets will not be written, no email sent, no git push.")

    # ── Step 1: Config ────────────────────────────────────────────────────────
    _step(1, "Config loaded")
    from core.data_source import load_config, _build_service
    config = load_config(CONFIG_PATH)
    _ok("Config", f"{CUSTOMER_ID} — {len(config.get('locations', []))} locations")

    # ── Step 2: Data source ───────────────────────────────────────────────────
    _step(2, "Data source — reading from Google Sheets")
    from core import data_source
    service = _build_service(config)
    locations = data_source.load_location_data(service, config)
    stylists  = data_source.load_stylist_data(service, config)
    history   = data_source.load_historical_data(service, config)
    _ok("Data source", f"{len(locations)} locations, {len(stylists)} stylists loaded")

    if not locations:
        log.error("No location data found in CURRENT tab — aborting pipeline.")
        sys.exit(1)

    # ── Step 3: Data processor ────────────────────────────────────────────────
    _step(3, "Data processor — enrichment")
    from core import data_processor
    data = data_processor.process(locations, stylists, history)
    _ok("Data processor", "enrichment complete")
    log.info(
        "  Network: %d locs, $%s total sales, avg PPH $%.2f",
        data["network"]["total_locations"],
        f"{data['network']['total_sales']:,.0f}",
        data["network"]["avg_pph"],
    )

    # ── Step 4: AI cards ──────────────────────────────────────────────────────
    _step(4, "AI cards — generating with Claude")
    from core import ai_cards as ai_module
    ai_cards = ai_module.generate_all(data, dry_run=DRY_RUN)
    _ok("AI cards", (
        f"{len(ai_cards['location_cards'])} location cards, "
        f"{len(ai_cards['stylist_cards'])} stylist cards, "
        f"1 coach briefing"
    ))

    # ── Step 4b: Manager coach cards ──────────────────────────────────────────
    _step(4, "Manager coach cards — Jess & Jenn territory briefs")
    from core import ai_coach_cards as coach_module
    coach_cards = coach_module.generate_all_coach_cards(config, data, dry_run=DRY_RUN)
    _ok("Coach cards", (
        f"{len(coach_cards)} manager brief(s) generated: {', '.join(coach_cards.keys())}"
        if coach_cards else "no managers configured with location_ids"
    ))

    # ── Step 5: Sheets writer ─────────────────────────────────────────────────
    _step(5, "Sheets writer — updating CURRENT, STYLISTS_CURRENT, ALERTS + coach briefs")
    from core import sheets_writer
    sheets_writer.write_all(config, data, ai_cards, coach_cards=coach_cards, dry_run=DRY_RUN)
    _ok("Sheets writer", "CURRENT, STYLISTS_CURRENT, ALERTS + coach briefs updated" if not DRY_RUN else "DRY RUN — skipped")

    # ── Step 6: Report builder ────────────────────────────────────────────────
    _step(6, "Report builder — building Excel file")
    from core import report_builder
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    report_path = report_builder.build_report(data, ai_cards, OUTPUT_DIR, dry_run=DRY_RUN)
    if report_path:
        _ok("Report builder", f"{report_path.name} created ({report_path.stat().st_size // 1024} KB)")
    else:
        _skip("Report builder", "openpyxl not available")

    # ── Step 7: Email sender — Karissa ────────────────────────────────────────
    _step(7, "Email sender — sending weekly report to Karissa")
    from core import email_sender
    email_sender.send_report(config, data, ai_cards, report_path, dry_run=DRY_RUN)
    if not DRY_RUN and os.environ.get("GMAIL_APP_PASSWORD"):
        _ok("Email sender (Karissa)", f"sent to {config.get('email_recipients', [])}")
    elif DRY_RUN:
        _ok("Email sender (Karissa)", "DRY RUN — printed to console")
    else:
        _skip("Email sender (Karissa)", "GMAIL_APP_PASSWORD not set")

    # ── Step 7b: Email sender — Manager coach cards ───────────────────────────
    _step(7, "Email sender — sending coach cards to Jess & Jenn")
    if coach_cards:
        week = data["network"].get("week_ending", "")
        email_sender.send_manager_coach_cards(config, coach_cards, week, dry_run=DRY_RUN)
        mgr_emails = {m["name"]: m.get("email","") for m in config.get("managers", [])}
        sent_to = [name for name, card in coach_cards.items() if mgr_emails.get(name)]
        skipped = [name for name, card in coach_cards.items() if not mgr_emails.get(name)]
        if sent_to or DRY_RUN:
            _ok("Email sender (managers)", f"coach cards: {', '.join(sent_to) or 'none with email configured'}")
        if skipped:
            _skip("Manager coach card emails", f"no email configured for: {', '.join(skipped)} — add 'email' to karissa_001.json managers")
    else:
        _skip("Manager coach card emails", "no coach cards generated")

    # ── Step 8: Dashboard builder ─────────────────────────────────────────────
    _step(8, "Dashboard builder — rebuilding HTML files with coach card data")
    from core import dashboard_builder
    results = dashboard_builder.rebuild_all(config, data, DOCS_DIR, coach_cards=coach_cards, dry_run=DRY_RUN)
    updated = [f for f, ok in results.items() if ok]
    failed  = [f for f, ok in results.items() if not ok]
    _ok("Dashboard builder", f"{', '.join(updated)} rebuilt")
    if failed:
        log.warning("  ⚠️  Failed to update: %s", failed)

    # ── Step 9: Git pusher ────────────────────────────────────────────────────
    _step(9, "Git pusher — committing docs/ (push handled by workflow)")
    from core import git_pusher
    week = data["network"].get("week_ending", "")
    pushed = git_pusher.push_dashboard(REPO_ROOT, week_ending=week, dry_run=DRY_RUN)
    if pushed:
        _ok("Git pusher", "committed docs/ — workflow will push" if not DRY_RUN else "DRY RUN — skipped")

    # ── Done ──────────────────────────────────────────────────────────────────
    elapsed = time.time() - start_time
    print()
    print("╔══════════════════════════════════════════════════════════╗")
    print("║   Pipeline complete ✓                                    ║")
    print(f"║   Total runtime: {elapsed:.1f}s{' ' * (42 - len(f'{elapsed:.1f}s'))}║")
    print(f"║   Week ending:   {week:<42}║")
    print("╚══════════════════════════════════════════════════════════╝")
    print()

    # Write pipeline log
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / f"pipeline_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    log_content = (
        f"Pipeline run: {datetime.datetime.now().isoformat()}\n"
        f"Customer: {CUSTOMER_ID}\n"
        f"Week ending: {week}\n"
        f"Locations: {len(data['locations'])}\n"
        f"Stylists: {len(data['stylists'])}\n"
        f"Total sales: ${data['network']['total_sales']:,.0f}\n"
        f"Avg PPH: ${data['network']['avg_pph']:.2f}\n"
        f"Dashboard files updated: {updated}\n"
        f"Runtime: {elapsed:.1f}s\n"
        f"Dry run: {DRY_RUN}\n"
    )
    log_file.write_text(log_content)
    log.info("Pipeline log saved: %s", log_file.name)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.info("Pipeline interrupted by user.")
        sys.exit(0)
    except Exception as exc:
        log.exception("Pipeline failed with unhandled error: %s", exc)
        # Fire alert BEFORE exiting — silent failures are not acceptable
        try:
            from core import alerter
            alerter.send(
                severity="CRITICAL",
                module="main",
                error_message=str(exc),
                diagnostic="Check the GitHub Actions log for the full traceback above.",
            )
        except Exception:
            pass  # Never let alerter failure mask the original error
        sys.exit(1)
