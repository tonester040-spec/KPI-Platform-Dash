#!/usr/bin/env python3
"""
scripts/sandbox_run.py
KPI Platform — Full end-to-end sandbox validation runner.

Triggered by: SANDBOX_MODE=true python scripts/sandbox_run.py

Purpose:
  Runs the entire pipeline against fully mocked data — no real API is called.
  Used before deploying new credentials or after any schema/code change.
  Verifies every module completes without errors.

What is mocked:
  - Google Sheets read  → returns realistic location + stylist data
  - Google Sheets write → prints row counts, no actual write
  - Claude AI calls     → returns placeholder text (DRY_RUN=true)
  - Gmail SMTP          → no email sent
  - Git commit/push     → no actual git operations

What is NOT mocked (runs for real):
  - Data processor enrichment (pure Python — no I/O)
  - Dashboard builder HTML generation (file write to /tmp)
  - Drift checker validation (pure Python)
  - Alerter test path (writes to GITHUB_STEP_SUMMARY if present, else prints)

Exit codes:
  0 = All modules passed
  1 = One or more modules failed

Usage:
  python scripts/sandbox_run.py
  SANDBOX_MODE=true python scripts/sandbox_run.py   # equivalent
"""

import sys
import os
import logging
import traceback
from pathlib import Path

# Add repo root to path
REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

# Force sandbox/dry-run mode
os.environ["DRY_RUN"]     = "true"
os.environ["SANDBOX_MODE"] = "true"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("sandbox")


# ─── Mock data ────────────────────────────────────────────────────────────────

def _mock_locations() -> list[dict]:
    """12 realistic location rows matching karissa_001.json."""
    locations = [
        {"loc_name": "Andover FS",   "loc_id": "z001", "week_ending": "2026-03-14", "platform": "zenoti",
         "guests": 210, "total_sales": 18500.0, "service": 15200.0, "product": 3300.0,
         "product_pct": 17.8, "ppg": 88.0, "pph": 112.0, "avg_ticket": 88.0,
         "prod_hours": 165.0, "wax_count": 42, "wax": 2100.0, "wax_pct": 11.4,
         "color": 7200.0, "color_pct": 38.9, "treat_count": 18, "treat": 1800.0, "treat_pct": 9.7},
        {"loc_name": "Blaine",       "loc_id": "z002", "week_ending": "2026-03-14", "platform": "zenoti",
         "guests": 185, "total_sales": 16200.0, "service": 13400.0, "product": 2800.0,
         "product_pct": 17.3, "ppg": 87.6, "pph": 108.0, "avg_ticket": 87.6,
         "prod_hours": 150.0, "wax_count": 38, "wax": 1900.0, "wax_pct": 11.7,
         "color": 6500.0, "color_pct": 40.1, "treat_count": 14, "treat": 1400.0, "treat_pct": 8.6},
        {"loc_name": "Crystal FS",   "loc_id": "z003", "week_ending": "2026-03-14", "platform": "zenoti",
         "guests": 160, "total_sales": 14100.0, "service": 11600.0, "product": 2500.0,
         "product_pct": 17.7, "ppg": 88.1, "pph": 105.0, "avg_ticket": 88.1,
         "prod_hours": 134.0, "wax_count": 32, "wax": 1600.0, "wax_pct": 11.3,
         "color": 5600.0, "color_pct": 39.7, "treat_count": 12, "treat": 1200.0, "treat_pct": 8.5},
        {"loc_name": "Elk River FS", "loc_id": "z004", "week_ending": "2026-03-14", "platform": "zenoti",
         "guests": 175, "total_sales": 15400.0, "service": 12800.0, "product": 2600.0,
         "product_pct": 16.9, "ppg": 88.0, "pph": 103.0, "avg_ticket": 88.0,
         "prod_hours": 149.0, "wax_count": 35, "wax": 1750.0, "wax_pct": 11.4,
         "color": 6100.0, "color_pct": 39.6, "treat_count": 13, "treat": 1300.0, "treat_pct": 8.4},
        {"loc_name": "Forest Lake",  "loc_id": "z005", "week_ending": "2026-03-14", "platform": "zenoti",
         "guests": 148, "total_sales": 13200.0, "service": 10900.0, "product": 2300.0,
         "product_pct": 17.4, "ppg": 89.2, "pph": 102.0, "avg_ticket": 89.2,
         "prod_hours": 129.0, "wax_count": 30, "wax": 1500.0, "wax_pct": 11.4,
         "color": 5200.0, "color_pct": 39.4, "treat_count": 11, "treat": 1100.0, "treat_pct": 8.3},
        {"loc_name": "Prior Lake",   "loc_id": "z006", "week_ending": "2026-03-14", "platform": "zenoti",
         "guests": 168, "total_sales": 14900.0, "service": 12300.0, "product": 2600.0,
         "product_pct": 17.4, "ppg": 88.7, "pph": 107.0, "avg_ticket": 88.7,
         "prod_hours": 139.0, "wax_count": 34, "wax": 1700.0, "wax_pct": 11.4,
         "color": 5900.0, "color_pct": 39.6, "treat_count": 13, "treat": 1300.0, "treat_pct": 8.7},
        {"loc_name": "Hudson",       "loc_id": "z007", "week_ending": "2026-03-14", "platform": "zenoti",
         "guests": 142, "total_sales": 12700.0, "service": 10500.0, "product": 2200.0,
         "product_pct": 17.3, "ppg": 89.4, "pph": 99.0, "avg_ticket": 89.4,
         "prod_hours": 128.0, "wax_count": 28, "wax": 1400.0, "wax_pct": 11.0,
         "color": 5000.0, "color_pct": 39.4, "treat_count": 11, "treat": 1100.0, "treat_pct": 8.7},
        {"loc_name": "New Richmond", "loc_id": "z008", "week_ending": "2026-03-14", "platform": "zenoti",
         "guests": 118, "total_sales": 10500.0, "service": 8700.0, "product": 1800.0,
         "product_pct": 17.1, "ppg": 89.0, "pph": 97.0, "avg_ticket": 89.0,
         "prod_hours": 108.0, "wax_count": 22, "wax": 1100.0, "wax_pct": 10.5,
         "color": 4100.0, "color_pct": 39.0, "treat_count": 9, "treat": 900.0, "treat_pct": 8.6},
        {"loc_name": "Roseville",    "loc_id": "z009", "week_ending": "2026-03-14", "platform": "zenoti",
         "guests": 222, "total_sales": 19800.0, "service": 16300.0, "product": 3500.0,
         "product_pct": 17.7, "ppg": 89.2, "pph": 115.0, "avg_ticket": 89.2,
         "prod_hours": 172.0, "wax_count": 44, "wax": 2200.0, "wax_pct": 11.1,
         "color": 7800.0, "color_pct": 39.4, "treat_count": 19, "treat": 1900.0, "treat_pct": 9.6},
        {"loc_name": "Apple Valley", "loc_id": "z010", "week_ending": "2026-03-14", "platform": "salon_ultimate",
         "guests": 162, "total_sales": 14400.0, "service": 11900.0, "product": 2500.0,
         "product_pct": 17.4, "ppg": 88.9, "pph": 106.0, "avg_ticket": 88.9,
         "prod_hours": 135.0, "wax_count": 32, "wax": 1600.0, "wax_pct": 11.1,
         "color": 5700.0, "color_pct": 39.6, "treat_count": 12, "treat": 1200.0, "treat_pct": 8.3},
        {"loc_name": "Lakeville",    "loc_id": "su001", "week_ending": "2026-03-14", "platform": "salon_ultimate",
         "guests": 145, "total_sales": 13000.0, "service": 10700.0, "product": 2300.0,
         "product_pct": 17.7, "ppg": 89.7, "pph": 104.0, "avg_ticket": 89.7,
         "prod_hours": 125.0, "wax_count": 28, "wax": 1400.0, "wax_pct": 10.8,
         "color": 5100.0, "color_pct": 39.2, "treat_count": 11, "treat": 1100.0, "treat_pct": 8.5},
        {"loc_name": "Farmington",   "loc_id": "su002", "week_ending": "2026-03-14", "platform": "salon_ultimate",
         "guests": 122, "total_sales": 10900.0, "service": 9000.0, "product": 1900.0,
         "product_pct": 17.4, "ppg": 89.3, "pph": 98.0, "avg_ticket": 89.3,
         "prod_hours": 111.0, "wax_count": 24, "wax": 1200.0, "wax_pct": 11.0,
         "color": 4300.0, "color_pct": 39.4, "treat_count": 9, "treat": 900.0, "treat_pct": 8.3},
    ]
    return locations


def _mock_stylists() -> list[dict]:
    """A small set of realistic stylist records."""
    return [
        {"name": "Ashley M.", "loc_name": "Andover FS", "loc_id": "z001", "status": "active",
         "tenure": 3.2, "weeks": ["2026-03-14"], "pph": [112.0], "rebook": [68.0],
         "product": [19.2], "ticket": [92.0], "services": [22], "color": [42.0],
         "cur_pph": 112.0, "cur_rebook": 68.0, "cur_product": 19.2, "cur_ticket": 92.0},
        {"name": "Brittany K.", "loc_name": "Andover FS", "loc_id": "z001", "status": "active",
         "tenure": 1.5, "weeks": ["2026-03-14"], "pph": [88.0], "rebook": [52.0],
         "product": [14.5], "ticket": [82.0], "services": [18], "color": [38.0],
         "cur_pph": 88.0, "cur_rebook": 52.0, "cur_product": 14.5, "cur_ticket": 82.0},
        {"name": "Jess C.", "loc_name": "Prior Lake", "loc_id": "z006", "status": "active",
         "tenure": 5.1, "weeks": ["2026-03-14"], "pph": [125.0], "rebook": [74.0],
         "product": [22.1], "ticket": [98.0], "services": [24], "color": [45.0],
         "cur_pph": 125.0, "cur_rebook": 74.0, "cur_product": 22.1, "cur_ticket": 98.0},
    ]


def _mock_history() -> dict:
    """Minimal history for one location (enough for delta calculations)."""
    return {
        "Andover FS": {
            "weeks":        ["2026-03-07", "2026-03-14"],
            "pph":          [109.0, 112.0],
            "total_sales":  [17800.0, 18500.0],
            "guests":       [205, 210],
            "product_pct":  [17.2, 17.8],
            "avg_ticket":   [86.8, 88.0],
            "wax_pct":      [11.1, 11.4],
            "color_pct":    [38.5, 38.9],
            "treat_pct":    [9.4, 9.7],
        },
    }


# ─── Sandbox runner ───────────────────────────────────────────────────────────

def run_sandbox() -> int:
    """
    Run all pipeline modules against mock data.
    Returns exit code (0=pass, 1=fail).
    """
    results = {}

    print()
    print("╔══════════════════════════════════════════════════════════╗")
    print("║   KPI Platform — Sandbox Validation                      ║")
    print("║   SANDBOX_MODE=true  |  No real API calls will be made   ║")
    print("╚══════════════════════════════════════════════════════════╝")
    print()

    # ── Module 1: data_processor ──────────────────────────────────────────────
    _section("1", "data_processor — enrichment with mock data")
    try:
        from core import data_processor
        data = data_processor.process(_mock_locations(), _mock_stylists(), _mock_history())
        assert len(data["locations"]) == 12, f"Expected 12 locations, got {len(data['locations'])}"
        assert data["network"]["total_sales"] > 0
        assert data["network"]["avg_pph"] > 0
        _pass("data_processor", f"{len(data['locations'])} locations, avg PPH ${data['network']['avg_pph']:.2f}")
        results["data_processor"] = True
    except Exception as exc:
        _fail("data_processor", exc)
        results["data_processor"] = False
        data = None  # downstream modules need this

    # ── Module 2: drift_checker ────────────────────────────────────────────────
    _section("2", "drift_checker — KPI range validation")
    if data:
        try:
            from core import drift_checker
            abort, warnings = drift_checker.check_all(data)
            if abort:
                _fail("drift_checker", Exception(f"Impossible value detected: {warnings}"))
                results["drift_checker"] = False
            else:
                _pass("drift_checker", f"{len(warnings)} range warning(s) — no hard stops")
                results["drift_checker"] = True
        except Exception as exc:
            _fail("drift_checker", exc)
            results["drift_checker"] = False
    else:
        _skip("drift_checker", "data_processor failed")
        results["drift_checker"] = None

    # ── Module 3: ai_cards (DRY_RUN=true — no Claude API call) ────────────────
    _section("3", "ai_cards — DRY RUN placeholder generation")
    if data:
        try:
            from core import ai_cards as ai_module
            cards = ai_module.generate_all(data, dry_run=True)
            assert "location_cards" in cards
            assert len(cards["location_cards"]) == 12
            assert "coach_briefing" in cards
            _pass("ai_cards", f"{len(cards['location_cards'])} location cards, {len(cards['stylist_cards'])} stylist cards")
            results["ai_cards"] = True
        except Exception as exc:
            _fail("ai_cards", exc)
            results["ai_cards"] = False
            cards = {"location_cards": {}, "stylist_cards": {}, "coach_briefing": ""}
    else:
        _skip("ai_cards", "data_processor failed")
        results["ai_cards"] = None
        cards = {"location_cards": {}, "stylist_cards": {}, "coach_briefing": ""}

    # ── Module 4: sheets_writer (DRY_RUN — no actual Sheets write) ────────────
    _section("4", "sheets_writer — DRY RUN (mock Sheets write)")
    if data:
        try:
            from core import sheets_writer
            # Mock config with fake sheet_id (no real write happens in dry_run=True)
            mock_config = {"sheet_id": "SANDBOX_MOCK_SHEET_ID", "locations": []}
            sheets_writer.write_all(mock_config, data, cards, dry_run=True)
            _pass("sheets_writer", "DRY RUN — row counts logged, no write issued")
            results["sheets_writer"] = True
        except Exception as exc:
            _fail("sheets_writer", exc)
            results["sheets_writer"] = False
    else:
        _skip("sheets_writer", "data_processor failed")
        results["sheets_writer"] = None

    # ── Module 5: report_builder (DRY_RUN — no file written) ──────────────────
    _section("5", "report_builder — DRY RUN (no Excel file written)")
    if data:
        try:
            from core import report_builder
            import tempfile
            tmp_dir = Path(tempfile.mkdtemp())
            report_path = report_builder.build_report(data, cards, tmp_dir, dry_run=True)
            _pass("report_builder", "DRY RUN — skipped as expected")
            results["report_builder"] = True
        except Exception as exc:
            _fail("report_builder", exc)
            results["report_builder"] = False
    else:
        _skip("report_builder", "data_processor failed")
        results["report_builder"] = None

    # ── Module 6: email_sender (DRY_RUN — no email sent) ──────────────────────
    _section("6", "email_sender — DRY RUN (no SMTP connection)")
    if data:
        try:
            from core import email_sender
            mock_config = {"email_recipients": ["sandbox@example.com"], "locations": []}
            email_sender.send_report(mock_config, data, cards, None, dry_run=True)
            _pass("email_sender", "DRY RUN — email body printed, no SMTP connection")
            results["email_sender"] = True
        except Exception as exc:
            _fail("email_sender", exc)
            results["email_sender"] = False
    else:
        _skip("email_sender", "data_processor failed")
        results["email_sender"] = None

    # ── Module 7: dashboard_builder ────────────────────────────────────────────
    _section("7", "dashboard_builder — HTML generation to temp dir")
    if data:
        try:
            from core import dashboard_builder
            import tempfile
            tmp_docs = Path(tempfile.mkdtemp())
            mock_config = {
                "locations": [{"id": l["loc_id"], "name": l["loc_name"], "platform": l["platform"]} for l in data["locations"]],
                "managers": [
                    {"name": "Jess", "filename": "jess.html", "pin": "XXXX", "location_ids": ["z006", "z010", "su001", "su002"]},
                    {"name": "Jenn", "filename": "jenn.html", "pin": "XXXX", "location_ids": ["z001", "z002", "z003", "z004", "z009"]},
                ]
            }
            results_map = dashboard_builder.rebuild_all(mock_config, data, tmp_docs, dry_run=True)
            _pass("dashboard_builder", f"DRY RUN — {len(results_map)} file(s) processed")
            results["dashboard_builder"] = True
        except Exception as exc:
            _fail("dashboard_builder", exc)
            results["dashboard_builder"] = False
    else:
        _skip("dashboard_builder", "data_processor failed")
        results["dashboard_builder"] = None

    # ── Module 8: alerter test ─────────────────────────────────────────────────
    _section("8", "alerter — deliberate test alert (Section 12 validation)")
    try:
        from core import alerter
        alerter.test_alert()
        _pass("alerter", "Test alert fired — check Job Summary for confirmation")
        results["alerter"] = True
    except Exception as exc:
        _fail("alerter", exc)
        results["alerter"] = False

    # ── Summary ────────────────────────────────────────────────────────────────
    print()
    print("╔══════════════════════════════════════════════════════════╗")
    print("║   Sandbox Validation Summary                              ║")
    print("╠══════════════════════════════════════════════════════════╣")
    passed   = sum(1 for v in results.values() if v is True)
    failed   = sum(1 for v in results.values() if v is False)
    skipped  = sum(1 for v in results.values() if v is None)

    for module, status in results.items():
        icon = "✓" if status is True else ("✗" if status is False else "⏭")
        print(f"║  {icon}  {module:<40} {'PASS' if status is True else ('FAIL' if status is False else 'SKIP'):<8}║")

    print("╠══════════════════════════════════════════════════════════╣")
    print(f"║  Passed: {passed}  |  Failed: {failed}  |  Skipped: {skipped:<31}║")
    print("╚══════════════════════════════════════════════════════════╝")
    print()

    if failed > 0:
        log.error("Sandbox validation FAILED — %d module(s) did not pass", failed)
        return 1

    log.info("Sandbox validation PASSED — all modules completed successfully")
    return 0


def _section(num: str, label: str):
    log.info("─" * 55)
    log.info("Module %s: %s", num, label)


def _pass(label: str, detail: str = ""):
    log.info("  ✓ %s%s", label, f" — {detail}" if detail else "")


def _fail(label: str, exc: Exception):
    log.error("  ✗ %s FAILED: %s", label, exc)
    log.debug(traceback.format_exc())


def _skip(label: str, reason: str = ""):
    log.info("  ⏭  %s skipped — %s", label, reason)


if __name__ == "__main__":
    sys.exit(run_sandbox())
