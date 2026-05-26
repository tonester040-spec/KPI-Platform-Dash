"""
scripts/backfill/run_batch.py
─────────────────────────────
CLI orchestrator for one backfill month at a time.

Usage:
  # Dry-run March (loads tracker + xlsx, prints diff, no Sheets write)
  python -m scripts.backfill.run_batch --month 2026-03 --dry-run

  # Dry-run April (loads 12 PDFs, prints validation)
  python -m scripts.backfill.run_batch --month 2026-04 --dry-run

  # Actually write March to DATA_MONTHLY (only after dry-run approval)
  python -m scripts.backfill.run_batch --month 2026-03 --write

  # Write April, explicitly accepting the known Prior Lake $34 anomaly
  python -m scripts.backfill.run_batch --month 2026-04 --write \\
      --accept "Prior Lake:STYLIST_SUM_MISMATCH"

Months supported:
  2026-03 -> tracker + xlsx cross-validation
  2026-04 -> monthly PDFs (with Prior Lake known anomaly)
  2026-05 -> monthly PDFs (partial 5/1-5/24)

The orchestrator REFUSES to write whenever any 'error'-severity issue is
not on the --accept list. All issues must be explicitly acknowledged.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from scripts.backfill.monthly_pdf_loader import load_monthly_pdfs
from scripts.backfill.render_review import (
    render_cross_check,
    render_kpi_table,
    render_validation_summary,
)
from scripts.backfill.tracker_loader import load_march_monthly_from_tracker
from scripts.backfill.zenoti_xlsx_loader import (
    diff_against_tracker,
    load_zenoti_march_stylists,
    load_zenoti_march_xlsxes,
)

log = logging.getLogger(__name__)


# Default paths — override with --tracker / --pdf-dir / --xlsx-dir if needed
DEFAULT_BASE = Path(r"C:\Users\TonyGrant\Downloads\KPI Historical Data")
DEFAULT_TRACKER = DEFAULT_BASE / "March 2026.xlsx"
DEFAULT_DIRS = {
    "2026-03": DEFAULT_BASE / "3-1-26 - 3-31-26",
    "2026-04": DEFAULT_BASE / "4-1-26 - 4-30-26",
    "2026-05": DEFAULT_BASE / "5-1-24 - 5-24-26",
}
PERIOD_RANGES = {
    "2026-03": ("2026-03-01", "2026-03-31"),
    "2026-04": ("2026-04-01", "2026-04-30"),
    "2026-05": ("2026-05-01", "2026-05-24"),  # known partial
}


def _load_config(config_path: Path) -> dict:
    return json.loads(config_path.read_text())


def _filter_accepted(issues: list[dict], accepted: set[str]) -> tuple[list[dict], list[dict]]:
    """Split issues into (still-blocking, accepted-and-suppressed)."""
    blocking: list[dict] = []
    suppressed: list[dict] = []
    for i in issues:
        if i["severity"] != "error":
            blocking.append(i)
            continue
        key = f"{i['location']}:{i['code']}"
        if key in accepted:
            suppressed.append(i)
        else:
            blocking.append(i)
    return blocking, suppressed


def run_march(args, config: dict) -> tuple[list[dict], list[dict], list[dict]]:
    """March path: tracker (location-level) + Zenoti xlsxes (stylist-level + cross-check).

    Returns (location_rows, stylist_rows, issues).
    """
    tracker_path = Path(args.tracker) if args.tracker else DEFAULT_TRACKER
    xlsx_dir = Path(args.xlsx_dir) if args.xlsx_dir else DEFAULT_DIRS["2026-03"]
    period_start, period_end = PERIOD_RANGES["2026-03"]

    print(f"Loading tracker:  {tracker_path}")
    location_rows = load_march_monthly_from_tracker(
        tracker_path, config,
        year_month="2026-03", period_start=period_start, period_end=period_end,
    )
    print(f"  -> {len(location_rows)} location rows from tracker Week 5 tab\n")

    print(f"Loading Zenoti xlsx Totals:  {xlsx_dir}")
    xlsx_totals = load_zenoti_march_xlsxes(xlsx_dir, config)
    print(f"  -> {len(xlsx_totals)} Zenoti xlsx Total rows for cross-check\n")

    print(f"Loading Zenoti xlsx active stylists:  {xlsx_dir}")
    stylist_rows = load_zenoti_march_stylists(
        xlsx_dir, config,
        year_month="2026-03", period_start=period_start, period_end=period_end,
    )
    print(f"  -> {len(stylist_rows)} active stylist rows (skipping inactive zero-rows)\n")

    print(render_kpi_table(location_rows, title="March 2026 — DATA_MONTHLY rows (from tracker Week 5)"))

    findings = diff_against_tracker(location_rows, xlsx_totals)
    print(render_cross_check(findings, title="Cross-check: tracker Week 5 vs Zenoti xlsx Total rows"))

    return location_rows, stylist_rows, findings


def run_pdf_month(args, config: dict, *, year_month: str) -> tuple[list[dict], list[dict], list[dict]]:
    """April/May path: load monthly PDFs, emit location + stylist rows + issues."""
    pdf_dir = Path(args.pdf_dir) if args.pdf_dir else DEFAULT_DIRS[year_month]
    period_start, period_end = PERIOD_RANGES[year_month]

    print(f"Loading monthly PDFs:  {pdf_dir}")
    rows, stylist_rows, issues = load_monthly_pdfs(
        pdf_dir, config,
        year_month=year_month, period_start=period_start, period_end=period_end,
    )
    print(
        f"  -> {len(rows)} location rows, {len(stylist_rows)} stylist rows, "
        f"{len(issues)} validation issues\n"
    )

    print(render_kpi_table(
        rows,
        title=f"{year_month} — DATA_MONTHLY rows (from monthly POS PDFs, period {period_start} -> {period_end})",
    ))

    print(render_validation_summary(issues, title=f"{year_month} validation summary"))

    return rows, stylist_rows, issues


def write_to_sheets(
    location_rows: list[dict],
    stylist_rows: list[dict],
    dry_run: bool,
) -> None:
    """Append rows to DATA_MONTHLY + STYLISTS_DATA_MONTHLY (dry_run gates real writes)."""
    from core.sheets_writer import (
        _build_service,
        append_to_data_monthly,
        append_to_stylists_data_monthly,
    )

    config_path = _REPO_ROOT / "config" / "customers" / "karissa_001.json"
    cfg = _load_config(config_path)

    if dry_run:
        print(">>> DRY RUN — no Sheets write. Would have appended:")
        print(f"    {len(location_rows)} rows to DATA_MONTHLY")
        print(f"    {len(stylist_rows)} rows to STYLISTS_DATA_MONTHLY")
        print(f"    (sheet {cfg['sheet_id']})\n")
        append_to_data_monthly(service=None, config=cfg, rows=location_rows, dry_run=True)
        append_to_stylists_data_monthly(service=None, config=cfg, rows=stylist_rows, dry_run=True)
        return

    print(f">>> WRITING {len(location_rows)} location rows + {len(stylist_rows)} stylist rows "
          f"(sheet {cfg['sheet_id']})")
    service = _build_service(cfg)
    append_to_data_monthly(service=service, config=cfg, rows=location_rows, dry_run=False)
    append_to_stylists_data_monthly(service=service, config=cfg, rows=stylist_rows, dry_run=False)
    print("    Done.\n")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Historical backfill orchestrator")
    parser.add_argument("--month", required=True, choices=["2026-03", "2026-04", "2026-05"],
                        help="Year-month to process")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--dry-run", action="store_true",
                       help="Parse + validate but never write to Sheets")
    group.add_argument("--write", action="store_true",
                       help="Actually write to DATA_MONTHLY (after dry-run approval)")
    parser.add_argument("--accept", action="append", default=[],
                        help="Accept a known error in 'LOC:CODE' form (e.g. 'Prior Lake:STYLIST_SUM_MISMATCH')")
    parser.add_argument("--tracker", default=None, help="Override default tracker path (March only)")
    parser.add_argument("--xlsx-dir", default=None, help="Override default xlsx dir (March only)")
    parser.add_argument("--pdf-dir", default=None, help="Override default PDF dir (April/May)")
    parser.add_argument("--config", default=None, help="Override default customer config path")
    parser.add_argument("--verbose", action="store_true", help="Verbose logging")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    config_path = Path(args.config) if args.config else _REPO_ROOT / "config" / "customers" / "karissa_001.json"
    config = _load_config(config_path)

    print(f"\n{'=' * 70}")
    print(f"  KPI BACKFILL — {args.month}  ({'DRY RUN' if args.dry_run else 'LIVE WRITE'})")
    print(f"{'=' * 70}\n")

    if args.month == "2026-03":
        location_rows, stylist_rows, issues = run_march(args, config)
    else:
        location_rows, stylist_rows, issues = run_pdf_month(args, config, year_month=args.month)

    print(f"\nStylist row count: {len(stylist_rows)} (target: STYLISTS_DATA_MONTHLY)\n")

    blocking, suppressed = _filter_accepted(issues, set(args.accept))

    if suppressed:
        print(">>> Accepted (suppressed) error(s):")
        for i in suppressed:
            print(f"    [{i['code']}] {i['location']}: {i['message']}")
        print()

    if blocking:
        unaccepted_errors = [i for i in blocking if i["severity"] == "error"]
        if unaccepted_errors and args.write:
            print(">>> REFUSING TO WRITE — unaccepted error(s) present:")
            for i in unaccepted_errors:
                print(f"    [{i['code']}] {i['location']}: {i['message']}")
            print(f"\n    To proceed anyway, re-run with: --accept '{unaccepted_errors[0]['location']}:{unaccepted_errors[0]['code']}'")
            return 1

    if args.dry_run:
        write_to_sheets(location_rows, stylist_rows, dry_run=True)
        print(">>> Dry run complete. To write to Sheets, re-run with --write.\n")
        return 0

    write_to_sheets(location_rows, stylist_rows, dry_run=False)
    print(">>> Backfill complete.\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
