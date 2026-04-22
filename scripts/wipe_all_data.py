#!/usr/bin/env python3
"""
scripts/wipe_all_data.py
────────────────────────
Nuclear option — wipe every pipeline-managed tab on V1 (and optionally V2)
so the next Monday pipeline run writes fully fresh data from the real PDFs.

WHAT GETS WIPED
───────────────
  V1 sheet (production)            data rows cleared, header kept
    • CURRENT                      (rebuilt by tier2_pdf_batch.py Monday)
    • DATA                         (append ledger — loses history, see note)
    • STYLISTS_CURRENT             (rebuilt by main.py Monday)
    • STYLISTS_DATA                (append ledger — loses history, see note)
    • ALERTS                       (rebuilt by main.py Monday)
    • WEEKLY_DATA                  (if present — auxiliary, safe)

WHAT IS PRESERVED (never touched)
─────────────────────────────────
    • GOALS            — Karissa's manually-entered annual targets +
                         daily_goal_divisor + day_goal_divisor. Loss = bad.
    • WEEK_ENDING      — Manual reference tab, not auto-regenerated.
    • any tab not in the wipe list above

HISTORICAL DATA NOTE
────────────────────
Wiping DATA + STYLISTS_DATA means week-over-week deltas on the dashboard
will show blank for the week AFTER the wipe — there's no prior week to
diff against. After 2 Monday runs, history rebuilds naturally. If that
matters, skip the historical tabs with --skip-history.

SAFETY RAILS
────────────
    1. DRY RUN by default. Prints what WOULD be cleared, writes nothing.
    2. Requires --apply to actually delete.
    3. Requires --confirm-wipe token to match "WIPE-{customer}" so you
       can't fat-finger it. Makes the prod-command look like:
           python scripts/wipe_all_data.py --apply \\
               --confirm-wipe WIPE-karissa_001
    4. V2 is opt-in via --include-v2 (separate gate).
    5. Writes an audit log to data/logs/wipe_{timestamp}.log.

Auth
────
GOOGLE_SERVICE_ACCOUNT_JSON base64 env var, same as the pipeline.

Usage
─────
    # Dry-run — recommended first. Shows every tab and row count.
    python scripts/wipe_all_data.py

    # Actually wipe V1 only
    python scripts/wipe_all_data.py --apply --confirm-wipe WIPE-karissa_001

    # Wipe V1 + V2
    python scripts/wipe_all_data.py --apply --include-v2 \\
        --confirm-wipe WIPE-karissa_001

    # Wipe V1 but keep DATA + STYLISTS_DATA (history intact)
    python scripts/wipe_all_data.py --apply --skip-history \\
        --confirm-wipe WIPE-karissa_001
"""
from __future__ import annotations

import argparse
import datetime as _dt
import json
import os
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT))

from core.sheets_writer import _build_service  # noqa: E402

_CUSTOMER_CONFIG_DIR = _REPO_ROOT / "config" / "customers"
_LOG_DIR = _REPO_ROOT / "data" / "logs"

# ── Tab wipe contract ─────────────────────────────────────────────────────────
# Each entry: (tab_name, clear_range, purpose_tag)
#   clear_range is "A2:Z" style so the header row stays put.

_V1_WIPE_TABS = [
    ("CURRENT",          "A2:T",  "current-week snapshot (rebuilt Monday)"),
    ("STYLISTS_CURRENT", "A2:L",  "current-week stylist snapshot"),
    ("ALERTS",           "A2:F",  "alerts + coach briefs"),
    ("WEEKLY_DATA",      "A2:ZZ", "auxiliary weekly aggregates (if present)"),
]

_V1_HISTORY_TABS = [
    ("DATA",             "A2:T",  "location history (append ledger)"),
    ("STYLISTS_DATA",    "A2:L",  "stylist history (append ledger)"),
]

_V1_PROTECTED = {"GOALS", "WEEK_ENDING"}

# V2 tabs — schema is the v2 store's view. Same safety: preserve GOALS.
# Names pulled from core/google_sheets_store.py. If a tab doesn't exist,
# the clear call 404s cleanly — we catch and skip.
_V2_WIPE_TABS = [
    ("LOCATIONS_CURRENT", "A2:ZZ", "v2 current locations"),
    ("STYLISTS_CURRENT",  "A2:ZZ", "v2 current stylists"),
    ("ALERTS",            "A2:ZZ", "v2 alerts"),
    ("COACH_BRIEFS",      "A2:ZZ", "v2 coach briefs"),
    ("AUDIT_LOG",         "A3:ZZ", "v2 audit log (keep schema-version note)"),
]
_V2_HISTORY_TABS = [
    ("LOCATIONS_DATA",    "A2:ZZ", "v2 location history"),
    ("STYLISTS_DATA",     "A2:ZZ", "v2 stylist history"),
]
_V2_PROTECTED = {"GOALS", "WEEK_ENDING"}


def _load_customer_config(customer_id: str) -> dict:
    path = _CUSTOMER_CONFIG_DIR / f"{customer_id}.json"
    if not path.exists():
        raise SystemExit(f"Customer config not found: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _count_rows(service, sheet_id: str, tab: str, data_range: str) -> int:
    """Return the number of data rows that WOULD be cleared. -1 on miss."""
    try:
        resp = service.spreadsheets().values().get(
            spreadsheetId=sheet_id,
            range=f"{tab}!{data_range}",
        ).execute()
        return len(resp.get("values", []) or [])
    except Exception:  # noqa: BLE001
        return -1


def _clear_range(service, sheet_id: str, tab: str, data_range: str) -> bool:
    try:
        service.spreadsheets().values().clear(
            spreadsheetId=sheet_id,
            range=f"{tab}!{data_range}",
        ).execute()
        return True
    except Exception as exc:  # noqa: BLE001
        print(f"    ⚠️  clear failed for {tab}!{data_range}: {exc}")
        return False


def _wipe_sheet(
    label: str,
    sheet_id: str,
    wipe_tabs: list,
    history_tabs: list,
    protected: set,
    apply: bool,
    skip_history: bool,
    audit_lines: list,
) -> None:
    print()
    print("=" * 78)
    print(f"  {label}")
    print(f"  sheet_id: {sheet_id}")
    print(f"  mode    : {'APPLY (will delete)' if apply else 'DRY RUN (read-only)'}")
    print(f"  history : {'SKIPPED (DATA/STYLISTS_DATA preserved)' if skip_history else 'WIPED'}")
    print(f"  protected tabs (never touched): {sorted(protected)}")
    print("=" * 78)

    audit_lines.append(f"\n=== {label} (sheet_id={sheet_id}) ===")
    audit_lines.append(f"mode={'apply' if apply else 'dry_run'} "
                       f"skip_history={skip_history}")

    service = _build_service({})

    all_tabs = list(wipe_tabs) + ([] if skip_history else list(history_tabs))

    for tab, data_range, purpose in all_tabs:
        if tab in protected:
            print(f"  ⏭  {tab}: PROTECTED — skipping")
            audit_lines.append(f"  SKIP   {tab} (protected)")
            continue

        row_count = _count_rows(service, sheet_id, tab, data_range)
        if row_count < 0:
            print(f"  ⏭  {tab}: tab not present — skipping ({purpose})")
            audit_lines.append(f"  MISS   {tab} (not present)")
            continue

        if row_count == 0:
            print(f"  ✓  {tab}: already empty ({purpose})")
            audit_lines.append(f"  EMPTY  {tab}")
            continue

        if apply:
            ok = _clear_range(service, sheet_id, tab, data_range)
            mark = "✓" if ok else "✗"
            status = "CLEARED" if ok else "FAILED"
            print(f"  {mark}  {tab}: {status} {row_count} row(s) — {purpose}")
            audit_lines.append(f"  {status:<7} {tab} rows={row_count}")
        else:
            print(f"  →  {tab}: WOULD CLEAR {row_count} row(s) — {purpose}")
            audit_lines.append(f"  PLAN   {tab} rows={row_count}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Wipe pipeline-managed tabs on V1 (and optionally V2).",
    )
    parser.add_argument(
        "--customer",
        default=os.environ.get("ACTIVE_CUSTOMER_ID", "karissa_001"),
    )
    parser.add_argument("--apply", action="store_true", help="Actually clear. Default is dry-run.")
    parser.add_argument("--include-v2", action="store_true", help="Also wipe V2 sheet.")
    parser.add_argument("--skip-history", action="store_true",
                        help="Preserve DATA + STYLISTS_DATA (history intact).")
    parser.add_argument("--confirm-wipe", default="",
                        help='Required for --apply. Must equal "WIPE-{customer}".')
    args = parser.parse_args()

    if not os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON"):
        raise SystemExit(
            "GOOGLE_SERVICE_ACCOUNT_JSON env var is not set. "
            "Run this via GitHub Actions (secrets auto-loaded) or locally."
        )

    expected_token = f"WIPE-{args.customer}"
    if args.apply and args.confirm_wipe != expected_token:
        raise SystemExit(
            f"--apply requires --confirm-wipe {expected_token!r} (got "
            f"{args.confirm_wipe!r}). Refusing to delete without confirmation."
        )

    cfg = _load_customer_config(args.customer)
    v1_id = cfg.get("sheet_id")
    if not v1_id:
        raise SystemExit(f"{args.customer}.json missing sheet_id")

    audit_lines = [
        f"wipe_all_data.py run at {_dt.datetime.utcnow().isoformat()}Z",
        f"customer={args.customer} apply={args.apply} "
        f"skip_history={args.skip_history} include_v2={args.include_v2}",
    ]

    _wipe_sheet(
        label="V1 (production)",
        sheet_id=v1_id,
        wipe_tabs=_V1_WIPE_TABS,
        history_tabs=_V1_HISTORY_TABS,
        protected=_V1_PROTECTED,
        apply=args.apply,
        skip_history=args.skip_history,
        audit_lines=audit_lines,
    )

    if args.include_v2:
        v2_id = os.environ.get("V2_MASTER_SHEET_ID", "").strip()
        if not v2_id:
            print()
            print("⚠️  --include-v2 passed but V2_MASTER_SHEET_ID env var is empty.")
            print("   Skipping V2 wipe.")
            audit_lines.append("V2 skipped (no V2_MASTER_SHEET_ID)")
        else:
            _wipe_sheet(
                label="V2 (shadow)",
                sheet_id=v2_id,
                wipe_tabs=_V2_WIPE_TABS,
                history_tabs=_V2_HISTORY_TABS,
                protected=_V2_PROTECTED,
                apply=args.apply,
                skip_history=args.skip_history,
                audit_lines=audit_lines,
            )

    # Write audit log
    _LOG_DIR.mkdir(parents=True, exist_ok=True)
    ts = _dt.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    log_path = _LOG_DIR / f"wipe_{ts}.log"
    log_path.write_text("\n".join(audit_lines) + "\n", encoding="utf-8")
    print()
    print(f"Audit log: {log_path}")

    if not args.apply:
        print()
        print("DRY RUN — nothing was written.")
        print(f"To execute: rerun with --apply --confirm-wipe {expected_token}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
