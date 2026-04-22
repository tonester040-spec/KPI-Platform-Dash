"""
scripts/clear_stale_stylists_data.py
────────────────────────────────────
One-shot ops script — clear junk / stale rows from the STYLISTS_DATA tab
before tier2_pdf_batch.py starts populating it for real on Monday
2026-04-27.

Why this script exists
──────────────────────
Until the tier2 stylist write path landed (PR after 2026-04-21), the
pipeline never had a clean path from POS exports → per-stylist data.
Anything currently in STYLISTS_DATA is one of:

  • Old dummy rows from load_stylist_dummy_data.py (pre-launch seed)
  • Junk rows populated from malformed PDFs during dev iteration
  • Location-shaped rows accidentally written as stylists (the original
    bug that kicked off this whole fix)

None of it is trustworthy. The easiest, safest path forward is to wipe
the tab before the first real Monday run.

What this script does
─────────────────────
  1. Reads the current contents of STYLISTS_DATA (audit trail in the log)
  2. Writes the header row back, leaves A2:L empty
  3. Leaves STYLISTS_CURRENT alone — main.py will rebuild that Monday
  4. Leaves DATA alone — location history is sound

Safety
──────
  • Dry-run by default. Pass --apply to actually clear the tab.
  • Prints the first 5 existing rows before the clear so you can eyeball
    that you're about to nuke the right thing.
  • Requires ACTIVE_CUSTOMER_ID env or --customer arg.
  • Requires GOOGLE_SERVICE_ACCOUNT_JSON env (same as the pipeline).

Usage
─────
    # Dry-run — shows what would be cleared, writes nothing
    python scripts/clear_stale_stylists_data.py

    # Actually clear (irreversible — existing rows GONE)
    python scripts/clear_stale_stylists_data.py --apply

    # Against a non-default customer
    ACTIVE_CUSTOMER_ID=some_customer python scripts/clear_stale_stylists_data.py --apply

Run this ONCE before the first Monday where tier2_pdf_batch.py writes
real stylist data. After that, the pipeline is idempotent and this
script should never be needed again.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT))

from core.sheets_writer import _build_service

_CUSTOMER_CONFIG_DIR = _REPO_ROOT / "config" / "customers"

# STYLISTS_DATA header (must match setup_stylist_tabs.py and SCOL in
# core/data_source.py). Writing this back guarantees downstream reads
# still see the header.
_STYLISTS_DATA_HEADER = [
    "week_ending",
    "stylist_name",
    "loc_name",
    "loc_id",
    "status",
    "tenure_yrs",
    "pph",
    "rebook_pct",
    "product_pct",
    "avg_ticket",
    "services",
    "color_pct",
]


def _load_customer_config(customer_id: str) -> dict:
    path = _CUSTOMER_CONFIG_DIR / f"{customer_id}.json"
    if not path.exists():
        raise SystemExit(f"Customer config not found: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Clear stale rows from STYLISTS_DATA tab (one-shot ops).",
    )
    parser.add_argument(
        "--customer",
        default=os.environ.get("ACTIVE_CUSTOMER_ID", "karissa_001"),
        help="Customer config ID (default: karissa_001 or $ACTIVE_CUSTOMER_ID)",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually perform the clear. Without this flag, runs dry.",
    )
    args = parser.parse_args()

    cfg = _load_customer_config(args.customer)
    sheet_id = cfg.get("sheet_id")
    if not sheet_id:
        raise SystemExit("Customer config missing sheet_id")

    service = _build_service(cfg)

    # ----- Audit read
    existing = service.spreadsheets().values().get(
        spreadsheetId=sheet_id,
        range="STYLISTS_DATA!A1:L",
    ).execute()
    rows = existing.get("values", []) or []

    print(f"Customer         : {args.customer}")
    print(f"Sheet ID         : {sheet_id}")
    print(f"STYLISTS_DATA    : {len(rows)} row(s) (including header)")
    if rows:
        print("First rows (up to 5):")
        for i, r in enumerate(rows[:5]):
            print(f"  [{i}] {r}")
    print()

    data_row_count = max(0, len(rows) - 1)
    if data_row_count == 0:
        print("No data rows to clear. Exiting.")
        return 0

    if not args.apply:
        print(
            f"DRY RUN — would clear {data_row_count} data row(s) from "
            f"STYLISTS_DATA!A2:L and leave the header intact."
        )
        print("Re-run with --apply to actually clear.")
        return 0

    # ----- Confirm header is intact; write it back if missing/mismatched
    header_needs_write = (not rows) or (rows[0] != _STYLISTS_DATA_HEADER)

    # ----- Clear data rows (A2:L)
    service.spreadsheets().values().clear(
        spreadsheetId=sheet_id,
        range="STYLISTS_DATA!A2:L",
    ).execute()
    print(f"CLEARED: STYLISTS_DATA!A2:L ({data_row_count} row(s) removed)")

    if header_needs_write:
        service.spreadsheets().values().update(
            spreadsheetId=sheet_id,
            range="STYLISTS_DATA!A1:L1",
            valueInputOption="USER_ENTERED",
            body={"values": [_STYLISTS_DATA_HEADER]},
        ).execute()
        print("Header row rewritten to STYLISTS_DATA!A1:L1")

    print()
    print("Done. Next Monday's pipeline will populate STYLISTS_DATA fresh.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
