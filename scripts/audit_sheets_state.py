#!/usr/bin/env python3
"""
scripts/audit_sheets_state.py
─────────────────────────────
Read-only audit of what is actually sitting in the V1 production sheet
and (optionally) the V2 parallel sheet. Prints:

  • every tab the API can see (including ones we forgot exist)
  • row count on each tab
  • the header row
  • the first 3 and last 2 data rows so you can eyeball what's real vs junk

NO writes. Safe to run anywhere, any time. Use this BEFORE wipe_all_data.py
so you know exactly what you're about to nuke.

Auth
────
Reuses the same GOOGLE_SERVICE_ACCOUNT_JSON base64 secret the pipeline uses.

Usage
─────
    # V1 only (default — reads sheet_id from karissa_001.json)
    python scripts/audit_sheets_state.py

    # V1 + V2 (requires V2_MASTER_SHEET_ID env var)
    python scripts/audit_sheets_state.py --include-v2

    # Specific customer
    ACTIVE_CUSTOMER_ID=karissa_001 python scripts/audit_sheets_state.py

    # In CI via workflow_dispatch — no flags needed, env vars handle it
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT))

from core.sheets_writer import _build_service  # noqa: E402

_CUSTOMER_CONFIG_DIR = _REPO_ROOT / "config" / "customers"


def _load_customer_config(customer_id: str) -> dict:
    path = _CUSTOMER_CONFIG_DIR / f"{customer_id}.json"
    if not path.exists():
        raise SystemExit(f"Customer config not found: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _list_tabs(service, sheet_id: str) -> list[str]:
    meta = service.spreadsheets().get(spreadsheetId=sheet_id).execute()
    return [s["properties"]["title"] for s in meta.get("sheets", [])]


def _read_tab(service, sheet_id: str, tab: str) -> list[list]:
    """Read all values from a tab. Returns empty list on missing/empty."""
    try:
        resp = service.spreadsheets().values().get(
            spreadsheetId=sheet_id,
            range=f"{tab}!A1:ZZ",
        ).execute()
        return resp.get("values", []) or []
    except Exception as exc:  # noqa: BLE001
        return [["<error reading tab:", str(exc), ">"]]


def _fmt_row(row: list, max_cols: int = 12, max_cell: int = 30) -> str:
    cells = row[:max_cols]
    out = []
    for c in cells:
        s = str(c)
        if len(s) > max_cell:
            s = s[: max_cell - 1] + "…"
        out.append(s)
    tail = f"  (+{len(row) - max_cols} more cols)" if len(row) > max_cols else ""
    return "| " + " | ".join(out) + " |" + tail


def _audit_sheet(label: str, sheet_id: str) -> None:
    print()
    print("=" * 78)
    print(f"  {label}")
    print(f"  sheet_id: {sheet_id}")
    print("=" * 78)

    service = _build_service({})  # only needs env var, not config
    try:
        tabs = _list_tabs(service, sheet_id)
    except Exception as exc:  # noqa: BLE001
        print(f"  ✗ could not list tabs: {exc}")
        return

    print(f"  tabs found: {len(tabs)} → {tabs}")
    print()

    for tab in tabs:
        rows = _read_tab(service, sheet_id, tab)
        total = len(rows)
        data_rows = max(0, total - 1)  # assume row 0 is header

        print("-" * 78)
        print(f"  TAB: {tab}")
        print(f"    total rows (incl. header): {total}")
        print(f"    data rows                 : {data_rows}")

        if total == 0:
            print("    (empty)")
            continue

        print(f"    header:   {_fmt_row(rows[0])}")

        if data_rows == 0:
            print("    (no data rows)")
            continue

        # First 3 and last 2
        show_first = rows[1 : min(4, total)]
        for i, r in enumerate(show_first, start=1):
            print(f"    row {i:<5}: {_fmt_row(r)}")

        if total > 6:
            print("    ...")
            show_last = rows[max(4, total - 2) : total]
            for r in show_last:
                idx = rows.index(r)  # ok for a short tail preview
                print(f"    row {idx:<5}: {_fmt_row(r)}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Read-only audit of V1 and V2 sheet contents.",
    )
    parser.add_argument(
        "--customer",
        default=os.environ.get("ACTIVE_CUSTOMER_ID", "karissa_001"),
    )
    parser.add_argument(
        "--include-v2",
        action="store_true",
        help="Also audit V2 sheet (requires V2_MASTER_SHEET_ID env var)",
    )
    args = parser.parse_args()

    # Sanity check
    if not os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON"):
        raise SystemExit(
            "GOOGLE_SERVICE_ACCOUNT_JSON env var is not set. "
            "Run this via GitHub Actions (secrets auto-loaded) or locally "
            "after exporting the base64-encoded service-account JSON."
        )

    cfg = _load_customer_config(args.customer)
    v1_id = cfg.get("sheet_id")
    if not v1_id:
        raise SystemExit(f"{args.customer}.json missing sheet_id")

    _audit_sheet("V1 (production — pipeline reads + writes here)", v1_id)

    if args.include_v2:
        v2_id = os.environ.get("V2_MASTER_SHEET_ID", "").strip()
        if not v2_id:
            print()
            print("⚠️  --include-v2 passed but V2_MASTER_SHEET_ID env var is empty.")
            print("   Skipping V2 audit.")
        else:
            _audit_sheet("V2 (shadow — only written when DUAL_WRITE_V2=true)", v2_id)

    print()
    print("Audit complete. No writes performed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
