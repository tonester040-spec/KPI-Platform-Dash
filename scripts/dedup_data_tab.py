#!/usr/bin/env python3
"""
scripts/dedup_data_tab.py
─────────────────────────
One-off cleanup: DATA tab and STYLISTS_DATA tab got polluted with
duplicate rows during manual workflow_dispatch runs (the idempotency
check was silently broken — date-format comparison mismatch between
ISO "YYYY-MM-DD" and Sheets-formatted "5/24/2026"). Fixed in
sheets_writer.py append_to_historical / append_to_stylists_historical
on 2026-05-27. This script cleans up the rows that existed before
the fix.

Strategy
────────
Read all rows from each tab.
Dedupe by:
  • DATA            → (loc_name, week_ending)  — keep first occurrence
  • STYLISTS_DATA   → (week_ending, stylist_name, loc_name)
Write the deduped rows back (clear-then-write).

Safety
──────
1. Backs up both tabs to local JSON before any write.
2. Confirms row counts shrink (sanity check) before clearing.
3. Dry-run by default — pass --write to actually modify the Sheet.

Usage
─────
    # Inspect what would change (default — no writes)
    python scripts/dedup_data_tab.py

    # Actually dedupe
    python scripts/dedup_data_tab.py --write
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT))

from core.sheets_writer import _build_service
from core.data_source import _to_date_str

# Build the auth env. Local .env may set GOOGLE_SERVICE_ACCOUNT_JSON to a
# file path (e.g. "service_account.json") rather than the base64 string the
# pipeline expects. Normalize: if it doesn't look like base64-encoded JSON,
# treat it as a path and encode the file's contents.
_sa_env = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")
_looks_like_path = _sa_env.endswith(".json") or "/" in _sa_env or "\\" in _sa_env
if (not _sa_env) or _looks_like_path:
    import base64
    sa_path = Path(_sa_env) if _looks_like_path else (_REPO_ROOT / "service_account.json")
    if not sa_path.is_absolute():
        sa_path = _REPO_ROOT / sa_path
    if sa_path.exists():
        os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"] = base64.b64encode(sa_path.read_bytes()).decode()


def dedup_tab(svc, sheet_id: str, *, tab: str, range_full: str, key_cols: list[int],
              key_normalizer=None) -> tuple[list[list], list[list]]:
    """Read the tab, return (original_rows, deduped_rows) preserving header.

    key_cols: list of column indices to use as the dedup key.
    key_normalizer: optional callable(row) -> tuple; defaults to picking
                    cells at key_cols, normalizing date-like cells via
                    _to_date_str.
    """
    resp = svc.spreadsheets().values().get(
        spreadsheetId=sheet_id,
        range=range_full,
        valueRenderOption="UNFORMATTED_VALUE",
        dateTimeRenderOption="FORMATTED_STRING",
    ).execute()
    rows = resp.get("values", []) or []
    if not rows:
        return [], []
    header, data = rows[0], rows[1:]

    def default_key(row):
        return tuple(_to_date_str(row[i]) if i in (0, 1) else str(row[i] or "")
                     for i in key_cols if i < len(row))

    keyfn = key_normalizer or default_key
    seen = set()
    deduped = []
    for r in data:
        k = keyfn(r)
        if k in seen:
            continue
        seen.add(k)
        deduped.append(r)
    return [header] + data, [header] + deduped


def write_dedup(svc, sheet_id: str, *, tab: str, deduped_rows: list[list], col_range: str) -> None:
    """Clear the data range and write the deduped rows (header included)."""
    svc.spreadsheets().values().clear(
        spreadsheetId=sheet_id,
        range=f"{tab}!{col_range}",
    ).execute()
    if not deduped_rows:
        return
    svc.spreadsheets().values().update(
        spreadsheetId=sheet_id,
        range=f"{tab}!{col_range}",
        valueInputOption="USER_ENTERED",
        body={"values": deduped_rows},
    ).execute()


def main():
    parser = argparse.ArgumentParser(description="Dedup DATA + STYLISTS_DATA tabs")
    parser.add_argument("--write", action="store_true",
                        help="Actually modify the Sheet (default: dry-run only)")
    args = parser.parse_args()

    cfg = json.loads((_REPO_ROOT / "config/customers/karissa_001.json").read_text())
    svc = _build_service({})
    sheet_id = cfg["sheet_id"]

    # Backup directory
    backup_dir = _REPO_ROOT / "data" / "backups"
    backup_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    targets = [
        # (tab name, full read range, write range, key columns)
        {
            "tab": "DATA",
            "range_full": "DATA!A1:T",  # 20 cols A-T
            "col_range": "A1:T",
            "key_cols": [0, 1],  # (loc_name, week_ending)
            "key_desc": "(loc_name, week_ending)",
        },
        {
            "tab": "STYLISTS_DATA",
            "range_full": "STYLISTS_DATA!A1:L",  # 12 cols A-L
            "col_range": "A1:L",
            "key_cols": [0, 1, 2],  # (week_ending, stylist_name, loc_name)
            "key_desc": "(week_ending, stylist_name, loc_name)",
        },
    ]

    print(f"{'DRY RUN' if not args.write else 'WRITE MODE'} — sheet={sheet_id}")
    print("=" * 70)

    for t in targets:
        print(f"\nTab: {t['tab']}")
        print(f"  Dedup key: {t['key_desc']}")

        original, deduped = dedup_tab(
            svc, sheet_id,
            tab=t["tab"], range_full=t["range_full"], key_cols=t["key_cols"],
        )
        orig_n = max(0, len(original) - 1)  # exclude header
        dedup_n = max(0, len(deduped) - 1)
        removed = orig_n - dedup_n
        print(f"  Original rows: {orig_n}")
        print(f"  Deduped rows : {dedup_n}")
        print(f"  Would remove : {removed}")

        # Backup
        backup_path = backup_dir / f"{t['tab']}_{ts}.json"
        backup_path.write_text(json.dumps(original, indent=2, default=str), encoding="utf-8")
        print(f"  Backup saved : {backup_path}")

        if not args.write:
            print("  [DRY RUN] not modifying the Sheet")
            continue

        if removed == 0:
            print("  Already clean — nothing to do")
            continue

        write_dedup(svc, sheet_id, tab=t["tab"], deduped_rows=deduped, col_range=t["col_range"])
        print(f"  [OK] Wrote {dedup_n} rows (was {orig_n})")

    print("\nDone.")


if __name__ == "__main__":
    main()
