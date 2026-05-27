#!/usr/bin/env python3
"""
scripts/format_sheets_pct_columns.py
────────────────────────────────────
One-time helper that applies the PERCENT number format to all decimal-stored
percentage columns across the master Google Sheet's tabs.

Pipeline convention (post-2026-05-27 fix): pct fields are stored as DECIMAL
(0.0796 = 7.96%) in the Sheet for mathematical purity. Without column
formatting, Karissa sees "0.0796" in the cells when she expects "7.96%".

This script sets the column number format = PERCENT on:
  DATA              cols H (product_pct), O (wax_pct), Q (color_pct), T (treat_pct)
  CURRENT           same 4 cols
  DATA_MONTHLY      cols I (product_pct), P (wax_pct), R (color_pct), U (treat_pct)
  CUMULATIVE_MTD    cols I (product_pct), P (wax_pct), R (color_pct), U (treat_pct)
  STYLISTS_DATA     cols H (cur_rebook), I (cur_product)

After running, the Sheet cells will display as "7.96%" while still storing
the raw decimal 0.0796 — API reads continue to return the number unchanged.

Usage:
    python scripts/format_sheets_pct_columns.py
    python scripts/format_sheets_pct_columns.py --dry-run   # show what would change

Requires service_account.json at repo root OR GOOGLE_SERVICE_ACCOUNT_JSON
env var set (base64-encoded JSON), same as the pipeline.
"""

from __future__ import annotations

import argparse
import base64
import json
import logging
import os
import sys
from pathlib import Path

from google.oauth2 import service_account
from googleapiclient.discovery import build

REPO_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = REPO_ROOT / "config" / "customers" / "karissa_001.json"

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


# Tab name → list of (col_letter, field_name) to format as percent.
PCT_COLUMNS: dict[str, list[tuple[str, str]]] = {
    "DATA": [
        ("H", "product_pct"),
        ("O", "wax_pct"),
        ("Q", "color_pct"),
        ("T", "treat_pct"),
    ],
    "CURRENT": [
        ("H", "product_pct"),
        ("O", "wax_pct"),
        ("Q", "color_pct"),
        ("T", "treat_pct"),
    ],
    "DATA_MONTHLY": [
        ("I", "product_pct"),
        ("P", "wax_pct"),
        ("R", "color_pct"),
        ("U", "treat_pct"),
    ],
    "CUMULATIVE_MTD": [
        ("I", "product_pct"),
        ("P", "wax_pct"),
        ("R", "color_pct"),
        ("U", "treat_pct"),
    ],
    "STYLISTS_DATA": [
        ("H", "cur_rebook"),
        ("I", "cur_product"),
    ],
}


def _col_letter_to_index(letter: str) -> int:
    """A -> 0, B -> 1, ..., Z -> 25, AA -> 26."""
    result = 0
    for ch in letter.upper():
        result = result * 26 + (ord(ch) - ord("A") + 1)
    return result - 1


def _build_service():
    """Build Sheets API service from service_account.json or env var."""
    sa_path = REPO_ROOT / "service_account.json"
    if sa_path.exists():
        log.info("Using service_account.json at %s", sa_path)
        sa_info = json.loads(sa_path.read_text(encoding="utf-8"))
    else:
        raw = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
        if not raw:
            raise EnvironmentError(
                "Need service_account.json at repo root OR GOOGLE_SERVICE_ACCOUNT_JSON env var."
            )
        sa_info = json.loads(base64.b64decode(raw).decode("utf-8"))
    creds = service_account.Credentials.from_service_account_info(sa_info, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds)


def _get_tab_id(service, sheet_id: str, tab_name: str) -> int | None:
    """Resolve tab name → sheetId (int) needed for batchUpdate calls."""
    meta = service.spreadsheets().get(spreadsheetId=sheet_id).execute()
    for sheet in meta.get("sheets", []):
        if sheet["properties"]["title"] == tab_name:
            return sheet["properties"]["sheetId"]
    return None


def _build_format_request(tab_id: int, col_index: int) -> dict:
    """Build a RepeatCellRequest that sets PERCENT format on a column."""
    return {
        "repeatCell": {
            "range": {
                "sheetId": tab_id,
                "startRowIndex": 1,  # skip header (row 0)
                "endRowIndex": 5000,  # generous; applies to existing + future rows
                "startColumnIndex": col_index,
                "endColumnIndex": col_index + 1,
            },
            "cell": {
                "userEnteredFormat": {
                    "numberFormat": {"type": "PERCENT", "pattern": "0.0%"},
                },
            },
            "fields": "userEnteredFormat.numberFormat",
        },
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Apply PERCENT format to decimal pct columns.")
    parser.add_argument("--dry-run", action="store_true", help="Show what would change, don't apply.")
    args = parser.parse_args(argv)

    config = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    sheet_id = config["sheet_id"]

    log.info("Target sheet: %s", sheet_id)
    service = _build_service()

    total_requests = 0
    for tab_name, columns in PCT_COLUMNS.items():
        tab_id = _get_tab_id(service, sheet_id, tab_name)
        if tab_id is None:
            log.warning("Tab %r not found — skipping (%d columns)", tab_name, len(columns))
            continue

        requests = []
        for col_letter, field_name in columns:
            col_index = _col_letter_to_index(col_letter)
            log.info("  %s.%s (col %s, idx %d) -> PERCENT", tab_name, field_name, col_letter, col_index)
            requests.append(_build_format_request(tab_id, col_index))
        total_requests += len(requests)

        if args.dry_run:
            log.info("DRY RUN: would send %d format requests for %s", len(requests), tab_name)
            continue

        service.spreadsheets().batchUpdate(
            spreadsheetId=sheet_id,
            body={"requests": requests},
        ).execute()
        log.info("Applied %d PERCENT formats to %s", len(requests), tab_name)

    if args.dry_run:
        log.info("DRY RUN complete: %d total requests would be sent.", total_requests)
    else:
        log.info("Done: %d PERCENT-format requests applied across %d tabs.",
                 total_requests, len(PCT_COLUMNS))
    return 0


if __name__ == "__main__":
    sys.exit(main())
