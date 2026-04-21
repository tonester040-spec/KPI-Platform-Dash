#!/usr/bin/env python3
"""
scripts/initialize_sheets_v2.py
KPI Platform — One-time initializer for the v2 parallel Google Sheets.

What this script does
---------------------
Given an existing (empty or near-empty) Google Sheet, this script stamps it
with the v2 schema defined in config/sheets_schema_v2.py:

    1. Creates every tab that doesn't exist yet.
    2. Writes the header row for each tab (frozen).
    3. Applies per-column formatting (dates as plain text, currencies,
       percentages, integers).
    4. Sets column widths.
    5. Writes the schema version note onto cell A1 of every tab.
    6. Applies header styling (bg color, bold, white text).

Run it twice and it becomes a no-op (idempotent) — existing tabs are updated
in place, not recreated, which means any rows already sitting in the sheet
are preserved.

Usage
-----
    # Create two fresh Sheets in Karissa's Drive, grab the IDs, then:
    export GOOGLE_SERVICE_ACCOUNT_JSON="<base64 service account json>"
    python scripts/initialize_sheets_v2.py \\
        --master-sheet-id <master sheet id> \\
        --coach-cards-sheet-id <coach cards sheet id>

Or dry-run to see what it would do:

    python scripts/initialize_sheets_v2.py \\
        --master-sheet-id <id> --dry-run

Safety
------
Running this against the legacy production Sheet
(1JY6L7H1Pb2JFmNoz2XNkvG0ogrYgagLVDwH01vuWT28) is a bad idea — it will add
tabs with v2 names (LOCATIONS_CURRENT etc.) alongside the legacy ones. If
you accidentally pass the production ID, the script will refuse unless you
also pass --allow-legacy-sheet-id.
"""

from __future__ import annotations

import argparse
import base64
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any

# Make imports work when run directly from the repo root.
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

try:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
except ImportError as e:
    print(f"Missing Google client libraries: {e}", file=sys.stderr)
    sys.exit(1)

from config.sheets_schema_v2 import (  # noqa: E402
    COACH_CARDS_SHEET_TABS,
    Column,
    DataType,
    MASTER_SHEET_TABS,
    SCHEMA_VERSION,
    SCHEMA_VERSION_NOTE,
    TabDefinition,
    col_letter,
)


log = logging.getLogger("initialize_sheets_v2")

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
LEGACY_PRODUCTION_SHEET_ID = "1JY6L7H1Pb2JFmNoz2XNkvG0ogrYgagLVDwH01vuWT28"


# ─── Auth ──────────────────────────────────────────────────────────────────

def _service():
    raw_b64 = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    if not raw_b64:
        raise EnvironmentError("GOOGLE_SERVICE_ACCOUNT_JSON env var is not set.")
    sa_info = json.loads(base64.b64decode(raw_b64).decode("utf-8"))
    creds = service_account.Credentials.from_service_account_info(sa_info, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


# ─── Format helpers ────────────────────────────────────────────────────────

def _number_format_for(col: Column) -> dict[str, Any] | None:
    """
    Translate schema_v2 format info into the Sheets API numberFormat payload.
    Returns None if the column has no numeric formatting (plain strings).
    """
    dt = col.data_type
    if dt is DataType.STRING:
        return None
    if dt is DataType.DATE:
        # Plain text — prevents locale drift (3/21 vs 21/3 etc.).
        return {"type": "TEXT", "pattern": "@"}
    if dt is DataType.JSON:
        return {"type": "TEXT", "pattern": "@"}
    if col.format_string:
        type_map = {
            DataType.INTEGER:   "NUMBER",
            DataType.FLOAT:     "NUMBER",
            DataType.CURRENCY:  "CURRENCY",
            DataType.PERCENTAGE: "PERCENT",
        }
        return {"type": type_map.get(dt, "NUMBER"), "pattern": col.format_string}
    return None


def _rgb_from_hex(hex_color: str) -> dict[str, float]:
    h = hex_color.lstrip("#")
    r = int(h[0:2], 16) / 255.0
    g = int(h[2:4], 16) / 255.0
    b = int(h[4:6], 16) / 255.0
    return {"red": r, "green": g, "blue": b}


# ─── Sheet introspection ───────────────────────────────────────────────────

def _get_spreadsheet(svc, sheet_id: str) -> dict[str, Any]:
    return svc.spreadsheets().get(spreadsheetId=sheet_id).execute()


def _existing_tabs(spreadsheet: dict[str, Any]) -> dict[str, int]:
    """Return {tab_title: sheetId}."""
    return {
        sh["properties"]["title"]: sh["properties"]["sheetId"]
        for sh in spreadsheet.get("sheets", [])
    }


# ─── Per-tab build ─────────────────────────────────────────────────────────

def _build_requests_for_tab(
    tab: TabDefinition,
    existing_tabs: dict[str, int],
) -> tuple[list[dict[str, Any]], bool]:
    """
    Build the batchUpdate requests needed to (re)create + style `tab`.
    Returns (requests, created_new). We return requests only for structural
    ops (addSheet, updateDimensionProperties, setDataValidation). Header
    values + notes are written separately via values_update.
    """
    requests: list[dict[str, Any]] = []
    created_new = tab.name not in existing_tabs

    if created_new:
        requests.append({
            "addSheet": {
                "properties": {
                    "title": tab.name,
                    "gridProperties": {
                        "rowCount": 1000,
                        "columnCount": len(tab.columns),
                        "frozenRowCount": tab.freeze_rows,
                        "frozenColumnCount": tab.freeze_cols,
                    },
                }
            }
        })

    return requests, created_new


def _style_requests_for_tab(
    tab: TabDefinition,
    sheet_id_int: int,
) -> list[dict[str, Any]]:
    """Column widths, header styling, per-column format."""
    requests: list[dict[str, Any]] = []

    # Column widths.
    for idx, col in enumerate(tab.columns):
        requests.append({
            "updateDimensionProperties": {
                "range": {
                    "sheetId": sheet_id_int,
                    "dimension": "COLUMNS",
                    "startIndex": idx,
                    "endIndex": idx + 1,
                },
                "properties": {"pixelSize": col.width},
                "fields": "pixelSize",
            }
        })

    # Header row styling: bg color + bold white text.
    requests.append({
        "repeatCell": {
            "range": {
                "sheetId": sheet_id_int,
                "startRowIndex": 0,
                "endRowIndex": 1,
                "startColumnIndex": 0,
                "endColumnIndex": len(tab.columns),
            },
            "cell": {
                "userEnteredFormat": {
                    "backgroundColor": _rgb_from_hex(tab.header_bg_hex),
                    "textFormat": {
                        "bold": True,
                        "foregroundColor": _rgb_from_hex(tab.header_fg_hex),
                    },
                    "horizontalAlignment": "LEFT",
                }
            },
            "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)",
        }
    })

    # Per-column format below the header row.
    for idx, col in enumerate(tab.columns):
        nf = _number_format_for(col)
        if nf is None:
            continue
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id_int,
                    "startRowIndex": 1,
                    "startColumnIndex": idx,
                    "endColumnIndex": idx + 1,
                },
                "cell": {"userEnteredFormat": {"numberFormat": nf}},
                "fields": "userEnteredFormat.numberFormat",
            }
        })

    # Freeze rows/cols (redundant with addSheet but safe to re-apply).
    requests.append({
        "updateSheetProperties": {
            "properties": {
                "sheetId": sheet_id_int,
                "gridProperties": {
                    "frozenRowCount": tab.freeze_rows,
                    "frozenColumnCount": tab.freeze_cols,
                },
            },
            "fields": "gridProperties.frozenRowCount,gridProperties.frozenColumnCount",
        }
    })

    return requests


def _header_values_request(tab: TabDefinition) -> dict[str, Any]:
    return {
        "range": f"{tab.name}!A1:{col_letter(len(tab.columns) - 1)}1",
        "values": [[c.name for c in tab.columns]],
    }


def _schema_note_request(tab: TabDefinition, sheet_id_int: int) -> dict[str, Any]:
    """Write the schema version note onto A1 of the tab."""
    return {
        "updateCells": {
            "range": {
                "sheetId": sheet_id_int,
                "startRowIndex": 0,
                "endRowIndex": 1,
                "startColumnIndex": 0,
                "endColumnIndex": 1,
            },
            "rows": [{"values": [{"note": SCHEMA_VERSION_NOTE}]}],
            "fields": "note",
        }
    }


# ─── Main orchestrator ─────────────────────────────────────────────────────

def initialize_sheet(
    svc,
    sheet_id: str,
    tabs: tuple[TabDefinition, ...],
    *,
    dry_run: bool,
) -> dict[str, Any]:
    """
    Initialize one Google Sheet with a set of tabs.
    Returns a summary dict for logging.
    """
    spreadsheet = _get_spreadsheet(svc, sheet_id)
    existing = _existing_tabs(spreadsheet)

    summary: dict[str, Any] = {
        "sheet_id": sheet_id,
        "sheet_title": spreadsheet.get("properties", {}).get("title", ""),
        "created": [],
        "updated": [],
    }

    # 1. Create any missing tabs in one batchUpdate so we get sheetIds back.
    add_requests: list[dict[str, Any]] = []
    for tab in tabs:
        reqs, created = _build_requests_for_tab(tab, existing)
        if created:
            summary["created"].append(tab.name)
        else:
            summary["updated"].append(tab.name)
        add_requests.extend(reqs)

    if add_requests:
        if dry_run:
            log.info("[DRY_RUN] would addSheet for: %s", summary["created"])
        else:
            log.info("Creating %d new tabs on %s", len(summary["created"]), sheet_id)
            svc.spreadsheets().batchUpdate(
                spreadsheetId=sheet_id,
                body={"requests": add_requests},
            ).execute()
            # Refresh to get the new sheetIds.
            spreadsheet = _get_spreadsheet(svc, sheet_id)
            existing = _existing_tabs(spreadsheet)

    # 2. For each tab, apply styling + header + schema note.
    for tab in tabs:
        sheet_id_int = existing.get(tab.name)
        if sheet_id_int is None and not dry_run:
            log.error("%s: tab missing after addSheet — skipping", tab.name)
            continue

        style_reqs = _style_requests_for_tab(tab, sheet_id_int or 0)
        note_req = _schema_note_request(tab, sheet_id_int or 0)
        header_req = _header_values_request(tab)

        if dry_run:
            log.info(
                "[DRY_RUN] %s: would apply %d style requests + header + schema note",
                tab.name, len(style_reqs),
            )
            continue

        # Styling + notes in one batchUpdate.
        svc.spreadsheets().batchUpdate(
            spreadsheetId=sheet_id,
            body={"requests": style_reqs + [note_req]},
        ).execute()

        # Header values via values_update (USER_ENTERED).
        svc.spreadsheets().values().update(
            spreadsheetId=sheet_id,
            range=header_req["range"],
            valueInputOption="USER_ENTERED",
            body={"values": header_req["values"]},
        ).execute()

        log.info("%s: styled + header + schema note written", tab.name)

    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--master-sheet-id", required=True, help="Google Sheet ID for master tabs")
    parser.add_argument("--coach-cards-sheet-id", default=None, help="Optional Google Sheet ID for coach cards tab")
    parser.add_argument("--dry-run", action="store_true", help="Plan only — no writes")
    parser.add_argument(
        "--allow-legacy-sheet-id",
        action="store_true",
        help="Required to operate on the legacy production Sheet ID",
    )
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    if (
        args.master_sheet_id == LEGACY_PRODUCTION_SHEET_ID
        and not args.allow_legacy_sheet_id
    ):
        log.error(
            "Refusing to operate on the legacy production Sheet %s. "
            "Pass --allow-legacy-sheet-id if you really mean it.",
            LEGACY_PRODUCTION_SHEET_ID,
        )
        return 2

    svc = _service()

    log.info("Schema version: %s", SCHEMA_VERSION)

    # Master.
    master_summary = initialize_sheet(
        svc,
        args.master_sheet_id,
        MASTER_SHEET_TABS,
        dry_run=args.dry_run,
    )
    log.info("Master sheet '%s' summary: %s", master_summary["sheet_title"], {
        "created": master_summary["created"],
        "updated": master_summary["updated"],
    })

    # Coach cards (optional).
    if args.coach_cards_sheet_id:
        coach_summary = initialize_sheet(
            svc,
            args.coach_cards_sheet_id,
            COACH_CARDS_SHEET_TABS,
            dry_run=args.dry_run,
        )
        log.info("Coach cards sheet '%s' summary: %s", coach_summary["sheet_title"], {
            "created": coach_summary["created"],
            "updated": coach_summary["updated"],
        })

    if args.dry_run:
        log.info("Dry run complete — no changes made.")
    else:
        log.info("Initialization complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
