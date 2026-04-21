#!/usr/bin/env python3
"""
scripts/verify_sheets_v2.py
KPI Platform — Post-initialization verification for the v2 parallel Sheets.

What this script checks
-----------------------
    1. Auth works (GOOGLE_SERVICE_ACCOUNT_JSON is valid).
    2. Master sheet is reachable.
    3. Every expected tab exists.
    4. Every expected tab has the expected header row.
    5. AUDIT_LOG!A1 carries the schema version note.
    6. GoogleSheetsStore.health_check() returns ok=True.
    7. A DRY_RUN write of a dummy row through each public store method
       succeeds (no network writes, just validation + shape checks).

Exit code 0 = all green, 1 = any failure. Usage:

    export GOOGLE_SERVICE_ACCOUNT_JSON="<base64>"
    python scripts/verify_sheets_v2.py \\
        --master-sheet-id <id> \\
        [--coach-cards-sheet-id <id>]

Add --live-dry-run to also exercise GoogleSheetsStore with dry_run=False but
skip the schema hard-fail (useful if you want to test retry/backoff without
needing the note stamped yet).
"""

from __future__ import annotations

import argparse
import base64
import datetime as _dt
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any

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
    MASTER_SHEET_TABS,
    SCHEMA_VERSION,
    SCHEMA_VERSION_NOTE,
    TabDefinition,
    col_letter,
)
from core.google_sheets_store import GoogleSheetsStore  # noqa: E402


log = logging.getLogger("verify_sheets_v2")

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


def _service():
    raw_b64 = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    if not raw_b64:
        raise EnvironmentError("GOOGLE_SERVICE_ACCOUNT_JSON env var is not set.")
    sa_info = json.loads(base64.b64decode(raw_b64).decode("utf-8"))
    creds = service_account.Credentials.from_service_account_info(sa_info, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


# ─── Checks ────────────────────────────────────────────────────────────────

def check_auth() -> tuple[bool, str]:
    try:
        _service()
        return True, "auth OK"
    except Exception as e:
        return False, f"auth failed: {e}"


def check_reachable(svc, sheet_id: str) -> tuple[bool, str]:
    try:
        meta = svc.spreadsheets().get(spreadsheetId=sheet_id).execute()
        return True, f"reachable: '{meta.get('properties', {}).get('title', '')}'"
    except Exception as e:
        return False, f"unreachable: {e}"


def check_tabs_present(svc, sheet_id: str, tabs: tuple[TabDefinition, ...]) -> tuple[bool, str]:
    try:
        meta = svc.spreadsheets().get(spreadsheetId=sheet_id).execute()
    except Exception as e:
        return False, f"cannot fetch spreadsheet: {e}"
    existing = {s["properties"]["title"] for s in meta.get("sheets", [])}
    expected = {t.name for t in tabs}
    missing = expected - existing
    if missing:
        return False, f"missing tabs: {sorted(missing)}"
    return True, f"all {len(expected)} tabs present"


def check_headers(svc, sheet_id: str, tabs: tuple[TabDefinition, ...]) -> tuple[bool, str]:
    problems: list[str] = []
    for tab in tabs:
        rng = f"{tab.name}!A1:{col_letter(len(tab.columns) - 1)}1"
        try:
            res = svc.spreadsheets().values().get(
                spreadsheetId=sheet_id, range=rng,
            ).execute()
        except Exception as e:
            problems.append(f"{tab.name}: read failed ({e})")
            continue
        values = res.get("values", [[]])
        header = values[0] if values else []
        expected = [c.name for c in tab.columns]
        if header != expected:
            problems.append(
                f"{tab.name}: header mismatch.\n"
                f"    expected: {expected}\n"
                f"    got:      {header}"
            )
    if problems:
        return False, "; ".join(problems)
    return True, f"headers match on all {len(tabs)} tabs"


def check_schema_note(svc, sheet_id: str) -> tuple[bool, str]:
    try:
        res = svc.spreadsheets().get(
            spreadsheetId=sheet_id,
            ranges=["AUDIT_LOG!A1"],
            includeGridData=True,
            fields="sheets.data.rowData.values.note",
        ).execute()
    except Exception as e:
        return False, f"cannot read AUDIT_LOG!A1 note: {e}"
    try:
        note = res["sheets"][0]["data"][0]["rowData"][0]["values"][0].get("note", "")
    except (KeyError, IndexError):
        note = ""
    if SCHEMA_VERSION_NOTE in note:
        return True, f"schema note present: {SCHEMA_VERSION!r}"
    return False, f"schema note missing or wrong. Got: {note!r}"


def check_store_health(master_sheet_id: str, coach_id: str | None) -> tuple[bool, str]:
    try:
        store = GoogleSheetsStore(
            master_sheet_id=master_sheet_id,
            coach_cards_sheet_id=coach_id,
            dry_run=False,
            skip_schema_check=False,  # We WANT the hard-fail to trigger here if broken.
        )
        h = store.health_check()
        if h.get("ok"):
            return True, f"health OK: {h['details']}"
        return False, f"health not OK: {h.get('details', '?')}"
    except Exception as e:
        return False, f"store init failed: {e}"


def check_dry_run_writes(master_sheet_id: str, coach_id: str | None) -> tuple[bool, str]:
    try:
        store = GoogleSheetsStore(
            master_sheet_id=master_sheet_id,
            coach_cards_sheet_id=coach_id,
            dry_run=True,
        )
    except Exception as e:
        return False, f"dry-run store init failed: {e}"

    period_end = _dt.date.today().isoformat()

    loc_row = {
        "location": "Test Location",
        "period_start": period_end,
        "period_end": period_end,
        "pos_system": "zenoti",
        "guest_count": 123,
        "total_sales": 10000.0,
        "service_net": 8000.0,
        "product_net": 2000.0,
        "product_pct": 0.20,
        "ppg_net": 40.0,
        "pph_net": 80.0,
        "avg_ticket": 85.0,
        "productive_hours": 125.0,
        "wax_count": 40.0,
        "wax_net": 600.0,
        "wax_pct": 0.06,
        "color_net": 3500.0,
        "color_pct": 0.35,
        "treatment_count": 25.0,
        "treatment_net": 1000.0,
        "treatment_pct": 0.10,
        "flag": "solid",
        "coach": "Karissa",
        "coach_emoji": "💇",
    }
    stylist_row = {
        "period_end": period_end,
        "stylist_name": "Test Stylist",
        "location": "Test Location",
        "location_id": "z001",
        "status": "active",
        "tenure_years": 3.5,
        "pph": 85.0,
        "rebook_pct": 0.55,
        "product_pct": 0.18,
        "avg_ticket": 90.0,
        "services": 50.0,
        "color": 10.0,
        "coach": "Karissa",
        "coach_emoji": "💇",
    }
    brief = {"jess": {"territory_headline": "test brief", "star_of_week": "N/A"}}

    try:
        store.write_locations_current([loc_row], caller="verify_sheets_v2")
        store.append_locations_historical([loc_row], caller="verify_sheets_v2")
        store.write_stylists_current([stylist_row], caller="verify_sheets_v2")
        store.append_stylists_historical([stylist_row], caller="verify_sheets_v2")
        store.write_coach_briefs(brief, period_end=period_end, model="dry_run", caller="verify_sheets_v2")
        store.write_anomaly(
            period_end=period_end,
            location="Test Location",
            alert_type="DRIFT_WARNING",
            severity="LOW",
            message="dry-run verify smoke test",
            caller="verify_sheets_v2",
        )
    except Exception as e:
        return False, f"dry-run write failed: {e}"

    return True, "dry-run writes for all 6 store methods succeeded"


# ─── Main ──────────────────────────────────────────────────────────────────

def _run_check(name: str, fn) -> bool:
    try:
        ok, msg = fn()
    except Exception as e:  # noqa: BLE001 — catch anything a check raises
        ok, msg = False, f"exception: {e}"
    symbol = "✓" if ok else "✗"
    log.info("%s %s — %s", symbol, name, msg)
    return ok


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--master-sheet-id", required=True)
    parser.add_argument("--coach-cards-sheet-id", default=None)
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(message)s",
    )

    log.info("=== v2 Sheets verification ===")
    log.info("Schema version: %s", SCHEMA_VERSION)
    log.info("Master sheet:   %s", args.master_sheet_id)
    if args.coach_cards_sheet_id:
        log.info("Coach sheet:    %s", args.coach_cards_sheet_id)
    log.info("")

    results: list[bool] = []

    # Auth
    results.append(_run_check("auth", check_auth))
    if not results[-1]:
        log.error("Auth failed — aborting remaining checks.")
        return 1

    svc = _service()

    # Master sheet checks
    results.append(_run_check(
        "master: reachable",
        lambda: check_reachable(svc, args.master_sheet_id),
    ))
    results.append(_run_check(
        "master: all tabs present",
        lambda: check_tabs_present(svc, args.master_sheet_id, MASTER_SHEET_TABS),
    ))
    results.append(_run_check(
        "master: headers match schema",
        lambda: check_headers(svc, args.master_sheet_id, MASTER_SHEET_TABS),
    ))
    results.append(_run_check(
        "master: schema version note on AUDIT_LOG!A1",
        lambda: check_schema_note(svc, args.master_sheet_id),
    ))

    # Coach sheet (optional)
    if args.coach_cards_sheet_id:
        results.append(_run_check(
            "coach: reachable",
            lambda: check_reachable(svc, args.coach_cards_sheet_id),
        ))
        results.append(_run_check(
            "coach: all tabs present",
            lambda: check_tabs_present(svc, args.coach_cards_sheet_id, COACH_CARDS_SHEET_TABS),
        ))
        results.append(_run_check(
            "coach: headers match schema",
            lambda: check_headers(svc, args.coach_cards_sheet_id, COACH_CARDS_SHEET_TABS),
        ))

    # Live store health (this will hard-fail if schema note is missing)
    results.append(_run_check(
        "GoogleSheetsStore.health_check()",
        lambda: check_store_health(args.master_sheet_id, args.coach_cards_sheet_id),
    ))

    # Dry-run smoke test (no network writes)
    results.append(_run_check(
        "GoogleSheetsStore dry-run writes (6 methods)",
        lambda: check_dry_run_writes(args.master_sheet_id, args.coach_cards_sheet_id),
    ))

    total = len(results)
    passed = sum(1 for r in results if r)
    log.info("")
    log.info("=== %d/%d checks passed ===", passed, total)
    return 0 if passed == total else 1


if __name__ == "__main__":
    sys.exit(main())
