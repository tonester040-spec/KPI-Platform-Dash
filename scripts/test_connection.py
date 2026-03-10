#!/usr/bin/env python3
"""
test_connection.py
KPI Platform — Karissa's Salon Network

Non-destructive connection test. Reads sheet metadata only — writes nothing.
Verifies that:
  1. GOOGLE_SERVICE_ACCOUNT_JSON env var is present and valid
  2. The service account can authenticate to the Sheets API
  3. The sheet_id in karissa_001.json is reachable and accessible
  4. All 4 expected tabs (CURRENT, DATA, GOALS, ALERTS) exist

Run this before running build_sheet.py or load_dummy_data.py.

Usage:
  python scripts/test_connection.py
"""

import os
import sys
import json
import base64
from pathlib import Path

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

REPO_ROOT   = Path(__file__).resolve().parent.parent
CONFIG_PATH = REPO_ROOT / "config" / "customers" / "karissa_001.json"
SCOPES      = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
EXPECTED_TABS = ["CURRENT", "DATA", "GOALS", "ALERTS"]


def main():
    print("╔══════════════════════════════════════════════════╗")
    print("║  KPI Connection Test — Google Sheets API         ║")
    print("╚══════════════════════════════════════════════════╝\n")

    all_passed = True

    # ── Check 1: Config file exists
    print("[1/4] Config file ...")
    if not CONFIG_PATH.exists():
        print(f"  ✗  Not found: {CONFIG_PATH}")
        print("     Create config/customers/karissa_001.json with a 'sheet_id' key.")
        sys.exit(1)

    config         = json.loads(CONFIG_PATH.read_text())
    spreadsheet_id = config.get("sheet_id", "").strip()

    if not spreadsheet_id or "REPLACE" in spreadsheet_id:
        print("  ✗  sheet_id is still a placeholder in karissa_001.json")
        print("     Open your Google Sheet, copy the ID from the URL, and paste it in.")
        sys.exit(1)

    print(f"  ✓  Config loaded")
    print(f"     Sheet ID : {spreadsheet_id}")

    # ── Check 2: Env var present and decodable
    print("\n[2/4] GOOGLE_SERVICE_ACCOUNT_JSON env var ...")
    raw_b64 = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()

    if not raw_b64:
        print("  ✗  Environment variable not set.")
        print("     Run: export GOOGLE_SERVICE_ACCOUNT_JSON=$(base64 -i your-key.json | tr -d '\\n')")
        sys.exit(1)

    try:
        sa_json = base64.b64decode(raw_b64).decode("utf-8")
        sa_info = json.loads(sa_json)
        sa_email = sa_info.get("client_email", "unknown")
        print(f"  ✓  Env var present and decodable")
        print(f"     Service account : {sa_email}")
    except Exception as exc:
        print(f"  ✗  Could not decode: {exc}")
        print("     Make sure you base64-encoded the full JSON key file.")
        sys.exit(1)

    # ── Check 3: Authenticate to Sheets API
    print("\n[3/4] Authenticating to Google Sheets API ...")
    try:
        creds   = service_account.Credentials.from_service_account_info(
            sa_info, scopes=SCOPES
        )
        service = build("sheets", "v4", credentials=creds)
        print("  ✓  Authentication successful")
    except Exception as exc:
        print(f"  ✗  Authentication failed: {exc}")
        sys.exit(1)

    # ── Check 4: Read sheet metadata
    print("\n[4/4] Reading sheet metadata ...")
    try:
        spreadsheet = service.spreadsheets().get(
            spreadsheetId=spreadsheet_id
        ).execute()

        title      = spreadsheet["properties"]["title"]
        tab_titles = [s["properties"]["title"] for s in spreadsheet["sheets"]]

        print(f"  ✓  Sheet accessible")
        print(f"     Title     : {title}")
        print(f"     Tab count : {len(tab_titles)}")
        print(f"     Tabs      : {', '.join(tab_titles)}")

        # Check for expected tabs
        missing = [t for t in EXPECTED_TABS if t not in tab_titles]
        if missing:
            print(f"\n  ⚠  Missing expected tabs: {', '.join(missing)}")
            print("     Run scripts/build_sheet.py to create them.")
            all_passed = False
        else:
            print(f"  ✓  All 4 required tabs present (CURRENT, DATA, GOALS, ALERTS)")

    except HttpError as exc:
        if exc.resp.status == 403:
            print(f"  ✗  Permission denied (403)")
            print(f"     The sheet exists but the service account doesn't have access.")
            print(f"     Share the sheet with: {sa_email}")
            print(f"     Give it Editor access.")
        elif exc.resp.status == 404:
            print(f"  ✗  Sheet not found (404)")
            print(f"     Sheet ID '{spreadsheet_id}' doesn't exist or isn't accessible.")
            print(f"     Double-check the ID in karissa_001.json.")
        else:
            print(f"  ✗  Google API error: {exc}")
        sys.exit(1)

    # ── Summary
    print()
    if all_passed:
        print("✅  All checks passed — connection is working.")
        print(f"   You're good to run: python scripts/build_sheet.py\n")
    else:
        print("⚠️  Connection works but some tabs are missing.")
        print(f"   Run build_sheet.py to set up the full sheet structure.\n")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nCancelled.")
        sys.exit(0)
