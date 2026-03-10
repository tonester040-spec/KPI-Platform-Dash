"""
setup_stylist_tabs.py
Adds STYLISTS_CURRENT and STYLISTS_DATA tabs to Karissa's Google Sheet.
Run once (or re-run to reset formatting — does NOT delete data in STYLISTS_DATA).

Usage:
  $env:GOOGLE_SERVICE_ACCOUNT_JSON = [Convert]::ToBase64String(
    [IO.File]::ReadAllBytes("karissa-service-account.json"))
  python scripts/setup_stylist_tabs.py
"""

import os, json, base64
from google.oauth2 import service_account
from googleapiclient.discovery import build

CONFIG_PATH = "config/customers/karissa_001.json"

HEADERS = [
    "stylist_name", "location_name", "location_id", "week_ending",
    "tenure_years", "pph", "rebook_pct", "product_attachment_pct",
    "avg_ticket", "total_services", "color_pct", "status"
]

# Column widths in pixels
COL_WIDTHS = {
    0: 160,   # stylist_name
    1: 160,   # location_name
    2: 100,   # location_id
    3: 110,   # week_ending
    4: 100,   # tenure_years
    5: 100,   # pph
    6: 100,   # rebook_pct
    7: 130,   # product_attachment_pct
    8: 100,   # avg_ticket
    9: 110,   # total_services
    10: 100,  # color_pct
    11: 100,  # status
}

NAVY       = {"red": 0.118, "green": 0.227, "blue": 0.373}  # #1E3A5F
WHITE_TEXT = {"red": 1.0,   "green": 1.0,   "blue": 1.0}
STRIPE_A   = {"red": 0.937, "green": 0.949, "blue": 0.965}  # #EFF2F6
STRIPE_B   = {"red": 1.0,   "green": 1.0,   "blue": 1.0}


# ── Auth ──────────────────────────────────────────────────────────────────────

def get_service():
    raw = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")
    if not raw:
        raise ValueError("GOOGLE_SERVICE_ACCOUNT_JSON env var not set")
    key_data = json.loads(base64.b64decode(raw + "=="))
    creds = service_account.Credentials.from_service_account_info(
        key_data,
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return build("sheets", "v4", credentials=creds)


# ── Sheet helpers ─────────────────────────────────────────────────────────────

def get_existing_sheets(service, sheet_id):
    meta = service.spreadsheets().get(spreadsheetId=sheet_id).execute()
    return {s["properties"]["title"]: s["properties"]["sheetId"]
            for s in meta["sheets"]}


def add_tab_if_missing(service, sheet_id, title, index):
    existing = get_existing_sheets(service, sheet_id)
    if title in existing:
        print(f"  Tab '{title}' already exists — skipping creation")
        return existing[title]
    resp = service.spreadsheets().batchUpdate(
        spreadsheetId=sheet_id,
        body={"requests": [{"addSheet": {"properties": {
            "title": title,
            "index": index
        }}}]}
    ).execute()
    new_id = resp["replies"][0]["addSheet"]["properties"]["sheetId"]
    print(f"  Created tab '{title}' (sheetId={new_id})")
    return new_id


def format_tab(service, sheet_id, tab_id, tab_title, add_filter=False):
    """Apply headers, freeze, banding, column widths."""
    num_cols = len(HEADERS)
    requests = []

    # 1. Write header row
    service.spreadsheets().values().update(
        spreadsheetId=sheet_id,
        range=f"{tab_title}!A1",
        valueInputOption="RAW",
        body={"values": [HEADERS]}
    ).execute()

    # 2. Bold navy header row
    requests.append({"repeatCell": {
        "range": {
            "sheetId": tab_id,
            "startRowIndex": 0, "endRowIndex": 1,
            "startColumnIndex": 0, "endColumnIndex": num_cols
        },
        "cell": {"userEnteredFormat": {
            "backgroundColor": NAVY,
            "textFormat": {
                "bold": True,
                "foregroundColor": WHITE_TEXT,
                "fontSize": 10
            },
            "horizontalAlignment": "CENTER",
            "verticalAlignment": "MIDDLE"
        }},
        "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,verticalAlignment)"
    }})

    # 3. Freeze row 1 + column A
    requests.append({"updateSheetProperties": {
        "properties": {
            "sheetId": tab_id,
            "gridProperties": {"frozenRowCount": 1, "frozenColumnCount": 1}
        },
        "fields": "gridProperties.frozenRowCount,gridProperties.frozenColumnCount"
    }})

    # 4. Alternating row banding
    requests.append({"addBanding": {
        "bandedRange": {
            "range": {
                "sheetId": tab_id,
                "startRowIndex": 1, "endRowIndex": 1000,
                "startColumnIndex": 0, "endColumnIndex": num_cols
            },
            "rowProperties": {
                "headerColor":      NAVY,
                "firstBandColor":   STRIPE_A,
                "secondBandColor":  STRIPE_B
            }
        }
    }})

    # 5. Column widths
    for col_idx, width_px in COL_WIDTHS.items():
        requests.append({"updateDimensionProperties": {
            "range": {
                "sheetId": tab_id,
                "dimension": "COLUMNS",
                "startIndex": col_idx,
                "endIndex": col_idx + 1
            },
            "properties": {"pixelSize": width_px},
            "fields": "pixelSize"
        }})

    # 6. Row height for header
    requests.append({"updateDimensionProperties": {
        "range": {
            "sheetId": tab_id,
            "dimension": "ROWS",
            "startIndex": 0,
            "endIndex": 1
        },
        "properties": {"pixelSize": 36},
        "fields": "pixelSize"
    }})

    # 7. Add filter (for STYLISTS_DATA)
    if add_filter:
        requests.append({"setBasicFilter": {
            "filter": {
                "range": {
                    "sheetId": tab_id,
                    "startRowIndex": 0,
                    "startColumnIndex": 0,
                    "endColumnIndex": num_cols
                }
            }
        }})

    service.spreadsheets().batchUpdate(
        spreadsheetId=sheet_id,
        body={"requests": requests}
    ).execute()
    print(f"  Formatted tab '{tab_title}' ✓")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    with open(CONFIG_PATH) as f:
        config = json.load(f)
    sheet_id = config["sheet_id"]
    print(f"Sheet ID: {sheet_id}")

    service = get_service()
    print("Connected to Sheets API ✓\n")

    # STYLISTS_CURRENT goes after GOALS (index 4)
    print("Setting up STYLISTS_CURRENT...")
    current_id = add_tab_if_missing(service, sheet_id, "STYLISTS_CURRENT", 4)
    format_tab(service, sheet_id, current_id, "STYLISTS_CURRENT", add_filter=False)

    # STYLISTS_DATA goes after STYLISTS_CURRENT (index 5)
    print("\nSetting up STYLISTS_DATA...")
    data_id = add_tab_if_missing(service, sheet_id, "STYLISTS_DATA", 5)
    format_tab(service, sheet_id, data_id, "STYLISTS_DATA", add_filter=True)

    print("\n✅ Both stylist tabs are ready.")
    print("Next: run  python scripts/load_stylist_dummy_data.py")


if __name__ == "__main__":
    main()
