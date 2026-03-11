#!/usr/bin/env python3
"""
core/data_source.py
KPI Platform — reads current and historical data from Google Sheets.

Returns:
  load_location_data()  → list of dicts, one per location (current week)
  load_stylist_data()   → list of dicts, one per stylist (all history rows)
  load_historical_data()→ list of dicts, full DATA tab (all locations, all weeks)
"""

import os
import json
import base64
import logging
from pathlib import Path

from google.oauth2 import service_account
from googleapiclient.discovery import build

log = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# Column indices for PERF_HEADERS (DATA / CURRENT tabs)
COL = {
    "loc_name":     0,
    "week_ending":  1,
    "platform":     2,
    "guests":       3,
    "total_sales":  4,
    "service":      5,
    "product":      6,
    "product_pct":  7,
    "ppg":          8,
    "pph":          9,
    "avg_ticket":  10,
    "prod_hours":  11,
    "wax_count":   12,
    "wax":         13,
    "wax_pct":     14,
    "color":       15,
    "color_pct":   16,
    "treat_count": 17,
    "treat":       18,
    "treat_pct":   19,
}

# Column indices for STYLISTS_DATA tab (matches setup_stylist_tabs.py)
SCOL = {
    "week_ending":   0,
    "stylist_name":  1,
    "loc_name":      2,
    "loc_id":        3,
    "status":        4,
    "tenure_yrs":    5,
    "pph":           6,
    "rebook_pct":    7,
    "product_pct":   8,
    "avg_ticket":    9,
    "services":     10,
    "color_pct":    11,
}


def _build_service(config: dict):
    """Build authenticated Google Sheets service from env var."""
    raw_b64 = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    if not raw_b64:
        raise EnvironmentError(
            "GOOGLE_SERVICE_ACCOUNT_JSON env var is not set. "
            "Run: $env:GOOGLE_SERVICE_ACCOUNT_JSON = [Convert]::ToBase64String(...)"
        )
    sa_json = base64.b64decode(raw_b64).decode("utf-8")
    sa_info = json.loads(sa_json)
    creds = service_account.Credentials.from_service_account_info(sa_info, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds)


def _safe_float(val, default=0.0) -> float:
    try:
        return float(str(val).replace(",", "").replace("$", "").replace("%", "").strip())
    except (ValueError, TypeError):
        return default


def _safe_int(val, default=0) -> int:
    try:
        return int(float(str(val).replace(",", "").strip()))
    except (ValueError, TypeError):
        return default


def load_config(config_path: Path) -> dict:
    return json.loads(config_path.read_text())


def load_location_data(service, config: dict) -> list[dict]:
    """
    Read CURRENT tab — one row per location (most recent week snapshot).
    Returns list of location dicts with all metrics.
    """
    sheet_id = config["sheet_id"]
    log.info("Reading CURRENT tab from sheet %s", sheet_id)

    result = service.spreadsheets().values().get(
        spreadsheetId=sheet_id,
        range="CURRENT!A2:T",
    ).execute()

    rows = result.get("values", [])
    if not rows:
        log.warning("CURRENT tab is empty — returning empty list")
        return []

    # Build loc_id lookup from config
    loc_id_map = {loc["name"]: loc["id"] for loc in config.get("locations", [])}

    locations = []
    for row in rows:
        # Pad row to at least 20 cols
        row = row + [""] * (20 - len(row))
        loc_name = row[COL["loc_name"]].strip()
        if not loc_name:
            continue
        locations.append({
            "loc_name":    loc_name,
            "loc_id":      loc_id_map.get(loc_name, ""),
            "week_ending": row[COL["week_ending"]],
            "platform":    row[COL["platform"]],
            "guests":      _safe_int(row[COL["guests"]]),
            "total_sales": _safe_float(row[COL["total_sales"]]),
            "service":     _safe_float(row[COL["service"]]),
            "product":     _safe_float(row[COL["product"]]),
            "product_pct": _safe_float(row[COL["product_pct"]]),
            "ppg":         _safe_float(row[COL["ppg"]]),
            "pph":         _safe_float(row[COL["pph"]]),
            "avg_ticket":  _safe_float(row[COL["avg_ticket"]]),
            "prod_hours":  _safe_float(row[COL["prod_hours"]]),
            "wax_count":   _safe_int(row[COL["wax_count"]]),
            "wax":         _safe_float(row[COL["wax"]]),
            "wax_pct":     _safe_float(row[COL["wax_pct"]]),
            "color":       _safe_float(row[COL["color"]]),
            "color_pct":   _safe_float(row[COL["color_pct"]]),
            "treat_count": _safe_int(row[COL["treat_count"]]),
            "treat":       _safe_float(row[COL["treat"]]),
            "treat_pct":   _safe_float(row[COL["treat_pct"]]),
        })

    log.info("Loaded %d location rows from CURRENT", len(locations))
    return locations


def load_historical_data(service, config: dict, weeks: int = 12) -> dict:
    """
    Read DATA tab — all historical rows for all locations.
    Returns dict: { loc_name: { metric_key: [list of weekly values, oldest→newest] } }
    Limits to the most recent `weeks` weeks per location.
    """
    sheet_id = config["sheet_id"]
    log.info("Reading DATA tab (last %d weeks) from sheet %s", weeks, sheet_id)

    result = service.spreadsheets().values().get(
        spreadsheetId=sheet_id,
        range="DATA!A2:T",
    ).execute()

    rows = result.get("values", [])
    if not rows:
        return {}

    # Group rows by location name, preserving insertion order (oldest→newest)
    from collections import defaultdict
    loc_rows: dict[str, list] = defaultdict(list)
    for row in rows:
        row = row + [""] * (20 - len(row))
        loc_name = row[COL["loc_name"]].strip()
        if loc_name:
            loc_rows[loc_name].append(row)

    # Build history dict — keep last `weeks` rows per location
    history = {}
    for loc_name, loc_data in loc_rows.items():
        recent = loc_data[-weeks:]
        history[loc_name] = {
            "weeks":      [r[COL["week_ending"]] for r in recent],
            "pph":        [_safe_float(r[COL["pph"]]) for r in recent],
            "total_sales":[_safe_float(r[COL["total_sales"]]) for r in recent],
            "guests":     [_safe_int(r[COL["guests"]]) for r in recent],
            "product_pct":[_safe_float(r[COL["product_pct"]]) for r in recent],
            "avg_ticket": [_safe_float(r[COL["avg_ticket"]]) for r in recent],
            "wax_pct":    [_safe_float(r[COL["wax_pct"]]) for r in recent],
            "color_pct":  [_safe_float(r[COL["color_pct"]]) for r in recent],
            "treat_pct":  [_safe_float(r[COL["treat_pct"]]) for r in recent],
        }

    log.info("Loaded history for %d locations", len(history))
    return history


def load_stylist_data(service, config: dict) -> list[dict]:
    """
    Read STYLISTS_DATA tab — all stylist rows (full history).
    Returns list of dicts grouped by stylist name.
    Each stylist dict contains arrays of weekly values.
    """
    sheet_id = config["sheet_id"]
    log.info("Reading STYLISTS_DATA tab from sheet %s", sheet_id)

    result = service.spreadsheets().values().get(
        spreadsheetId=sheet_id,
        range="STYLISTS_DATA!A2:L",
    ).execute()

    rows = result.get("values", [])
    if not rows:
        log.warning("STYLISTS_DATA tab is empty")
        return []

    # Group rows by stylist name
    from collections import defaultdict, OrderedDict
    stylist_rows: dict[str, list] = OrderedDict()
    for row in rows:
        row = row + [""] * (12 - len(row))
        name = row[SCOL["stylist_name"]].strip()
        if not name:
            continue
        if name not in stylist_rows:
            stylist_rows[name] = []
        stylist_rows[name].append(row)

    stylists = []
    for name, srows in stylist_rows.items():
        # Use the most recent row for static fields
        latest = srows[-1]
        stylists.append({
            "name":        name,
            "loc_name":    latest[SCOL["loc_name"]],
            "loc_id":      latest[SCOL["loc_id"]],
            "status":      latest[SCOL["status"]],
            "tenure":      _safe_float(latest[SCOL["tenure_yrs"]]),
            "weeks":       [r[SCOL["week_ending"]] for r in srows],
            "pph":         [_safe_float(r[SCOL["pph"]]) for r in srows],
            "rebook":      [_safe_float(r[SCOL["rebook_pct"]]) for r in srows],
            "product":     [_safe_float(r[SCOL["product_pct"]]) for r in srows],
            "ticket":      [_safe_float(r[SCOL["avg_ticket"]]) for r in srows],
            "services":    [_safe_int(r[SCOL["services"]]) for r in srows],
            "color":       [_safe_float(r[SCOL["color_pct"]]) for r in srows],
            # Current-week convenience fields
            "cur_pph":     _safe_float(latest[SCOL["pph"]]),
            "cur_rebook":  _safe_float(latest[SCOL["rebook_pct"]]),
            "cur_product": _safe_float(latest[SCOL["product_pct"]]),
            "cur_ticket":  _safe_float(latest[SCOL["avg_ticket"]]),
        })

    log.info("Loaded %d stylists from STYLISTS_DATA", len(stylists))
    return stylists


def load_all(config_path: Path) -> tuple[dict, list, list, dict]:
    """
    Convenience loader: returns (config, locations, stylists, history).
    Authenticates once and reads all three data sources.
    """
    config = load_config(config_path)
    service = _build_service(config)
    locations = load_location_data(service, config)
    stylists  = load_stylist_data(service, config)
    history   = load_historical_data(service, config)
    return config, locations, stylists, history
