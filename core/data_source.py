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
import datetime
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


# Sheets/Excel date epoch — serial 1 = 1899-12-31 in Lotus 1-2-3 / Excel convention,
# but Google Sheets follows Excel which has a phantom 1900-02-29, so day 0 maps to
# 1899-12-30. Verified: serial 46166 → 2026-05-08, serial 46181 → 2026-05-23.
_EXCEL_EPOCH = datetime.date(1899, 12, 30)


def _to_date_str(val) -> str:
    """Normalize any cell value to 'YYYY-MM-DD'.

    Sheets stores dates entered via USER_ENTERED as serial numbers (integers).
    Reads with UNFORMATTED_VALUE return those integers. This helper converts:
      - int/float serial → 'YYYY-MM-DD'
      - 'YYYY-MM-DD' string → passes through unchanged
      - 'M/D/YYYY' style string → normalized to 'YYYY-MM-DD'
      - None/empty → '' (so existence checks downstream work uniformly)
    """
    if val is None or val == "":
        return ""
    if isinstance(val, (int, float)):
        try:
            return (_EXCEL_EPOCH + datetime.timedelta(days=int(val))).isoformat()
        except (ValueError, OverflowError):
            return ""
    if isinstance(val, datetime.date):
        return val.isoformat()
    s = str(val).strip()
    # If already ISO format YYYY-MM-DD, fast path
    if len(s) == 10 and s[4] == "-" and s[7] == "-":
        return s
    # Try US-style date formats Sheets users sometimes type
    for fmt in ("%m/%d/%Y", "%m/%d/%y", "%Y/%m/%d"):
        try:
            return datetime.datetime.strptime(s, fmt).date().isoformat()
        except ValueError:
            continue
    return s  # Pass through whatever — caller decides what to do with unparseable input


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
        valueRenderOption="UNFORMATTED_VALUE",
        dateTimeRenderOption="FORMATTED_STRING",
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
            "week_ending": _to_date_str(row[COL["week_ending"]]),
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


def load_historical_data(service, config: dict, weeks: int = 12,
                         include_monthly_fallback: bool = True) -> dict:
    """
    Read DATA tab — all historical rows for all locations.
    Returns dict: { loc_name: { metric_key: [list of weekly values, oldest→newest] } }
    Limits to the most recent `weeks` weeks per location.

    Phase 2.5: when include_monthly_fallback=True (default), also reads
    DATA_MONTHLY and merges those entries chronologically BEFORE weekly rows.
    This gives the dashboard real backfilled history (Mar/Apr/May 2026 monthly
    aggregates) when DATA is sparse — without polluting DATA itself.
    """
    sheet_id = config["sheet_id"]
    log.info("Reading DATA tab (last %d weeks) from sheet %s", weeks, sheet_id)

    result = service.spreadsheets().values().get(
        spreadsheetId=sheet_id,
        range="DATA!A2:T",
        valueRenderOption="UNFORMATTED_VALUE",
        dateTimeRenderOption="FORMATTED_STRING",
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
            "weeks":      [_to_date_str(r[COL["week_ending"]]) for r in recent],
            "pph":        [_safe_float(r[COL["pph"]]) for r in recent],
            "total_sales":[_safe_float(r[COL["total_sales"]]) for r in recent],
            "guests":     [_safe_int(r[COL["guests"]]) for r in recent],
            "product_pct":[_safe_float(r[COL["product_pct"]]) for r in recent],
            "avg_ticket": [_safe_float(r[COL["avg_ticket"]]) for r in recent],
            "wax_pct":    [_safe_float(r[COL["wax_pct"]]) for r in recent],
            "color_pct":  [_safe_float(r[COL["color_pct"]]) for r in recent],
            "treat_pct":  [_safe_float(r[COL["treat_pct"]]) for r in recent],
        }

    log.info("Loaded weekly history for %d locations", len(history))

    if include_monthly_fallback:
        monthly = load_data_monthly_history(service, config)
        if monthly:
            history = merge_history(history, monthly, weeks=weeks)
            log.info("Merged in monthly history; total locations now %d", len(history))

    return history


# ─── Phase 2.5: DATA_MONTHLY fallback for dashboard history ───────────────────
#
# When DATA tab is sparse (typical right after go-live), dashboard charts
# look bare. These helpers pull backfilled monthly aggregates from
# DATA_MONTHLY / STYLISTS_DATA_MONTHLY and emit them in the same shape as
# weekly history, with month-end dates as synthetic week_endings. Treats
# "March monthly total" as one data point in the historical series.

def load_data_monthly_history(service, config: dict) -> dict:
    """Read DATA_MONTHLY and return history dict matching load_historical_data shape.

    Each monthly row becomes one historical entry with week_ending = the row's
    period_end (or month-end derived from year_month if period_end is blank).
    Sorted oldest -> newest per location.

    Returns {} if tab missing or empty. Safe to call on first-ever pipeline run.
    """
    sheet_id = config["sheet_id"]
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=sheet_id, range="DATA_MONTHLY!A2:W",
            valueRenderOption="UNFORMATTED_VALUE",
            dateTimeRenderOption="FORMATTED_STRING",
        ).execute()
    except Exception as e:
        log.info("DATA_MONTHLY tab not present (or read failed): %s", e)
        return {}

    rows = result.get("values", [])
    if not rows:
        return {}

    from collections import defaultdict
    loc_rows: dict[str, list] = defaultdict(list)
    for row in rows:
        row = row + [""] * (23 - len(row))
        loc_name = str(row[0]).strip()
        if loc_name:
            loc_rows[loc_name].append(row)

    history = {}
    for loc_name, mrows in loc_rows.items():
        # Sort by year_month (col B / index 1) — oldest first
        mrows.sort(key=lambda r: str(r[1]))
        # week_ending from period_end (W/22); fall back to year_month + "-28" mid-month-ish
        def week_for(r):
            we = _to_date_str(r[22])
            return we if we else (f"{r[1]}-28" if r[1] else "")
        history[loc_name] = {
            "weeks":       [week_for(r) for r in mrows],
            "pph":         [_safe_float(r[9])  for r in mrows],   # col J = pph
            "total_sales": [_safe_float(r[4])  for r in mrows],   # col E
            "guests":      [_safe_int(r[3])    for r in mrows],   # col D
            "product_pct": [_safe_float(r[7])  for r in mrows],   # col H
            "avg_ticket":  [_safe_float(r[10]) for r in mrows],   # col K
            "wax_pct":     [_safe_float(r[14]) for r in mrows],   # col O
            "color_pct":   [_safe_float(r[16]) for r in mrows],   # col Q
            "treat_pct":   [_safe_float(r[19]) for r in mrows],   # col T
        }
    log.info("Loaded monthly history for %d locations from DATA_MONTHLY", len(history))
    return history


def merge_history(weekly: dict, monthly: dict, weeks: int = 12) -> dict:
    """Merge monthly history (oldest first) + weekly history into one series.

    For each location key in either source, returns a dict with the last
    `weeks` entries combined, with monthly entries placed BEFORE weekly ones
    chronologically. Both sources MUST share the same KPI keys.
    """
    out: dict[str, dict] = {}
    keys = set(weekly.keys()) | set(monthly.keys())
    for loc in keys:
        m = monthly.get(loc, {})
        w = weekly.get(loc, {})
        # Detect KPI keys to merge (anything that's a list)
        kpi_keys = set()
        for src in (m, w):
            for k, v in src.items():
                if isinstance(v, list):
                    kpi_keys.add(k)
        merged: dict[str, list] = {}
        for k in kpi_keys:
            m_vals = m.get(k, [])
            w_vals = w.get(k, [])
            combined = m_vals + w_vals  # monthly first (older), then weekly (newer)
            merged[k] = combined[-weeks:] if weeks else combined
        out[loc] = merged
    return out


def _merge_stylist_rosters(weekly: list[dict], monthly: dict) -> list[dict]:
    """Merge weekly stylist dicts (from STYLISTS_DATA) with monthly stylist
    dicts (from STYLISTS_DATA_MONTHLY).

    Resulting list contains the UNION of stylists from both sources. For
    stylists present in both, monthly history is prepended (older) to weekly
    arrays. Static fields (loc_name, loc_id, status, tenure) prefer weekly
    when present.

    Args:
      weekly: list of stylist dicts from load_stylist_data's main read
      monthly: dict keyed by stylist name from load_stylists_data_monthly_history

    Returns: merged list of stylist dicts in stable order
      (weekly first, then monthly-only stylists alphabetical).
    """
    by_name: dict[str, dict] = {s["name"]: s for s in weekly}
    # Merge monthly history into existing weekly entries
    for name, m in monthly.items():
        if name in by_name:
            existing = by_name[name]
            # Prepend monthly arrays before weekly arrays (oldest first).
            # `product` is derived in load_stylists_data_monthly_history from
            # Karissa's golden formula product_net/(net_service+net_product).
            # `rebook` and `color` aren't captured at stylist grain in monthly
            # — pad the prepend with zeros so chart arrays stay aligned with
            # `weeks` length (dashboard does `.slice(-take)` across keys).
            monthly_len = len(m.get("weeks", []))
            for arr_key in ("weeks", "pph", "ticket", "services", "product"):
                if arr_key in m and arr_key in existing:
                    existing[arr_key] = list(m[arr_key]) + list(existing[arr_key])
            for arr_key in ("rebook", "color"):
                if arr_key in existing:
                    existing[arr_key] = [0] * monthly_len + list(existing[arr_key])
        else:
            # Stylist only in monthly — synthesize a historical-only record.
            #
            # `is_historical_only=True` is the contract that says: this stylist
            # has NO current-week data. The cumulative differencing pipeline
            # and `enrich_stylists` filter these out, then re-attach them after
            # so they still appear on the dashboard (Stylists tab gets monthly
            # history rendered) but never crash on missing cur_* fields and
            # never get incorrectly tagged with a "Needs Coaching" archetype
            # just because they have no live week.
            monthly_weeks = list(m.get("weeks", []))
            monthly_product = list(m.get("product", []))
            # product comes from monthly (computed per Karissa's formula);
            # rebook/color are zero-padded because we don't have per-stylist
            # values for them in any source today.
            by_name[name] = {
                "name":        name,
                "loc_name":    m.get("loc_name", ""),
                "loc_id":      m.get("loc_id", ""),
                "status":      "active",
                "tenure":      0,
                "is_historical_only": True,
                "weeks":       monthly_weeks,
                "pph":         list(m.get("pph", [])),
                "rebook":      [0] * len(monthly_weeks),
                "product":     monthly_product if monthly_product else [0] * len(monthly_weeks),
                "ticket":      list(m.get("ticket", [])),
                "services":    list(m.get("services", [])),
                "color":       [0] * len(monthly_weeks),
                # cur_* present but zeroed — downstream filters skip these
                # records, but the keys exist so any defensive `.get()` works.
                "cur_pph":     0,
                "cur_rebook":  0,
                "cur_product": monthly_product[-1] if monthly_product else 0,
                "cur_ticket":  0,
            }
    # Stable ordering: original weekly order, then monthly-only stylists sorted alphabetically
    weekly_names = [s["name"] for s in weekly]
    monthly_only = sorted(n for n in monthly if n not in set(weekly_names))
    ordered = [by_name[n] for n in weekly_names] + [by_name[n] for n in monthly_only]
    return ordered


def load_stylists_data_monthly_history(service, config: dict) -> dict:
    """Read STYLISTS_DATA_MONTHLY and return per-stylist history dict.

    Returns {stylist_name: {loc_name, loc_id, weeks[], pph[], product[], ticket[], services[]}}.
    Each monthly snapshot becomes one historical entry with week_ending = period_end.
    For stylists who worked at multiple locations, the latest loc_name is used.
    """
    sheet_id = config["sheet_id"]
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=sheet_id, range="STYLISTS_DATA_MONTHLY!A2:P",
            valueRenderOption="UNFORMATTED_VALUE",
            dateTimeRenderOption="FORMATTED_STRING",
        ).execute()
    except Exception as e:
        log.info("STYLISTS_DATA_MONTHLY tab not present (or read failed): %s", e)
        return {}

    rows = result.get("values", [])
    if not rows:
        return {}

    # Group by stylist name + loc_name (a stylist can have entries at multiple locs)
    from collections import defaultdict
    stylist_rows: dict[str, list] = defaultdict(list)
    for row in rows:
        row = row + [""] * (16 - len(row))
        name = str(row[1]).strip()  # col B
        if name:
            stylist_rows[name].append(row)

    stylists: dict[str, dict] = {}
    for name, srows in stylist_rows.items():
        # Sort by year_month (col A / index 0) — oldest first
        srows.sort(key=lambda r: str(r[0]))
        latest = srows[-1]
        def week_for(r):
            we = _to_date_str(r[15])  # period_end (col P / index 15)
            return we if we else f"{r[0]}-28"

        # Karissa golden rule: product_pct = product_net / total_sales,
        # where total_sales = service_net + product_net (NOT tax-inclusive).
        # Per-stylist values come from EMPLOYEE SALE DETAILS (net_service, net_product).
        # Returned as a percentage (0-100) to match the weekly STYLISTS_DATA column.
        def product_pct_for(r):
            net_service = _safe_float(r[7])  # col H
            net_product = _safe_float(r[8])  # col I
            total = net_service + net_product
            return round(net_product / total * 100, 2) if total > 0 else 0.0

        stylists[name] = {
            "name":     name,
            "loc_name": str(latest[2]),       # col C
            "loc_id":   str(latest[3]),       # col D
            "weeks":    [week_for(r) for r in srows],
            "pph":      [_safe_float(r[10]) for r in srows],  # col K (zero in March)
            "ticket":   [_safe_float(r[9])  for r in srows],  # col J avg_ticket
            "ppg":      [_safe_float(r[11]) for r in srows],  # col L
            "services": [_safe_int(r[5])    for r in srows],  # col F invoices
            "product":  [product_pct_for(r) for r in srows],
        }
    log.info("Loaded monthly history for %d stylists from STYLISTS_DATA_MONTHLY", len(stylists))
    return stylists


def load_stylist_data(service, config: dict,
                      include_monthly_fallback: bool = True) -> list[dict]:
    """
    Read STYLISTS_DATA tab — all stylist rows (full history).
    Returns list of dicts grouped by stylist name.
    Each stylist dict contains arrays of weekly values.

    Phase 2.5: when include_monthly_fallback=True (default), also reads
    STYLISTS_DATA_MONTHLY. If STYLISTS_DATA is empty (typical right after
    go-live before any real weekly Mondays have run), the monthly roster
    becomes the entire return so the dashboard Stylists tab has content.
    If STYLISTS_DATA has rows, they're merged with monthly entries placed
    chronologically before (older).
    """
    sheet_id = config["sheet_id"]
    log.info("Reading STYLISTS_DATA tab from sheet %s", sheet_id)

    result = service.spreadsheets().values().get(
        spreadsheetId=sheet_id,
        range="STYLISTS_DATA!A2:L",
        valueRenderOption="UNFORMATTED_VALUE",
        dateTimeRenderOption="FORMATTED_STRING",
    ).execute()

    rows = result.get("values", [])
    if not rows:
        log.warning("STYLISTS_DATA tab is empty")
        # Phase 2.5: fall through to monthly fallback below instead of returning []
        rows = []

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
            "weeks":       [_to_date_str(r[SCOL["week_ending"]]) for r in srows],
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

    if include_monthly_fallback:
        monthly_stylists = load_stylists_data_monthly_history(service, config)
        stylists = _merge_stylist_rosters(stylists, monthly_stylists)
        log.info("After monthly fallback merge: %d stylists total", len(stylists))
    return stylists


def read_cumulative_mtd_snapshots(service, config: dict, year_month: str) -> list[dict]:
    """Read all CUMULATIVE_MTD snapshot rows for a given year_month.

    Returns list of dicts mirroring CUMULATIVE_MTD_HEADERS in core/sheets_writer.py.
    Returns [] if the tab doesn't exist, is empty, or has no rows for that
    year_month. Never raises — missing tab is normal on first run.
    """
    sheet_id = config["sheet_id"]
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=sheet_id,
            range="CUMULATIVE_MTD!A2:V",
            valueRenderOption="UNFORMATTED_VALUE",
            dateTimeRenderOption="FORMATTED_STRING",
        ).execute()
    except Exception as e:
        # Most likely "Unable to parse range" because tab doesn't exist yet
        log.info("CUMULATIVE_MTD tab not yet present (or read failed): %s", e)
        return []

    rows = result.get("values", [])
    out: list[dict] = []
    for row in rows:
        row = row + [""] * (22 - len(row))
        if not row[0] or row[1] != year_month:
            continue
        out.append({
            "loc_name":    row[0],
            "year_month":  row[1],
            "week_ending": _to_date_str(row[2]),
            "platform":    row[3],
            "guests":      _safe_int(row[4]),
            "total_sales": _safe_float(row[5]),
            "service":     _safe_float(row[6]),
            "product":     _safe_float(row[7]),
            "product_pct": _safe_float(row[8]),
            "ppg":         _safe_float(row[9]),
            "pph":         _safe_float(row[10]),
            "avg_ticket":  _safe_float(row[11]),
            "prod_hours":  _safe_float(row[12]),
            "wax_count":   _safe_int(row[13]),
            "wax":         _safe_float(row[14]),
            "wax_pct":     _safe_float(row[15]),
            "color":       _safe_float(row[16]),
            "color_pct":   _safe_float(row[17]),
            "treat_count": _safe_int(row[18]),
            "treat":       _safe_float(row[19]),
            "treat_pct":   _safe_float(row[20]),
            "source":      row[21],
        })
    log.info("Loaded %d CUMULATIVE_MTD snapshots for %s", len(out), year_month)
    return out


def read_stylists_cumulative_mtd_snapshots(service, config: dict, year_month: str) -> list[dict]:
    """Read all STYLISTS_CUMULATIVE_MTD snapshot rows for a given year_month.

    Same pattern as read_cumulative_mtd_snapshots but for stylist-grain data.
    """
    sheet_id = config["sheet_id"]
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=sheet_id,
            range="STYLISTS_CUMULATIVE_MTD!A2:O",
            valueRenderOption="UNFORMATTED_VALUE",
            dateTimeRenderOption="FORMATTED_STRING",
        ).execute()
    except Exception as e:
        log.info("STYLISTS_CUMULATIVE_MTD tab not yet present (or read failed): %s", e)
        return []

    rows = result.get("values", [])
    out: list[dict] = []
    for row in rows:
        row = row + [""] * (15 - len(row))
        if not row[0] or row[0] != year_month:
            continue
        out.append({
            "year_month":       row[0],
            "week_ending":      _to_date_str(row[1]),
            "name":             row[2],
            "loc_name":         row[3],
            "loc_id":           row[4],
            "platform":         row[5],
            "invoices":         _safe_int(row[6]),
            "guests":           _safe_int(row[7]),
            "net_service":      _safe_float(row[8]),
            "net_product":      _safe_float(row[9]),
            "avg_ticket":       _safe_float(row[10]),
            "pph":              _safe_float(row[11]),
            "ppg":              _safe_float(row[12]),
            "production_hours": _safe_float(row[13]),
            "source":           row[14],
        })
    log.info("Loaded %d STYLISTS_CUMULATIVE_MTD snapshots for %s", len(out), year_month)
    return out


def read_data_monthly_rows(service, config: dict, year_months: list[str] | None = None) -> list[dict]:
    """Read DATA_MONTHLY rows, optionally filtered to a set of year_month values.

    Returns list of dicts mirroring DATA_MONTHLY_HEADERS in core/sheets_writer.py.
    ``year_months=None`` returns every row; pass a list (e.g. ``["2025-05", "2019-05",
    "2025-Q2", "2019-Q2", "2026-04", "2026-05"]``) to filter for the YoY report tab.
    Returns [] if the tab doesn't exist.
    """
    sheet_id = config["sheet_id"]
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=sheet_id,
            range="DATA_MONTHLY!A2:W",
            valueRenderOption="UNFORMATTED_VALUE",
            dateTimeRenderOption="FORMATTED_STRING",
        ).execute()
    except Exception as e:
        log.info("DATA_MONTHLY tab not yet present (or read failed): %s", e)
        return []

    rows = result.get("values", [])
    keep = set(year_months) if year_months else None
    out: list[dict] = []
    for row in rows:
        row = row + [""] * (23 - len(row))
        if not row[0]:
            continue
        if keep is not None and row[1] not in keep:
            continue
        out.append({
            "loc_name":     row[0],
            "year_month":   row[1],
            "platform":     row[2],
            "guests":       _safe_int(row[3]),
            "total_sales":  _safe_float(row[4]),
            "service":      _safe_float(row[5]),
            "product":      _safe_float(row[6]),
            "product_pct":  _safe_float(row[7]),
            "ppg":          _safe_float(row[8]),
            "pph":          _safe_float(row[9]),
            "avg_ticket":   _safe_float(row[10]),
            "prod_hours":   _safe_float(row[11]),
            "wax_count":    _safe_int(row[12]),
            "wax":          _safe_float(row[13]),
            "wax_pct":      _safe_float(row[14]),
            "color":        _safe_float(row[15]),
            "color_pct":    _safe_float(row[16]),
            "treat_count":  _safe_int(row[17]),
            "treat":        _safe_float(row[18]),
            "treat_pct":    _safe_float(row[19]),
            "source":       row[20],
            "period_start": row[21],
            "period_end":   row[22],
        })
    log.info("Loaded %d DATA_MONTHLY rows (filter=%s)", len(out), year_months or "all")
    return out


def read_monthly_goals(service, config: dict, year_month: str) -> dict[str, dict]:
    """Read MONTHLY_GOALS rows for a year_month. Returns {loc_name: row_dict}.

    Returns {} if the tab doesn't exist or has no rows for that month. The
    report builder uses this to populate the Goal / Daily Goal / Day Goal
    columns on the YoY tab.
    """
    sheet_id = config["sheet_id"]
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=sheet_id,
            range="MONTHLY_GOALS!A2:F",
            valueRenderOption="UNFORMATTED_VALUE",
            dateTimeRenderOption="FORMATTED_STRING",
        ).execute()
    except Exception as e:
        log.info("MONTHLY_GOALS tab not yet present (or read failed): %s", e)
        return {}

    rows = result.get("values", [])
    out: dict[str, dict] = {}
    for row in rows:
        row = row + [""] * (6 - len(row))
        if not row[0] or row[1] != year_month:
            continue
        out[row[0]] = {
            "loc_name":           row[0],
            "year_month":         row[1],
            "monthly_goal":       _safe_float(row[2]),
            "daily_goal_divisor": _safe_int(row[3]),
            "day_goal_divisor":   _safe_int(row[4]),
            "source":             row[5],
        }
    log.info("Loaded %d MONTHLY_GOALS rows for %s", len(out), year_month)
    return out


def derive_year_month(week_ending: str) -> str:
    """'2026-04-12' -> '2026-04'. Tolerant of empty/short input.

    Month-boundary rule (per Karissa Q2/Q3): Week 1 of a month starts on
    the 1st, last week ends on month-end. So the year_month is always
    derivable from the first 7 chars of any valid week_ending.
    """
    return week_ending[:7] if week_ending and len(week_ending) >= 7 else ""


def find_latest_priors_by_location(
    snapshots: list[dict],
    current_week_ending: str,
) -> dict[str, dict]:
    """For each loc_name in snapshots, return the snapshot with the largest
    week_ending that is STRICTLY LESS THAN current_week_ending.

    Returns {} if no priors exist (e.g., Week 1 of month — all snapshots
    are the current ones we just stored).
    """
    by_loc: dict[str, dict] = {}
    for s in snapshots:
        we = s.get("week_ending", "")
        if not we or we >= current_week_ending:
            continue  # skip empty, current, or future
        loc = s.get("loc_name", "")
        if not loc:
            continue
        if loc not in by_loc or we > by_loc[loc]["week_ending"]:
            by_loc[loc] = s
    return by_loc


def find_latest_priors_by_stylist(
    snapshots: list[dict],
    current_week_ending: str,
) -> dict[tuple[str, str], dict]:
    """For each (name, loc_name), return the snapshot with the largest
    week_ending < current_week_ending. Returns {} if none."""
    by_key: dict[tuple[str, str], dict] = {}
    for s in snapshots:
        we = s.get("week_ending", "")
        if not we or we >= current_week_ending:
            continue
        key = (s.get("name", ""), s.get("loc_name", ""))
        if not key[0]:
            continue
        if key not in by_key or we > by_key[key]["week_ending"]:
            by_key[key] = s
    return by_key


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
