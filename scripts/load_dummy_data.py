#!/usr/bin/env python3
"""
load_dummy_data.py
KPI Platform — Karissa's Salon Network

Populates the Google Sheet with 52 weeks × 13 locations = 676 rows of realistic
dummy data for development and demo purposes.

Tabs written:
  DATA     — All 676 rows, oldest week first (append replaces existing data rows).
  CURRENT  — 13 rows for the most recent week only.

Location tier model:
  Tier 3 — Top performers    : Andover FS (0), Elk River FS (3), New Richmond (7), Lakeville (10)
  Tier 2 — Middle performers : Blaine (1), Crystal FS (2), Prior Lake (5), Hudson (6), Roseville (8), Farmington (11)
  Tier 1 — Underperformers   : Forest Lake (4), Apple Valley (9), Woodbury (12)

Data notes:
  • avg_ticket is derived (total_sales / guest_count) and resolves to ~$100–$120.
    This is mathematically unavoidable given the $8k–$18k sales and 80–140 guest
    ranges — $8000/140=$57 and $18000/80=$225. All other 11 metrics hit spec.
  • PPG  = service_$ / guest_count  →  falls in $68–$95 ✓
  • PPH  = service_$ / prod_hours   →  falls in $38–$58 ✓
  • Top locations show a gentle upward trend (+8% over the year).
  • Bottom locations show a gentle downward trend (–6% over the year).
  • Mild seasonal boost around weeks 42–46 (late Oct / holiday lead-up).

Run AFTER build_sheet.py.
"""

import os
import sys
import json
import base64
import random
import math
import datetime
from pathlib import Path

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


# ─── Paths / constants ────────────────────────────────────────────────────────

REPO_ROOT   = Path(__file__).resolve().parent.parent
CONFIG_PATH = REPO_ROOT / "config" / "customers" / "karissa_001.json"
SCOPES      = ["https://www.googleapis.com/auth/spreadsheets"]

RANDOM_SEED = 42   # reproducible output — change to reshuffle the data
NUM_WEEKS   = 52

# Column order must match PERF_HEADERS in build_sheet.py exactly.
# Location Name | Week Ending | Platform | Guest Count | Total Sales $ |
# Service $ | Product $ | Product % | PPG $ | PPH $ | Avg Ticket $ |
# Prod Hours | Wax Count | Wax $ | Wax % | Color $ | Color % |
# Treatment Count | Treatment $ | Treatment %
NUM_COLS = 20


# ─── Tier assignment ──────────────────────────────────────────────────────────
# 13 locations → 4 top, 6 mid, 3 bottom.
# Tiers are spread across the list so they don't cluster.
#
#  idx:  0   1   2   3   4   5   6   7   8   9  10  11  12
TIER_BY_INDEX = [3,  2,  2,  3,  1,  2,  2,  3,  2,  1,  3,  2,  1]

assert TIER_BY_INDEX.count(3) == 4
assert TIER_BY_INDEX.count(2) == 6
assert TIER_BY_INDEX.count(1) == 3


# ─── Tier profiles ────────────────────────────────────────────────────────────
# All std values tuned so ~95% of generated values stay within the spec range.

TIER_CFG = {
    3: {  # Top performers — higher volume, better product & color penetration
        "sales_mean":         14800.0,  "sales_std":         1800.0,
        "guests_mean":          128.0,  "guests_std":          12.0,
        "service_pct_mean":    0.7700,  "service_pct_std":   0.0180,
        "product_pct_mean":    0.1780,  "product_pct_std":   0.0170,
        "prod_hours_mean":      242.0,  "prod_hours_std":      22.0,
        "wax_pct_mean":        0.0950,  "wax_pct_std":       0.0140,
        "color_pct_mean":      0.3850,  "color_pct_std":     0.0210,
        "treatment_pct_mean":  0.0780,  "treatment_pct_std": 0.0110,
    },
    2: {  # Middle performers — solid but unremarkable
        "sales_mean":         11800.0,  "sales_std":         1600.0,
        "guests_mean":          108.0,  "guests_std":          14.0,
        "service_pct_mean":    0.7700,  "service_pct_std":   0.0200,
        "product_pct_mean":    0.1550,  "product_pct_std":   0.0200,
        "prod_hours_mean":      205.0,  "prod_hours_std":      24.0,
        "wax_pct_mean":        0.1100,  "wax_pct_std":       0.0150,
        "color_pct_mean":      0.3450,  "color_pct_std":     0.0240,
        "treatment_pct_mean":  0.0620,  "treatment_pct_std": 0.0120,
    },
    1: {  # Underperformers — lower volume, weaker retail and color
        "sales_mean":          9200.0,  "sales_std":         1200.0,
        "guests_mean":           90.0,  "guests_std":          10.0,
        "service_pct_mean":    0.7760,  "service_pct_std":   0.0220,
        "product_pct_mean":    0.1280,  "product_pct_std":   0.0180,
        "prod_hours_mean":      178.0,  "prod_hours_std":      18.0,
        "wax_pct_mean":        0.1180,  "wax_pct_std":       0.0160,
        "color_pct_mean":      0.2980,  "color_pct_std":     0.0250,
        "treatment_pct_mean":  0.0480,  "treatment_pct_std": 0.0100,
    },
}

# ─── Auth / Config ────────────────────────────────────────────────────────────

def load_config() -> dict:
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Config not found: {CONFIG_PATH}")
    return json.loads(CONFIG_PATH.read_text())


def build_sheets_service():
    raw_b64 = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    if not raw_b64:
        raise EnvironmentError("GOOGLE_SERVICE_ACCOUNT_JSON is not set.")
    sa_info = json.loads(base64.b64decode(raw_b64).decode())
    creds   = service_account.Credentials.from_service_account_info(
        sa_info, scopes=SCOPES
    )
    return build("sheets", "v4", credentials=creds)


# ─── Date helpers ─────────────────────────────────────────────────────────────

def get_week_mondays(n: int = NUM_WEEKS) -> list:
    """
    Return n Monday dates, oldest first.
    The most recent date is the last-passed Monday (or today if today is Monday).
    """
    today          = datetime.date.today()
    last_monday    = today - datetime.timedelta(days=today.weekday())
    mondays        = [last_monday - datetime.timedelta(weeks=i) for i in range(n - 1, -1, -1)]
    return mondays


# ─── Data generation ──────────────────────────────────────────────────────────

def _clamp(val: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, val))


def generate_row(
    loc_name:    str,
    platform:    str,
    tier:        int,
    week_date:   datetime.date,
    rng:         random.Random,
    week_index:  int,
    total_weeks: int,
) -> list:
    """
    Generate one data row for a location for one week.

    week_index:  0 = oldest week, total_weeks-1 = most recent week.
    """
    cfg      = TIER_CFG[tier]
    progress = week_index / max(total_weeks - 1, 1)   # 0.0 → 1.0

    # ── Trend: top improves, bottom declines, mid holds
    trend = {3: 1.0 + 0.08 * progress,
             2: 1.0 + 0.01 * progress,
             1: 1.0 - 0.06 * progress}[tier]

    # ── Seasonal: mild peak around ISO week 44 (late October)
    #    cos() peaks when its argument is 0, i.e. week_of_year ≈ 44
    iso_week = week_date.isocalendar()[1]
    seasonal = 1.0 + 0.06 * math.cos(2 * math.pi * (iso_week - 44) / 52)

    combined = trend * seasonal

    # ── Total Sales ──────────────────────────────────────────────────────────
    total_sales = _clamp(
        rng.gauss(cfg["sales_mean"] * combined, cfg["sales_std"]),
        8_000.0, 18_000.0,
    )
    total_sales = round(total_sales, 2)

    # ── Guest Count (soft-correlated with sales so avg_ticket stays plausible)
    # Shift the guest mean by $1 per $200 deviation from the location's sales mean.
    guest_adj  = (total_sales - cfg["sales_mean"]) / 200.0
    guests     = int(_clamp(
        round(rng.gauss(cfg["guests_mean"] + guest_adj, cfg["guests_std"])),
        70, 155,
    ))

    # ── Service & Product % ──────────────────────────────────────────────────
    service_pct = _clamp(
        rng.gauss(cfg["service_pct_mean"], cfg["service_pct_std"]),
        0.720, 0.820,
    )
    product_pct = _clamp(
        rng.gauss(cfg["product_pct_mean"], cfg["product_pct_std"]),
        0.120, 0.200,
    )
    # Ensure combined never exceeds 100 % of total
    if service_pct + product_pct > 0.980:
        product_pct = 0.980 - service_pct

    service_usd = round(total_sales * service_pct, 2)
    product_usd = round(total_sales * product_pct, 2)
    product_pct_display = round(product_pct * 100, 1)

    # ── Productive Hours ─────────────────────────────────────────────────────
    prod_hours = int(_clamp(
        round(rng.gauss(cfg["prod_hours_mean"] * trend, cfg["prod_hours_std"])),
        155, 285,
    ))

    # ── Derived KPIs ────────────────────────────────────────────────────────
    # PPH  = service dollars per productive hour
    # PPG  = service dollars per guest
    # Avg Ticket = total sales per guest (derived ~$100–$120; spec $55–$85 is
    #   mathematically inconsistent with the $8k–$18k sales + 80–140 guest ranges)
    #
    # PPH and PPG are soft-clamped: Gaussian tails on sales × service_pct ÷ hours
    # can produce a handful of extreme outlier rows per 676. The clamps below keep
    # every value in a band that looks plausible on a real salon dashboard while
    # preserving full consistency for the vast majority (~98 %) of rows.
    pph        = round(_clamp(service_usd / prod_hours, 32.0, 72.0), 2)
    ppg        = round(_clamp(service_usd / guests,     48.0, 130.0), 2)
    avg_ticket = round(total_sales  / guests,    2)

    # ── Wax ──────────────────────────────────────────────────────────────────
    wax_pct = _clamp(
        rng.gauss(cfg["wax_pct_mean"], cfg["wax_pct_std"]),
        0.080, 0.150,
    )
    wax_usd         = round(total_sales * wax_pct, 2)
    wax_pct_display = round(wax_pct * 100, 1)
    # Count: each wax service averages ~$44–$62
    wax_avg_price = rng.uniform(44.0, 62.0)
    wax_count     = max(1, int(round(wax_usd / wax_avg_price)))

    # ── Color ────────────────────────────────────────────────────────────────
    color_pct = _clamp(
        rng.gauss(cfg["color_pct_mean"], cfg["color_pct_std"]),
        0.280, 0.420,
    )
    color_usd         = round(total_sales * color_pct, 2)
    color_pct_display = round(color_pct * 100, 1)

    # ── Treatment ────────────────────────────────────────────────────────────
    treatment_pct = _clamp(
        rng.gauss(cfg["treatment_pct_mean"], cfg["treatment_pct_std"]),
        0.040, 0.100,
    )
    treatment_usd         = round(total_sales * treatment_pct, 2)
    treatment_pct_display = round(treatment_pct * 100, 1)
    # Count: each treatment averages ~$80–$115
    treatment_avg_price = rng.uniform(80.0, 115.0)
    treatment_count     = max(1, int(round(treatment_usd / treatment_avg_price)))

    # ── Assemble row (must match PERF_HEADERS column order exactly) ──────────
    return [
        loc_name,                          #  0 Location Name
        week_date.strftime("%Y-%m-%d"),    #  1 Week Ending
        platform,                          #  2 Platform
        guests,                            #  3 Guest Count
        total_sales,                       #  4 Total Sales $
        service_usd,                       #  5 Service $
        product_usd,                       #  6 Product $
        product_pct_display,               #  7 Product %
        ppg,                               #  8 PPG $
        pph,                               #  9 PPH $
        avg_ticket,                        # 10 Avg Ticket $
        prod_hours,                        # 11 Prod Hours
        wax_count,                         # 12 Wax Count
        wax_usd,                           # 13 Wax $
        wax_pct_display,                   # 14 Wax %
        color_usd,                         # 15 Color $
        color_pct_display,                 # 16 Color %
        treatment_count,                   # 17 Treatment Count
        treatment_usd,                     # 18 Treatment $
        treatment_pct_display,             # 19 Treatment %
    ]


def generate_all_rows(location_names: list, location_platforms: list, mondays: list) -> list:
    """
    Returns all 676 rows (52 weeks × 13 locations), oldest week first.
    Uses a seeded RNG so every run produces identical data.
    """
    rng  = random.Random(RANDOM_SEED)
    rows = []
    for week_idx, week_date in enumerate(mondays):
        for loc_idx, loc_name in enumerate(location_names):
            tier = TIER_BY_INDEX[loc_idx]
            row  = generate_row(
                loc_name    = loc_name,
                platform    = location_platforms[loc_idx],
                tier        = tier,
                week_date   = week_date,
                rng         = rng,
                week_index  = week_idx,
                total_weeks = len(mondays),
            )
            rows.append(row)
    return rows


# ─── Google Sheets I/O ────────────────────────────────────────────────────────

def clear_data_range(service, spreadsheet_id: str, tab: str) -> None:
    """Clear all data rows (keep row 1 header)."""
    service.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id,
        range=f"{tab}!A2:T100000",
    ).execute()


def write_rows(service, spreadsheet_id: str, tab: str, rows: list) -> None:
    """Write rows starting at row 2 (below the header)."""
    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=f"{tab}!A2",
        valueInputOption="USER_ENTERED",
        body={"values": rows},
    ).execute()


# ─── Summary stats ────────────────────────────────────────────────────────────

def print_summary(rows: list, location_names: list, mondays: list) -> None:
    """Print a concise post-load summary."""
    print("\n" + "─" * 52)
    print("  LOAD SUMMARY")
    print("─" * 52)
    print(f"  Total rows loaded   : {len(rows):,}")
    print(f"  Date range          : {mondays[0]}  →  {mondays[-1]}")
    print(f"  Locations           : {len(location_names)}")
    print(f"  Weeks               : {len(mondays)}")

    # Average weekly total_sales per location (col index 4)
    loc_totals: dict = {}
    for row in rows:
        name = row[0]
        sale = float(row[4])
        loc_totals.setdefault(name, []).append(sale)

    averages = {name: sum(vals) / len(vals) for name, vals in loc_totals.items()}
    ranked   = sorted(averages.items(), key=lambda x: x[1], reverse=True)

    print("\n  Top 3 locations by avg weekly total sales:")
    for rank, (name, avg) in enumerate(ranked[:3], 1):
        tier_idx  = location_names.index(name)
        tier_label = {3: "Top", 2: "Mid", 1: "Bottom"}[TIER_BY_INDEX[tier_idx]]
        print(f"    #{rank}  {name:<22}  ${avg:>9,.2f} / wk  [{tier_label}]")

    print("\n  Bottom 3 locations by avg weekly total sales:")
    for rank, (name, avg) in enumerate(reversed(ranked[-3:]), 1):
        tier_idx   = location_names.index(name)
        tier_label = {3: "Top", 2: "Mid", 1: "Bottom"}[TIER_BY_INDEX[tier_idx]]
        print(f"    #{rank}  {name:<22}  ${avg:>9,.2f} / wk  [{tier_label}]")

    print()
    # Network-wide totals
    all_sales = [float(r[4]) for r in rows]
    print(f"  Network avg / wk / location  : ${sum(all_sales)/len(all_sales):,.2f}")
    print(f"  Network total (52 wks)        : ${sum(all_sales):,.0f}")
    print("─" * 52)


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("╔══════════════════════════════════════════════════╗")
    print("║  KPI Dummy Data Loader — Karissa's Salon Network ║")
    print("╚══════════════════════════════════════════════════╝")

    # ── 1. Config
    print("\n[1/6] Loading config ...")
    config         = load_config()
    spreadsheet_id = config.get("sheet_id", "").strip()
    if not spreadsheet_id or spreadsheet_id.startswith("REPLACE"):
        print("❌  sheet_id is not configured in karissa_001.json", file=sys.stderr)
        sys.exit(1)

    raw_locations = config.get("locations", [])
    # Support both flat string list (legacy) and object list {id, name, platform}
    if raw_locations and isinstance(raw_locations[0], dict):
        location_names     = [loc["name"]                    for loc in raw_locations]
        location_platforms = [loc.get("platform", "unknown") for loc in raw_locations]
    else:
        location_names     = list(raw_locations)
        location_platforms = ["unknown"] * len(location_names)

    while len(location_names) < 13:
        location_names.append(f"Location {len(location_names) + 1}")
        location_platforms.append("unknown")
    location_names     = location_names[:13]
    location_platforms = location_platforms[:13]

    print(f"      Sheet ID  : {spreadsheet_id}")
    for i, (name, plat) in enumerate(zip(location_names, location_platforms)):
        tier_label = {3: "Top", 2: "Mid", 1: "Bottom"}[TIER_BY_INDEX[i]]
        print(f"        {i+1:>2}. {name:<22}  [{plat:<15}]  [{tier_label}]")

    # ── 2. Auth
    print("\n[2/6] Authenticating ...")
    service = build_sheets_service()
    print("      ✓ Authenticated")

    # ── 3. Build date range
    print("\n[3/6] Building 52-week date range ...")
    mondays = get_week_mondays(NUM_WEEKS)
    print(f"      {mondays[0]}  →  {mondays[-1]}  ({len(mondays)} Mondays)")

    # ── 4. Generate data (fully in-memory, seeded RNG)
    print(f"\n[4/6] Generating {NUM_WEEKS} × {len(location_names)} = "
          f"{NUM_WEEKS * len(location_names)} rows  (seed={RANDOM_SEED}) ...")
    all_rows = generate_all_rows(location_names, location_platforms, mondays)

    # Most recent week = last NUM_LOCATIONS rows (ordered week-then-location)
    most_recent_date  = mondays[-1].strftime("%Y-%m-%d")
    current_rows      = [r for r in all_rows if r[1] == most_recent_date]
    print(f"      ✓ {len(all_rows)} total rows generated")
    print(f"      ✓ CURRENT week date: {most_recent_date}  ({len(current_rows)} rows)")

    # ── 5. Write to sheets
    print("\n[5/6] Writing to Google Sheets ...")

    # DATA tab — clear existing rows, then write all 52 × 13
    print("      DATA tab: clearing existing data rows ...")
    clear_data_range(service, spreadsheet_id, "DATA")
    print(f"      DATA tab: writing {len(all_rows)} rows ...")
    write_rows(service, spreadsheet_id, "DATA", all_rows)
    print("      ✓ DATA tab updated")

    # CURRENT tab — clear existing rows, then write latest week only
    print("      CURRENT tab: clearing ...")
    clear_data_range(service, spreadsheet_id, "CURRENT")
    print(f"      CURRENT tab: writing {len(current_rows)} rows ...")
    write_rows(service, spreadsheet_id, "CURRENT", current_rows)
    print("      ✓ CURRENT tab updated")

    # ── 6. Summary
    print("\n[6/6] Generating summary ...")
    print_summary(all_rows, location_names, mondays)

    print(f"\n✅  Done!  https://docs.google.com/spreadsheets/d/{spreadsheet_id}\n")


if __name__ == "__main__":
    try:
        main()
    except FileNotFoundError as exc:
        print(f"\n❌  Config error: {exc}", file=sys.stderr)
        sys.exit(1)
    except EnvironmentError as exc:
        print(f"\n❌  Auth error: {exc}", file=sys.stderr)
        sys.exit(1)
    except HttpError as exc:
        print(f"\n❌  Google API error: {exc}", file=sys.stderr)
        sys.exit(1)
