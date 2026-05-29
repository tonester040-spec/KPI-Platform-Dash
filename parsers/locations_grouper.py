"""
KPI LOCATIONS_DATA generator — SALON-grain table (Tier 1 + Tier 2).

DATA SOURCE (2026-05-28 pivot): salon-grain inputs come from the **Salon Summary
.xls/.pdf** (one file per window), parsed by `pdf_hours_parser.parse_salon_summary`:
  - SALES block        → service_net, product_net
  - SERVICE DETAILS    → color/wax(+waxing)/treatment qty + net
  - Invoice Summary    → guest_count ("Total invoices with services or product")
  - Employee Perf      → productive_hours (per-stylist sum)
One Salon Summary file already carries its date range, so there is NO Python
date-windowing here — the file IS the window.

The Tableau revenue/guest CSV path, the encoding sniffer, and guest-shape
detection live on the STYLISTS_DATA / grouper_v3 path, NOT here.

ENGINE FROZEN: `_build_row`, the 39-column schema, the derivations, None-safety,
and tier logic are unchanged from the original build. Only the extraction layer
swapped (Tableau CSV → Salon Summary).

  Tier 1 (built + populated): extracted + derived current-week metrics + audit.
  Tier 2 (built + populated): projection, monthly_goal (from goals dict), daily_goal.
  Tier 3 (columns present, value None): prior_year_*, *_yoy, baseline_2019.

Salon-supremacy: guest_count is the salon-level invoice count straight from the
Invoice Summary — never a sum of per-stylist counts.
"""
from __future__ import annotations

import hashlib
from datetime import datetime, timezone

# pandas is guarded so the schema + pure helpers import even where pandas isn't
# installed. run_month_locations (DataFrame assembly) raises if it's missing.
try:
    import pandas as pd  # type: ignore
except ImportError:  # pragma: no cover
    pd = None  # type: ignore

from parsers.pdf_hours_parser import parse_salon_summary


# ─────────────────────────────────────────────────────────────────────────────
# SCHEMA — single source of truth. Order MUST match KPI_LOCATIONS_DATA_Schema.xlsx.
# tier ∈ {"key", "1", "2", "3"}.
# ─────────────────────────────────────────────────────────────────────────────
SCHEMA: list[tuple[str, str]] = [
    ("location_id",              "key"),
    ("location_name_canonical",  "key"),
    ("tableau_center_name",      "key"),
    ("pos_system",               "key"),
    ("period_start",             "key"),
    ("period_end",               "key"),
    ("period_type",              "key"),
    ("period_label",             "1"),
    ("guest_count",              "1"),
    ("total_sales_net",          "1"),
    ("service_net",              "1"),
    ("product_net",              "1"),
    ("productive_hours",         "1"),
    ("product_pct",              "1"),
    ("ppg_net",                  "1"),
    ("avg_ticket",               "1"),
    ("pph_net",                  "1"),
    ("wax_count",                "1"),
    ("wax_net",                  "1"),
    ("color_net",                "1"),
    ("treatment_count",          "1"),
    ("treatment_net",            "1"),
    ("wax_pct",                  "1"),
    ("color_pct",                "1"),
    ("treatment_pct",            "1"),
    ("projection",               "2"),
    ("monthly_goal",             "2"),
    ("daily_goal",               "2"),
    ("prior_year_total_sales",   "3"),
    ("prior_year_guest_count",   "3"),
    ("prior_year_ppg",           "3"),
    ("prior_year_avg_ticket",    "3"),
    ("ppg_diff_yoy",             "3"),
    ("avg_ticket_diff_yoy",      "3"),
    ("sales_pct_yoy",            "3"),
    ("baseline_2019_total",      "3"),
    ("source_extract_hash",      "1"),
    ("generated_at",             "1"),
    ("data_complete_flag",       "1"),
]
COLUMNS: list[str] = [name for name, _ in SCHEMA]
TIER3_FIELDS: list[str] = [name for name, tier in SCHEMA if tier == "3"]

# "HAVE" fields — if ANY is null the row is provisional (data_complete_flag=False).
HAVE_FIELDS: list[str] = [
    "guest_count", "total_sales_net", "service_net", "product_net", "productive_hours",
]


# ─────────────────────────────────────────────────────────────────────────────
# CROSSWALK — join by location_id, NEVER by name. Forest Lake complete; rest pending.
# ─────────────────────────────────────────────────────────────────────────────
CROSSWALK: dict[str, dict[str, str]] = {
    # 9 Zenoti salons — location_id = Zenoti org-center code (prefix of the Tableau
    # "center name"). Verified live from the KPI_Extracts Revenue view 2026-05-29.
    "888-11812": {"location_name_canonical": "Forest Lake",  "tableau_center_name": "888-11812-Forest Lake",      "pos_system": "zenoti"},
    "888-10278": {"location_name_canonical": "Andover FS",   "tableau_center_name": "888-10278-Andover",          "pos_system": "zenoti"},
    "888-9816":  {"location_name_canonical": "Blaine",       "tableau_center_name": "888-9816-Blaine",            "pos_system": "zenoti"},
    "888-7663":  {"location_name_canonical": "Crystal FS",   "tableau_center_name": "888-7663-Crystal",           "pos_system": "zenoti"},
    "887-10199": {"location_name_canonical": "Elk River FS", "tableau_center_name": "887-10199-Elk River",        "pos_system": "zenoti"},
    "888-40098": {"location_name_canonical": "Roseville",    "tableau_center_name": "888-40098-FS Roseville, MN", "pos_system": "zenoti"},
    "888-11091": {"location_name_canonical": "Prior Lake",   "tableau_center_name": "888-11091-Prior Lake",       "pos_system": "zenoti"},
    "910-7232":  {"location_name_canonical": "Hudson",       "tableau_center_name": "910-7232-Hudson",            "pos_system": "zenoti"},
    "910-6916":  {"location_name_canonical": "New Richmond", "tableau_center_name": "910-6916-New Richmond",      "pos_system": "zenoti"},
    # 3 Salon Ultimate salons — separate SU export, NOT in Zenoti Tableau.
    # location_id + center string TBD when the SU intake is wired:
    #   "Apple Valley", "Lakeville", "Farmington"  (pos_system="salon_ultimate")
}

# Reverse map: raw Tableau "center name" -> location_id (for the stylist/Tableau
# path to resolve a center string to its canonical location; excludes the "All" row).
CENTER_NAME_TO_ID: dict[str, str] = {
    v["tableau_center_name"]: lid for lid, v in CROSSWALK.items() if v.get("tableau_center_name")
}


def _safe_div(num, den, ndigits):
    """None if numerator None or denominator None/0/NaN. Never raises."""
    if num is None or den is None:
        return None
    try:
        if den == 0 or (pd is not None and pd.isna(den)) or (pd is not None and pd.isna(num)):
            return None
    except TypeError:
        return None
    return round(num / den, ndigits)


def _lookup_goal(goals, location_id, canonical_name):
    """monthly_goal from a goals dict keyed by location_id (fallback canonical
    name). Value may be a scalar or a dict with 'monthly_goal'. None if absent."""
    if not goals:
        return None
    for key in (location_id, canonical_name):
        if key in goals:
            v = goals[key]
            if isinstance(v, dict):
                return v.get("monthly_goal")
            return v
    return None


def _build_row(*, location_id, period_start, period_end, period_type, period_label,
               metrics, guest_count, unique_guests, productive_hours,
               monthly_goal, extract_hash, generated_at, extra_flags):
    """Assemble one schema-ordered salon row. All derived fields None-safe.
    *** ENGINE — unchanged from the original build. Do not alter. ***"""
    xw = CROSSWALK.get(location_id, {})
    service_net = metrics.get("service_net")
    product_net = metrics.get("product_net")
    total_sales_net = metrics.get("total_sales_net")

    # derived (None-safe; denominators guarded)
    product_pct = _safe_div(product_net, total_sales_net, 4)
    ppg_net = _safe_div(product_net, guest_count, 2)
    avg_ticket = _safe_div(total_sales_net, guest_count, 2)   # CONFIRMED 2026-05-29: total/guests (= Zenoti "Avg invoice $"; Karissa Q2 resolved)
    pph_net = _safe_div(total_sales_net, productive_hours, 2)  # schema: total/hours
    color_net = metrics.get("color_net")
    wax_count = metrics.get("wax_count")
    treatment_count = metrics.get("treatment_count")
    wax_pct = _safe_div(wax_count, guest_count, 4)
    color_pct = _safe_div(color_net, service_net, 4)
    treatment_pct = _safe_div(treatment_count, guest_count, 4)

    projection = _safe_div(total_sales_net, 2, 2)
    projection = round(projection * 25, 2) if projection is not None else None
    daily_goal = _safe_div(monthly_goal, 26, 2)

    row = {
        "location_id": location_id,
        "location_name_canonical": xw.get("location_name_canonical"),
        "tableau_center_name": xw.get("tableau_center_name"),
        "pos_system": xw.get("pos_system"),
        "period_start": str(period_start),
        "period_end": str(period_end),
        "period_type": period_type,
        "period_label": period_label,
        "guest_count": guest_count,
        "total_sales_net": total_sales_net,
        "service_net": service_net,
        "product_net": product_net,
        "productive_hours": productive_hours,
        "product_pct": product_pct,
        "ppg_net": ppg_net,
        "avg_ticket": avg_ticket,
        "pph_net": pph_net,
        "wax_count": wax_count,
        "wax_net": metrics.get("wax_net"),
        "color_net": color_net,
        "treatment_count": treatment_count,
        "treatment_net": metrics.get("treatment_net"),
        "wax_pct": wax_pct,
        "color_pct": color_pct,
        "treatment_pct": treatment_pct,
        "projection": projection,
        "monthly_goal": monthly_goal,
        "daily_goal": daily_goal,
        "source_extract_hash": extract_hash,
        "generated_at": generated_at,
    }
    for f in TIER3_FIELDS:
        row[f] = None  # blocked on historical store — column present, value null

    row["data_complete_flag"] = all(row.get(f) is not None for f in HAVE_FIELDS)
    return row


# ─────────────────────────────────────────────────────────────────────────────
# Salon-Summary-sourced orchestration (replaces the Tableau CSV run_month).
# ─────────────────────────────────────────────────────────────────────────────
def build_location_row(salon_summary_path, *, location_id, period_start, period_end,
                       period_type, period_label, goals=None):
    """Parse ONE Salon Summary file → one schema-ordered salon row (dict).
    The file already defines the window; we do not re-window in Python."""
    ss = parse_salon_summary(salon_summary_path)
    metrics = {
        "service_net": ss["service_net"],
        "product_net": ss["product_net"],
        "total_sales_net": ss["total_sales_net"],
        "color_net": ss["color_net"],
        "wax_net": ss["wax_net"],
        "wax_count": ss["wax_count"],
        "treatment_net": ss["treatment_net"],
        "treatment_count": ss["treatment_count"],
    }
    canonical = CROSSWALK.get(location_id, {}).get("location_name_canonical")
    extract_hash = hashlib.sha256(open(salon_summary_path, "rb").read()).hexdigest()
    return _build_row(
        location_id=location_id, period_start=period_start, period_end=period_end,
        period_type=period_type, period_label=period_label, metrics=metrics,
        guest_count=ss["guest_count"], unique_guests=None,
        productive_hours=ss["productive_hours"],
        monthly_goal=_lookup_goal(goals, location_id, canonical),
        extract_hash=extract_hash, generated_at=datetime.now(timezone.utc).isoformat(),
        extra_flags=None,
    )


def run_month_locations(window_files, location_id, goals=None):
    """Build a DataFrame[COLUMNS] from a list of per-window Salon Summary files.

    window_files: list of {"period_type","period_start","period_end",
                            "period_label","path"} — one Salon Summary file each
                  (e.g. weekly + mtd exports for a month from Zenoti).
    """
    if pd is None:
        raise RuntimeError("pandas is required to assemble the DataFrame (pip install pandas).")
    rows = [
        build_location_row(
            wf["path"], location_id=location_id,
            period_start=wf["period_start"], period_end=wf["period_end"],
            period_type=wf["period_type"], period_label=wf["period_label"], goals=goals,
        )
        for wf in window_files
    ]
    return pd.DataFrame(rows, columns=COLUMNS)  # columns= enforces exact order


def reconcile_check(out):
    """Salon weekly metric sums vs final MTD row. Nets MUST reconcile to 0.00.
    guest_count is EXPECTED to vary (MTD distinct ≤ sum of weekly distincts)."""
    weekly = out[out.period_type == "weekly"]
    mtd = out[out.period_type == "mtd"]
    if mtd.empty:
        return {}
    final_end = mtd.period_end.max()
    final_mtd = mtd[mtd.period_end == final_end]
    diffs = {}
    for m in ["service_net", "product_net", "total_sales_net", "color_net", "wax_net", "treatment_net"]:
        wk = round(float(weekly[m].fillna(0).sum()), 2)
        md = round(float(final_mtd[m].fillna(0).sum()), 2)
        diffs[m] = round(wk - md, 2)
    diffs["_guest_count_note"] = "weekly-sum vs MTD expected to differ (salon-supremacy)"
    return diffs


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Build one LOCATIONS_DATA row from a Salon Summary file.")
    ap.add_argument("--summary", required=True, help="Salon Summary .xls/.pdf path")
    ap.add_argument("--location-id", default="888-11812")
    ap.add_argument("--period-start", required=True)
    ap.add_argument("--period-end", required=True)
    ap.add_argument("--period-type", default="mtd", choices=["weekly", "mtd"])
    ap.add_argument("--period-label", default="MTD")
    args = ap.parse_args()
    row = build_location_row(args.summary, location_id=args.location_id,
                             period_start=args.period_start, period_end=args.period_end,
                             period_type=args.period_type, period_label=args.period_label)
    for c in COLUMNS:
        print(f"  {c:<26} {row[c]}")
