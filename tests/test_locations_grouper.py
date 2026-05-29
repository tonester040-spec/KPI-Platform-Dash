"""
Tests for locations_grouper (salon-grain LOCATIONS_DATA, Salon Summary source).

Layers:
  1. ENGINE      — schema order, _safe_div, _build_row derivation + None-safety (no files).
                   avg_ticket = total/guests (CONFIRMED 2026-05-29).
  2. PARSER      — parse_salon_summary on the real Forest Lake APRIL Salon Summary PDF.
  3. END-TO-END  — build_location_row on the PDF, derived metrics CROSS-CHECKED vs Zenoti's
                   own printed stats (avg_ticket == "Avg invoice $ (all items)").
  4. ACCEPTANCE  — real windowed .xls (4/1-4/12), penny/integer exact incl guest_count=254.

Run:  python test_locations_grouper.py
"""
from __future__ import annotations
import os
import sys

from pathlib import Path
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
from parsers import locations_grouper as L  # noqa: E402
from parsers import pdf_hours_parser as H  # noqa: E402

MONTHLY_PDF = r"C:\Users\TonyGrant\Downloads\KPI Historical Data\4-1-26 - 4-30-26\Forest Lake.pdf"
ROSEVILLE_PDF = r"C:\Users\TonyGrant\Downloads\KPI Historical Data\4-1-26 - 4-30-26\Roseville.pdf"
ROSEVILLE_XLS = r"C:\Users\TonyGrant\Downloads\Salon Summary Roseville 4-1 - 4-12.xls"
WINDOW_XLS_CANDIDATES = [
    os.environ.get("FL_SUMMARY_XLS"),
    r"C:\Users\TonyGrant\Downloads\Salon Summary Forest Lake.xls",
    "Salon Summary Forest Lake.xls",
]
FAILS: list[str] = []


def check(name, cond):
    print(("PASS" if cond else "FAIL"), "-", name)
    if not cond:
        FAILS.append(name)


def engine_tests():
    EXPECTED = ["location_id","location_name_canonical","tableau_center_name","pos_system",
        "period_start","period_end","period_type","period_label","guest_count","total_sales_net",
        "service_net","product_net","productive_hours","product_pct","ppg_net","avg_ticket","pph_net",
        "wax_count","wax_net","color_net","treatment_count","treatment_net","wax_pct","color_pct",
        "treatment_pct","projection","monthly_goal","daily_goal","prior_year_total_sales",
        "prior_year_guest_count","prior_year_ppg","prior_year_avg_ticket","ppg_diff_yoy",
        "avg_ticket_diff_yoy","sales_pct_yoy","baseline_2019_total","source_extract_hash",
        "generated_at","data_complete_flag"]
    check("39 columns, exact order", L.COLUMNS == EXPECTED)
    tiers = {}
    for _, t in L.SCHEMA:
        tiers[t] = tiers.get(t, 0) + 1
    check("tier counts key=7,1=21,2=3,3=8", tiers == {"key": 7, "1": 21, "2": 3, "3": 8})
    check("safe_div /0 None", L._safe_div(5, 0, 2) is None)
    check("safe_div None num None", L._safe_div(None, 3, 2) is None)
    check("safe_div normal", L._safe_div(945.50, 254, 2) == 3.72)

    # _build_row on FL 4/1-4/12 numbers, guest_count=254 (corrected)
    m = {"service_net": 12395.70, "product_net": 945.50, "total_sales_net": 13341.20,
         "color_net": 3600.50, "wax_net": 618.92, "wax_count": 33, "treatment_net": 664.80, "treatment_count": 38}
    r = L._build_row(location_id="888-11812", period_start="2026-04-01", period_end="2026-04-12",
        period_type="mtd", period_label="MTD thru Week 2", metrics=m, guest_count=254,
        unique_guests=None, productive_hours=267.37, monthly_goal=None,
        extract_hash="x", generated_at="t", extra_flags=None)
    check("engine product_pct 0.0709", r["product_pct"] == 0.0709)
    check("engine ppg_net 3.72", r["ppg_net"] == 3.72)
    check("engine avg_ticket 52.52 (total/guests)", r["avg_ticket"] == 52.52)
    check("engine pph_net 49.90 (total/hours)", r["pph_net"] == 49.90)
    check("engine projection 166765.00", r["projection"] == 166765.00)
    check("engine data_complete TRUE", r["data_complete_flag"] is True)
    check("engine 39 keys", set(r.keys()) == set(L.COLUMNS))
    check("engine tier3 None", all(r[f] is None for f in L.TIER3_FIELDS))

    # None-safety (closed week)
    mE = {k: None for k in ("service_net","product_net","total_sales_net","color_net","wax_net","wax_count","treatment_net","treatment_count")}
    rE = L._build_row(location_id="888-11812", period_start="2026-04-06", period_end="2026-04-12",
        period_type="weekly", period_label="Week 2", metrics=mE, guest_count=None,
        unique_guests=None, productive_hours=None, monthly_goal=None,
        extract_hash="x", generated_at="t", extra_flags=None)
    check("closed week avg_ticket None", rE["avg_ticket"] is None)
    check("closed week pph None", rE["pph_net"] is None)
    check("closed week data_complete FALSE", rE["data_complete_flag"] is False)
    # goals
    rg = L._build_row(location_id="888-11812", period_start="x", period_end="y", period_type="mtd",
        period_label="z", metrics=m, guest_count=254, unique_guests=None, productive_hours=267.37,
        monthly_goal=52000.0, extract_hash="x", generated_at="t", extra_flags=None)
    check("daily_goal = goal/26 (2000.0)", rg["daily_goal"] == 2000.0)


def parser_tests():
    if not os.path.exists(MONTHLY_PDF):
        print("SKIP - parser tests (Forest Lake PDF not found)"); return
    ss = H.parse_salon_summary(MONTHLY_PDF)
    exp = {"service_net": 29828.70, "product_net": 1861.35, "total_sales_net": 31690.05,
           "guest_count": 615, "unique_guests": 585, "color_net": 8436.49, "color_count": 108,
           "wax_net": 1549.37, "wax_count": 84, "treatment_net": 1582.66, "treatment_count": 90,
           "productive_hours": 660.87}
    for k, v in exp.items():
        check(f"parse(PDF) {k} == {v}", ss.get(k) == v)
    check("parse(PDF) service_mix RECONCILED (cats sum == service_net)", ss.get("service_mix_reconciled") is True)
    check("parse(PDF) service_mix gap 0.0", ss.get("service_mix_gap") == 0.0)


def end_to_end_realpdf():
    if not os.path.exists(MONTHLY_PDF):
        print("SKIP - end-to-end (Forest Lake PDF not found)"); return
    row = L.build_location_row(MONTHLY_PDF, location_id="888-11812",
        period_start="2026-04-01", period_end="2026-04-30", period_type="mtd", period_label="MTD thru April")
    check("e2e total_sales_net 31690.05", row["total_sales_net"] == 31690.05)
    check("e2e guest_count 615", row["guest_count"] == 615)
    check("e2e productive_hours 660.87", row["productive_hours"] == 660.87)
    # CROSS-CHECK vs Zenoti's own printed stats:
    check("e2e avg_ticket 51.53 == PDF 'Avg invoice $ (all items) 51.53'", row["avg_ticket"] == 51.53)
    check("e2e pph_net 47.95 == PDF 'Net Service and Product Sales 47.95'", row["pph_net"] == 47.95)
    check("e2e ppg_net 3.03 == PDF 'Net product sales $/invoice 3.03'", row["ppg_net"] == 3.03)
    check("e2e color_pct 0.2828 == PDF Color category (28.28)", row["color_pct"] == 0.2828)
    check("e2e product_pct 0.0587", row["product_pct"] == 0.0587)
    check("e2e data_complete TRUE", row["data_complete_flag"] is True)


def acceptance_xls():
    xls = next((c for c in WINDOW_XLS_CANDIDATES if c and os.path.exists(c)), None)
    if not xls:
        print("SKIP - acceptance: 'Salon Summary Forest Lake.xls' not found (set FL_SUMMARY_XLS)"); return
    print(f"(acceptance file: {xls})")
    ss = H.parse_salon_summary(xls)
    pexp = {"service_net": 12395.70, "product_net": 945.50, "total_sales_net": 13341.20,
            "guest_count": 254, "unique_guests": 247, "productive_hours": 267.37,
            "color_net": 3600.50, "wax_net": 618.92, "treatment_net": 664.80,
            "wax_count": 33, "treatment_count": 38}
    for k, v in pexp.items():
        check(f"ACCEPT parse {k} == {v}", ss.get(k) == v)
    check("ACCEPT service_mix RECONCILED (cats sum == service_net)", ss.get("service_mix_reconciled") is True)
    row = L.build_location_row(xls, location_id="888-11812", period_start="2026-04-01",
        period_end="2026-04-12", period_type="mtd", period_label="MTD thru Week 2")
    check("ACCEPT row guest_count 254", row["guest_count"] == 254)
    check("ACCEPT row avg_ticket 52.52 (total/guests)", row["avg_ticket"] == 52.52)
    check("ACCEPT row pph_net 49.90", row["pph_net"] == 49.90)
    check("ACCEPT row data_complete TRUE", row["data_complete_flag"] is True)


def roseville_recon():
    """2nd-location reconciliation — Roseville (alt 'FS ' service names, Waxing-only header)."""
    if not os.path.exists(ROSEVILLE_PDF):
        print("SKIP - Roseville (PDF not found)"); return
    ss = H.parse_salon_summary(ROSEVILLE_PDF)
    check("ROSE service_net 40791.64", ss.get("service_net") == 40791.64)
    check("ROSE color_net 11556.59", ss.get("color_net") == 11556.59)
    check("ROSE wax_net 1167.07 (Waxing-only header)", ss.get("wax_net") == 1167.07)
    check("ROSE treatment_net 1520.92", ss.get("treatment_net") == 1520.92)
    check("ROSE service_mix RECONCILED (cats sum == service_net, incl Styling+Texture)",
          ss.get("service_mix_reconciled") is True)
    check("ROSE service_mix gap 0.0", ss.get("service_mix_gap") == 0.0)
    found = set(ss.get("categories_found") or [])
    check("ROSE enumerates non-bucket cats (styling, texture, haircut)",
          {"styling", "texture", "haircut"}.issubset(found))


def roseville_windowed():
    """ITEM 1 — penny-verify the WINDOWED Roseville .xls (4/1-4/12, center 888-40098)."""
    if not os.path.exists(ROSEVILLE_XLS):
        print("SKIP - windowed Roseville .xls not found"); return
    print(f"(file: {ROSEVILLE_XLS})")
    ss = H.parse_salon_summary(ROSEVILLE_XLS)
    exp = {"service_net": 15413.15, "product_net": 298.99, "total_sales_net": 15712.14,
           "guest_count": 401, "unique_guests": 396, "color_net": 4449.00,
           "wax_net": 327.15, "treatment_net": 580.00, "wax_count": 17}
    for k, v in exp.items():
        check(f"ROSE.xls {k} == {v}", ss.get(k) == v)
    check("ROSE.xls service_mix RECONCILED", ss.get("service_mix_reconciled") is True)
    check("ROSE.xls service_mix gap 0.0", ss.get("service_mix_gap") == 0.0)
    print("   treatment_count =", ss.get("treatment_count"), "| categories_found =", ss.get("categories_found"))
    row = L.build_location_row(ROSEVILLE_XLS, location_id="888-40098", period_start="2026-04-01",
        period_end="2026-04-12", period_type="mtd", period_label="MTD thru Week 2")
    check("ROSE.xls row avg_ticket 39.18 (total/guests)", row["avg_ticket"] == 39.18)
    check("ROSE.xls row guest_count 401", row["guest_count"] == 401)
    check("ROSE.xls row crosswalk name = Roseville", row["location_name_canonical"] == "Roseville")
    check("ROSE.xls row tableau center = 888-40098-FS Roseville, MN", row["tableau_center_name"] == "888-40098-FS Roseville, MN")
    check("ROSE.xls row data_complete TRUE", row["data_complete_flag"] is True)


if __name__ == "__main__":
    print("=== ENGINE ==="); engine_tests()
    print("\n=== PARSER (real PDF) ==="); parser_tests()
    print("\n=== END-TO-END (real PDF, cross-checked vs Zenoti stats) ==="); end_to_end_realpdf()
    print("\n=== ACCEPTANCE: Forest Lake windowed .xls (4/1-4/12) ==="); acceptance_xls()
    print("\n=== ROSEVILLE reconciliation (monthly PDF) ==="); roseville_recon()
    print("\n=== ROSEVILLE windowed .xls (penny-verify, 4/1-4/12) ==="); roseville_windowed()
    print("\n" + ("ALL PASS" if not FAILS else f"{len(FAILS)} FAILED: {FAILS}"))
    sys.exit(1 if FAILS else 0)
