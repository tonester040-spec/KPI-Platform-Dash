"""
Tests for su_dashboard_parser (Salon Ultimate salon-grain LOCATIONS_DATA).

Layers:
  1. HELPERS    — _parse_hours, resolve_su_location, _parse_service_categories, and
                  the reconciliation fail-loud path. Pure, synthetic grids — no
                  LibreOffice, no real file.
  2. ENGINE     — the SHARED locations_grouper._build_row emits the identical 39-col
                  contract for an SU location_id (z010 -> Apple Valley / salon_ultimate).
                  Proves SU rides the same engine — NO schema fork.
  3. ACCEPTANCE — real SU .xls files (Apple Valley, Lakeville, Farmington) via
                  soffice -> openpyxl, penny/integer exact. Data-driven; each location
                  skips cleanly if its file (or LibreOffice) is absent.

Run:  python tests/test_su_dashboard_parser.py
"""
from __future__ import annotations
import os
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
from parsers import su_dashboard_parser as S  # noqa: E402
from parsers import locations_grouper as L  # noqa: E402

# Real-file acceptance specs — one per SU location, penny/integer exact. Each entry
# skips cleanly if its file is absent. avg_ticket / ppg_net in "row" equal SU's OWN
# printed Avg Ticket / PPG (independent cross-check). Values verified 2026-05-29.
SU_ACCEPTANCE = [
    {
        "label": "Apple Valley (z010)",
        "candidates": [os.environ.get("SU_APPLE_VALLEY_XLS"),
                       r"C:\Users\TonyGrant\Downloads\FS_Salon_Dashboard Apple Valley.xls",
                       "FS_Salon_Dashboard Apple Valley.xls"],
        "parse": {"store_name": "FS - Apple Valley Pilot Knob",
                  "period_start": "2026-05-17", "period_end": "2026-05-23",
                  "service_net": 22810.65, "product_net": 2219.25, "total_sales_net": 25029.90,
                  "guest_count": 428, "productive_hours": 427.45, "color_net": 6501.50,
                  "wax_net": 1587.20, "wax_count": 84, "treatment_net": 2652.50,
                  "treatment_count": 136, "tips_total": 4242.02},
        "row": {"location_id": "z010", "location_name_canonical": "Apple Valley",
                "pos_system": "salon_ultimate", "guest_count": 428,
                "avg_ticket": 58.48, "ppg_net": 5.19, "color_pct": 0.285},
        "must_enumerate": {"None", "Other"},     # two negative adjustment buckets
    },
    {
        "label": "Lakeville (su001)",
        "candidates": [os.environ.get("SU_LAKEVILLE_XLS"),
                       r"C:\Users\TonyGrant\Downloads\Salon Summary Lakeville 4-1 - 4-12.xls"],
        "parse": {"store_name": "FS - Lakeville Timbercrest",
                  "period_start": "2026-04-01", "period_end": "2026-04-12",
                  "service_net": 13280.60, "product_net": 1200.05, "total_sales_net": 14480.65,
                  "guest_count": 298, "productive_hours": 366.63, "color_net": 3192.00,
                  "wax_net": 678.40, "wax_count": 38, "treatment_net": 1061.40,
                  "treatment_count": 53, "tips_total": 2489.99},
        "row": {"location_id": "su001", "location_name_canonical": "Lakeville",
                "pos_system": "salon_ultimate", "guest_count": 298,
                "avg_ticket": 48.59, "ppg_net": 4.03, "color_pct": 0.2404},
        "must_enumerate": {"None", "Extensions"},  # Lakeville adds an Extensions category
    },
    {
        "label": "Farmington (su002)",
        "candidates": [os.environ.get("SU_FARMINGTON_XLS"),
                       r"C:\Users\TonyGrant\Downloads\Salon Summary Farmington 4-1 - 4-12.xls"],
        "parse": {"store_name": "FS - Farmington",
                  "period_start": "2026-04-01", "period_end": "2026-04-12",
                  "service_net": 22619.75, "product_net": 2050.35, "total_sales_net": 24670.10,
                  "guest_count": 525, "productive_hours": 413.93, "color_net": 4854.50,
                  "wax_net": 1019.40, "wax_count": 58, "treatment_net": 1788.40,
                  "treatment_count": 98, "tips_total": 4398.70},
        "row": {"location_id": "su002", "location_name_canonical": "Farmington",
                "pos_system": "salon_ultimate", "guest_count": 525,
                "avg_ticket": 46.99, "ppg_net": 3.91, "color_pct": 0.2146},
        "must_enumerate": {"None"},
    },
]
FAILS: list[str] = []


def check(name, cond):
    print(("PASS" if cond else "FAIL"), "-", name)
    if not cond:
        FAILS.append(name)


def _synthetic_grid(color_sales=6501.50, totals_sales=22810.65):
    """A grid mirroring the real Apple Valley layout (the rows the parser reads).
    color_sales/totals_sales are tunable so a tampered grid can exercise the
    reconciliation fail-loud path."""
    return [
        ["Store name:", "FS - Apple Valley Pilot Knob"],
        ["Report period:", "05/17/2026 - 05/23/2026"],
        ["Date generated:", "05/29/2026 09:36 AM"],
        [],
        ["Sales", "$", "%"],
        ["Total Service", 22810.65, 91.13],
        ["Total Retail", 2219.25, 8.87],
        ["Total Revenue", 25029.90],
        ["Tips", 4242.02],
        ["Guest Count"],
        ["Serviced Guests", 420, 98.13],
        ["Retail Only Guests", 8, 1.87],
        ["TOTAL Guests", 428],
        ["Statistics"],
        ["PPH", 53.36],
        ["PPG", 5.19],
        ["TOTAL Avg Ticket", 58.48],
        ["Production Hours", "427h 27m"],
        ["Service Categories", "Qty", "% Qty", "Avg Time (min)", "Sales", "% Sales", "Avg Ticket"],
        ["Haircut", 379, 55.74, 24.83, 11867.05, 52.02, 33.43],
        ["None", 20, 2.94, 0, -200, -0.88, -10],          # negative adjustment bucket
        ["Treatment", 136, 20, 9.38, 2652.50, 11.63, 27.35],
        ["Color", 45, 6.62, 117.42, color_sales, 28.5, 138.33],
        ["Wax", 84, 12.35, 11.08, 1587.20, 6.96, 26.02],
        ["Other", 0, 0, 0, -162, -0.71, -81],             # negative adjustment bucket
        ["Style", 13, 1.91, 22.86, 267.40, 1.17, 22.28],
        ["Perm", 3, 0.44, 86.73, 297, 1.3, 99],
        ["TOTALS", 680, 100, 0.42, totals_sales, 100, 56.88],
        [],
        ["Top Product Lines", "Qty", "% Qty", "Sales", "% Sales"],
    ]


def helper_tests():
    # Xh Ym -> decimal hours
    check("hours '427h 27m' -> 427.45", S._parse_hours("427h 27m") == 427.45)
    check("hours '63h 0m' -> 63.0", S._parse_hours("63h 0m") == 63.0)
    check("hours '8h 30m' -> 8.5", S._parse_hours("8h 30m") == 8.5)
    check("hours '490h 27m' -> 490.45", S._parse_hours("490h 27m") == 490.45)
    check("hours numeric passthrough", S._parse_hours(120.0) == 120.0)
    check("hours None -> None", S._parse_hours(None) is None)

    # store-name resolution
    check("resolve Apple Valley", S.resolve_su_location("FS - Apple Valley Pilot Knob") == ("z010", "Apple Valley"))
    check("resolve Lakeville substring", S.resolve_su_location("FS - Lakeville") == ("su001", "Lakeville"))
    check("resolve Farmington substring", S.resolve_su_location("Farmington Town Center") == ("su002", "Farmington"))
    try:
        S.resolve_su_location("FS - Woodbury")
        check("resolve unknown raises", False)
    except ValueError:
        check("resolve unknown raises", True)

    # Service Categories table extraction + enumeration
    cats = S._parse_service_categories(_synthetic_grid())
    check("cats color_net 6501.5", cats["color_net"] == 6501.5)
    check("cats wax_net 1587.2", cats["wax_net"] == 1587.2)
    check("cats wax_count 84", cats["wax_count"] == 84)
    check("cats treatment_net 2652.5", cats["treatment_net"] == 2652.5)
    check("cats treatment_count 136", cats["treatment_count"] == 136)
    check("cats sum incl negatives == 22810.65", cats["categories_net_sum"] == 22810.65)
    check("cats TOTALS row sales 22810.65", cats["totals_row_sales"] == 22810.65)
    check("cats enumerate None + Other (not bucketed)",
          {"None", "Other"}.issubset(set(cats["categories_found"])))
    check("cats Color NOT double-counted in enumeration",
          cats["categories_found"].count("Color") == 1)

    # full pure parse — reconciles
    p = S._parse_grid(_synthetic_grid())
    check("grid service_net 22810.65", p["service_net"] == 22810.65)
    check("grid product_net 2219.25", p["product_net"] == 2219.25)
    check("grid total 25029.90", p["total_sales_net"] == 25029.90)
    check("grid guest_count 428 (int)", p["guest_count"] == 428 and isinstance(p["guest_count"], int))
    check("grid productive_hours 427.45", p["productive_hours"] == 427.45)
    check("grid tips_total 4242.02", p["tips_total"] == 4242.02)
    check("grid unique_guests None (SU has none)", p["unique_guests"] is None)
    check("grid location resolves z010/Apple Valley", (p["location_id"], p["location_name_canonical"]) == ("z010", "Apple Valley"))
    check("grid period 2026-05-17 .. 2026-05-23", (p["period_start"], p["period_end"]) == ("2026-05-17", "2026-05-23"))
    check("grid service_mix RECONCILED", p["service_mix_reconciled"] is True)
    check("grid service_mix gap 0.0", p["service_mix_gap"] == 0.0)
    check("grid flags empty", p["flags"] == [])

    # reconciliation FAIL-LOUD: drop $501.50 of Color but leave TOTALS untouched
    tampered = _synthetic_grid(color_sales=6000.00)  # cats sum now 22309.15 != service_net 22810.65
    try:
        S._parse_grid(tampered, strict=True)
        check("tampered grid raises (strict)", False)
    except ValueError as e:
        check("tampered grid raises (strict)", True)
        check("raise names the gap 501.5", "501.5" in str(e))
    pn = S._parse_grid(tampered, strict=False)
    check("tampered strict=False -> reconciled False", pn["service_mix_reconciled"] is False)
    check("tampered gap 501.5", pn["service_mix_gap"] == 501.5)
    check("tampered records a flag", any("unreconciled" in f for f in pn["flags"]))


def engine_tests():
    # CROSSWALK now carries the 3 SU salons (wired for Track C) — pos_system + None Tableau.
    for lid, name in (("z010", "Apple Valley"), ("su001", "Lakeville"), ("su002", "Farmington")):
        xw = L.CROSSWALK.get(lid, {})
        check(f"CROSSWALK {lid} -> {name}", xw.get("location_name_canonical") == name)
        check(f"CROSSWALK {lid} pos_system salon_ultimate", xw.get("pos_system") == "salon_ultimate")
        check(f"CROSSWALK {lid} tableau center None (not in Tableau)", xw.get("tableau_center_name") is None)

    # The SHARED engine emits the identical 39-col row for an SU location_id.
    m = {"service_net": 22810.65, "product_net": 2219.25, "total_sales_net": 25029.90,
         "color_net": 6501.50, "wax_net": 1587.20, "wax_count": 84,
         "treatment_net": 2652.50, "treatment_count": 136}
    r = L._build_row(location_id="z010", period_start="2026-05-17", period_end="2026-05-23",
                     period_type="weekly", period_label="05/17/2026 - 05/23/2026", metrics=m,
                     guest_count=428, unique_guests=None, productive_hours=427.45,
                     monthly_goal=None, extract_hash="x", generated_at="t", extra_flags=None)
    check("engine SU 39 keys", set(r.keys()) == set(L.COLUMNS))
    check("engine SU name Apple Valley", r["location_name_canonical"] == "Apple Valley")
    check("engine SU pos_system salon_ultimate", r["pos_system"] == "salon_ultimate")
    check("engine SU avg_ticket 58.48 (total/guests)", r["avg_ticket"] == 58.48)
    check("engine SU pph_net 58.56 (total/hours, NOT SU's 53.36)", r["pph_net"] == 58.56)
    check("engine SU color_pct 0.285 (color/service)", r["color_pct"] == 0.285)
    check("engine SU product_pct 0.0887", r["product_pct"] == 0.0887)
    check("engine SU wax_pct 0.1963 (count/guests)", r["wax_pct"] == 0.1963)
    check("engine SU projection 312873.75", r["projection"] == 312873.75)
    check("engine SU data_complete TRUE", r["data_complete_flag"] is True)
    check("engine SU tier3 None", all(r[f] is None for f in L.TIER3_FIELDS))


def acceptance_xls():
    try:
        S._find_soffice()
    except RuntimeError:
        print("SKIP - acceptance: LibreOffice 'soffice' not available (set SOFFICE_PATH)")
        return
    ran = 0
    for spec in SU_ACCEPTANCE:
        xls = next((c for c in spec["candidates"] if c and os.path.exists(c)), None)
        if not xls:
            print(f"SKIP - {spec['label']}: file not found")
            continue
        ran += 1
        lbl = spec["label"]
        print(f"-- {lbl}: {xls}")
        ss = S.parse_su_dashboard(xls)
        for k, v in spec["parse"].items():
            check(f"[{lbl}] parse {k} == {v}", ss.get(k) == v)
        check(f"[{lbl}] parse unique_guests None", ss.get("unique_guests") is None)
        check(f"[{lbl}] parse service_mix RECONCILED (gap $0.00)", ss.get("service_mix_reconciled") is True)
        check(f"[{lbl}] parse service_mix gap 0.0", ss.get("service_mix_gap") == 0.0)
        check(f"[{lbl}] parse no flags", ss.get("flags") == [])
        check(f"[{lbl}] enumerates {sorted(spec['must_enumerate'])} (not bucketed)",
              spec["must_enumerate"].issubset(set(ss.get("categories_found") or [])))
        row = S.build_su_location_row(xls)
        for k, v in spec["row"].items():
            check(f"[{lbl}] row {k} == {v}", row[k] == v)
        check(f"[{lbl}] row data_complete TRUE", row["data_complete_flag"] is True)
    if ran == 0:
        print("SKIP - acceptance: no SU dashboard files found")


if __name__ == "__main__":
    print("=== HELPERS (pure, synthetic grids) ==="); helper_tests()
    print("\n=== ENGINE (shared _build_row, SU location_id) ==="); engine_tests()
    print("\n=== ACCEPTANCE: real Apple Valley .xls (soffice -> openpyxl) ==="); acceptance_xls()
    print("\n" + ("ALL PASS" if not FAILS else f"{len(FAILS)} FAILED: {FAILS}"))
    sys.exit(1 if FAILS else 0)
