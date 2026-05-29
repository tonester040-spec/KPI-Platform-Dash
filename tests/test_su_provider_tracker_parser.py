"""
Tests for su_provider_tracker_parser (Salon Ultimate stylist-grain STYLISTS_DATA — Track D).

Layers (mirrors test_su_dashboard_parser.py):
  1. HELPERS    — header detection, artifact filtering, the active-row rule (incl.
                  worked-but-zero-service kept), derived-KPI math, and the reconciliation
                  fail-loud path. Pure synthetic grids — no LibreOffice, no real file.
  2. CONTRACT   — _stylist_rows_from_parsed emits the EXACT 18-key dict the Zenoti/SU
                  monthly path (monthly_pdf_loader._build_stylist_rows) produces. Proves
                  Track D rides the shared STYLISTS_DATA contract — NO schema fork.
  3. ACCEPTANCE — real SU Provider Tracker .xls (Apple Valley / Lakeville / Farmington)
                  via soffice -> openpyxl, penny/integer exact, with a cross-quadrant check
                  that the Totals row ties to the Track C salon-grain service/retail. Each
                  location skips cleanly if its file (or LibreOffice) is absent.

Run:  python tests/test_su_provider_tracker_parser.py
"""
from __future__ import annotations
import os
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
from parsers import su_provider_tracker_parser as P  # noqa: E402

# Real-file acceptance specs — one per SU location. Each skips cleanly if absent.
# salon_service / salon_retail are the Track C salon-grain figures for the SAME 4/1-4/12
# window (Apple Valley from the matched Salon Dashboard Total Service per the Track D brief;
# Lakeville / Farmington from the Track C Salon Summary acceptance constants). The Provider
# Tracker Totals row must tie to them to the penny — the cross-quadrant safety net.
PT_ACCEPTANCE = [
    {
        "label": "Apple Valley (z010)",
        "candidates": [os.environ.get("SU_PT_APPLE_VALLEY_XLS"),
                       r"C:\Users\TonyGrant\Downloads\Provider Tracker Report - Apple Valley - 4-1 - 4-12.xls"],
        "location_id": "z010", "location_name": "Apple Valley",
        "totals_service": 36718.05, "totals_retail": 4624.38,
        "totals_service_clients": 647, "active_count": 17, "zero_row_count": 16,
        "artifacts": ["Booked Online", "House _", "Salon Ultimate"],
        "active_vs_totals_service_gap": 0.0, "active_vs_totals_retail_gap": 131.88,  # House _ holds 131.88 unattributed retail
        "salon_service": 36718.05, "salon_retail": 4624.38,   # matched Salon Dashboard Total Service / Retail
        # spot-check keys are RICH-DICT field names (parsed["stylists"]), not emitted-row names
        "spot": {
            "Megan Petersen":  {"service_net": 3737.0, "product_net": 578.0, "guest_count": 52,
                                "hours_worked": 55.31, "pph": 67.56, "ppg_net": 11.12, "avg_ticket": 82.98},
            "Chloe Benage":    {"service_net": 3184.0, "product_net": 266.0, "guest_count": 50},
        },
        "color_spot": {"Megan Petersen": {"color_net": 1423.75, "color_count": 6, "color_pct": 0.381}},
        "worked_zero_service": {"Brittany Gold", "Jessica Marsh"},
    },
    {
        "label": "Lakeville (su001)",
        "candidates": [os.environ.get("SU_PT_LAKEVILLE_XLS"),
                       r"C:\Users\TonyGrant\Downloads\Provider Tracker Report - Lakeville - 4-1 - 4-12.xls"],
        "location_id": "su001", "location_name": "Lakeville",
        "totals_service": 13280.60, "totals_retail": 1200.05,
        "totals_service_clients": 291, "active_count": 10, "zero_row_count": 13,
        "artifacts": ["Booked Online", "House _", "Salon Ultimate"],
        # Lakeville routes a -$17.50 refund through "House _" (SERVICE adj): active service
        # exceeds Totals by 17.50, but active+filtered still reconciles to the penny.
        "active_vs_totals_service_gap": -17.50, "active_vs_totals_retail_gap": 0.0,
        "salon_service": 13280.60, "salon_retail": 1200.05,
        "spot": {"Austin House": {"service_net": 2772.40, "hours_worked": 64.80, "guest_count": 51}},
        "color_spot": {}, "worked_zero_service": set(),
    },
    {
        "label": "Farmington (su002)",
        "candidates": [os.environ.get("SU_PT_FARMINGTON_XLS"),
                       r"C:\Users\TonyGrant\Downloads\Provider Tracker Report - Farmington - 4-1 - 4-12.xls"],
        "location_id": "su002", "location_name": "Farmington",
        "totals_service": 22619.75, "totals_retail": 2050.35,
        "totals_service_clients": 514, "active_count": 11, "zero_row_count": 9,
        # Farmington ships TWO "Salon Ultimate" artifact rows — both must be filtered.
        "artifacts": ["Booked Online", "House _", "Salon Ultimate", "Salon Ultimate"],
        # House _ holds +$26.25 unattributed RETAIL — active retail trails Totals by that.
        "active_vs_totals_service_gap": 0.0, "active_vs_totals_retail_gap": 26.25,
        "salon_service": 22619.75, "salon_retail": 2050.35,
        "spot": {"Kayla Leahy": {"service_net": 3238.0, "hours_worked": 70.06, "guest_count": 86}},
        "color_spot": {}, "worked_zero_service": {"Kylie White"},
    },
]
FAILS: list[str] = []


def check(name, cond):
    print(("PASS" if cond else "FAIL"), "-", name)
    if not cond:
        FAILS.append(name)


_HEADER = ["Provider Name", "Service Sales", "Retail Sales", "Hours Worked", "Product $/Ticket",
           "Serv $ Hour", "Retail Client", "Service Clients", "Prebk. Guest", "Prebk. %",
           "First Time", "Avg Ticket", "Color Sales", "Total Color", "Color %", "Total Req", "Req %"]


def _synthetic_grid(house_service=0.0, house_retail=0.0, totals_service=6921.0, totals_retail=844.0):
    """Mirror the real Apple Valley Provider Tracker layout (the rows the parser reads).
    Active stylists = Brittany Gold (worked, zero service) + Chloe + Megan: svc 6921, retail 844.
    house_service / house_retail / totals_service / totals_retail are tunable to exercise the
    artifact-money-adjustment (Lakeville -17.50 svc / Farmington +26.25 retail) and the
    fail-loud reconciliation paths on EITHER money leg."""
    return [
        ["Store name:", "FS - Apple Valley Pilot Knob"],
        ["Report period:", "04/01/2026 - 04/12/2026"],
        ["Date generated:", "05/29/2026 11:11 AM"],
        [],
        list(_HEADER),
        ["Amanda Klingler", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],        # all-zero -> filtered
        ["Booked Online", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],          # artifact
        ["Brittany Gold", 0, 0, 18.08, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],      # worked, zero service -> ACTIVE
        ["Chloe Benage", 3184, 266, 61.56, 5.32, 51.72, 2, 50, 6, 12, 3, 69, 1226.5, 11, 22, 43, 86],
        ["House _", house_service, house_retail, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],  # artifact: carries real money (svc and/or retail)
        ["Megan Petersen", 3737, 578, 55.31, 10.7, 67.56, 2, 52, 8, 15.38, 7, 79.91, 1423.75, 6, 11.54, 34, 65.38],
        ["Salon Ultimate", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],         # artifact
        ["Totals:", totals_service, totals_retail, 776.45, 7.27, 47.29, 26, 647, 95, 14.68, 102, 65, 11143.25, 80, 12.36, 344, 53.17],
    ]


def helper_tests():
    # ── header detection + content gate ──
    idx, colmap = P._find_header_row(_synthetic_grid())
    check("header row found at idx 4", idx == 4)
    check("colmap service_net col 1", colmap["service_net"] == 1)
    check("colmap service_clients col 7", colmap["service_clients"] == 7)
    check("colmap color_net col 12", colmap["color_net"] == 12)
    # A Salon Dashboard grid (no 'Provider Name' header) must be rejected
    dash_grid = [["Store name:", "FS - X"], ["Report period:", "04/01/2026 - 04/12/2026"],
                 ["Sales", "$", "%"], ["Total Service", 100, 91]]
    try:
        P._find_header_row(dash_grid)
        check("non-PT grid raises", False)
    except ValueError:
        check("non-PT grid raises", True)

    # ── full pure parse — clean grid reconciles ──
    p = P._parse_grid(_synthetic_grid())
    check("store resolves z010/Apple Valley",
          (p["location_id"], p["location_name_canonical"]) == ("z010", "Apple Valley"))
    check("period 2026-04-01 .. 2026-04-12",
          (p["period_start"], p["period_end"]) == ("2026-04-01", "2026-04-12"))
    check("active_count 3 (Brittany worked-zero kept)", p["active_count"] == 3)
    check("artifacts filtered (3)", p["artifacts"] == ["Booked Online", "House _", "Salon Ultimate"])
    check("zero_row_count 1 (Amanda)", p["zero_row_count"] == 1)
    check("totals_service 6921.0", p["totals_service"] == 6921.0)
    check("totals_retail 844.0", p["totals_retail"] == 844.0)
    check("active_service_sum 6921.0", p["active_service_sum"] == 6921.0)
    check("active_retail_sum 844.0", p["active_retail_sum"] == 844.0)
    check("reconciled True (both money legs)", p["reconciled"] is True)
    check("service_gap 0.0", p["service_gap"] == 0.0)
    check("retail_gap 0.0", p["retail_gap"] == 0.0)
    check("active_vs_totals_service_gap 0.0 (clean)", p["active_vs_totals_service_gap"] == 0.0)
    check("active_vs_totals_retail_gap 0.0 (clean)", p["active_vs_totals_retail_gap"] == 0.0)
    check("no flags (clean)", p["flags"] == [])
    names = {s["name"] for s in p["stylists"]}
    check("Brittany Gold kept (worked, zero service)", "Brittany Gold" in names)
    check("Amanda Klingler filtered (all-zero)", "Amanda Klingler" not in names)
    check("Booked Online not a stylist", "Booked Online" not in names)

    # ── derived KPI math (Megan, computed from primitives — NOT the pos_* columns) ──
    megan = next(s for s in p["stylists"] if s["name"] == "Megan Petersen")
    check("Megan guest_count 52 (service clients)", megan["guest_count"] == 52)
    check("Megan ppg_net 11.12 (product/guests)", megan["ppg_net"] == 11.12)
    check("Megan pph 67.56 (service/hours == col F)", megan["pph"] == 67.56)
    check("Megan avg_ticket 82.98 ((svc+prod)/guests)", megan["avg_ticket"] == 82.98)
    check("Megan color_pct 0.381 (color/service)", megan["color_pct"] == 0.381)
    check("Megan req_pct 0.6538 (col Q/100)", megan["req_pct"] == 0.6538)
    check("Megan pos_avg_ticket 79.91 retained (cross-check, total-client denom)", megan["pos_avg_ticket"] == 79.91)
    check("Megan pos_color_pct 11.54 retained (count penetration, NOT used)", megan["pos_color_pct"] == 11.54)

    # ── artifact SERVICE adjustment (Lakeville-style House _ -17.50 refund): all rows incl
    #    the artifact reconcile, even though active-only would be off by 17.50 ──
    adj = P._parse_grid(_synthetic_grid(house_service=-17.5, totals_service=6903.5))
    check("svc-adjustment reconciled (all rows incl artifact == totals)", adj["reconciled"] is True)
    check("svc-adjustment service_gap 0.0", adj["service_gap"] == 0.0)
    check("svc-adjustment retail_gap 0.0", adj["retail_gap"] == 0.0)
    check("svc-adjustment filtered_service_sum -17.5", adj["filtered_service_sum"] == -17.5)
    check("svc-adjustment active_vs_totals_service_gap -17.5", adj["active_vs_totals_service_gap"] == -17.5)
    check("svc-adjustment surfaces service flag", any("artifact_service_adjustment" in f for f in adj["flags"]))

    # ── artifact RETAIL adjustment (Farmington-style House _ +26.25 unattributed retail) ──
    radj = P._parse_grid(_synthetic_grid(house_retail=26.25, totals_retail=870.25))
    check("retail-adjustment reconciled", radj["reconciled"] is True)
    check("retail-adjustment retail_gap 0.0", radj["retail_gap"] == 0.0)
    check("retail-adjustment filtered_retail_sum 26.25", radj["filtered_retail_sum"] == 26.25)
    check("retail-adjustment active_vs_totals_retail_gap 26.25", radj["active_vs_totals_retail_gap"] == 26.25)
    check("retail-adjustment surfaces retail flag", any("artifact_retail_adjustment" in f for f in radj["flags"]))

    # ── reconciliation FAIL-LOUD on the SERVICE leg ──
    try:
        P._parse_grid(_synthetic_grid(totals_service=7000.0), strict=True)
        check("tampered SERVICE totals raises (strict)", False)
    except ValueError as e:
        check("tampered SERVICE totals raises (strict)", True)
        check("raise names SERVICE + gap 79.0", "SERVICE" in str(e) and "79.0" in str(e))
    pn = P._parse_grid(_synthetic_grid(totals_service=7000.0), strict=False)
    check("tampered service strict=False -> reconciled False", pn["reconciled"] is False)
    check("tampered service_gap 79.0", pn["service_gap"] == 79.0)
    check("tampered records unreconciled flag", any("unreconciled" in f for f in pn["flags"]))

    # ── reconciliation FAIL-LOUD on the RETAIL leg (the leg the v1 build missed) ──
    try:
        P._parse_grid(_synthetic_grid(totals_retail=900.0), strict=True)
        check("tampered RETAIL totals raises (strict)", False)
    except ValueError as e:
        check("tampered RETAIL totals raises (strict)", True)
        check("raise names RETAIL + gap 56.0", "RETAIL" in str(e) and "56.0" in str(e))
    rn = P._parse_grid(_synthetic_grid(totals_retail=900.0), strict=False)
    check("tampered retail strict=False -> reconciled False", rn["reconciled"] is False)
    check("tampered retail_gap 56.0", rn["retail_gap"] == 56.0)


def contract_tests():
    # The 18-key STYLISTS_DATA(_MONTHLY) contract from the existing SU monthly path.
    from scripts.backfill.monthly_pdf_loader import _build_stylist_rows
    fake = {"employees": [{"name": "X", "net_service": 100.0, "guests": 2, "net_retail": 10.0,
                           "avg_ticket": 55.0, "production_hours": 3.0, "pph": 33.0, "ppg": 5.0,
                           "request_pct": 0.5, "avg_service_time_min": 20.0}]}
    cfg = {"name": "Apple Valley", "id": "z010", "platform": "salon_ultimate"}
    monthly_keys = set(_build_stylist_rows(
        fake, cfg, year_month="2026-04", period_start="2026-04-01",
        period_end="2026-04-30", source="su_monthly_pdf")[0].keys())

    parsed = P._parse_grid(_synthetic_grid())
    rows = P._stylist_rows_from_parsed(
        parsed, location_id="z010", year_month="2026-04",
        period_start="2026-04-01", period_end="2026-04-12", source="su_provider_tracker")
    check("emits a row per active stylist (3)", len(rows) == 3)
    r = next(x for x in rows if x["name"] == "Megan Petersen")
    check("row keys IDENTICAL to SU monthly contract (no fork)", set(r.keys()) == monthly_keys)
    check("row platform salon_ultimate", r["platform"] == "salon_ultimate")
    check("row loc_id z010", r["loc_id"] == "z010")
    check("row loc_name Apple Valley", r["loc_name"] == "Apple Valley")
    check("row source su_provider_tracker", r["source"] == "su_provider_tracker")
    check("row net_service 3737.0", r["net_service"] == 3737.0)
    check("row net_product 578.0", r["net_product"] == 578.0)
    check("row guests 52 (service clients)", r["guests"] == 52)
    check("row invoices == guests (SU has no split)", r["invoices"] == 52)
    check("row production_hours 55.31 (WORKED hours, not Production Hours)", r["production_hours"] == 55.31)
    check("row pph 67.56", r["pph"] == 67.56)
    check("row ppg 11.12", r["ppg"] == 11.12)
    check("row avg_ticket 82.98", r["avg_ticket"] == 82.98)
    check("row req_pct 0.6538", r["req_pct"] == 0.6538)
    check("row avg_service_time_min 0.0 (not reported)", r["avg_service_time_min"] == 0.0)
    check("row year_month 2026-04", r["year_month"] == "2026-04")
    # worked-but-zero-service stylist still emits a row (retention signal preserved)
    bg = next((x for x in rows if x["name"] == "Brittany Gold"), None)
    check("Brittany Gold row emitted (svc 0, hrs 18.08)", bg is not None and bg["net_service"] == 0.0 and bg["production_hours"] == 18.08)


def acceptance_xls():
    try:
        P._find_soffice()
    except RuntimeError:
        print("SKIP - acceptance: LibreOffice 'soffice' not available (set SOFFICE_PATH)")
        return
    ran = 0
    for spec in PT_ACCEPTANCE:
        xls = next((c for c in spec["candidates"] if c and os.path.exists(c)), None)
        if not xls:
            print(f"SKIP - {spec['label']}: file not found")
            continue
        ran += 1
        lbl = spec["label"]
        print(f"-- {lbl}: {xls}")
        parsed = P.parse_su_provider_tracker(xls)
        check(f"[{lbl}] location_id {spec['location_id']}", parsed["location_id"] == spec["location_id"])
        check(f"[{lbl}] location_name {spec['location_name']}", parsed["location_name_canonical"] == spec["location_name"])
        check(f"[{lbl}] period 2026-04-01..2026-04-12",
              (parsed["period_start"], parsed["period_end"]) == ("2026-04-01", "2026-04-12"))
        check(f"[{lbl}] totals_service == {spec['totals_service']}", parsed["totals_service"] == spec["totals_service"])
        check(f"[{lbl}] totals_retail == {spec['totals_retail']}", parsed["totals_retail"] == spec["totals_retail"])
        check(f"[{lbl}] totals_service_clients == {spec['totals_service_clients']}",
              parsed["totals_service_clients"] == spec["totals_service_clients"])
        check(f"[{lbl}] active_count == {spec['active_count']}", parsed["active_count"] == spec["active_count"])
        check(f"[{lbl}] zero_row_count == {spec['zero_row_count']}", parsed["zero_row_count"] == spec["zero_row_count"])
        check(f"[{lbl}] artifacts == {spec['artifacts']}", parsed["artifacts"] == spec["artifacts"])
        check(f"[{lbl}] RECONCILED both legs (all rows incl artifacts, service & retail, gap $0.00)",
              parsed["reconciled"] is True and parsed["service_gap"] == 0.0 and parsed["retail_gap"] == 0.0)
        check(f"[{lbl}] active_vs_totals_service_gap == {spec['active_vs_totals_service_gap']}",
              parsed["active_vs_totals_service_gap"] == spec["active_vs_totals_service_gap"])
        check(f"[{lbl}] active_vs_totals_retail_gap == {spec['active_vs_totals_retail_gap']}",
              parsed["active_vs_totals_retail_gap"] == spec["active_vs_totals_retail_gap"])
        # guest count is NOT reconciled (per-stylist client sums drift from the Totals row,
        # e.g. Farmington 517 vs 514) — we assert the Totals-row value directly, never a sum.
        # Cross-quadrant: Provider Tracker Totals tie to Track C salon-grain service/retail
        check(f"[{lbl}] x-check: Totals service == salon-grain {spec['salon_service']}",
              parsed["totals_service"] == spec["salon_service"])
        check(f"[{lbl}] x-check: Totals retail == salon-grain {spec['salon_retail']}",
              parsed["totals_retail"] == spec["salon_retail"])
        # worked-but-zero-service stylists kept
        kept_wz = {s["name"] for s in parsed["stylists"] if (s["service_net"] or 0) == 0 and (s["hours_worked"] or 0) > 0}
        check(f"[{lbl}] worked-zero-service kept == {sorted(spec['worked_zero_service'])}",
              kept_wz == spec["worked_zero_service"])
        # spot-check primitives + derived
        by_name = {s["name"]: s for s in parsed["stylists"]}
        for sname, want in spec["spot"].items():
            s = by_name.get(sname)
            check(f"[{lbl}] {sname} present", s is not None)
            if s:
                for k, v in want.items():
                    check(f"[{lbl}] {sname} {k} == {v}", s[k] == v)
        for sname, want in spec["color_spot"].items():
            s = by_name.get(sname)
            if s:
                for k, v in want.items():
                    check(f"[{lbl}] {sname} {k} == {v}", s[k] == v)
        # emitted rows: 18-key contract, one per active stylist
        rows = P.build_su_stylist_rows(xls)
        check(f"[{lbl}] emitted {spec['active_count']} rows", len(rows) == spec["active_count"])
        check(f"[{lbl}] every row 18 keys + salon_ultimate platform",
              all(len(r) == 18 and r["platform"] == "salon_ultimate" and r["loc_id"] == spec["location_id"] for r in rows))
        # per-row reconciliation through the EMITTED rows: emitted active + filtered == totals,
        # for BOTH service and retail (proves no active stylist's money was lost in emission)
        emitted_svc = round(sum(r["net_service"] for r in rows), 2)
        emitted_ret = round(sum(r["net_product"] for r in rows), 2)
        check(f"[{lbl}] sum(emitted net_service)+filtered == totals_service",
              round(emitted_svc + parsed["filtered_service_sum"], 2) == spec["totals_service"])
        check(f"[{lbl}] sum(emitted net_product)+filtered == totals_retail",
              round(emitted_ret + parsed["filtered_retail_sum"], 2) == spec["totals_retail"])
    if ran == 0:
        print("SKIP - acceptance: no Provider Tracker files found")


if __name__ == "__main__":
    print("=== HELPERS (pure, synthetic grids) ==="); helper_tests()
    print("\n=== CONTRACT (shared 18-key STYLISTS_DATA dict, no fork) ==="); contract_tests()
    print("\n=== ACCEPTANCE: real Provider Tracker .xls (soffice -> openpyxl) ==="); acceptance_xls()
    print("\n" + ("ALL PASS" if not FAILS else f"{len(FAILS)} FAILED: {FAILS}"))
    sys.exit(1 if FAILS else 0)
