"""
Tests for zenoti_stylist_parser (Zenoti Salon Summary stylist-grain STYLISTS_DATA — Track B).

Layers (mirrors test_su_provider_tracker_parser.py — Track D):
  1. HELPERS    — period/location/tips parsing, the SALE-table token walker, role-rollup
                  skip, artifact classification (money kept, row filtered), the join,
                  derived-KPI math, and the fail-loud reconciliation path. Pure SYNTHETIC
                  Salon Summary text — no real file, no pdfplumber.
  2. CONTRACT   — _stylist_rows_from_parsed emits the EXACT 18-key dict the Zenoti monthly
                  path (monthly_pdf_loader._build_stylist_rows, zenoti branch) produces.
                  Proves Track B rides the shared STYLISTS_DATA contract — NO schema fork.
  3. ACCEPTANCE — the REAL Forest Lake Salon Summary in the repo (data/inbox/Forest Lake.pdf,
                  888-11812, 5/1-5/24/2026) — ALWAYS runs, penny-exact, reconciling the
                  per-stylist sums to the INDEPENDENTLY-parsed salon sections (SALES block,
                  TIPS COLLECTED). Plus a GATED 4/1-4/12 .xls slot (the Track B brief's
                  example window) that SKIPS cleanly when the file is absent.

Run:  python tests/test_zenoti_stylist_parser.py
"""
from __future__ import annotations
import os
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
from parsers import zenoti_stylist_parser as P  # noqa: E402

FAILS: list[str] = []


def check(name, cond):
    print(("PASS" if cond else "FAIL"), "-", name)
    if not cond:
        FAILS.append(name)


# ─────────────────────────────────────────────────────────────────────────────
# Synthetic Salon Summary text — mirrors the real Zenoti layout (section order,
# wrapped numbers, role-rollup interleaving, page-break noise) closely enough to
# exercise every helper without a real file. Two active stylists + one role
# rollup + one money-bearing artifact ("Sample mgr": 0 svc / 0 hrs / $25 retail).
#   Salon truth: service 1500.00 / product 325.00 / tips 150.00 / prod-hours 50.00
#   Alice Adams  svc 1000 / tips 100 / qty 20 / inv 15 / prod 200 / hrs 30
#   Bob Brown    svc  500 / tips  50 / qty 10 / inv  8 / prod 100 / hrs 20
#   Sample mgr   svc    0 / tips   0 / qty  0 / inv  2 / prod  25 / hrs  0   (artifact)
# ─────────────────────────────────────────────────────────────────────────────
def _synthetic_text(service_sales_net="1,500.00", product_sales_net="325.00",
                    tips_total="150.00"):
    return f"""888-11812-Forest Lake
Salon Summary
From: 5/1/2026 To: 5/24/2026 Printed on:5/26/2026
SALES GROSS NET (%)
Service sales 30 1,600.00 {service_sales_net} (82.20)
Product sales 5 350.00 {product_sales_net} (17.80)
TIPS COLLECTED AMOUNT
Cash 0.00
Card {tips_total}
Others 0.00
Total {tips_total}
SERVICE DETAILS ITEM QTY (%) AVG TIME NET SALES $ (%) DISC $
Color 12 (40.00) 45.00 600.00 (40.00) 0.00
EMPLOYEE SALE DETAILS NET COMM COMM NET TIPS
STYLIST 1,500.00 1,500.00 0.00 150.00 30 300.00 300.00 0.00 100.00
Alice Adams 1,000.00 1,000.00 0.00 100.00 20 15 200.00 200.00 13.33 80.00 0.00 50.00
about:blank 3/7
5/26/26, 7:24 AM about:blank
Bob Brown 500.00 500.00 0.00 50.00 10 8 100.00 100.00 12.50 75.00 0.00 50.00
Sample mgr 0.00 0.00 0.00 0.00 0 2 25.00 25.00 12.50 12.50 0.00 0.00
Total 1,500.00 1,500.00 0.00 150.00 30 325.00 325.00 0.00 100.00
EMPLOYEE PERFORMANCE DETAILS PRODU HOURS HOURS HOURS
STYLIST 0.45 40.00 55.00 50.00 0.00 5.00 30.00 31.00 30.00 12
(50.00)
Alice Adams 0.50 25.00 32.00 30.00 0.00 2.00 33.33 34.00 33.33 8
(0.00)
Bob Brown 0.40 18.00 22.00 20.00 0.00 3.00 25.00 26.00 25.00 4
(0.00)
Sample mgr 0.00 0.00 0.00 0.00 0.00 0.00 0.00 0.00 0.00 0
(0.00)
Total 0.30 43.00 54.00 50.00 0.00 5.00 30.00 31.00 30.00 12
HOURLY WORK
12AM 1AM 2AM TOTAL
"""


def helper_tests():
    # ── period + location resolution from the header ──
    t = _synthetic_text()
    check("period 2026-05-01 .. 2026-05-24", P._parse_period(t) == ("2026-05-01", "2026-05-24"))
    lid, canonical, center, pos = P._resolve_location(t)
    check("location_id 888-11812", lid == "888-11812")
    check("canonical Forest Lake", canonical == "Forest Lake")
    check("center 888-11812-Forest Lake", center == "888-11812-Forest Lake")
    check("pos_system zenoti", pos == "zenoti")
    check("tips collected total 150.00", P._parse_tips_collected_from_text(t) == 150.00)

    # ── SALE-table token walker: 3 provider rows (rollups STYLIST/Total skipped) ──
    sales = P._walk_employee_sales(t)
    names = [s["name"] for s in sales]
    check("walker finds 3 provider rows (rollups skipped)", names == ["Alice Adams", "Bob Brown", "Sample mgr"])
    alice = sales[0]
    check("Alice net_service 1000.0 (col 0)", alice["net_service"] == 1000.0)
    check("Alice tips 100.0 (col 3)", alice["tips"] == 100.0)
    check("Alice service_qty 20 (col 4)", alice["service_qty"] == 20)
    check("Alice invoice_count 15 (col 5)", alice["invoice_count"] == 15)
    check("Alice net_product 200.0 (col 6)", alice["net_product"] == 200.0)
    check("page-break noise rejected (no 'about:blank' row)",
          all("about" not in n.lower() for n in names))

    # ── artifact classifier ──
    check("'Sample mgr' is artifact (' mgr')", P._is_artifact("Sample mgr") is True)
    check("'House Sale' is artifact", P._is_artifact("House Sale") is True)
    check("'Alice Adams' not artifact", P._is_artifact("Alice Adams") is False)

    # ── full pure parse — clean text reconciles on ALL four legs ──
    p = P._parse_text(t)
    check("active_count 2 (Sample mgr filtered)", p["active_count"] == 2)
    check("artifacts ['Sample mgr']", p["artifacts"] == ["Sample mgr"])
    check("reconciled True", p["reconciled"] is True)
    check("no flags (clean)", p["flags"] == [])
    check("salon_service_net 1500.0", p["salon_service_net"] == 1500.0)
    check("sum_service 1500.0 (incl artifact 0)", p["sum_service"] == 1500.0)
    check("service_gap 0.0", p["service_gap"] == 0.0)
    check("salon_product_net 325.0", p["salon_product_net"] == 325.0)
    check("sum_product 325.0 (incl artifact $25)", p["sum_product"] == 325.0)
    check("product_gap 0.0 (artifact retail INCLUDED in sum)", p["product_gap"] == 0.0)
    check("tips_gap 0.0", p["tips_gap"] == 0.0)
    check("salon_productive_hours 50.0", p["salon_productive_hours"] == 50.0)
    check("hours_gap 0.0", p["hours_gap"] == 0.0)
    active_names = {s["name"] for s in p["stylists"]}
    check("Alice + Bob active", active_names == {"Alice Adams", "Bob Brown"})
    check("Sample mgr NOT emitted (artifact, $25 retail kept in sum)", "Sample mgr" not in active_names)

    # ── derived KPI math (Alice, computed from primitives == report's printed cols) ──
    a = next(s for s in p["stylists"] if s["name"] == "Alice Adams")
    check("Alice guest_count 15 (= invoice count)", a["guest_count"] == 15)
    check("Alice avg_ticket 80.0 ((1000+200)/15)", a["avg_ticket"] == 80.0)
    check("Alice pph 33.33 (1000/30)", a["pph"] == 33.33)
    check("Alice ppg 13.33 (200/15)", a["ppg"] == 13.33)

    # ── reconciliation FAIL-LOUD: tamper the independent SALES service net ──
    bad = _synthetic_text(service_sales_net="1,600.00")  # stylists still sum 1500 -> gap 100
    try:
        P._parse_text(bad, strict=True)
        check("tampered SERVICE raises (strict)", False)
    except ValueError as e:
        check("tampered SERVICE raises (strict)", True)
        check("raise names SERVICE + gap 100.0", "SERVICE" in str(e) and "100.0" in str(e))
    pn = P._parse_text(bad, strict=False)
    check("tampered strict=False -> reconciled False", pn["reconciled"] is False)
    check("tampered service_gap 100.0", pn["service_gap"] == 100.0)
    check("tampered records unreconciled flag", any("unreconciled" in f for f in pn["flags"]))

    # ── fail-loud on the TIPS leg too (the leg a naive build would skip) ──
    badtips = _synthetic_text(tips_total="200.00")  # stylists sum 150 -> gap 50
    try:
        P._parse_text(badtips, strict=True)
        check("tampered TIPS raises (strict)", False)
    except ValueError as e:
        check("tampered TIPS raises (strict)", "TIPS" in str(e) and "50.0" in str(e))


def contract_tests():
    # The 18-key STYLISTS_DATA(_MONTHLY) contract from the existing Zenoti monthly path.
    from scripts.backfill.monthly_pdf_loader import _build_stylist_rows
    fake = {"employees": [{"name": "X", "net_service": 100.0, "invoice_count": 2,
                           "net_product": 10.0, "avg_invoice_value": 55.0,
                           "production_hours": 3.0, "net_service_per_hr": 33.0,
                           "req_services_count": 1, "service_qty": 2}]}
    cfg = {"name": "Forest Lake", "id": "888-11812", "platform": "zenoti"}
    monthly_keys = set(_build_stylist_rows(
        fake, cfg, year_month="2026-05", period_start="2026-05-01",
        period_end="2026-05-24", source="zenoti_monthly_pdf")[0].keys())

    parsed = P._parse_text(_synthetic_text())
    rows = P._stylist_rows_from_parsed(
        parsed, location_id="888-11812", year_month="2026-05",
        period_start="2026-05-01", period_end="2026-05-24", source="zenoti_salon_summary")
    check("emits a row per active stylist (2)", len(rows) == 2)
    r = next(x for x in rows if x["name"] == "Alice Adams")
    check("row keys IDENTICAL to Zenoti monthly contract (no fork)", set(r.keys()) == monthly_keys)
    check("row platform zenoti", r["platform"] == "zenoti")
    check("row loc_id 888-11812", r["loc_id"] == "888-11812")
    check("row loc_name Forest Lake", r["loc_name"] == "Forest Lake")
    check("row source zenoti_salon_summary", r["source"] == "zenoti_salon_summary")
    check("row net_service 1000.0", r["net_service"] == 1000.0)
    check("row net_product 200.0", r["net_product"] == 200.0)
    check("row guests 15 (= invoice count)", r["guests"] == 15)
    check("row invoices == guests (Zenoti invoice_count = guest_count)", r["invoices"] == 15)
    check("row production_hours 30.0", r["production_hours"] == 30.0)
    check("row avg_ticket 80.0", r["avg_ticket"] == 80.0)
    check("row pph 33.33", r["pph"] == 33.33)
    check("row ppg 13.33", r["ppg"] == 13.33)
    check("row req_pct 0.0 (deferred)", r["req_pct"] == 0.0)
    check("row avg_service_time_min 0.0 (deferred)", r["avg_service_time_min"] == 0.0)
    check("row year_month 2026-05", r["year_month"] == "2026-05")


# Real-file acceptance specs.
#  PRIMARY  — the Forest Lake Salon Summary committed in the repo (5/1-5/24/2026). Always
#             runs. Reconciles per-stylist sums to the INDEPENDENTLY-parsed salon sections.
#  SECONDARY (gated) — the Track B brief's 4/1-4/12 example window. Drop the .xls at one of
#             the candidate paths (or set ZENOTI_FL_XLS) and it validates the brief's
#             constants too; otherwise it SKIPS cleanly (Track D pattern).
ACCEPTANCE = [
    {
        "label": "Forest Lake repo PDF (5/1-5/24)",
        "candidates": [str(_REPO_ROOT / "data" / "inbox" / "Forest Lake.pdf")],
        "location_id": "888-11812", "location_name": "Forest Lake",
        "period": ("2026-05-01", "2026-05-24"),
        "salon_service": 20508.35, "salon_product": 2211.65, "salon_tips": 3250.37,
        "salon_hours": 439.54, "active_count": 6, "artifacts": ["Forest Lake mgr"],
        "spot": {
            "Danielle Carlson": {"net_service": 4925.10, "tips": 652.81, "service_qty": 141,
                                 "invoice_count": 103, "production_hours": 84.97,
                                 "avg_ticket": 51.84, "pph": 57.96, "ppg": 4.03},
            "Dani Shearen":     {"net_service": 4169.00, "tips": 636.59, "invoice_count": 82,
                                 "production_hours": 79.73},
        },
    },
    {
        # The real HTML-disguised .xls Tony exports (UTF-8 BOM + <html>; 'From : 01 Apr
        # 2026' date dialect). Proves the parser is format-agnostic (same code path as the
        # PDF) AND ties to the Track B brief's example window to the penny. In Downloads
        # (not committed), so SKIPS cleanly off Tony's machine — Track D pattern.
        "label": "Forest Lake .xls (4/1-4/12 brief example)",
        "candidates": [os.environ.get("ZENOTI_FL_XLS"),
                       r"C:\Users\TonyGrant\Downloads\Salon Summary Forest Lake.xls",
                       r"C:\Users\TonyGrant\Downloads\Salon_Summary_Forest_Lake.xls"],
        "location_id": "888-11812", "location_name": "Forest Lake",
        "period": ("2026-04-01", "2026-04-12"),
        "salon_service": 12395.70, "salon_tips": 1844.70,
        "salon_product": 945.50, "salon_hours": 267.37, "active_count": 6, "artifacts": [],
        "spot": {
            "Danielle Carlson": {"net_service": 3793.20, "tips": 573.29, "service_qty": 99,
                                 "invoice_count": 73, "production_hours": 60.28},
            "Dani Shearen":     {"net_service": 2820.40, "production_hours": 53.03,
                                 "tips": 455.50, "service_qty": 75, "invoice_count": 58},
        },
    },
]


def acceptance():
    ran = 0
    for spec in ACCEPTANCE:
        path = next((c for c in spec["candidates"] if c and os.path.exists(c)), None)
        if not path:
            print(f"SKIP - {spec['label']}: file not found")
            continue
        ran += 1
        lbl = spec["label"]
        print(f"-- {lbl}: {path}")
        parsed = P.parse_zenoti_salon_summary(path)  # strict=True -> raises if it doesn't reconcile
        check(f"[{lbl}] location_id {spec['location_id']}", parsed["location_id"] == spec["location_id"])
        check(f"[{lbl}] location_name {spec['location_name']}",
              parsed["location_name_canonical"] == spec["location_name"])
        check(f"[{lbl}] period {spec['period']}",
              (parsed["period_start"], parsed["period_end"]) == spec["period"])
        check(f"[{lbl}] RECONCILED (per-stylist sums == independent salon sections, gap $0.00)",
              parsed["reconciled"] is True)
        check(f"[{lbl}] salon_service {spec['salon_service']}", parsed["salon_service_net"] == spec["salon_service"])
        check(f"[{lbl}] sum_service == salon_service", parsed["sum_service"] == spec["salon_service"])
        check(f"[{lbl}] service_gap 0.0", parsed["service_gap"] == 0.0)
        check(f"[{lbl}] salon_tips {spec['salon_tips']}", parsed["salon_tips"] == spec["salon_tips"])
        check(f"[{lbl}] tips_gap 0.0", parsed["tips_gap"] == 0.0)
        check(f"[{lbl}] salon_hours {spec['salon_hours']}", parsed["salon_productive_hours"] == spec["salon_hours"])
        check(f"[{lbl}] hours_gap 0.0", parsed["hours_gap"] == 0.0)
        check(f"[{lbl}] active_count {spec['active_count']}", parsed["active_count"] == spec["active_count"])
        if spec["salon_product"] is not None:
            check(f"[{lbl}] salon_product {spec['salon_product']}", parsed["salon_product_net"] == spec["salon_product"])
            check(f"[{lbl}] product_gap 0.0 (artifact retail INCLUDED)", parsed["product_gap"] == 0.0)
        if spec["artifacts"] is not None:
            check(f"[{lbl}] artifacts {spec['artifacts']}", parsed["artifacts"] == spec["artifacts"])
        # spot-check primitives + derived
        by_name = {s["name"]: s for s in parsed["stylists"]}
        for sname, want in spec["spot"].items():
            s = by_name.get(sname)
            check(f"[{lbl}] {sname} present (active)", s is not None)
            if s:
                for k, v in want.items():
                    check(f"[{lbl}] {sname} {k} == {v}", s[k] == v)
        # emitted rows: 18-key contract, one per active stylist, and active money preserved
        rows = P.build_zenoti_stylist_rows(path)
        check(f"[{lbl}] emitted {spec['active_count']} rows", len(rows) == spec["active_count"])
        check(f"[{lbl}] every row 18 keys + zenoti platform + loc_id",
              all(len(r) == 18 and r["platform"] == "zenoti" and r["loc_id"] == spec["location_id"] for r in rows))
        # artifacts carry no SERVICE (0 svc), so emitted service sum still ties to salon service
        check(f"[{lbl}] sum(emitted net_service) == salon_service",
              round(sum(r["net_service"] for r in rows), 2) == spec["salon_service"])
    if ran == 0:
        print("SKIP - acceptance: no Salon Summary files found")


# Breadth sweep — ALL NINE Zenoti salons' real 4/1-4/12/2026 .xls exports (verified
# 2026-05-29). Forest Lake gets the deep per-stylist spot-checks above; this proves the
# OTHER 8 reconcile to the penny on every leg (service/product/tips/hours) with clean
# 18-key emission, incl. artifact handling on real data (Blaine/Crystal/Hudson carry 2
# routing artifacts, Elk River 1 — included in the sum, excluded from emission, still
# tying out). Files live in Tony's Downloads (not committed) so the whole sweep SKIPS
# cleanly off his machine / on CI — Track D pattern.
_DL = r"C:\Users\TonyGrant\Downloads"
ZENOTI_XLS_SWEEP = [
    {"file": "Andover.xls",     "loc_id": "888-10278", "active": 7,  "artifacts": 0,
     "service": 11216.65, "product": 1056.69, "tips": 1505.50, "hours": 296.66},
    {"file": "Blaine.xls",      "loc_id": "888-9816",  "active": 12, "artifacts": 2,
     "service": 20979.45, "product": 974.05,  "tips": 3141.88, "hours": 504.75},
    {"file": "Crystal.xls",     "loc_id": "888-7663",  "active": 13, "artifacts": 2,
     "service": 20763.70, "product": 1238.55, "tips": 3139.33, "hours": 460.54},
    {"file": "Elk River.xls",   "loc_id": "887-10199", "active": 8,  "artifacts": 1,
     "service": 11775.40, "product": 599.45,  "tips": 2023.85, "hours": 326.62},
    {"file": "Roseville.xls",   "loc_id": "888-40098", "active": 11, "artifacts": 0,
     "service": 15413.15, "product": 298.99,  "tips": 2773.56, "hours": 437.69},
    {"file": "Prior Lake.xls",  "loc_id": "888-11091", "active": 9,  "artifacts": 0,
     "service": 16443.75, "product": 1290.00, "tips": 2422.43, "hours": 440.13},
    {"file": "Hudson.xls",      "loc_id": "910-7232",  "active": 9,  "artifacts": 2,
     "service": 20641.95, "product": 1697.87, "tips": 3434.39, "hours": 323.31},
    {"file": "New Richmond.xls", "loc_id": "910-6916", "active": 6,  "artifacts": 0,
     "service": 7536.55,  "product": 639.00,  "tips": 1135.03, "hours": 298.80},
]


def acceptance_sweep():
    ran = 0
    for spec in ZENOTI_XLS_SWEEP:
        path = os.path.join(_DL, spec["file"])
        if not os.path.exists(path):
            print(f"SKIP - {spec['file']}: file not found")
            continue
        ran += 1
        lbl = spec["file"]
        parsed = P.parse_zenoti_salon_summary(path)  # strict=True -> raises if any leg drifts
        check(f"[{lbl}] loc_id {spec['loc_id']}", parsed["location_id"] == spec["loc_id"])
        check(f"[{lbl}] period 2026-04-01..2026-04-12",
              (parsed["period_start"], parsed["period_end"]) == ("2026-04-01", "2026-04-12"))
        check(f"[{lbl}] RECONCILED all 4 legs gap $0.00", parsed["reconciled"] is True
              and parsed["service_gap"] == 0.0 and parsed["product_gap"] == 0.0
              and parsed["tips_gap"] == 0.0 and parsed["hours_gap"] == 0.0)
        check(f"[{lbl}] salon service {spec['service']}", parsed["salon_service_net"] == spec["service"])
        check(f"[{lbl}] salon product {spec['product']}", parsed["salon_product_net"] == spec["product"])
        check(f"[{lbl}] salon tips {spec['tips']}", parsed["salon_tips"] == spec["tips"])
        check(f"[{lbl}] salon hours {spec['hours']}", parsed["salon_productive_hours"] == spec["hours"])
        check(f"[{lbl}] active_count {spec['active']}", parsed["active_count"] == spec["active"])
        check(f"[{lbl}] {spec['artifacts']} artifact row(s)", len(parsed["artifacts"]) == spec["artifacts"])
        rows = P.build_zenoti_stylist_rows(path)
        check(f"[{lbl}] emits {spec['active']} clean 18-key zenoti rows w/ year_month",
              len(rows) == spec["active"] and all(
                  len(r) == 18 and r["platform"] == "zenoti" and r["loc_id"] == spec["loc_id"]
                  and r["year_month"] == "2026-04" for r in rows))
        check(f"[{lbl}] sum(emitted net_service) == salon_service",
              round(sum(r["net_service"] for r in rows), 2) == spec["service"])
    if ran == 0:
        print("SKIP - sweep: no other-location .xls files found")


if __name__ == "__main__":
    print("=== HELPERS (pure, synthetic Salon Summary text) ==="); helper_tests()
    print("\n=== CONTRACT (shared 18-key STYLISTS_DATA dict, no fork) ==="); contract_tests()
    print("\n=== ACCEPTANCE: real Forest Lake Salon Summary ==="); acceptance()
    print("\n=== SWEEP: all 8 other Zenoti salons' real .xls (4/1-4/12) ==="); acceptance_sweep()
    print("\n" + ("ALL PASS" if not FAILS else f"{len(FAILS)} FAILED: {FAILS}"))
    sys.exit(1 if FAILS else 0)
