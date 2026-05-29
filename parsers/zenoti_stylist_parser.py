"""
KPI Zenoti Salon Summary parser — STYLIST-grain STYLISTS_DATA source (Track B).

Track B of the 2x2 (POS x grain) data architecture. The Zenoti "Salon Summary"
export is the stylist-grain SIBLING of the salon-grain Track A
(parsers/pdf_hours_parser.parse_salon_summary -> parsers/locations_grouper
LOCATIONS_DATA): SAME vendor, SAME file, SAME read path. Track A already opens
this file for the SALES / SERVICE DETAILS / Invoice Summary / Employee
Performance blocks; Track B EXTENDS it to also read the two per-stylist Employee
tables it currently skips, and joins them.

FILE-FIRST, no API. The original Track B concept ("pull Zenoti stylist data from
the Tableau REST API") is SUPERSEDED. Elaina's reports now export as .xls/CSV
directly, and the Salon Summary already carries complete per-stylist data —
hours included — that reconciles to the penny. The Tableau API is repositioned
to a SEPARATE historical-backfill track (departed-stylist retention depth only),
NOT the routine weekly path. Because stylist + salon data come from the SAME
file, they CANNOT drift out of sync: salon-supremacy holds by construction.

THE TWO EMPLOYEE TABLES (both per stylist, joined on name):
  • EMPLOYEE SALE DETAILS    — NET SERVICE $ | COMM SERVICE $ | COMM DISC $ |
                               TIPS | SERVICE QTY | INVOICE COUNT | NET PRODUCT $ | ...
  • EMPLOYEE PERFORMANCE      — IN-SERVICE PRODUCTIVITY | IN-SERVICE HOURS |
                               ACTUAL HOURS | PRODUCTION HOURS | NON-PROD | BLOCKED | ...
Both interleave role-group rollup rows (MANAGER, STYLIST, SHIFT LEADER, Total)
with the individual stylist rows beneath them; the rollup's values equal the SUM
of its members, so including a rollup would double-count. We match REAL stylist
names and skip the rollups. We read PRODUCTION HOURS (4th perf numeric), never
ACTUAL HOURS, for salon-grain parity (a worked-vs-production distinction Zenoti
shares with SU — see Track D).

FORMAT-AGNOSTIC. We reuse Track A's `pdf_hours_parser._extract_text`, which
normalises BOTH the real PDF (pdfplumber) AND the HTML-disguised-.xls
(BeautifulSoup get_text) into one newline-joined text stream, then tokenise. The
same token-walker that pdf_hours_parser already uses to sum PRODUCTION HOURS to
the penny drives the SALE table here — so a future .xls export and today's PDF
parse identically with no second code path. (Contrast Track C/D's SU files,
which are binary OLE2 and need LibreOffice.)

RECONCILIATION (the safety net). Because both grains come from the SAME file,
this is a true internal-consistency check (salon-supremacy by construction). We
sum the per-stylist primitives over ALL provider rows (artifacts INCLUDED) and
assert they tie, to the penny, to the INDEPENDENTLY-parsed salon-level sections:

    sum(net_service) == SALES-block Service sales NET
    sum(net_product) == SALES-block Product sales NET
    sum(tips)        == TIPS COLLECTED Total
    sum(prod_hours)  == salon production hours (the per-stylist sum Track A uses)

Artifact / non-stylist rows (e.g. "Forest Lake mgr": 0 service, 0 hours, but $350
of unattributed retail and 3 invoices) carry real money that belongs to the salon
total — so they are INCLUDED in the reconciliation sum but EXCLUDED from the
emitted STYLISTS_DATA rows (the exact Track D artifact pattern). Fail loud
(strict=True) on any residual > $0.01 on any money leg — that gap is the
signature of a dropped or misparsed row. strict=False downgrades the raise to a
`reconciled=False` dict + flag (Track A/C/D parity).

GUEST COUNT IS READ, NOT RECONCILED (Karissa-confirmed). The SALE table gives a
per-stylist INVOICE COUNT; per Karissa's locked salon rule Zenoti guest_count =
"Total invoices with services or product", so the per-stylist dashboard-canonical
analog is the stylist's INVOICE COUNT — that is the guest_count we emit. We do
NOT reconcile the per-stylist invoice-count sum up to the salon guest count: guest
counts legitimately drift across grains (Forest Lake 5/1-5/24: stylist invoice sum
419 vs salon guest_count 415). Money reconciles; guests are read. If coaching later
needs unique-guest-per-stylist, the Sales Accrual CSV (distinct Guest Name per Sold
By) is the enrichment source — NOT this build.

DERIVED KPIs are computed from primitives (KARISSA_GOLDEN_RULES.md / Track D
convention), not read from the POS columns: avg_ticket = (service+product)/
guest_count; pph = service_net/prod_hours; ppg = product_net/guest_count. For
Zenoti these computed values equal the report's own printed columns to the cent
(Forest Lake / Danielle: (4925.10+414.75)/103 = 51.84 = the printed Avg Invoice
Value; 414.75/103 = 4.03 = the printed product/invoice), so "compute from
primitives" and the monthly Zenoti path agree — no fork, no divergence.

THE 18-KEY CONTRACT (no fork). Emits the SAME 18-key STYLISTS_DATA(_MONTHLY) dict
that scripts/backfill/monthly_pdf_loader._build_stylist_rows produces for the
zenoti branch. req_pct and avg_service_time_min are not cleanly extractable per
stylist from the Salon Summary (the perf table's REQ QTY rides a wrapped "(x.xx)"
artifact, and in-service-vs-production hours give different denominators), so they
ride at 0.0 — exactly how the monthly Zenoti path already defers
avg_service_time_min. The contract KEY SET is identical.

SINGLE-WINDOW PARSE. One Salon Summary file = one window; the file carries its own
date range (header "From: M/D/YYYY To: M/D/YYYY"), so there is NO Python
windowing here. Downstream MTD accumulation (append_to_historical, per Karissa's
month-to-date model — each stylist's numbers build up MTD, resetting monthly) is a
SEPARATE concern and is NOT built here.

VALIDATED — real Forest Lake Salon Summary (888-11812, 5/1-5/24/2026), penny-exact
against the independent salon sections:
    service 20,508.35 / product 2,211.65 / tips 3,250.37 / prod-hours 439.54
    6 active stylists (+ "Forest Lake mgr" artifact filtered: 0 svc / 0 hrs / $350 retail)
    Danielle Carlson  svc 4,925.10  tips 652.81  qty 141  inv 103  prod-hrs 84.97
    Dani Shearen      svc 4,169.00  tips 636.59  qty 115  inv  82  prod-hrs 79.73
"""
from __future__ import annotations

import re

# Reuse Track A's proven Salon Summary read path + helpers — SAME vendor, SAME
# file, so the text extraction, the SALES-block parser, the production-hours
# token-walker, and the name/role/artifact rules are shared verbatim (no
# duplication, no second extraction layer to maintain).
from parsers.pdf_hours_parser import (  # noqa: F401
    _extract_text,
    _parse_sales_from_text,
    parse_production_hours,
    ROLE_LABELS,
    ARTIFACT_NAMES,
    NUM,
    PCT_PREFIX,
)

# Center-name -> location crosswalk, shared with Track A's salon-grain generator
# so the stylist rows and the salon row for the SAME file resolve to the SAME
# location_id (the whole point of file-first: internal consistency).
from parsers.locations_grouper import CENTER_NAME_TO_ID, CROSSWALK

# Non-stylist routing pseudo-providers to EXCLUDE from emission (but INCLUDE in
# the reconciliation sum — they carry real salon money). Matched case-insensitive,
# whitespace-collapsed, by exact name or substring. The active-row rule (service>0
# OR hours>0) already filters the common "Forest Lake mgr" 0/0 case; this set is
# the belt-and-suspenders name guard for money-bearing artifacts (a "House Sale"
# row holding retail with no hours, etc.).
_ARTIFACT_SUBSTRINGS = ("house sale", "unknown", " mgr")
_ARTIFACT_EXACT = {"all"} | set(ARTIFACT_NAMES)

# SALE-table individual-row column positions (left-anchored — stable across PDF and
# .xls, and robust to right-side column drift). Role rollups omit INVOICE COUNT, so
# their layout differs; we never read a rollup (matched out by name).
_SALE_NET_SERVICE = 0
_SALE_TIPS = 3
_SALE_SERVICE_QTY = 4
_SALE_INVOICE_COUNT = 5
_SALE_NET_PRODUCT = 6
_SALE_MIN_NUMS = 7  # need indices 0..6 present to trust the row


# ─────────────────────────────────────────────────────────────────────────────
# Pure helpers over text (no I/O — synthetic-text testable).
# ─────────────────────────────────────────────────────────────────────────────
def _num(s):
    """'4,925.10' -> 4925.10; None on empty/non-numeric. Never raises."""
    if s is None:
        return None
    try:
        return float(str(s).replace(",", ""))
    except ValueError:
        return None


def _div(num, den, ndigits):
    """None if numerator None, or denominator None/0. Never raises."""
    if num is None or den is None or den == 0:
        return None
    return round(num / den, ndigits)


# Month abbreviations for the .xls 'DD Mon YYYY' date dialect.
_MONTHS = {m: i for i, m in enumerate(
    ("jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"),
    start=1)}

# One date token in EITHER Salon Summary dialect: PDF 'M/D/YYYY' or .xls 'DD Mon YYYY'.
_DATE = r"(\d{1,2}/\d{1,2}/\d{4}|\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4})"


def _to_iso(s):
    """Salon Summary date -> 'YYYY-MM-DD'. Handles BOTH the PDF 'M/D/YYYY' (e.g.
    '5/1/2026') and the .xls 'DD Mon YYYY' (e.g. '01 Apr 2026') forms. None if it
    matches neither."""
    s = (s or "").strip()
    m = re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})$", s)
    if m:
        mo, d, y = (int(x) for x in m.groups())
        return f"{y:04d}-{mo:02d}-{d:02d}"
    m = re.match(r"(\d{1,2})\s+([A-Za-z]{3,9})\s+(\d{4})$", s)
    if m:
        mo = _MONTHS.get(m.group(2)[:3].lower())
        if mo:
            return f"{int(m.group(3)):04d}-{mo:02d}-{int(m.group(1)):02d}"
    return None


def _parse_period(text):
    """Salon Summary header window -> (start_iso, end_iso). Tolerates BOTH header
    dialects: the PDF 'From: 5/1/2026 To: 5/24/2026' and the .xls
    'From : 01 Apr 2026 To : 12 Apr 2026' (note the space-before-colon and the
    non-breaking-space padding, both absorbed by \\s). 'From'/'To' are matched
    independently so whatever pads between them is irrelevant."""
    fm = re.search(rf"\bFrom\s*:\s*{_DATE}", text)
    tm = re.search(rf"\bTo\s*:\s*{_DATE}", text)
    return (_to_iso(fm.group(1)) if fm else None,
            _to_iso(tm.group(1)) if tm else None)


def _resolve_location(text):
    """Resolve the salon from the Salon Summary header center name (e.g.
    '888-11812-Forest Lake') via the shared crosswalk. Returns
    (location_id, canonical_name, center_name, pos_system) — all None if the
    center name isn't recognised (the raw center string is still captured)."""
    for center_name, lid in CENTER_NAME_TO_ID.items():
        if center_name and center_name in text:
            xw = CROSSWALK.get(lid, {})
            return lid, xw.get("location_name_canonical"), center_name, xw.get("pos_system")
    # Unknown center: surface whatever the header shows for the flag/raw record.
    raw = re.search(r"(\d{3}-\d{3,6}-[^\n]+)", text)
    return None, None, (raw.group(1).strip() if raw else None), None


def _parse_tips_collected_from_text(text):
    """TIPS COLLECTED block Total -> float (independent salon anchor). The block
    is 'TIPS COLLECTED AMOUNT / Cash .. / Card .. / Others .. / Total <amount>'."""
    i = text.upper().find("TIPS COLLECTED")
    if i < 0:
        return None
    region = text[i:i + 400]
    m = re.search(r"Total\s+(-?[\d,]+\.\d+)", region)
    return _num(m.group(1)) if m else None


def _walk_employee_sales(text):
    """Walk the EMPLOYEE SALE DETAILS region -> list of per-stylist sale dicts.

    Uses the SAME token-walker pdf_hours_parser uses for PRODUCTION HOURS (proven
    on this exact file), so SALE-table names segment IDENTICALLY to the perf-table
    names — making the name join exact. Skips role rollups (MANAGER / STYLIST /
    SHIFT LEADER / Total) and page-break noise; keeps person-like rows (incl.
    money-bearing artifacts like 'Forest Lake mgr') with >= 7 trailing numerics.
    Maps the left-anchored columns only."""
    i = text.upper().find("EMPLOYEE SALE")
    if i < 0:
        return []
    j = text.upper().find("EMPLOYEE PERFORMANCE", i)
    region = text[i:(j if j > 0 else i + 4000)]

    tokens = [t.strip() for t in re.split(r"[|\n\t ]+", region) if t.strip()]
    tokens = [t for t in tokens if not PCT_PREFIX.match(t)]  # drop wrapped '(xx.xx)'

    rows: list[dict] = []
    k = 0
    while k < len(tokens) - _SALE_MIN_NUMS:
        if NUM.match(tokens[k]):
            k += 1
            continue
        name_words: list[str] = []
        m = k
        while m < len(tokens) and not NUM.match(tokens[m]) and len(name_words) < 3:
            name_words.append(tokens[m])
            m += 1
        name = " ".join(name_words)
        nums: list[str] = []
        p = m
        while p < len(tokens) and NUM.match(tokens[p]) and len(nums) < 12:
            nums.append(tokens[p])
            p += 1

        nl = name.lower()
        is_role = nl in ROLE_LABELS or any(w.upper() == w and len(w) > 3 for w in name_words)
        looks_person = len(name_words) >= 2 and name[0].isupper()
        if looks_person and not is_role and len(nums) >= _SALE_MIN_NUMS:
            net_product = _num(nums[_SALE_NET_PRODUCT])
            inv = _num(nums[_SALE_INVOICE_COUNT])
            qty = _num(nums[_SALE_SERVICE_QTY])
            rows.append({
                "name": name,
                "net_service": _num(nums[_SALE_NET_SERVICE]),
                "tips": _num(nums[_SALE_TIPS]),
                "service_qty": int(qty) if qty is not None else None,
                "invoice_count": int(inv) if inv is not None else None,
                "net_product": net_product,
            })
            k = p
        else:
            k += 1
    return rows


def _is_artifact(name):
    """A non-stylist routing/pseudo row (excluded from emission, included in the
    reconciliation sum). Case-insensitive, whitespace-collapsed."""
    nl = " ".join(str(name).split()).lower()
    if nl in _ARTIFACT_EXACT:
        return True
    return any(sub in nl for sub in _ARTIFACT_SUBSTRINGS)


def _build_stylist_fields(sale, prod_hours):
    """One provider row -> primitives + derived KPIs (computed from primitives,
    None/0-safe). guest_count = INVOICE COUNT per the Karissa Zenoti rule."""
    net_service = sale["net_service"]
    net_product = sale["net_product"]
    guest_count = sale["invoice_count"]
    total_net = None
    if net_service is not None or net_product is not None:
        total_net = round((net_service or 0.0) + (net_product or 0.0), 2)
    return {
        "name": sale["name"],
        "net_service": net_service,
        "net_product": net_product,
        "tips": sale["tips"],
        "service_qty": sale["service_qty"],
        "invoice_count": guest_count,
        "guest_count": guest_count,                 # = invoice count (dashboard-canonical)
        "production_hours": prod_hours,             # PRODUCTION HOURS (not Actual)
        "total_net": total_net,
        "avg_ticket": _div(total_net, guest_count, 2),
        "pph": _div(net_service, prod_hours, 2),
        "ppg": _div(net_product, guest_count, 2),
    }


def _parse_text(text, *, strict=True):
    """Pure Salon-Summary stylist parse over an in-memory text stream (no I/O).

    Joins EMPLOYEE SALE DETAILS (sales/tips/qty/invoices) with EMPLOYEE
    PERFORMANCE (production hours) on stylist name, reconciles the per-stylist
    sums to the independent salon sections, and classifies active vs artifact.
    Fails loud (strict) on any money-leg residual > $0.01."""
    location_id, canonical, center_name, pos_system = _resolve_location(text)
    period_start, period_end = _parse_period(text)

    sales = _walk_employee_sales(text)
    hours = parse_production_hours(text=text)  # {name: production_hours}

    # ── join on name (segmented identically by the shared walker) ──
    sale_names = [s["name"] for s in sales]
    providers: list[dict] = []
    flags: list[str] = []
    for s in sales:
        ph = hours.get(s["name"])
        if ph is None:
            flags.append(f"zss_sale_without_hours [{canonical or center_name}]: "
                         f"'{s['name']}' in SALE table has no EMPLOYEE PERFORMANCE row")
        providers.append(_build_stylist_fields(s, ph if ph is not None else 0.0))
    for hname in hours:
        if hname not in sale_names:
            flags.append(f"zss_hours_without_sale [{canonical or center_name}]: "
                         f"'{hname}' in PERFORMANCE table has no EMPLOYEE SALE row")

    # ── classify: active (emitted) vs artifact (filtered, but money kept) ──
    active: list[dict] = []
    artifacts: list[str] = []
    for f in providers:
        svc = f["net_service"] or 0.0
        hrs = f["production_hours"] or 0.0
        if _is_artifact(f["name"]) or (svc <= 0 and hrs <= 0):
            artifacts.append(f["name"])
        else:
            active.append(f)

    # ── reconciliation vs INDEPENDENT salon sections (sum over ALL providers) ──
    salon = _parse_sales_from_text(text)
    salon_service = salon.get("service_net")
    salon_product = salon.get("product_net")
    salon_tips = _parse_tips_collected_from_text(text)
    salon_hours = round(sum(hours.values()), 2) if hours else None

    sum_service = round(sum(f["net_service"] or 0.0 for f in providers), 2)
    sum_product = round(sum(f["net_product"] or 0.0 for f in providers), 2)
    sum_tips = round(sum(f["tips"] or 0.0 for f in providers), 2)
    sum_hours = round(sum(f["production_hours"] or 0.0 for f in providers), 2)

    def _gap(salon_v, sum_v):
        return round(salon_v - sum_v, 2) if salon_v is not None else None

    service_gap = _gap(salon_service, sum_service)
    product_gap = _gap(salon_product, sum_product)
    tips_gap = _gap(salon_tips, sum_tips)
    hours_gap = _gap(salon_hours, sum_hours)

    legs = [
        ("SERVICE", salon_service, sum_service, service_gap),
        ("PRODUCT", salon_product, sum_product, product_gap),
        ("TIPS", salon_tips, sum_tips, tips_gap),
        ("HOURS", salon_hours, sum_hours, hours_gap),
    ]
    problems = [
        f"{leg} sum({sv}) != salon({salon_v}) gap={gap}"
        for leg, salon_v, sv, gap in legs
        if salon_v is None or abs(gap) > 0.01
    ]
    reconciled = not problems
    if problems:
        msg = f"zss_unreconciled [{canonical or center_name}]: " + " ; ".join(problems)
        flags.append(msg)
        if strict:
            raise ValueError(msg)

    return {
        "report_type": "zenoti_salon_summary",
        "center_name": center_name,
        "location_id": location_id,
        "location_name_canonical": canonical,
        "pos_system": pos_system,
        "period_start": period_start,
        "period_end": period_end,
        "stylists": active,                 # ACTIVE only — what build_zenoti_stylist_rows emits
        "artifacts": artifacts,
        "active_count": len(active),
        "salon_service_net": salon_service,
        "salon_product_net": salon_product,
        "salon_tips": salon_tips,
        "salon_productive_hours": salon_hours,
        "sum_service": sum_service,
        "sum_product": sum_product,
        "sum_tips": sum_tips,
        "sum_hours": sum_hours,
        "service_gap": service_gap,
        "product_gap": product_gap,
        "tips_gap": tips_gap,
        "hours_gap": hours_gap,
        "reconciled": reconciled,
        "flags": flags,
    }


def _stylist_rows_from_parsed(parsed, *, location_id, year_month, period_start,
                              period_end, source):
    """Map a parsed dict's ACTIVE stylists into the 18-key STYLISTS_DATA(_MONTHLY)
    contract — byte-for-byte the keys monthly_pdf_loader._build_stylist_rows emits
    for the zenoti branch (no schema fork). Pure (no I/O).

    invoices == guests == invoice_count (Karissa's Zenoti invoice_count =
    guest_count convention). req_pct / avg_service_time_min ride at 0.0 — not
    cleanly extractable per stylist from the Salon Summary (see module docstring)."""
    loc_name = parsed["location_name_canonical"]
    rows: list[dict] = []
    for s in parsed["stylists"]:
        guests = s["guest_count"] or 0
        rows.append({
            "year_month": year_month,
            "name": s["name"],
            "loc_name": loc_name,
            "loc_id": location_id,
            "platform": "zenoti",
            "invoices": guests,            # = guests (Zenoti invoice_count = guest_count)
            "guests": guests,              # = INVOICE COUNT (dashboard-canonical)
            "net_service": round(s["net_service"] or 0.0, 2),
            "net_product": round(s["net_product"] or 0.0, 2),
            "avg_ticket": s["avg_ticket"] if s["avg_ticket"] is not None else 0.0,
            "pph": s["pph"] if s["pph"] is not None else 0.0,
            "ppg": s["ppg"] if s["ppg"] is not None else 0.0,
            "production_hours": round(s["production_hours"] or 0.0, 2),  # PRODUCTION HOURS
            "source": source,
            "period_start": period_start,
            "period_end": period_end,
            "req_pct": 0.0,                # not reliably per-stylist in the Salon Summary
            "avg_service_time_min": 0.0,   # deferred (Zenoti parity — monthly path defers too)
        })
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# I/O wrappers (public API).
# ─────────────────────────────────────────────────────────────────────────────
def parse_zenoti_salon_summary(path, *, strict=True):
    """Parse ONE Zenoti Salon Summary (.pdf or HTML-.xls) -> rich stylist-grain
    dict. See the module docstring for the reconciliation contract. strict=False
    downgrades a reconciliation failure from raise to a `reconciled=False` dict +
    flag (Track A/C/D parity)."""
    return _parse_text(_extract_text(path), strict=strict)


def build_zenoti_stylist_rows(path, *, location_id=None, year_month=None,
                              period_start=None, period_end=None,
                              source="zenoti_salon_summary", strict=True):
    """Parse ONE Salon Summary -> list of 18-key STYLISTS_DATA(_MONTHLY) rows for
    the ACTIVE stylists (the shared Zenoti/SU stylist contract — no fork).

    The file carries its own window and center name, so location_id / period_* /
    year_month default to the parsed values (location_id from the center-name
    crosswalk — same id Track A's salon row uses; year_month from period_start)."""
    parsed = parse_zenoti_salon_summary(path, strict=strict)
    location_id = location_id or parsed["location_id"]
    period_start = period_start or parsed["period_start"]
    period_end = period_end or parsed["period_end"]
    year_month = year_month or (period_start[:7] if period_start else None)
    return _stylist_rows_from_parsed(
        parsed, location_id=location_id, year_month=year_month,
        period_start=period_start, period_end=period_end, source=source,
    )


if __name__ == "__main__":
    import argparse
    import json as _json

    ap = argparse.ArgumentParser(description="Parse one Zenoti Salon Summary (.pdf/.xls) at stylist grain.")
    ap.add_argument("path", help="Path to the Zenoti Salon Summary file")
    ap.add_argument("--rows", action="store_true", help="Also print the assembled STYLISTS_DATA rows")
    args = ap.parse_args()

    parsed = parse_zenoti_salon_summary(args.path)
    summary = {k: v for k, v in parsed.items() if k != "stylists"}
    summary["stylists_preview"] = [
        {"name": s["name"], "net_service": s["net_service"], "tips": s["tips"],
         "guest_count": s["guest_count"], "production_hours": s["production_hours"]}
        for s in parsed["stylists"]
    ]
    print(_json.dumps(summary, indent=2, default=str))
    if args.rows:
        rows = build_zenoti_stylist_rows(args.path)
        print(f"\n{len(rows)} STYLISTS_DATA rows:")
        for r in rows:
            print(f"  {r['name']:<22} svc={r['net_service']:>9.2f} prod={r['net_product']:>8.2f} "
                  f"guests={r['guests']:>3} hrs={r['production_hours']:>6.2f} "
                  f"pph={r['pph']:>6.2f} ppg={r['ppg']:>5.2f} avg={r['avg_ticket']:>6.2f}")
