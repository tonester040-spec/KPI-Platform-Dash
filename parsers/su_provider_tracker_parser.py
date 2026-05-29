"""
KPI Salon Ultimate (SU) Provider Tracker parser — STYLIST-grain STYLISTS_DATA source.

Track D of the 2x2 (POS x grain) data architecture. The SU "Provider Tracker" export is
the stylist-grain sibling of the SU "Salon Dashboard" (Track C, parsers/su_dashboard_parser.py):
same vendor, same binary OLE2 .xls container, same read path (LibreOffice soffice -> .xlsx
-> openpyxl over a single flat 'Worksheet'). It is NOT a Tableau/Zenoti source — SU has no
Tableau; its per-stylist data comes straight from this file, so SU stylist-grain is
self-serve with NO API/backfill dependency.

This parser emits per-stylist rows on the SAME 18-key STYLISTS_DATA(_MONTHLY) contract the
Zenoti/SU monthly path already produces (scripts/backfill/monthly_pdf_loader._build_stylist_rows
and zenoti_xlsx_loader._read_stylist_rows) — there is NO schema fork. The Provider Tracker's
extra per-stylist richness (color $/count/%, prebook, first-time, requests, retail clients)
has no home in that 18-key contract, so it rides along in the rich parse dict only — the
same way su_dashboard_parser keeps `tips_total` out of the 39-col LOCATIONS_DATA row.

RECONCILIATION (the safety net). Unlike Zenoti — where salon-supremacy is a *manual*
reconciliation because stylist and salon totals diverge — SU stylist-grain ties to SU
salon-grain natively, to the penny. The Provider Tracker Totals row Service Sales equals the
matched Salon Dashboard's Total Service exactly (Apple Valley 4/1-4/12: 36,718.05 ==
36,718.05). The per-row sum reconciles to the Totals row:

    sum(active stylist Service) + sum(filtered/artifact Service) == Totals Service

We assert THIS identity (active + filtered == Totals), NOT the narrower "active == Totals",
because artifact rows can carry service adjustments: Lakeville 4/1-4/12 routes a -$17.50
refund through the "House _" pseudo-provider, so its active-stylist sum (13,298.10) exceeds
the Totals (13,280.60) by exactly that 17.50. Apple Valley's artifacts are all zero, so there
active == Totals == 36,718.05. Fail loud (strict=True) on any residual > $0.01 — that gap is
the signature of a dropped or misparsed stylist row.

HOURS (do NOT reconcile into a false match). Col D "Hours Worked" (AV total 776.45) is
clocked/worked hours — a DIFFERENT metric from the Salon Dashboard's "Production Hours"
(689.27), the same worked-vs-production distinction Zenoti has. They SHOULD differ. We store
"Hours Worked" as the per-stylist productive_hours (it is what the report gives per stylist)
and never tie it to Production Hours.

DERIVED KPIs are computed from primitives per the KPI contract (KARISSA_GOLDEN_RULES.md),
not read from the POS columns. The Provider Tracker's printed Avg Ticket (col L), Product
$/Ticket (col E) and Color % (col O) use a (service + retail)-client denominator — or, for
Color %, a count penetration (color clients / service clients) — that differs from Karissa's
definitions. The printed values are retained in the rich dict as cross-checks (the pos_*
fields) but are never emitted.

ACTIVE-ROW RULE (§ retention signal). Keep a provider row if Service Sales > 0 OR Hours
Worked > 0 — never service alone. Some stylists clock hours with zero service (Apple Valley:
Brittany Gold 18.08h, Jessica Marsh 28.49h — front-desk/training/assist, and a tipping-point
departure signal). Those rows are kept; rows that are all-zero, and the artifact
pseudo-providers, are filtered.

VALIDATED: Apple Valley 04/01-04/12/2026 -> Totals service 36,718.05 / retail 4,624.38 /
hours worked 776.45 / service clients 647; 17 active stylists; 3 artifacts (Booked Online /
House _ / Salon Ultimate) + 16 all-zero rows filtered; reconciliation gap $0.00.
"""
from __future__ import annotations

import hashlib
import os
from datetime import datetime, timezone

# Reuse Track C's proven SU read path + helpers — same vendor, same OLE2 format, so the
# soffice->xlsx->openpyxl pipeline and the number/store/period helpers are shared verbatim
# (no duplication, no second LibreOffice integration to maintain).
from parsers.su_dashboard_parser import (  # noqa: F401  (resolve/SU_LOCATIONS re-exported for callers)
    _find_soffice,
    _first_value,
    _num,
    _norm,
    _parse_period,
    _read_su_grid,
    SU_LOCATIONS,
    resolve_su_location,
)

# OLE2 / Compound File Binary signature — every SU .xls starts with these 8 bytes. The
# content-level "is this really a Provider Tracker?" gate is the 'Provider Name' header
# (see _find_header_row); this magic check just rejects HTML-disguised or wrong-format files
# before we hand them to LibreOffice.
_OLE2_MAGIC = bytes.fromhex("d0cf11e0a1b11ae1")

# Non-stylist routing pseudo-providers (SU's equivalent of the Tableau path's House Sale /
# FN Unknown). Matched exact, case-insensitive, whitespace-collapsed. Each may appear more
# than once (Farmington ships two "Salon Ultimate" rows) and may carry a service adjustment
# (Lakeville's "House _" = -$17.50) that legitimately belongs to the salon total.
_ARTIFACT_NAMES = {"booked online", "house _", "salon ultimate"}

# Provider Tracker column-header text (lowercased, whitespace-collapsed) -> parse-dict key.
# Resolved by NAME so a future column reorder can't silently shift the mapping.
_HEADER_MAP = {
    "provider name":    "name",
    "service sales":    "service_net",
    "retail sales":     "product_net",
    "hours worked":     "hours_worked",
    "product $/ticket": "pos_product_per_ticket",
    "serv $ hour":      "pos_pph",
    "retail client":    "retail_clients",
    "retail clients":   "retail_clients",   # tolerate the plural form
    "service clients":  "service_clients",
    "prebk. guest":     "prebook_guests",
    "prebk. %":         "prebook_pct",
    "first time":       "first_time",
    "avg ticket":       "pos_avg_ticket",
    "color sales":      "color_net",
    "total color":      "color_count",
    "color %":          "pos_color_pct",
    "total req":        "total_req",
    "req %":            "req_pct_raw",
}

# Every per-stylist field the parser surfaces in the rich dict (primitives + derived +
# pos_* cross-checks). Used to build the Totals-row dict too.
_PRIMITIVE_KEYS = (
    "service_net", "product_net", "hours_worked", "service_clients", "retail_clients",
    "color_net", "color_count", "prebook_guests", "prebook_pct", "first_time",
    "total_req", "req_pct_raw", "pos_product_per_ticket", "pos_pph", "pos_avg_ticket",
    "pos_color_pct",
)


# ─────────────────────────────────────────────────────────────────────────────
# Pure helpers over the grid (no I/O — synthetic-grid testable).
# ─────────────────────────────────────────────────────────────────────────────
def _key(v) -> str:
    """Lowercase + whitespace-collapse a cell for header/name matching."""
    return " ".join(str(v).split()).strip().lower() if v is not None else ""


def _div(num, den, ndigits):
    """None if numerator None, or denominator None/0. Never raises."""
    if num is None or den is None or den == 0:
        return None
    return round(num / den, ndigits)


def _cell(row, colmap, name_key):
    """Value at the resolved column for `name_key`, or None if the column is
    absent or the row is short."""
    c = colmap.get(name_key)
    if c is None or c >= len(row):
        return None
    return row[c]


def _find_header_row(grid):
    """Locate the Provider Tracker column-header row (first cell == 'Provider Name')
    and return (header_idx, colmap: dict name_key -> column index).

    This IS the content-level "is this a Provider Tracker?" gate. A Salon Dashboard
    has a 'Sales | $ | %' block where this header would be, so a missing 'Provider
    Name' header means wrong report type — raise (fail loud)."""
    for i, row in enumerate(grid):
        if not row:
            continue
        if _key(row[0]) == "provider name":
            colmap: dict[str, int] = {}
            for c, cell in enumerate(row):
                k = _HEADER_MAP.get(_key(cell))
                if k and k not in colmap:
                    colmap[k] = c
            if "service_net" not in colmap:
                raise ValueError(
                    "Provider Tracker header row found but no 'Service Sales' column "
                    f"(headers seen: {[_key(c) for c in row if c is not None]})"
                )
            return i, colmap
    raise ValueError(
        "Not a Salon Ultimate Provider Tracker: no 'Provider Name' header row found. "
        "A Salon Dashboard has a 'Sales/$/%' block here — use su_dashboard_parser instead."
    )


def _build_stylist_fields(row, colmap) -> dict:
    """Extract one provider row into primitives + cross-checks + derived KPIs.

    guest_count uses 'Service Clients' (col H) per the Track D contract — per-stylist
    serviced guests. Derived KPIs are computed from primitives (NOT the pos_* columns):
      ppg_net    = product_net / guest_count
      pph        = service_net / hours_worked   (equals the printed 'Serv $ Hour' col F)
      avg_ticket = (service_net + product_net) / guest_count
      color_pct  = color_net / service_net      (Karissa's service-revenue share)
      req_pct    = 'Req %' / 100                 (printed as a percent number; stored as a fraction)
    """
    service_net = _num(_cell(row, colmap, "service_net"))
    product_net = _num(_cell(row, colmap, "product_net"))
    hours_worked = _num(_cell(row, colmap, "hours_worked"))
    service_clients = _num(_cell(row, colmap, "service_clients"))
    retail_clients = _num(_cell(row, colmap, "retail_clients"))
    color_net = _num(_cell(row, colmap, "color_net"))
    color_count = _num(_cell(row, colmap, "color_count"))
    prebook_guests = _num(_cell(row, colmap, "prebook_guests"))
    prebook_pct = _num(_cell(row, colmap, "prebook_pct"))
    first_time = _num(_cell(row, colmap, "first_time"))
    total_req = _num(_cell(row, colmap, "total_req"))
    req_pct_raw = _num(_cell(row, colmap, "req_pct_raw"))

    guest_count = int(service_clients) if service_clients is not None else None
    total_net = None
    if service_net is not None or product_net is not None:
        total_net = round((service_net or 0.0) + (product_net or 0.0), 2)

    return {
        "name": _norm(_cell(row, colmap, "name")),
        # primitives
        "service_net": service_net,
        "product_net": product_net,
        "hours_worked": hours_worked,
        "service_clients": service_clients,
        "retail_clients": retail_clients,
        "guest_count": guest_count,                 # = service_clients (serviced guests)
        "color_net": color_net,
        "color_count": int(color_count) if color_count is not None else None,
        # SU-extended (no home in the 18-key contract — ride along here only)
        "prebook_guests": int(prebook_guests) if prebook_guests is not None else None,
        "prebook_pct": prebook_pct,
        "first_time": int(first_time) if first_time is not None else None,
        "total_req": int(total_req) if total_req is not None else None,
        # POS-printed cross-checks (different denominators — never emitted)
        "pos_product_per_ticket": _num(_cell(row, colmap, "pos_product_per_ticket")),
        "pos_pph": _num(_cell(row, colmap, "pos_pph")),
        "pos_avg_ticket": _num(_cell(row, colmap, "pos_avg_ticket")),
        "pos_color_pct": _num(_cell(row, colmap, "pos_color_pct")),
        # derived from primitives (None/0-safe)
        "total_net": total_net,
        "ppg_net": _div(product_net, guest_count, 2),
        "pph": _div(service_net, hours_worked, 2),
        "avg_ticket": _div(total_net, guest_count, 2),
        "color_pct": _div(color_net, service_net, 4),
        "req_pct": round(req_pct_raw / 100.0, 4) if req_pct_raw is not None else None,
    }


def _parse_grid(grid, *, strict=True) -> dict:
    """Pure Provider-Tracker parse over an in-memory grid (no I/O).

    Filters artifact pseudo-providers and all-zero rows; keeps active stylists
    (service > 0 OR hours > 0). Reconciles active + filtered service to the Totals
    row and fails loud (strict) on a residual > $0.01."""
    store_name = _norm(_first_value(grid, "Store name:")) or None
    period_start, period_end, period_label = _parse_period(grid)
    location_id, canonical = (None, None)
    if store_name:
        location_id, canonical = resolve_su_location(store_name)

    header_idx, colmap = _find_header_row(grid)

    active: list[dict] = []
    artifact_names: list[str] = []
    zero_row_count = 0
    filtered_service = 0.0      # service summed over artifact + all-zero rows
    totals: dict | None = None

    for row in grid[header_idx + 1:]:
        if not row:
            continue
        raw_name = _cell(row, colmap, "name")
        nkey = _key(raw_name)
        if not nkey:
            continue
        if nkey.startswith("totals"):
            totals = _build_stylist_fields(row, colmap)
            break
        fields = _build_stylist_fields(row, colmap)
        svc = fields["service_net"] or 0.0
        hrs = fields["hours_worked"] or 0.0
        if nkey in _ARTIFACT_NAMES:
            artifact_names.append(fields["name"])
            filtered_service += svc
            continue
        if svc > 0 or hrs > 0:
            active.append(fields)
        else:
            zero_row_count += 1
            filtered_service += svc

    totals_service = totals["service_net"] if totals else None
    active_service_sum = round(sum(f["service_net"] or 0.0 for f in active), 2)
    filtered_service_sum = round(filtered_service, 2)
    all_sum = round(active_service_sum + filtered_service_sum, 2)
    gap = round(totals_service - all_sum, 2) if totals_service is not None else None
    reconciled = gap is not None and abs(gap) <= 0.01
    active_vs_totals_gap = (
        round(totals_service - active_service_sum, 2) if totals_service is not None else None
    )

    flags: list[str] = []
    if totals_service is None:
        msg = f"su_pt_no_totals_row [{canonical or store_name}]: Totals row not found"
        flags.append(msg)
        if strict:
            raise ValueError(msg)
    elif not reconciled:
        msg = (
            f"su_pt_unreconciled [{canonical or store_name}]: "
            f"active({active_service_sum}) + filtered({filtered_service_sum}) = {all_sum} "
            f"!= Totals service({totals_service}); gap={gap}"
        )
        flags.append(msg)
        if strict:
            raise ValueError(msg)

    # Informational (NOT fatal): artifacts carried a service adjustment, so the active
    # sum differs from the Totals even though the row sum reconciles (e.g. Lakeville's
    # -$17.50 House _ refund). Surfaced so the divergence is never a silent surprise.
    if active_vs_totals_gap not in (None, 0.0):
        flags.append(
            f"su_pt_artifact_service_adjustment [{canonical or store_name}]: active stylist "
            f"service {active_service_sum} != Totals {totals_service} by {active_vs_totals_gap} "
            f"(routed through artifact/zero rows; included in the salon total)"
        )

    return {
        "report_type": "provider_tracker",
        "store_name": store_name,
        "location_id": location_id,
        "location_name_canonical": canonical,
        "period_start": period_start,
        "period_end": period_end,
        "period_label": period_label,
        "stylists": active,                 # ACTIVE only — what build_su_stylist_rows emits
        "artifacts": artifact_names,
        "zero_row_count": zero_row_count,
        "active_count": len(active),
        "totals": totals,
        "totals_service": totals_service,
        "totals_retail": totals["product_net"] if totals else None,
        "totals_hours_worked": totals["hours_worked"] if totals else None,
        "totals_service_clients": totals["guest_count"] if totals else None,
        "active_service_sum": active_service_sum,
        "filtered_service_sum": filtered_service_sum,
        "reconciliation_gap": gap,
        "reconciled": reconciled,
        "active_vs_totals_gap": active_vs_totals_gap,
        "flags": flags,
    }


def _stylist_rows_from_parsed(parsed, *, location_id, year_month, period_start,
                              period_end, source) -> list[dict]:
    """Map a parsed dict's ACTIVE stylists into the 18-key STYLISTS_DATA(_MONTHLY)
    contract — byte-for-byte the same keys monthly_pdf_loader._build_stylist_rows
    emits for the salon_ultimate branch (no schema fork). Pure (no I/O)."""
    loc_name = parsed["location_name_canonical"]
    rows: list[dict] = []
    for s in parsed["stylists"]:
        guests = s["guest_count"] or 0
        rows.append({
            "year_month": year_month,
            "name": s["name"],
            "loc_name": loc_name,
            "loc_id": location_id,
            "platform": "salon_ultimate",
            "invoices": guests,            # SU has no invoice/guest split at stylist grain
            "guests": guests,              # = Service Clients (serviced guests)
            "net_service": round(s["service_net"] or 0.0, 2),
            "net_product": round(s["product_net"] or 0.0, 2),
            "avg_ticket": s["avg_ticket"] if s["avg_ticket"] is not None else 0.0,
            "pph": s["pph"] if s["pph"] is not None else 0.0,
            "ppg": s["ppg_net"] if s["ppg_net"] is not None else 0.0,
            "production_hours": round(s["hours_worked"] or 0.0, 2),  # WORKED hrs (see docstring; NOT Production Hours)
            "source": source,
            "period_start": period_start,
            "period_end": period_end,
            "req_pct": s["req_pct"] if s["req_pct"] is not None else 0.0,
            "avg_service_time_min": 0.0,   # not reported per-stylist by the Provider Tracker
        })
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# I/O wrappers (public API).
# ─────────────────────────────────────────────────────────────────────────────
def _confirm_ole2(xls_path) -> None:
    """Reject anything that isn't a binary OLE2 .xls before invoking LibreOffice."""
    if not os.path.exists(xls_path):
        raise FileNotFoundError(xls_path)
    with open(xls_path, "rb") as f:
        head = f.read(8)
    if head != _OLE2_MAGIC:
        raise ValueError(
            f"{os.path.basename(xls_path)} is not an OLE2 .xls (magic={head.hex()!r}); "
            "SU Provider Tracker exports are binary OLE2. Refusing to parse."
        )


def parse_su_provider_tracker(xls_path, *, strict=True) -> dict:
    """Parse ONE SU Provider Tracker .xls -> rich stylist-grain dict.

    Confirms the file is OLE2 (magic bytes) and a Provider Tracker (the 'Provider
    Name' header, enforced inside _parse_grid) before trusting it. See module docstring
    for the reconciliation contract. strict=False downgrades a reconciliation failure
    from raise to a `reconciled=False` dict + flag (Track C parity)."""
    _confirm_ole2(xls_path)
    return _parse_grid(_read_su_grid(xls_path), strict=strict)


def build_su_stylist_rows(xls_path, *, location_id=None, year_month=None,
                          period_start=None, period_end=None,
                          source="su_provider_tracker", strict=True) -> list[dict]:
    """Parse ONE Provider Tracker .xls -> list of 18-key STYLISTS_DATA(_MONTHLY) rows
    for the ACTIVE stylists (the shared Zenoti/SU stylist contract — no fork).

    The file carries its own window, so location_id / period_* / year_month default to
    the parsed values (location_id from the store-name crosswalk; year_month from
    period_start's YYYY-MM)."""
    parsed = parse_su_provider_tracker(xls_path, strict=strict)
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

    ap = argparse.ArgumentParser(description="Parse one Salon Ultimate Provider Tracker .xls.")
    ap.add_argument("xls", help="Path to the SU Provider Tracker .xls")
    ap.add_argument("--rows", action="store_true", help="Also print the assembled STYLISTS_DATA rows")
    args = ap.parse_args()

    parsed = parse_su_provider_tracker(args.xls)
    # Trim the per-stylist dicts for a readable summary
    summary = {k: v for k, v in parsed.items() if k != "stylists"}
    summary["stylists_preview"] = [
        {"name": s["name"], "service_net": s["service_net"], "product_net": s["product_net"],
         "hours_worked": s["hours_worked"], "guest_count": s["guest_count"]}
        for s in parsed["stylists"]
    ]
    print(_json.dumps(summary, indent=2, default=str))
    if args.rows:
        rows = build_su_stylist_rows(args.xls)
        print(f"\n{len(rows)} STYLISTS_DATA rows:")
        for r in rows:
            print(f"  {r['name']:<26} svc={r['net_service']:>9.2f} prod={r['net_product']:>8.2f} "
                  f"guests={r['guests']:>3} hrs={r['production_hours']:>6.2f} "
                  f"pph={r['pph']:>6.2f} ppg={r['ppg']:>5.2f} avg={r['avg_ticket']:>6.2f}")
