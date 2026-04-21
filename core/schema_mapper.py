"""
core/schema_mapper.py
KPI Platform — Translates existing data_processor output into v2 schema.

Why this exists
---------------
The production pipeline's data_processor emits dicts keyed by the legacy
Google-Sheet column names (loc_name, week_ending, platform, guests, service,
product_pct, ppg, pph, treat_count, ...). The v2 schema uses cleaner names
(location, period_end, pos_system, guest_count, service_net, product_pct,
ppg_net, pph_net, treatment_count, ...) and stores percentages as fractions
(0.185 not 18.5) so they render correctly with Sheets' native "0.00%" format.

Rather than refactor data_processor (which is production-critical and well-tested),
we translate at the edge. Every dual-write call runs its rows through the
appropriate map_* function before calling GoogleSheetsStore.

Percent handling
----------------
    Legacy: 18.5  means "18.5 percent"
    v2:     0.185 means "18.5 percent" (stored as a fraction, Sheets format "0.00%")

All *_pct fields go through _pct_fraction() which divides by 100 unless the value
already looks like a fraction (<= 1.0).

Missing fields
--------------
map_* functions never raise on missing keys. Missing fields default to "" (string)
or 0 (numeric) so the resulting rows always satisfy the v2 schema's required
column set. Callers who care about completeness should validate upstream.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any


def _pct_fraction(value: Any) -> float:
    """
    Convert a legacy percentage to a 0-1 fraction.

    Handles the most common inputs:
        18.5   -> 0.185
        0.185  -> 0.185  (already a fraction; pass-through)
        "18.5" -> 0.185
        None   -> 0.0
        ""     -> 0.0
    """
    if value is None or value == "":
        return 0.0
    try:
        v = float(value)
    except (TypeError, ValueError):
        return 0.0
    # Heuristic: if |v| <= 1.0, assume it's already a fraction.
    # (Edge case: 100% stored as 1.0 passes through; 101% stored as 1.01 also
    # passes through — both are the correct interpretation.)
    if -1.0 <= v <= 1.0:
        return v
    return v / 100.0


def _to_float(value: Any, default: float = 0.0) -> float:
    if value is None or value == "":
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_int(value: Any, default: int = 0) -> int:
    if value is None or value == "":
        return default
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _to_str(value: Any, default: str = "") -> str:
    if value is None:
        return default
    return str(value)


def _derive_period_start(period_end: str) -> str:
    """
    Derive period_start as 6 days before period_end (a weekly window).

    Falls back to the same value as period_end if parsing fails —
    downstream consumers should treat period_start as best-effort.
    """
    if not period_end:
        return ""
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%d-%b-%Y"):
        try:
            dt = datetime.strptime(period_end, fmt)
            return (dt - timedelta(days=6)).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return period_end  # parse failed — punt


# ─── Locations ───────────────────────────────────────────────────────────────

def map_location_row(loc: dict[str, Any]) -> dict[str, Any]:
    """
    Translate one data_processor location dict -> v2 LOCATION_COLUMNS dict.

    Input shape (data_processor output):
        loc_name, week_ending, platform, guests, total_sales, service, product,
        product_pct, ppg, pph, avg_ticket, prod_hours, wax_count, wax, wax_pct,
        color, color_pct, treat_count, treat, treat_pct, flag, coach, coach_emoji

    Output shape (v2):
        location, period_start, period_end, pos_system, guest_count, total_sales,
        service_net, product_net, product_pct, ppg_net, pph_net, avg_ticket,
        productive_hours, wax_count, wax_net, wax_pct, color_net, color_pct,
        treatment_count, treatment_net, treatment_pct, flag, coach, coach_emoji
    """
    period_end = _to_str(loc.get("week_ending"))
    return {
        "location":         _to_str(loc.get("loc_name")),
        "period_start":     _derive_period_start(period_end),
        "period_end":       period_end,
        "pos_system":       _to_str(loc.get("platform")),
        "guest_count":      _to_int(loc.get("guests")),
        "total_sales":      _to_float(loc.get("total_sales")),
        "service_net":      _to_float(loc.get("service")),
        "product_net":      _to_float(loc.get("product")),
        "product_pct":      _pct_fraction(loc.get("product_pct")),
        "ppg_net":          _to_float(loc.get("ppg")),
        "pph_net":          _to_float(loc.get("pph")),
        "avg_ticket":       _to_float(loc.get("avg_ticket")),
        "productive_hours": _to_float(loc.get("prod_hours")),
        "wax_count":        _to_float(loc.get("wax_count")),
        "wax_net":          _to_float(loc.get("wax")),
        "wax_pct":          _pct_fraction(loc.get("wax_pct")),
        "color_net":        _to_float(loc.get("color")),
        "color_pct":        _pct_fraction(loc.get("color_pct")),
        "treatment_count":  _to_float(loc.get("treat_count")),
        "treatment_net":    _to_float(loc.get("treat")),
        "treatment_pct":    _pct_fraction(loc.get("treat_pct")),
        "flag":             _to_str(loc.get("flag"), default="solid"),
        "coach":            _to_str(loc.get("coach")),
        "coach_emoji":      _to_str(loc.get("coach_emoji")),
    }


def map_location_rows(locs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [map_location_row(loc) for loc in locs]


# ─── Stylists ────────────────────────────────────────────────────────────────

def map_stylist_row(
    s: dict[str, Any],
    *,
    period_end: str | None = None,
) -> dict[str, Any]:
    """
    Translate one data_processor stylist dict -> v2 STYLIST_COLUMNS dict.

    Input shape (data_processor output; note the inconsistency between
    'tenure' in STYLISTS_CURRENT and 'tenure_yrs' in STYLISTS_DATA — we
    check both):
        name, loc_name, loc_id, status, tenure / tenure_yrs,
        cur_pph, cur_rebook, cur_product, cur_ticket,
        services (list), color (list), weeks (list), coach, coach_emoji

    Output shape (v2):
        period_end, stylist_name, location, location_id, status, tenure_years,
        pph, rebook_pct, product_pct, avg_ticket, services, color,
        coach, coach_emoji
    """
    # period_end resolution order:
    #   1. explicit kwarg
    #   2. last value of s["weeks"] (STYLISTS_CURRENT convention)
    #   3. empty
    if period_end is None:
        weeks = s.get("weeks")
        if isinstance(weeks, list) and weeks:
            period_end = _to_str(weeks[-1])
        else:
            period_end = ""

    # Tenure: accept either key (existing code has both).
    tenure_raw = s.get("tenure_yrs", s.get("tenure"))

    # services / color are lists in data_processor — take last value.
    services_list = s.get("services") or []
    color_list = s.get("color") or []
    services_latest = services_list[-1] if services_list else 0
    color_latest = color_list[-1] if color_list else 0

    return {
        "period_end":    _to_str(period_end),
        "stylist_name":  _to_str(s.get("name")),
        "location":      _to_str(s.get("loc_name")),
        "location_id":   _to_str(s.get("loc_id")),
        "status":        _to_str(s.get("status"), default="active"),
        "tenure_years":  _to_float(tenure_raw),
        "pph":           _to_float(s.get("cur_pph")),
        "rebook_pct":    _pct_fraction(s.get("cur_rebook")),
        "product_pct":   _pct_fraction(s.get("cur_product")),
        "avg_ticket":    _to_float(s.get("cur_ticket")),
        "services":      _to_float(services_latest),
        "color":         _to_float(color_latest),
        "coach":         _to_str(s.get("coach")),
        "coach_emoji":   _to_str(s.get("coach_emoji")),
    }


def map_stylist_rows(
    stylists: list[dict[str, Any]],
    *,
    period_end: str | None = None,
) -> list[dict[str, Any]]:
    return [map_stylist_row(s, period_end=period_end) for s in stylists]


# ─── Coach briefs ────────────────────────────────────────────────────────────
#
# coach_cards in the existing pipeline is a dict: {"Jess": {...}, "Jenn": {...}}
# with title-case manager names. v2 COACH_BRIEFS expects lowercase manager keys.

def normalize_manager_name(manager: str) -> str:
    """Lowercase + strip so 'Jess' / 'jess ' / 'JESS' all map to 'jess'."""
    return (manager or "").strip().lower()


def map_coach_briefs(coach_cards: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """
    Normalize a coach_cards dict to {lowercase_manager: brief_dict}.

    Skips entries where the brief is None or empty — those represent
    preservation cases (missing briefs should NOT overwrite prior-week data,
    which is the GoogleSheetsStore write_coach_briefs contract).
    """
    out: dict[str, dict[str, Any]] = {}
    if not coach_cards:
        return out
    for manager, brief in coach_cards.items():
        key = normalize_manager_name(manager)
        if not key:
            continue
        if isinstance(brief, dict) and brief:
            out[key] = brief
    return out
