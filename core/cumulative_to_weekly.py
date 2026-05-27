"""
core/cumulative_to_weekly.py
────────────────────────────
Pure-function differencing math for the cumulative-MTD → weekly conversion.

Karissa's POS reports are cumulative month-to-date (confirmed 2026-05-26 — see
sheets_writer.py CUMULATIVE_MTD section for context). This module converts:

    current_mtd_snapshot - prior_mtd_snapshot = true_weekly_value

for ADDITIVE primitives (sales, guests, hours, counts), then RECOMPUTES
DERIVED ratios (PPH, PPG, percentages, averages) from the differenced
primitives. Recomputing is essential — you cannot subtract ratios.

Edge cases handled:
  - prior_mtd is None → Week 1 of month → weekly = current_mtd as-is
  - Division by zero in derived KPIs → returns 0 (not NaN, not error)
  - Negative differences (refunds, corrections) → pass through; the
    resulting weekly value is genuinely negative, that's accurate
  - Missing fields in either input → treated as 0

The functions here are PURE — they never touch I/O, sheets, or globals.
All inputs come in as dicts, all outputs are dicts. Test density is high
(test_cumulative_to_weekly.py) because this is the heart of the pipeline.
"""

from __future__ import annotations


# Fields that are MTD-cumulative and SHOULD be subtracted to get weekly.
ADDITIVE_LOCATION_FIELDS = (
    "guests",
    "total_sales",
    "service",
    "product",
    "prod_hours",
    "wax_count",
    "wax",
    "color",
    "treat_count",
    "treat",
)

ADDITIVE_STYLIST_FIELDS = (
    "invoices",
    "guests",
    "net_service",
    "net_product",
    "production_hours",
)


def _safe_div(numerator: float, denominator: float) -> float:
    """Division with zero-denominator returning 0 (not NaN, not exception)."""
    if denominator == 0:
        return 0.0
    return numerator / denominator


def _additive_diff(
    current: dict,
    prior: dict | None,
    fields: tuple[str, ...],
) -> dict[str, float]:
    """Compute current[f] - prior[f] for each field in `fields`.

    Treats missing fields as 0. If `prior` is None (Week 1 case),
    returns current values directly.
    """
    out: dict[str, float] = {}
    for f in fields:
        cur_v = current.get(f, 0) or 0
        prior_v = (prior or {}).get(f, 0) or 0
        out[f] = cur_v - prior_v
    return out


def _recompute_location_kpis(diffed: dict) -> dict:
    """Recompute derived KPIs from differenced primitives.

    DO NOT subtract ratios from the source records — that's mathematically
    meaningless. Instead, take the already-differenced primitives and
    recompute each ratio fresh.
    """
    guests = diffed.get("guests", 0)
    total_sales = diffed.get("total_sales", 0)
    service = diffed.get("service", 0)
    product = diffed.get("product", 0)
    prod_hours = diffed.get("prod_hours", 0)
    wax_count = diffed.get("wax_count", 0)
    color = diffed.get("color", 0)
    treat_count = diffed.get("treat_count", 0)

    return {
        "product_pct": _safe_div(product, total_sales),
        "ppg": _safe_div(product, guests),
        "pph": _safe_div(service, prod_hours),
        "avg_ticket": _safe_div(total_sales, guests),
        "wax_pct": _safe_div(wax_count, guests),
        "color_pct": _safe_div(color, service),  # service_net denominator per CLAUDE.md
        "treat_pct": _safe_div(treat_count, guests),
    }


def _recompute_stylist_kpis(diffed: dict) -> dict:
    """Recompute derived stylist KPIs from differenced primitives."""
    invoices = diffed.get("invoices", 0)
    guests = diffed.get("guests", 0)
    net_service = diffed.get("net_service", 0)
    net_product = diffed.get("net_product", 0)
    production_hours = diffed.get("production_hours", 0)

    # Karissa uses invoice count == guest count for Zenoti; for SU they may
    # differ slightly. Use invoices when non-zero for avg_ticket / ppg
    # denominators (matches the original parser convention), fall back to
    # guests if invoices=0.
    denom = invoices if invoices else guests

    return {
        "avg_ticket": _safe_div(net_service + net_product, denom),
        "pph": _safe_div(net_service, production_hours),
        "ppg": _safe_div(net_product, denom),
    }


def difference_location_record(
    current_mtd: dict,
    prior_mtd: dict | None,
) -> dict:
    """Compute the true weekly record from a cumulative-MTD pair.

    Args:
      current_mtd: This Monday's cumulative-MTD snapshot for one location.
        Must have at least loc_name, year_month, week_ending, platform.
      prior_mtd: Last Monday's cumulative-MTD snapshot for the same
        (loc, year_month). Pass None when there is no prior snapshot
        (i.e., this is Week 1 of the month) — in that case the weekly
        value is the current_mtd as-is.

    Returns a dict with identity fields, differenced additive primitives,
    and freshly-recomputed derived KPIs.
    """
    diffed_additive = _additive_diff(current_mtd, prior_mtd, ADDITIVE_LOCATION_FIELDS)
    recomputed = _recompute_location_kpis(diffed_additive)

    return {
        # Identity carried from current
        "loc_name": current_mtd.get("loc_name", ""),
        "year_month": current_mtd.get("year_month", ""),
        "week_ending": current_mtd.get("week_ending", ""),
        "platform": current_mtd.get("platform", ""),
        # Differenced primitives
        **diffed_additive,
        # Recomputed derived KPIs
        **recomputed,
        # Provenance: keep the original source label so we know where the
        # cumulative came from
        "source": current_mtd.get("source", ""),
    }


def difference_stylist_record(
    current_mtd: dict,
    prior_mtd: dict | None,
) -> dict:
    """Compute the true weekly stylist record from a cumulative-MTD pair."""
    diffed_additive = _additive_diff(current_mtd, prior_mtd, ADDITIVE_STYLIST_FIELDS)
    recomputed = _recompute_stylist_kpis(diffed_additive)

    return {
        "year_month": current_mtd.get("year_month", ""),
        "week_ending": current_mtd.get("week_ending", ""),
        "name": current_mtd.get("name", ""),
        "loc_name": current_mtd.get("loc_name", ""),
        "loc_id": current_mtd.get("loc_id", ""),
        "platform": current_mtd.get("platform", ""),
        **diffed_additive,
        **recomputed,
        "source": current_mtd.get("source", ""),
    }


def difference_location_batch(
    current_snapshots: list[dict],
    prior_snapshots_by_loc: dict[str, dict],
) -> list[dict]:
    """Difference a batch of locations.

    Args:
      current_snapshots: list of this-week cumulative-MTD records, one per location.
      prior_snapshots_by_loc: {loc_name: prior_mtd_record}, lookup of prior week's
        cumulative for the same (loc, year_month). Locations not in this dict
        are treated as Week 1 (no prior).

    Returns weekly records in the same order as current_snapshots.
    """
    return [
        difference_location_record(cur, prior_snapshots_by_loc.get(cur.get("loc_name", "")))
        for cur in current_snapshots
    ]


def difference_stylist_batch(
    current_snapshots: list[dict],
    prior_snapshots_by_key: dict[tuple[str, str], dict],
) -> list[dict]:
    """Difference a batch of stylists.

    Args:
      current_snapshots: list of this-week cumulative-MTD stylist records.
      prior_snapshots_by_key: {(name, loc_name): prior_mtd_record}, lookup
        of prior week's cumulative for the same (name, loc, year_month).
        Stylists not in this dict are treated as Week 1.

    Returns weekly records in the same order as current_snapshots.
    """
    return [
        difference_stylist_record(
            cur,
            prior_snapshots_by_key.get((cur.get("name", ""), cur.get("loc_name", ""))),
        )
        for cur in current_snapshots
    ]
