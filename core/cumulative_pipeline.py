"""
core/cumulative_pipeline.py
───────────────────────────
Orchestration for the cumulative-MTD → weekly transformation step.

This module runs ONCE per Monday pipeline execution, between data_source
(which reads CURRENT as cumulative-MTD) and data_processor (which expects
true-weekly values). Sequence:

  1. Take the cumulative-MTD records just read from CURRENT/STYLISTS
  2. Snapshot them to CUMULATIVE_MTD / STYLISTS_CUMULATIVE_MTD (raw archive)
  3. Read all prior snapshots for the same year_month
  4. Find each location's / stylist's most recent prior snapshot
  5. Difference current vs prior to get true weekly values
  6. Return weekly records to the caller

Edge cases:
  - Week 1 of month: no prior found → weekly = current as-is
  - Empty CUMULATIVE_MTD tab (first run ever): treated as Week 1
  - Tab doesn't exist yet: auto-created by the writer
  - Dry-run: snapshot writes skipped; differencing still happens against
    whatever snapshots already exist in the tab (could be stale but
    consistent for testing)

Per Karissa 2026-05-26 Q5 + Q9: negative differences (refunds) and
stylist-sum drift vs salon total are EXPECTED and acceptable. Don't
raise — pass values through honestly.
"""

from __future__ import annotations

import logging

from core.cumulative_to_weekly import (
    difference_location_batch,
    difference_stylist_batch,
)
from core.data_source import (
    derive_year_month,
    find_latest_priors_by_location,
    find_latest_priors_by_stylist,
    read_cumulative_mtd_snapshots,
    read_stylists_cumulative_mtd_snapshots,
)
from core.sheets_writer import (
    append_to_cumulative_mtd,
    append_to_stylists_cumulative_mtd,
)

log = logging.getLogger(__name__)


def _location_to_cumulative_row(loc: dict, *, year_month: str, source: str) -> dict:
    """Convert a CURRENT-tab location dict into a CUMULATIVE_MTD row dict."""
    return {
        "loc_name":    loc.get("loc_name", ""),
        "year_month":  year_month,
        "week_ending": loc.get("week_ending", ""),
        "platform":    loc.get("platform", ""),
        "guests":      loc.get("guests", 0),
        "total_sales": loc.get("total_sales", 0),
        "service":     loc.get("service", 0),
        "product":     loc.get("product", 0),
        "product_pct": loc.get("product_pct", 0),
        "ppg":         loc.get("ppg", 0),
        "pph":         loc.get("pph", 0),
        "avg_ticket":  loc.get("avg_ticket", 0),
        "prod_hours":  loc.get("prod_hours", 0),
        "wax_count":   loc.get("wax_count", 0),
        "wax":         loc.get("wax", 0),
        "wax_pct":     loc.get("wax_pct", 0),
        "color":       loc.get("color", 0),
        "color_pct":   loc.get("color_pct", 0),
        "treat_count": loc.get("treat_count", 0),
        "treat":       loc.get("treat", 0),
        "treat_pct":   loc.get("treat_pct", 0),
        "source":      source,
    }


def _stylist_to_cumulative_row(
    s: dict,
    *,
    year_month: str,
    week_ending: str,
    source: str,
) -> dict:
    """Convert a STYLISTS_DATA-shape stylist dict into a STYLISTS_CUMULATIVE_MTD row.

    The CURRENT-stylist load returns dicts shaped for the existing display
    layer (arrays of weekly history + 'cur_*' convenience fields). For the
    cumulative snapshot we want the SCALAR current-week values.
    """
    # For arrays, take the latest entry; for cur_* fields, take them directly
    cur_pph = s.get("cur_pph", 0)
    cur_ticket = s.get("cur_ticket", 0)
    return {
        "year_month":       year_month,
        "week_ending":      week_ending,
        "name":             s.get("name", ""),
        "loc_name":         s.get("loc_name", ""),
        "loc_id":           s.get("loc_id", ""),
        "platform":         s.get("platform", ""),
        "invoices":         s.get("invoices", 0),
        "guests":           s.get("guests", 0),
        "net_service":      s.get("net_service", 0),
        "net_product":      s.get("net_product", 0),
        "avg_ticket":       cur_ticket,
        "pph":              cur_pph,
        "ppg":              s.get("ppg", 0),
        "production_hours": s.get("production_hours", 0),
        "source":           source,
    }


def _attach_history_to_weekly_stylist(original_input: dict, differenced: dict) -> dict:
    """Re-shape a differenced live-stylist record back into the data_processor
    contract: history arrays preserved, latest array entry replaced with the
    differenced true-weekly value, cur_* convenience fields refreshed from
    the differenced weekly.

    Why this matters: `difference_stylist_record()` returns a dict with SCALAR
    pph/avg_ticket/ppg and no history arrays at all — but downstream code
    (`data_processor.enrich_stylists`, `sheets_writer.write_stylists_current`,
    `dashboard_builder`) expects the original `load_stylist_data` shape:
    pph/rebook/product/ticket/services/color/weeks as LISTS, plus cur_pph /
    cur_rebook / cur_product / cur_ticket as SCALARS. Without this reshape,
    `enrich_stylists` crashes on `len(pph)` because pph is a float.

    Replacement semantics: the latest entry in pph/ticket was the cumulative-
    MTD value Karissa's team entered this Monday. Differencing produced the
    true-weekly value; replace pph[-1] / ticket[-1] in the historical arrays
    so WoW deltas downstream compute against a true-weekly current week
    rather than against the cumulative-MTD scalar.
    """
    shaped = dict(original_input)

    new_pph = differenced.get("pph", 0)
    new_ticket = differenced.get("avg_ticket", 0)
    # product_pct comes back from _recompute_stylist_kpis as a decimal (0-1).
    # The stylist `product` array on the dashboard convention is percent scale
    # (0-100), so scale here at the boundary. Karissa golden rule:
    # product_pct = product_net / (net_service + net_product).
    new_product_pct = differenced.get("product_pct", 0) * 100

    # Empty-placeholder guard: STYLISTS_DATA currently contains all-zero rows
    # for everyone except `total_services` (Karissa's team hasn't started
    # entering per-stylist cumulative-MTD values yet). Differencing those
    # placeholders against a zero prior gives all-zero "weekly" values. If
    # we mechanically overwrite the last array entry with these zeros, the
    # dashboard's `cur_pph = pph[pph.length-1]` reads 0 and the entire
    # stylist table renders as $0.00 — even though we have real monthly
    # backfill data behind it.
    #
    # When the differenced primitives are all zero, leave the historical
    # arrays untouched and fall back cur_* to the most recent non-zero
    # historical entry. Once Karissa's team starts entering real cumulative
    # values into STYLISTS_DATA, the differenced output will be non-zero
    # and this guard becomes a no-op (normal replacement path runs).
    is_empty_placeholder = (
        new_pph == 0 and new_ticket == 0 and new_product_pct == 0
    )

    def _last_or_zero(arr):
        return arr[-1] if arr else 0

    if is_empty_placeholder:
        # Drop the trailing zero-placeholder entry from each array — it came
        # from the weekly STYLISTS_DATA row that _merge_stylist_rosters
        # appended onto the prepended monthly history. With it removed, the
        # dashboard's `s.pph[s.pph.length-1]` reads the most recent monthly
        # value (e.g. Aleksis's May $42.16) instead of the placeholder 0.
        # Once Karissa's team enters real data, we hit the else branch.
        for arr_key in ("weeks", "pph", "ticket", "services", "product", "rebook", "color"):
            arr = shaped.get(arr_key) or []
            # Only drop if there's a tail to drop AND it's the placeholder 0.
            # Services may be non-zero in the placeholder row (e.g. 15 for
            # Aleksis), so don't filter on it — match the other arrays' length.
            if len(arr) >= 2:
                shaped[arr_key] = list(arr[:-1])

        shaped["cur_pph"] = _last_or_zero(shaped.get("pph") or [])
        shaped["cur_ticket"] = _last_or_zero(shaped.get("ticket") or [])
        shaped["cur_product"] = _last_or_zero(shaped.get("product") or [])
    else:
        pph_arr = shaped.get("pph") or []
        shaped["pph"] = list(pph_arr[:-1]) + [new_pph] if pph_arr else [new_pph]

        ticket_arr = shaped.get("ticket") or []
        shaped["ticket"] = list(ticket_arr[:-1]) + [new_ticket] if ticket_arr else [new_ticket]

        product_arr = shaped.get("product") or []
        shaped["product"] = (
            list(product_arr[:-1]) + [new_product_pct] if product_arr else [new_product_pct]
        )

        shaped["cur_pph"] = new_pph
        shaped["cur_ticket"] = new_ticket
        shaped["cur_product"] = new_product_pct

    # cur_rebook is salon-level only (no per-stylist source in any POS); preserve
    # whatever the original input had (typically 0) so archetype classification
    # in enrich_stylists doesn't crash on a missing key.
    shaped.setdefault("cur_rebook", original_input.get("cur_rebook", 0))

    return shaped


def snapshot_and_difference(
    service,
    config: dict,
    cumulative_locations: list[dict],
    cumulative_stylists: list[dict],
    *,
    source_label: str = "current_tab",
    dry_run: bool = False,
) -> tuple[list[dict], list[dict]]:
    """The full cumulative-MTD → weekly transformation.

    Args:
      service: Google Sheets API service (built by sheets_writer._build_service).
      config: customer config dict (needs sheet_id).
      cumulative_locations: list of location dicts just read from CURRENT
        (interpreted as cumulative-MTD as of this Monday).
      cumulative_stylists: same shape, stylist-grain.
      source_label: provenance string written to CUMULATIVE_MTD.source
        (e.g., "current_tab" for Karissa's team entry, or "tier2_pdf"
        when Tier 2 auto-populates from Elaina's PDF directly).
      dry_run: if True, skip the snapshot writes but still read + difference.

    Returns:
      (weekly_locations, weekly_stylists) — true weekly records ready to
      pass to data_processor + downstream pipeline steps.
    """
    if not cumulative_locations:
        log.warning("snapshot_and_difference: no cumulative_locations — returning empty")
        return [], []

    # Derive year_month from the first location's week_ending. All locations
    # SHOULD have the same week_ending in any given Monday run; we don't
    # currently enforce that here.
    current_week_ending = cumulative_locations[0].get("week_ending", "")
    year_month = derive_year_month(current_week_ending)
    if not year_month:
        log.error(
            "snapshot_and_difference: cannot derive year_month from week_ending=%r — aborting",
            current_week_ending,
        )
        return cumulative_locations, cumulative_stylists  # pass through unchanged

    # Split cumulative_stylists into live (current-week present) and historical-only
    # (synthesized monthly-only entries from Phase 2.5 fallback). Only LIVE stylists
    # go through snapshot + differencing; historical-only entries skip both phases
    # and get re-attached at the end so the Stylists tab still shows their roster.
    live_stylists = [s for s in cumulative_stylists if not s.get("is_historical_only")]
    historical_only_stylists = [s for s in cumulative_stylists if s.get("is_historical_only")]

    log.info(
        "snapshot_and_difference: processing %s (week_ending=%s, %d locations, "
        "%d live stylists, %d historical-only stylists pass-through)",
        year_month, current_week_ending,
        len(cumulative_locations), len(live_stylists), len(historical_only_stylists),
    )

    # Step 1: snapshot current to CUMULATIVE_MTD (skipped under dry_run)
    location_snapshot_rows = [
        _location_to_cumulative_row(loc, year_month=year_month, source=source_label)
        for loc in cumulative_locations
    ]
    append_to_cumulative_mtd(service, config, location_snapshot_rows, dry_run=dry_run)

    stylist_snapshot_rows = [
        _stylist_to_cumulative_row(
            s, year_month=year_month, week_ending=current_week_ending, source=source_label,
        )
        for s in live_stylists
    ]
    append_to_stylists_cumulative_mtd(service, config, stylist_snapshot_rows, dry_run=dry_run)

    # Step 2: read all prior snapshots for this year_month (READS happen
    # even under dry_run so we can preview what would be differenced)
    all_loc_snapshots = read_cumulative_mtd_snapshots(service, config, year_month)
    all_styl_snapshots = read_stylists_cumulative_mtd_snapshots(service, config, year_month)

    # Step 3: find each location's / stylist's most recent prior snapshot
    loc_priors = find_latest_priors_by_location(all_loc_snapshots, current_week_ending)
    styl_priors = find_latest_priors_by_stylist(all_styl_snapshots, current_week_ending)

    # Tell the operator what we found
    locs_with_priors = sum(1 for loc in cumulative_locations if loc.get("loc_name") in loc_priors)
    log.info(
        "snapshot_and_difference: found priors for %d/%d locations (rest treated as Week 1)",
        locs_with_priors, len(cumulative_locations),
    )

    # Step 4: difference current vs prior — gives true weekly records
    weekly_locations = difference_location_batch(
        # Map our richer location dicts into the cumulative_to_weekly input shape
        [_location_to_cumulative_row(loc, year_month=year_month, source=source_label)
         for loc in cumulative_locations],
        loc_priors,
    )
    weekly_stylists = difference_stylist_batch(
        [_stylist_to_cumulative_row(
            s, year_month=year_month, week_ending=current_week_ending, source=source_label,
         ) for s in live_stylists],
        styl_priors,
    )

    # Step 5: re-shape weekly records back into the dict shape data_processor expects
    # (loc_id is needed downstream; preserve from original cumulative_locations)
    loc_id_by_name = {loc.get("loc_name", ""): loc.get("loc_id", "") for loc in cumulative_locations}
    for w in weekly_locations:
        w["loc_id"] = loc_id_by_name.get(w.get("loc_name", ""), "")

    # Re-shape differenced live-stylist records back into the data_processor
    # contract (history arrays + cur_* scalars). Without this, downstream
    # enrich_stylists crashes on len(pph) because the differenced record has
    # pph as a scalar float, not the historical list it expects.
    original_live_by_key = {
        (s.get("name", ""), s.get("loc_name", "")): s for s in live_stylists
    }
    shaped_live_weekly: list[dict] = []
    for w in weekly_stylists:
        key = (w.get("name", ""), w.get("loc_name", ""))
        orig = original_live_by_key.get(key)
        if orig is None:
            log.warning(
                "snapshot_and_difference: differenced stylist has no matching "
                "original input — passing through as-is (name=%r loc=%r)",
                w.get("name"), w.get("loc_name"),
            )
            shaped_live_weekly.append(w)
            continue
        shaped_live_weekly.append(_attach_history_to_weekly_stylist(orig, w))

    # Re-attach historical-only stylists pass-through (preserves Phase 2.5
    # backfilled roster on the Stylists tab without sending these records
    # through differencing, which would strip their cur_* fields).
    weekly_stylists = shaped_live_weekly + historical_only_stylists

    return weekly_locations, weekly_stylists
