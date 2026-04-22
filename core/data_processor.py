#!/usr/bin/env python3
"""
core/data_processor.py
KPI Platform — enriches raw location + stylist data.

Takes raw lists from data_source.py and adds:
  - Week-over-week deltas for every metric
  - Network-wide ranks (PPH, product%, guests)
  - Performance flags (star, watch, struggling)
  - Stylist star threshold (85th percentile PPH)
  - Summary stats for the network
"""

import logging
import statistics
from typing import Any

log = logging.getLogger(__name__)


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _pct_delta(current: float, previous: float) -> float | None:
    """Return (current - previous) / previous * 100, or None if no previous."""
    if previous is None or previous == 0:
        return None
    return round((current - previous) / abs(previous) * 100, 1)


def _abs_delta(current: float, previous: float) -> float | None:
    if previous is None:
        return None
    return round(current - previous, 2)


def _rank(items: list, key: str, reverse: bool = True) -> list:
    """Add 'rank_{key}' field to each item dict (1 = best)."""
    sorted_items = sorted(items, key=lambda x: x.get(key, 0), reverse=reverse)
    for rank, item in enumerate(sorted_items, start=1):
        item[f"rank_{key}"] = rank
    return items


def _percentile(values: list[float], pct: float) -> float:
    """Return the value at the given percentile (0–100)."""
    if not values:
        return 0.0
    sorted_v = sorted(values)
    idx = int(len(sorted_v) * pct / 100)
    idx = min(idx, len(sorted_v) - 1)
    return sorted_v[idx]


# ─── Location enrichment ──────────────────────────────────────────────────────

def enrich_locations(locations: list[dict], history: dict) -> list[dict]:
    """
    Adds delta fields and ranks to each location dict.
    Mutates items in-place and returns the list.
    """
    if not locations:
        return locations

    # Attach prior-week values from history for delta calculation
    for loc in locations:
        name = loc["loc_name"]
        hist = history.get(name, {})
        pph_hist = hist.get("pph", [])
        sales_hist = hist.get("total_sales", [])
        prod_hist = hist.get("product_pct", [])
        guests_hist = hist.get("guests", [])

        # Use second-to-last as "previous week" if history has ≥2 entries
        def prior(arr):
            return arr[-2] if len(arr) >= 2 else None

        loc["pph_prev"]         = prior(pph_hist)
        loc["total_sales_prev"] = prior(sales_hist)
        loc["product_pct_prev"] = prior(prod_hist)
        loc["guests_prev"]      = prior(guests_hist)

        # Week-over-week deltas
        loc["pph_delta"]         = _abs_delta(loc["pph"], loc["pph_prev"])
        loc["pph_delta_pct"]     = _pct_delta(loc["pph"], loc["pph_prev"])
        loc["sales_delta_pct"]   = _pct_delta(loc["total_sales"], loc["total_sales_prev"])
        loc["product_pct_delta"] = _abs_delta(loc["product_pct"], loc["product_pct_prev"])
        loc["guests_delta"]      = (
            loc["guests"] - loc["guests_prev"]
            if loc["guests_prev"] is not None else None
        )

        # Attach history arrays for dashboard injection
        loc["hist"] = hist

    # Rank by PPH (ascending → rank 1 = highest PPH)
    _rank(locations, "pph", reverse=True)
    _rank(locations, "total_sales", reverse=True)
    _rank(locations, "product_pct", reverse=True)
    _rank(locations, "guests", reverse=True)

    # Performance flags
    all_pph = [loc["pph"] for loc in locations if loc["pph"] > 0]
    pph_mean = statistics.mean(all_pph) if all_pph else 0
    pph_stdev = statistics.stdev(all_pph) if len(all_pph) > 1 else 1

    for loc in locations:
        pph = loc["pph"]
        z = (pph - pph_mean) / pph_stdev if pph_stdev else 0
        if z >= 1.0:
            loc["flag"] = "star"
        elif z <= -1.0:
            loc["flag"] = "watch"
        else:
            loc["flag"] = "solid"

    log.info("Enriched %d locations", len(locations))
    return locations


# ─── Network summary ──────────────────────────────────────────────────────────

def build_network_summary(locations: list[dict]) -> dict:
    """Compute network-wide aggregate metrics."""
    if not locations:
        return {}

    def avg(key):
        vals = [loc[key] for loc in locations if loc.get(key, 0) > 0]
        return round(statistics.mean(vals), 2) if vals else 0.0

    total_guests = sum(loc.get("guests", 0) for loc in locations)
    total_sales  = sum(loc.get("total_sales", 0) for loc in locations)

    week_ending = locations[0].get("week_ending", "")

    summary = {
        "week_ending":       week_ending,
        "total_locations":   len(locations),
        "total_guests":      total_guests,
        "total_sales":       round(total_sales, 2),
        "avg_pph":           avg("pph"),
        "avg_product_pct":   avg("product_pct"),
        "avg_avg_ticket":    avg("avg_ticket"),
        "avg_wax_pct":       avg("wax_pct"),
        "avg_color_pct":     avg("color_pct"),
        "top_pph_loc":       next((l["loc_name"] for l in locations if l.get("rank_pph") == 1), ""),
        "low_pph_loc":       next((l["loc_name"] for l in locations if l.get("rank_pph") == len(locations)), ""),
    }
    log.info("Network summary: %d locs, total sales $%.0f, avg PPH $%.2f",
             len(locations), total_sales, summary["avg_pph"])
    return summary


# ─── Stylist enrichment ───────────────────────────────────────────────────────

def enrich_stylists(stylists: list[dict], network_summary: dict) -> list[dict]:
    """
    Adds delta fields, network comparison, and star flag to each stylist.
    """
    if not stylists:
        return stylists, 0

    net_avg_pph = network_summary.get("avg_pph", 0)
    net_avg_prod = network_summary.get("avg_product_pct", 0)

    # Compute 85th percentile PPH threshold across all stylists
    all_pph = sorted(s["cur_pph"] for s in stylists if s["cur_pph"] > 0)
    star_threshold = _percentile(all_pph, 85)

    for s in stylists:
        pph = s.get("pph", [])
        rebook = s.get("rebook", [])

        # Week-over-week deltas
        s["pph_delta"]    = _abs_delta(pph[-1] if pph else 0, pph[-2] if len(pph) >= 2 else None)
        s["rebook_delta"] = _abs_delta(
            rebook[-1] if rebook else 0,
            rebook[-2] if len(rebook) >= 2 else None
        )

        # vs network
        s["vs_net_pph"]  = round(s["cur_pph"] - net_avg_pph, 2)
        s["vs_net_prod"] = round(s["cur_product"] - net_avg_prod, 1)

        # Star badge
        s["is_star"] = s["cur_pph"] >= star_threshold

        # Archetype label (used in AI prompts)
        if s["is_star"]:
            s["arch_label"] = "Star Performer"
        elif s["vs_net_pph"] <= -8:
            s["arch_label"] = "Needs Coaching"
        elif s["cur_product"] >= net_avg_prod * 1.2:
            s["arch_label"] = "Product Champion"
        else:
            s["arch_label"] = "Solid Contributor"

    log.info("Enriched %d stylists. Star threshold PPH: $%.2f", len(stylists), star_threshold)
    return stylists, star_threshold


# ─── Main entry point ─────────────────────────────────────────────────────────

def process(locations: list[dict], stylists: list[dict], history: dict) -> dict:
    """
    Full enrichment pipeline.
    Returns a processed data dict ready for all downstream modules.
    """
    locations = enrich_locations(locations, history)
    network   = build_network_summary(locations)
    stylists, star_threshold = enrich_stylists(stylists, network)

    return {
        "locations":       locations,
        "stylists":        stylists,
        "network":         network,
        "history":         history,
        "star_threshold":  star_threshold,
    }
