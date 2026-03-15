#!/usr/bin/env python3
"""
core/drift_checker.py
KPI Platform — Per-location KPI drift monitoring.

Runs after KPI calculations are complete but BEFORE the dashboard is updated.
Checks every computed output against per-location thresholds defined in
config/drift_config.json.

Behavior:
  - VALUES OUTSIDE RANGE:  log WARNING + fire alert (does NOT block dashboard update)
  - PHYSICALLY IMPOSSIBLE: log ERROR + fire alert + return abort=True (blocks dashboard update)

Physically impossible values (hard stop):
  - revenue < 0
  - appointments_per_stylist == 0 across all stylists (full week of zero services)
  - product_attachment_rate < 0 or > 1.0
  - rebooking_rate < 0 or > 1.0

Usage (in main.py, after data processor, before dashboard builder):
  from core import drift_checker
  abort, warnings = drift_checker.check_all(data)
  if abort:
      log.error("Drift check: physically impossible value detected — aborting dashboard update")
      sys.exit(1)
  for w in warnings:
      log.warning("Drift: %s", w)
"""

import json
import logging
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

REPO_ROOT        = Path(__file__).resolve().parent.parent
DRIFT_CONFIG_PATH = REPO_ROOT / "config" / "drift_config.json"


def _load_drift_config() -> dict[str, dict]:
    """
    Load drift_config.json and return a dict keyed by location_id.
    Returns empty dict if file is missing — drift checking is then skipped.
    """
    if not DRIFT_CONFIG_PATH.exists():
        log.warning("drift_config.json not found — drift checking disabled")
        return {}
    try:
        raw = json.loads(DRIFT_CONFIG_PATH.read_text())
        return {loc["location_id"]: loc for loc in raw.get("locations", [])}
    except Exception as exc:
        log.warning("Could not load drift_config.json: %s — drift checking disabled", exc)
        return {}


def _check_impossible(loc_id: str, loc_name: str, metric: str, value: Any) -> str | None:
    """
    Check for physically impossible values.
    Returns error string if impossible, None if OK.
    """
    if metric == "weekly_revenue" and value is not None and value < 0:
        return f"[{loc_id} {loc_name}] {metric}={value:.2f} is NEGATIVE (physically impossible)"

    if metric == "product_attachment_rate" and value is not None and (value < 0 or value > 1.0):
        return f"[{loc_id} {loc_name}] {metric}={value:.4f} is outside 0-1 range (impossible)"

    if metric == "rebooking_rate" and value is not None and (value < 0 or value > 1.0):
        return f"[{loc_id} {loc_name}] {metric}={value:.4f} is outside 0-1 range (impossible)"

    return None


def _check_threshold(
    loc_id: str,
    loc_name: str,
    metric: str,
    value: Any,
    threshold: dict,
) -> str | None:
    """
    Check a value against its configured min/max thresholds.
    Returns warning string if out of range, None if OK.
    """
    if value is None:
        return None

    min_key = f"{metric}_min"
    max_key = f"{metric}_max"
    low  = threshold.get(min_key)
    high = threshold.get(max_key)

    if low is not None and value < low:
        return (
            f"[{loc_id} {loc_name}] {metric}={value:.2f} is BELOW configured floor "
            f"({low}) — possible data issue or bad week"
        )
    if high is not None and value > high:
        return (
            f"[{loc_id} {loc_name}] {metric}={value:.2f} is ABOVE configured ceiling "
            f"({high}) — possible duplicate records or data error"
        )
    return None


def check_all(data: dict) -> tuple[bool, list[str]]:
    """
    Run drift checks on all locations in the processed data dict.

    Args:
        data: The processed data dict from data_processor.process()

    Returns:
        (abort: bool, warnings: list[str])
        abort=True  → a physically impossible value was found — do NOT update dashboard
        abort=False → all values are plausible (warnings may still have out-of-range alerts)
    """
    drift_cfg = _load_drift_config()
    if not drift_cfg:
        log.info("Drift checking skipped — config not loaded")
        return False, []

    locations = data.get("locations", [])
    stylists  = data.get("stylists", [])

    abort    = False
    warnings = []

    for loc in locations:
        loc_id   = loc.get("loc_id", "unknown")
        loc_name = loc.get("loc_name", "unknown")
        cfg      = drift_cfg.get(loc_id)

        if not cfg:
            log.debug("No drift config for location %s — skipping", loc_id)
            continue

        thresholds    = cfg.get("thresholds", {})
        stylist_count = cfg.get("stylist_count_estimate", 1)

        # ── Metric: weekly_revenue ──────────────────────────────────────────
        revenue = loc.get("total_sales")

        impossible_err = _check_impossible(loc_id, loc_name, "weekly_revenue", revenue)
        if impossible_err:
            log.error("IMPOSSIBLE VALUE: %s", impossible_err)
            abort = True
        elif revenue is not None:
            w = _check_threshold(loc_id, loc_name, "weekly_revenue", revenue, thresholds)
            if w:
                warnings.append(w)

        # ── Metric: appointments_per_stylist ────────────────────────────────
        # Derive from guests + stylist count
        guests  = loc.get("guests", 0)
        apt_per = guests / max(stylist_count, 1)

        # Hard stop: zero appointments across all stylists for a full week
        if guests == 0 and stylist_count > 0:
            err = f"[{loc_id} {loc_name}] appointments_per_stylist=0 for all stylists — HARD STOP"
            log.error("IMPOSSIBLE VALUE: %s", err)
            abort = True
        else:
            w = _check_threshold(loc_id, loc_name, "appointments_per_stylist", apt_per, thresholds)
            if w:
                warnings.append(w)

        # ── Metric: product_attachment_rate ─────────────────────────────────
        prod_pct = loc.get("product_pct")
        # product_pct stored as percentage (e.g. 18.5 for 18.5%)
        # Convert to 0-1 for impossible check
        prod_rate = prod_pct / 100 if prod_pct is not None else None

        impossible_err = _check_impossible(loc_id, loc_name, "product_attachment_rate", prod_rate)
        if impossible_err:
            log.error("IMPOSSIBLE VALUE: %s", impossible_err)
            abort = True
        elif prod_rate is not None:
            # Check raw percentage against threshold (stored as fraction in config)
            w = _check_threshold(loc_id, loc_name, "product_attachment_rate", prod_rate, thresholds)
            if w:
                warnings.append(w)

    # ── Stylist-level rebook rate check ─────────────────────────────────────
    for s in stylists:
        loc_id   = s.get("loc_id", "unknown")
        loc_name = s.get("loc_name", "unknown")
        cfg      = drift_cfg.get(loc_id)
        if not cfg:
            continue

        thresholds   = cfg.get("thresholds", {})
        rebook_list  = s.get("rebook", [])
        rebook_cur   = rebook_list[-1] if rebook_list else None
        rebook_rate  = rebook_cur / 100 if rebook_cur is not None else None

        impossible_err = _check_impossible(loc_id, loc_name, "rebooking_rate", rebook_rate)
        if impossible_err:
            log.error("IMPOSSIBLE VALUE: %s", impossible_err)
            abort = True
        elif rebook_rate is not None:
            w = _check_threshold(loc_id, loc_name, "rebooking_rate", rebook_rate, thresholds)
            if w:
                warnings.append(w)

    # ── Summary ─────────────────────────────────────────────────────────────
    if abort:
        log.error(
            "Drift check FAILED: %d physically impossible value(s) found — dashboard update blocked",
            sum(1 for w in warnings if "IMPOSSIBLE" in w),
        )
    elif warnings:
        log.warning("Drift check: %d out-of-range warning(s) — dashboard will still update", len(warnings))
    else:
        log.info("Drift check passed — all KPI outputs within configured ranges")

    return abort, warnings
