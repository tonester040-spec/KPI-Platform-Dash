#!/usr/bin/env python3
"""
core/dashboard_builder.py
KPI Platform — injects fresh data into dashboard HTML files.

Finds the KPI_DATA_START / KPI_DATA_END marker comments in each HTML file
and replaces the data block with freshly generated JavaScript constants.

Handles:
  docs/index.html  → full 13-location + all-stylist view (Karissa)
  docs/jess.html   → filtered view (Jess's locations only)
  docs/jenn.html   → filtered view (Jenn's locations only)

Manager location assignments are read from config["managers"].
If a manager has no location_ids configured, they see all locations.
"""

import json
import logging
import datetime
from pathlib import Path

log = logging.getLogger(__name__)

MARKER_START = "// KPI_DATA_START"
MARKER_END   = "// KPI_DATA_END"


# ─── JavaScript data block builder ────────────────────────────────────────────

def _build_weeks_12(locations: list[dict]) -> list[str]:
    """Extract the 12 most recent week labels from location history."""
    # All locations should have the same weeks — use the first one
    for loc in locations:
        hist = loc.get("hist", {})
        weeks = hist.get("weeks", [])
        if weeks:
            return weeks[-12:] if len(weeks) > 12 else weeks
    # Fallback: generate the last 12 Sunday dates
    today = datetime.date.today()
    days_to_sunday = today.weekday() + 1  # Monday=0, so +1 to get to Sunday
    last_sunday = today - datetime.timedelta(days=days_to_sunday % 7)
    return [(last_sunday - datetime.timedelta(weeks=i)).strftime("%Y-%m-%d") for i in range(11, -1, -1)]


def _build_loc_hist(locations: list[dict]) -> str:
    """Build the LOC_HIST JavaScript constant."""
    lines = ["const LOC_HIST = {"]
    for loc in locations:
        name     = loc["loc_name"]
        plat     = loc.get("platform", "Zenoti")
        hist     = loc.get("hist", {})
        pph_arr  = hist.get("pph", [loc.get("pph", 0)] * 12)
        sales_arr= hist.get("total_sales", [loc.get("total_sales", 0)] * 12)
        prod_arr = hist.get("product_pct", [loc.get("product_pct", 0)] * 12)
        guests_arr=hist.get("guests", [loc.get("guests", 0)] * 12)
        tick_arr = hist.get("avg_ticket", [loc.get("avg_ticket", 0)] * 12)
        wax_arr  = hist.get("wax_pct", [loc.get("wax_pct", 0)] * 12)

        def fmt_arr(arr, prec=2):
            return "[" + ",".join(f"{float(v):.{prec}f}" for v in arr[-12:]) + "]"

        lines.append(f"  '{name}': {{ plat:'{plat}',")
        lines.append(f"    s:  {fmt_arr(sales_arr, 0)},")
        lines.append(f"    h:  {fmt_arr(pph_arr, 2)},")
        lines.append(f"    p:  {fmt_arr(prod_arr, 1)},")
        lines.append(f"    g:  {fmt_arr(guests_arr, 0)},")
        lines.append(f"    at: {fmt_arr(tick_arr, 2)},")
        lines.append(f"    w:  {fmt_arr(wax_arr, 1)},")
        lines.append(f"  }},")

    lines.append("};")
    return "\n".join(lines)


def _build_stylist_data(stylists: list[dict]) -> str:
    """Build the STYLIST_DATA JavaScript constant."""
    records = []
    for s in stylists:
        records.append({
            "name":    s["name"],
            "loc":     s["loc_name"],
            "loc_id":  s["loc_id"],
            "tenure":  round(s.get("tenure", 0), 1),
            "status":  s.get("status", "active"),
            "weeks":   s.get("weeks", [])[-12:],
            "pph":     [round(v, 2) for v in s.get("pph", [])[-12:]],
            "rebook":  [round(v, 1) for v in s.get("rebook", [])[-12:]],
            "product": [round(v, 1) for v in s.get("product", [])[-12:]],
            "ticket":  [round(v, 2) for v in s.get("ticket", [])[-12:]],
            "services":[int(v) for v in s.get("services", [])[-12:]],
            "color":   [round(v, 1) for v in s.get("color", [])[-12:]],
        })

    return "const STYLIST_DATA = " + json.dumps(records, separators=(",", ":")) + ";"


def _build_data_block(locations: list[dict], stylists: list[dict]) -> str:
    """Build the full JavaScript data block to inject between markers."""
    weeks_12 = _build_weeks_12(locations)
    weeks_js = "const WEEKS_12 = [\n  " + ",".join(f"'{w}'" for w in weeks_12) + "\n];"

    parts = [
        "// KPI_DATA_START",
        f"// Generated: {datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')}",
        weeks_js,
        _build_loc_hist(locations),
        "",
        "/* ═══════════════════════════════════════════════════════",
        "   STYLIST DATA",
        "═══════════════════════════════════════════════════════ */",
        _build_stylist_data(stylists),
        "// KPI_DATA_END",
    ]
    return "\n".join(parts)


# ─── HTML injection ───────────────────────────────────────────────────────────

def _inject_data(html: str, data_block: str) -> tuple[str, bool]:
    """
    Replace content between KPI_DATA_START and KPI_DATA_END markers.
    Returns (updated_html, success).
    """
    start_marker = MARKER_START
    end_marker   = MARKER_END

    start_idx = html.find(start_marker)
    end_idx   = html.find(end_marker)

    if start_idx == -1 or end_idx == -1:
        log.error("KPI_DATA_START or KPI_DATA_END marker not found in HTML")
        return html, False

    if end_idx < start_idx:
        log.error("KPI_DATA_END appears before KPI_DATA_START — markers corrupted")
        return html, False

    # Keep everything before START marker, inject new block, then skip to after END marker
    before  = html[:start_idx]
    after   = html[end_idx + len(end_marker):]
    updated = before + data_block + after
    return updated, True


# ─── File update ──────────────────────────────────────────────────────────────

def _filter_for_manager(locations: list[dict], stylists: list[dict], loc_ids: list[str]):
    """Filter locations and stylists to a manager's assigned location IDs."""
    if not loc_ids:
        return locations, stylists
    filtered_locs = [loc for loc in locations if loc.get("loc_id") in loc_ids]
    filtered_styl = [s for s in stylists if s.get("loc_id") in loc_ids]
    return filtered_locs, filtered_styl


def update_html_file(
    html_path: Path,
    locations: list[dict],
    stylists: list[dict],
    dry_run: bool = False,
) -> bool:
    """Inject data into a single HTML file. Returns True on success."""
    if not html_path.exists():
        log.warning("HTML file not found: %s — skipping", html_path)
        return False

    html = html_path.read_text(encoding="utf-8")
    data_block = _build_data_block(locations, stylists)
    updated, success = _inject_data(html, data_block)

    if not success:
        log.error("Failed to inject data into %s — markers missing. "
                  "Add // KPI_DATA_START and // KPI_DATA_END comments around the data constants.", html_path.name)
        return False

    if dry_run:
        log.info("DRY RUN: Would update %s (%d locs, %d stylists)", html_path.name, len(locations), len(stylists))
        return True

    html_path.write_text(updated, encoding="utf-8")
    log.info("Updated %s (%d locs, %d stylists, %.1f KB)", html_path.name, len(locations), len(stylists), len(updated) / 1024)
    return True


# ─── Main entry ───────────────────────────────────────────────────────────────

def rebuild_all(config: dict, data: dict, docs_dir: Path, dry_run: bool = False):
    """
    Rebuild index.html (full view) + manager dashboards (filtered views).
    """
    locations = data["locations"]
    stylists  = data["stylists"]
    managers  = config.get("managers", [])

    results = {}

    # Main dashboard (Karissa — all locations)
    main_html = docs_dir / "index.html"
    results["index.html"] = update_html_file(main_html, locations, stylists, dry_run)

    # Manager dashboards (filtered)
    for mgr in managers:
        filename  = mgr.get("filename", "")
        loc_ids   = mgr.get("location_ids", [])
        mgr_name  = mgr.get("name", filename)

        if not filename:
            continue

        mgr_html = docs_dir / filename
        mgr_locs, mgr_styl = _filter_for_manager(locations, stylists, loc_ids)

        if not mgr_locs:
            log.warning("Manager %s has no matching locations — using full data", mgr_name)
            mgr_locs, mgr_styl = locations, stylists

        results[filename] = update_html_file(mgr_html, mgr_locs, mgr_styl, dry_run)

    success_count = sum(1 for v in results.values() if v)
    log.info(
        "Dashboard rebuild complete: %d/%d files updated (dry_run=%s)",
        success_count, len(results), dry_run
    )
    return results
