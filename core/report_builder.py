#!/usr/bin/env python3
"""
core/report_builder.py
KPI Platform — builds weekly Excel report with color-coded performance data.

Sheets:
  Summary     → Network headline, top/bottom performers, AI coach briefing
  Locations   → One row per location, all metrics, traffic-light color coding
  Stylists    → One row per stylist, key metrics, star/watch indicators

Color logic:
  Green  → >= network avg
  Yellow → within 10% below network avg
  Red    → > 10% below network avg
"""

import logging
import datetime
from pathlib import Path

log = logging.getLogger(__name__)

try:
    import openpyxl
    from openpyxl.styles import (
        PatternFill, Font, Alignment, Border, Side, numbers
    )
    from openpyxl.utils import get_column_letter
    OPENPYXL_AVAILABLE = True
except ImportError:
    OPENPYXL_AVAILABLE = False
    log.warning("openpyxl not installed — report_builder will be a no-op")


# ─── Colors ───────────────────────────────────────────────────────────────────

NAVY   = "0F1117"
WHITE  = "FFFFFF"
GREEN  = "D4EDDA"
YELLOW = "FFF3CD"
RED    = "F8D7DA"
LGRAY  = "F8F9FA"
DGRAY  = "6C757D"
GOLD   = "FFC107"
BLUE   = "4A90D9"


def _fill(hex_color: str) -> "PatternFill":
    return PatternFill("solid", fgColor=hex_color)


def _font(bold=False, color=WHITE, size=11) -> "Font":
    return Font(bold=bold, color=color, size=size)


def _align(horizontal="left", wrap=False) -> "Alignment":
    return Alignment(horizontal=horizontal, vertical="center", wrap_text=wrap)


def _border() -> "Border":
    thin = Side(style="thin", color="DDDDDD")
    return Border(left=thin, right=thin, top=thin, bottom=thin)


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _pph_color(pph: float, avg_pph: float) -> str:
    if pph >= avg_pph:
        return GREEN
    elif pph >= avg_pph * 0.90:
        return YELLOW
    else:
        return RED


def _pct_color(val: float, avg: float) -> str:
    if val >= avg:
        return GREEN
    elif val >= avg * 0.90:
        return YELLOW
    else:
        return RED


def _style_header(ws, row: int, cols: int, title: str = None):
    """Apply navy header style across `cols` columns on `row`."""
    for col in range(1, cols + 1):
        cell = ws.cell(row=row, column=col)
        cell.fill = _fill(NAVY)
        cell.font = _font(bold=True, color=WHITE)
        cell.alignment = _align("center")
        cell.border = _border()


def _write_row(ws, row: int, values: list, fills: list = None):
    for col, val in enumerate(values, start=1):
        cell = ws.cell(row=row, column=col)
        cell.value = val
        cell.alignment = _align("left")
        cell.border = _border()
        if fills and col - 1 < len(fills) and fills[col - 1]:
            cell.fill = _fill(fills[col - 1])


# ─── Summary sheet ────────────────────────────────────────────────────────────

def _build_summary(wb, data: dict, ai_cards: dict):
    ws = wb.create_sheet("Summary")
    ws.column_dimensions["A"].width = 28
    ws.column_dimensions["B"].width = 40

    net = data["network"]
    week = net.get("week_ending", "")
    briefing = ai_cards.get("coach_briefing", "")

    # Title
    ws.merge_cells("A1:B1")
    title_cell = ws["A1"]
    title_cell.value = f"KPI Weekly Report — Week Ending {week}"
    title_cell.font = Font(bold=True, size=14, color=NAVY)
    title_cell.alignment = _align("center")

    rows = [
        ("", ""),
        ("NETWORK SUMMARY", ""),
        ("Total Locations", net.get("total_locations", 0)),
        ("Total Guests", f"{net.get('total_guests', 0):,}"),
        ("Total Sales", f"${net.get('total_sales', 0):,.0f}"),
        ("Avg PPH", f"${net.get('avg_pph', 0):.2f}"),
        ("Avg Product %", f"{net.get('avg_product_pct', 0):.1f}%"),
        ("Avg Ticket", f"${net.get('avg_avg_ticket', 0):.2f}"),
        ("", ""),
        ("TOP PERFORMER (PPH)", net.get("top_pph_loc", "")),
        ("NEEDS ATTENTION", net.get("low_pph_loc", "")),
        ("", ""),
        ("COACH BRIEFING", ""),
    ]

    for i, (label, value) in enumerate(rows, start=2):
        ws.cell(row=i, column=1, value=label).font = Font(bold=(label.isupper()), color=NAVY)
        ws.cell(row=i, column=2, value=value)

    # Briefing text block
    briefing_row = len(rows) + 2
    ws.merge_cells(f"A{briefing_row}:B{briefing_row + 10}")
    cell = ws.cell(row=briefing_row, column=1)
    cell.value = briefing
    cell.alignment = Alignment(wrap_text=True, vertical="top")
    ws.row_dimensions[briefing_row].height = 180

    log.debug("Summary sheet built")


# ─── Locations sheet ──────────────────────────────────────────────────────────

def _build_locations(wb, data: dict, ai_cards: dict):
    ws = wb.create_sheet("Locations")

    headers = [
        "Location", "Platform", "Week Ending", "Rank", "PPH $",
        "vs Avg PPH", "Product %", "vs Avg Prod", "Guests", "Total Sales $",
        "Avg Ticket $", "Wax %", "Color %", "Flag", "AI Summary"
    ]
    col_widths = [22, 12, 12, 6, 10, 10, 10, 10, 10, 14, 12, 8, 8, 8, 45]

    for col, (header, width) in enumerate(zip(headers, col_widths), start=1):
        ws.column_dimensions[get_column_letter(col)].width = width
        cell = ws.cell(row=1, column=col, value=header)
        cell.fill = _fill(NAVY)
        cell.font = _font(bold=True, color=WHITE, size=10)
        cell.alignment = _align("center")
        cell.border = _border()

    ws.freeze_panes = "A2"
    net = data["network"]
    avg_pph  = net.get("avg_pph", 0)
    avg_prod = net.get("avg_product_pct", 0)

    for row_idx, loc in enumerate(data["locations"], start=2):
        name     = loc["loc_name"]
        pph      = loc.get("pph", 0)
        prod     = loc.get("product_pct", 0)
        pph_delta = loc.get("pph_delta", 0) or 0
        prod_delta = loc.get("product_pct_delta", 0) or 0
        flag     = loc.get("flag", "solid")
        card     = ai_cards.get("location_cards", {}).get(name, "")

        flag_str = "⭐ STAR" if flag == "star" else ("⚠️ WATCH" if flag == "watch" else "✓ SOLID")
        pph_c    = _pph_color(pph, avg_pph)
        prod_c   = _pct_color(prod, avg_prod)

        values = [
            name,
            loc.get("platform", ""),
            loc.get("week_ending", ""),
            loc.get("rank_pph", ""),
            f"${pph:.2f}",
            f"{pph_delta:+.2f}" if pph_delta else "—",
            f"{prod:.1f}%",
            f"{prod_delta:+.1f}" if prod_delta else "—",
            loc.get("guests", 0),
            f"${loc.get('total_sales', 0):,.0f}",
            f"${loc.get('avg_ticket', 0):.2f}",
            f"{loc.get('wax_pct', 0):.1f}%",
            f"{loc.get('color_pct', 0):.1f}%",
            flag_str,
            card,
        ]

        fills = [
            None, None, None, None,
            pph_c,   # PPH color
            None,
            prod_c,  # Product % color
            None, None, None, None, None, None, None, None
        ]

        bg = LGRAY if row_idx % 2 == 0 else WHITE
        for col, (val, fill) in enumerate(zip(values, fills), start=1):
            cell = ws.cell(row=row_idx, column=col, value=val)
            cell.fill = _fill(fill if fill else bg)
            cell.alignment = _align("left", wrap=(col == len(values)))
            cell.border = _border()
            cell.font = Font(size=10)

        ws.row_dimensions[row_idx].height = 45

    log.debug("Locations sheet built")


# ─── Stylists sheet ───────────────────────────────────────────────────────────

def _build_stylists(wb, data: dict, ai_cards: dict):
    ws = wb.create_sheet("Stylists")

    headers = [
        "Stylist", "Location", "Status", "Tenure (yrs)", "Star?",
        "PPH $", "vs Net Avg", "Rebook %", "Product %", "Avg Ticket $", "Coaching Note"
    ]
    col_widths = [22, 18, 10, 12, 8, 10, 10, 10, 10, 12, 45]

    for col, (header, width) in enumerate(zip(headers, col_widths), start=1):
        ws.column_dimensions[get_column_letter(col)].width = width
        cell = ws.cell(row=1, column=col, value=header)
        cell.fill = _fill(NAVY)
        cell.font = _font(bold=True, color=WHITE, size=10)
        cell.alignment = _align("center")
        cell.border = _border()

    ws.freeze_panes = "A2"
    net = data["network"]
    avg_pph = net.get("avg_pph", 0)

    for row_idx, s in enumerate(data["stylists"], start=2):
        pph      = s.get("cur_pph", 0)
        vs_net   = s.get("vs_net_pph", 0)
        is_star  = s.get("is_star", False)
        note     = ai_cards.get("stylist_cards", {}).get(s["name"], "")

        pph_c = _pph_color(pph, avg_pph)
        bg    = LGRAY if row_idx % 2 == 0 else WHITE

        values = [
            s.get("name", ""),
            s.get("loc_name", ""),
            s.get("status", "").upper(),
            s.get("tenure", 0),
            "⭐" if is_star else "",
            f"${pph:.2f}",
            f"{vs_net:+.2f}" if vs_net else "—",
            f"{s.get('cur_rebook', 0):.1f}%",
            f"{s.get('cur_product', 0):.1f}%",
            f"${s.get('cur_ticket', 0):.2f}",
            note,
        ]

        fills = [None, None, None, None, None, pph_c, None, None, None, None, None]

        for col, (val, fill) in enumerate(zip(values, fills), start=1):
            cell = ws.cell(row=row_idx, column=col, value=val)
            cell.fill = _fill(fill if fill else bg)
            cell.alignment = _align("left", wrap=(col == len(values)))
            cell.border = _border()
            cell.font = Font(size=10)

        ws.row_dimensions[row_idx].height = 45

    log.debug("Stylists sheet built")


# ─── Main entry ───────────────────────────────────────────────────────────────

def build_report(data: dict, ai_cards: dict, output_dir: Path, dry_run: bool = False) -> Path | None:
    """
    Build the Excel report. Returns the Path to the created file.
    Returns None if openpyxl is not available or in dry_run mode.
    """
    if not OPENPYXL_AVAILABLE:
        log.warning("openpyxl not available — skipping report build")
        return None

    week = data["network"].get("week_ending", datetime.date.today().strftime("%Y-%m-%d"))
    filename = f"KPI_Weekly_Report_{week}.xlsx"
    output_path = output_dir / filename

    if dry_run:
        log.info("DRY RUN: Would create report at %s", output_path)
        # Create a minimal placeholder file so the email step has something to attach
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "Summary"
        ws["A1"] = f"[DRY RUN] KPI Report — {week}"
        ws["A2"] = "This is a placeholder generated in DRY_RUN mode."
        wb.save(output_path)
        log.info("DRY RUN: placeholder report saved to %s", output_path)
        return output_path

    wb = openpyxl.Workbook()
    # Remove default sheet
    if "Sheet" in wb.sheetnames:
        del wb["Sheet"]

    _build_summary(wb, data, ai_cards)
    _build_locations(wb, data, ai_cards)
    _build_stylists(wb, data, ai_cards)

    output_dir.mkdir(parents=True, exist_ok=True)
    wb.save(output_path)
    log.info("Report saved: %s (%.1f KB)", output_path, output_path.stat().st_size / 1024)
    return output_path
