"""
parsers/pdf_zenoti_v2.py
────────────────────────
Zenoti "Salon Summary" PDF parser — full extraction of every field the
Tier 2 pipeline needs to populate the V1 CURRENT tab.

This supersedes the legacy `zenoti_pdf.py`, which was a narrow counts-only
extractor that silently dropped any location whose header didn't start with
"888-" (Elk River is 887-, Hudson and New Richmond are 910-). The legacy
parser is retained only for backward compatibility with the Excel+PDF
merge flow in `tier2_batch_processor.py`; new PDF-only ingestion uses
THIS module.

Why a rewrite
─────────────
The legacy parser could not cope with:
    • Non-888 cluster prefixes (887-, 910-)
    • Roseville's unusual header "888-40098-FS Roseville, MN"
    • Locations that split wax into separate "Wax" and "Waxing" categories
      (Crystal, Elk River, Hudson all ship both; Roseville ONLY ships
      "Waxing"). Karissa's wax_count must sum both rows.
    • Per-stylist data from EMPLOYEE SALE DETAILS and EMPLOYEE PERFORMANCE
      DETAILS (two separate tables that must be joined by stylist name).
    • Production hours at the location level (HOURLY WORK DETAILS Total).
    • Karissa-canonical guest_count = "Total invoices with services or
      product" (NOT the "Total guest count" statistics row — that's
      unique guests, and the values disagree).

Validated against the 2026-04-01 → 04-05 production exports for all 9
Zenoti locations: Andover, Blaine, Crystal, Elk River, Forest Lake,
Hudson, New Richmond, Prior Lake, Roseville.

Public API
──────────
    parser = ZenotiV2Parser(text)   # already-extracted text
    result = parser.parse()         # returns dict

    # Or from a PDF file:
    parse_file("/path/to/andover.pdf")

The returned dict structure is documented in `ZenotiV2Parser.parse()`.

Karissa-canonical KPIs (from CLAUDE.md contract)
───────────────────────────────────────────────
    guest_count   = invoice_count (NOT unique guests)
    service_net   = Service sales NET
    product_net   = Product sales NET
    total_sales   = service_net + product_net (NOT "Sales(Inc. Tax)")
    avg_ticket    = total_sales / guest_count
    pph           = service_net / production_hours
    ppg           = product_net / guest_count
    wax_count     = Wax.qty + Waxing.qty (sum both if present)
    wax_pct       = wax_count / guest_count
    color_sales   = Color.sales
    color_pct     = color_sales / total_sales  (revenue share, NOT penetration)
    treatment_pct = Treatment.qty / guest_count (penetration)
"""

from __future__ import annotations

import logging
import re
from typing import Any, Dict, List, Optional, Tuple

from parsers.pdf_common import (
    parse_date_range,
    safe_parse_int,
    safe_parse_money,
    safe_parse_percent,
)
from parsers.pdf_location_normalizer import extract_zenoti_location

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Regex — top SALES block
# ---------------------------------------------------------------------------
# Real row shapes (from actual PDFs):
#
#     "Service sales         110    3,954.00    3,876.20 (91.81)"
#     "Product sales          68     395.00        346.00 (8.19)"
#     "Service &             178    4,349.00   4,222.20 (100.00)"
#     "product"                                                      ← wrapped
#     "subtotal"                                                     ← wrapped
#     "Item total            178    4,349.00   4,222.20 (100.00)"
#     "Sales(Inc. Tax)                                 4,245.44"
#     "Tax                                                  23.24"
#
# Shape: LABEL  COUNT(int)  GROSS(money)  NET(money)  (PCT%)
# Product row may have 4 money columns; Item total is our ground-truth total.

_RE_SERVICE_SALES = re.compile(
    r"Service\s+sales\s+(\d[\d,]*)\s+([\d,]+\.\d+)\s+([\d,]+\.\d+)\s*\(([\d\.]+)\)",
    re.IGNORECASE,
)
_RE_PRODUCT_SALES = re.compile(
    r"Product\s+sales\s+(\d[\d,]*)\s+([\d,]+\.\d+)\s+([\d,]+\.\d+)\s*\(([\d\.]+)\)",
    re.IGNORECASE,
)
# "Service & product subtotal" wraps across 2-3 lines but the numeric body
# stays on one. We anchor on "Service &" and let the next numeric triple pop.
_RE_SERVICE_PRODUCT_SUBTOTAL = re.compile(
    r"Service\s*&\s+(\d[\d,]*)\s+([\d,]+\.\d+)\s+([\d,]+\.\d+)\s*\(([\d\.]+)\)",
    re.IGNORECASE,
)
_RE_ITEM_TOTAL = re.compile(
    r"Item\s+total\s+(\d[\d,]*)\s+([\d,]+\.\d+)\s+([\d,]+\.\d+)\s*\(([\d\.]+)\)",
    re.IGNORECASE,
)
_RE_SALES_INC_TAX = re.compile(
    r"Sales\(Inc\.\s*Tax\)\s+([\d,]+\.\d+)",
    re.IGNORECASE,
)
_RE_TAX = re.compile(r"^\s*Tax\s+([\d,]+\.\d+)\s*$", re.IGNORECASE | re.MULTILINE)

# ---------------------------------------------------------------------------
# Regex — STATISTICS block (audit values; most are rejected per Karissa)
# ---------------------------------------------------------------------------
_RE_AVG_INVOICE_VALUE = re.compile(
    r"Avg\.\s*invoice\s+value\s+([\d,]+\.\d+)", re.IGNORECASE
)
_RE_TOTAL_GUEST_COUNT_PDF = re.compile(
    r"Total\s+guest\s+count\s+(\d[\d,]*)", re.IGNORECASE
)
_RE_NET_PRODUCT_PER_GUEST = re.compile(
    r"Net\s+product\s+sales\s+per\s+guest\s+([\d,]+\.\d+)", re.IGNORECASE
)
_RE_NET_PRODUCT_PER_INVOICE = re.compile(
    r"Net\s+product\s+sales/invoice\s+([\d,]+\.\d+)", re.IGNORECASE
)
_RE_NET_SERVICE_PER_INVOICE = re.compile(
    r"Net\s+service\s+sales/invoice\s+([\d,]+\.\d+)", re.IGNORECASE
)
_RE_NET_SALES_PER_HR = re.compile(
    r"Net\s+Sales/hr\s+([\d,]+\.\d+)", re.IGNORECASE
)
_RE_NET_ITEM_SALES_CENTER_HR = re.compile(
    r"Net\s+Item\s+sales/center\s+hour\s+([\d,]+\.\d+)", re.IGNORECASE
)
_RE_NET_SERVICE_SALES_CENTER_HR = re.compile(
    r"Net\s+service\s+sales/center\s+hour\s+([\d,]+\.\d+)", re.IGNORECASE
)
_RE_NET_PRODUCT_SALES_CENTER_HR = re.compile(
    r"Net\s+product\s+sales/center\s+hour\s+([\d,]+\.\d+)", re.IGNORECASE
)
_RE_DAYS_WITH_NO_INVOICES = re.compile(
    r"Days\s+with\s+no\s+invoices\s+(\d+)", re.IGNORECASE
)
_RE_REFUND_INVOICES = re.compile(
    r"Refund\s+invoices\s+(\d+)", re.IGNORECASE
)

# ---------------------------------------------------------------------------
# Regex — TIPS COLLECTED (we want the final Total)
# ---------------------------------------------------------------------------
# The block looks like:
#   TIPS COLLECTED                                   AMOUNT
#   Cash                                                  0.00
#   Card                                              632.00
#   Others                                                0.00
#   Total                                             632.00
#
# We anchor on "TIPS COLLECTED", then capture the first "Total <amount>"
# that appears (using DOTALL so whitespace/newlines don't matter).
_RE_TIPS_SECTION_TOTAL = re.compile(
    r"TIPS\s+COLLECTED.*?Total\s+([\d,]+\.\d+)",
    re.IGNORECASE | re.DOTALL,
)

# ---------------------------------------------------------------------------
# Regex — "Total invoices with services or product" (Karissa's guest_count)
# ---------------------------------------------------------------------------
# Canonical shape in real PDFs:
#   "Total invoices with services or product             95"
#   "Total invoices with services or product             159"
# Occasionally the number wraps to the next line after the label. We use a
# forward-scan regex that accepts a number within ~200 chars after the label.
_RE_INVOICE_COUNT = re.compile(
    r"Total\s+invoices\s+with\s+services?\s+or\s+product\s+(\d[\d,]*)",
    re.IGNORECASE,
)

# Invoice Summary companion fields (useful for trust layer):
_RE_NO_SHOW_COUNT = re.compile(
    r"No\s+show\s+(\d+)", re.IGNORECASE
)
_RE_VOIDS_COUNT = re.compile(
    r"Voids\s+(\d+)", re.IGNORECASE
)
# "Rebooked  4 (4.12)" — the percentage is Karissa's rebook_pct for Zenoti.
_RE_REBOOKED = re.compile(
    r"Rebooked\s+(\d[\d,]*)\s*\(([\d\.]+)\)", re.IGNORECASE
)

# ---------------------------------------------------------------------------
# Regex — SERVICE DETAILS top-level category rows
# ---------------------------------------------------------------------------
# Top-level rows ALWAYS have exactly 3-space indent, sub-rows have 5-space.
# Format:  "   <Label>   <qty> (<%qty>)   <avg_time>   <sales> (<%sales>)   <disc>"
# e.g.:    "   Haircut                    81 (73.64)   0.60   2,547.20 (65.71)   58.80"
#
# We anchor on start-of-line + 3 spaces + NOT another space (to exclude
# 5-space-indented sub-rows). The label is one of our known top-level
# categories. Sales can be "0.00" so we allow that too.
_RE_TOP_LEVEL_SERVICE_CATEGORY = re.compile(
    r"^ {3}(?! )"                                       # exactly 3 spaces
    # NOTE: "Hair Cut" (with space) appears alongside "Haircut" at
    # some locations (e.g. New Richmond). Both are captured and then
    # folded into the same `haircut` bucket by _extract_service_categories.
    r"(?P<label>Haircut|Hair\s+Cut|Color|Wax|Waxing|Treatment|Styling|Style|Texture|Perm|Extensions|Other|None)"
    r"\s+"
    r"(?P<qty>\d[\d,]*)\s*"                             # item qty
    r"\(\s*[\d\.]+\s*\)\s+"                             # (% qty)
    r"(?P<avg_time>[\d\.]+)\s+"                         # avg time (mins)
    r"(?P<sales>[\d,]+\.\d+)\s*"                        # net sales $
    r"\(\s*[\d\.]+\s*\)"                                # (% sales)
    r".*$",
    re.MULTILINE,
)

# The Service Details "Total" row — used as a sanity check against service_net.
# Shape: "   Total                   110 (100.00)   0.62   3,876.20 (100.00)   77.80"
_RE_SERVICE_DETAILS_TOTAL = re.compile(
    r"^ {3}Total\s+"
    r"(\d[\d,]*)\s*\(\s*100\.0+\s*\)\s+"
    r"[\d\.]+\s+"
    r"([\d,]+\.\d+)\s*\(\s*100\.0+\s*\)",
    re.MULTILINE | re.IGNORECASE,
)

# Anchor for locating the Service Details table (so the Total regex above
# doesn't match an earlier table's Total row).
_RE_SERVICE_DETAILS_HEADER = re.compile(
    r"SERVICE\s+DETAILS\s+ITEM\s+QTY",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Regex — HOURLY WORK DETAILS Production Hours line (location-level total)
# ---------------------------------------------------------------------------
# Real line (Andover):
#   "Production Hours           0 0 0 0 0 0 0 0 0.47 10.13 11.8 12.8 15.73 15.18 12.9 7.82 7 5.07 5 1.98 105.88"
# The trailing number is the daily total (always the last numeric value on
# the line). We search a line that starts with "Production Hours" and has
# hourly buckets, then grab the last float.
# Must NOT match "Non-Production" (which the HOURLY section never has anyway
# but we use negative lookbehind for safety).
_RE_HOURLY_PRODUCTION_HOURS = re.compile(
    r"^\s*Production\s+Hours\s+([0-9].+)$",
    re.MULTILINE,
)

# ---------------------------------------------------------------------------
# Regex — EMPLOYEE SALE DETAILS — individual stylist row
# ---------------------------------------------------------------------------
# Shape (12 numeric fields after name):
#   "     Katelyn Kuchinski  694.00  694.00  0.00  89.00  15  10  0.00  0.00  0.00  69.40  19.00  14.98"
# Fields:
#   1. net_service_$
#   2. comm_service_$
#   3. comm_disc_$
#   4. tips
#   5. service_qty (int)
#   6. invoice_count (int)
#   7. net_product_$
#   8. comm_product_$
#   9. net_prod_per_product_inv
#  10. avg_invoice_value
#  11. disc_$
#  12. disc_%
#
# Anchored at 5-space indent (`^ {4,5}`) — the SALE DETAILS section uses 5
# spaces for individuals, 3 for group headers. We'll accept 4-5 to be safe.
# Name is 1-N tokens of letters/dots/apostrophes/hyphens.
_RE_EMPLOYEE_SALE_INDIV = re.compile(
    r"^ {4,6}"
    r"(?P<name>[A-Za-z][A-Za-z\.\'\-_ ]+?)"
    r"\s{2,}"
    r"(?P<net_service>-?[\d,]+\.\d+)\s+"
    r"(?P<comm_service>-?[\d,]+\.\d+)\s+"
    r"(?P<comm_disc>-?[\d,]+\.\d+)\s+"
    r"(?P<tips>-?[\d,]+\.\d+)\s+"
    r"(?P<service_qty>-?\d[\d,]*)\s+"
    r"(?P<invoice_count>-?\d[\d,]*)\s+"
    r"(?P<net_product>-?[\d,]+\.\d+)\s+"
    r"(?P<comm_product>-?[\d,]+\.\d+)\s+"
    r"(?P<net_prod_per_pi>-?[\d,]+\.\d+)\s+"
    r"(?P<avg_invoice_value>-?[\d,]+\.\d+)\s+"
    r"(?P<disc_dollars>-?[\d,]+\.\d+)\s+"
    r"(?P<disc_pct>-?[\d,]+\.\d+)\s*$",
    re.MULTILINE,
)

# Role-group header row (9 numeric fields — skips invoice_count,
# net_prod_per_pi, avg_invoice_value). Shape:
#   "   MANAGER  694.00  694.00  0.00  89.00  15   0.00  0.00   19.00  14.98"
# Not needed for KPI extraction but useful for role classification.
_RE_EMPLOYEE_SALE_GROUP = re.compile(
    r"^ {2,3}"
    r"(?P<role>MANAGER|STYLIST|Shift\s+Leader|Master\s+Stylist|Senior\s+Stylist|Total)"
    r"\s{2,}"
    r"(?P<net_service>-?[\d,]+\.\d+)",
    re.MULTILINE | re.IGNORECASE,
)

# SALE DETAILS section bounds
_RE_EMP_SALE_START = re.compile(r"EMPLOYEE\s+SALE", re.IGNORECASE)
_RE_EMP_PERF_START = re.compile(r"EMPLOYEE\s+PERFORMANCE", re.IGNORECASE)
_RE_HOURLY_START   = re.compile(r"HOURLY\s+WORK", re.IGNORECASE)


# ---------------------------------------------------------------------------
# Regex — EMPLOYEE PERFORMANCE DETAILS — individual stylist row
# ---------------------------------------------------------------------------
# Shape (10 numeric fields after name; indented 4 spaces — note: this
# section uses 2-space/4-space indent, NOT 3/5 like SALE DETAILS):
#   "    Katelyn Kuchinski   0.05 0.83 18.30 18.30 0.00 0.00 37.92 38.96 37.92 9"
# Fields:
#   1. in_service_productivity
#   2. in_service_hours
#   3. actual_hours
#   4. production_hours
#   5. non_production_hours
#   6. blocked_hours
#   7. net_service_per_hr
#   8. gross_service_per_hr
#   9. service_comm_prod_per_hr
#  10. req_services_count (int)
# Followed on the NEXT line by: "(x.xx)" — req_services_pct. We match the
# 10 numeric fields here; the percent is picked up as a forward-scan hit.
_RE_EMPLOYEE_PERF_INDIV = re.compile(
    r"^ {3,5}"
    r"(?P<name>[A-Za-z][A-Za-z\.\'\-_ ]+?)"
    r"\s{2,}"
    r"(?P<in_svc_prod>-?[\d,]+\.\d+)\s+"
    r"(?P<in_svc_hours>-?[\d,]+\.\d+)\s+"
    r"(?P<actual_hours>-?[\d,]+\.\d+)\s+"
    r"(?P<production_hours>-?[\d,]+\.\d+)\s+"
    r"(?P<non_prod_hours>-?[\d,]+\.\d+)\s+"
    r"(?P<blocked_hours>-?[\d,]+\.\d+)\s+"
    r"(?P<net_svc_per_hr>-?[\d,]+\.\d+)\s+"
    r"(?P<gross_svc_per_hr>-?[\d,]+\.\d+)\s+"
    r"(?P<svc_comm_per_hr>-?[\d,]+\.\d+)\s+"
    r"(?P<req_services>-?\d[\d,]*)\s*$",
    re.MULTILINE,
)

# Percent-only continuation line: "       (17.65)" or "(0.00)"
_RE_REQ_PCT_LINE = re.compile(r"^\s*\(\s*([\d\.]+)\s*\)\s*$", re.MULTILINE)


# ---------------------------------------------------------------------------
# Flag constants
# ---------------------------------------------------------------------------
FLAG_NO_LOCATION          = "LOCATION_NOT_IDENTIFIED"
FLAG_NO_PERIOD            = "PERIOD_NOT_IDENTIFIED"
FLAG_NO_GUEST_COUNT       = "GUEST_COUNT_NOT_IDENTIFIED"
FLAG_NO_SERVICE_NET       = "SERVICE_NET_NOT_IDENTIFIED"
FLAG_NO_PRODUCT_NET       = "PRODUCT_NET_NOT_IDENTIFIED"
FLAG_NO_PRODUCTION_HOURS  = "PRODUCTION_HOURS_NOT_IDENTIFIED"
FLAG_NO_TIPS              = "TIPS_NOT_IDENTIFIED"
FLAG_TOTAL_SALES_MISMATCH = "TOTAL_SALES_MISMATCH"
FLAG_GUEST_COUNT_MISMATCH = "GUEST_COUNT_MISMATCH"
FLAG_SERVICE_NET_MISMATCH = "SERVICE_NET_MISMATCH"
FLAG_PARSE_CRASH          = "PARSE_CRASH"


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------

class ZenotiV2Parser:
    """
    Parse a Zenoti Salon Summary PDF's extracted text into a normalised dict.

    Design notes
    ────────────
    • Input is text (already extracted). Same design as SalonUltimateV2Parser.
    • Non-raising: every field we fail to locate becomes None + a flag.
    • Karissa-canonical KPIs are recomputed from raw fields. The PDF's
      pre-computed stats (avg. invoice value, net product sales/invoice,
      etc.) are captured in `raw` for audit but never used in `karissa`.
    • Wax / Waxing category duplication is always summed into `wax_combined`.
    • Employee data is merged from TWO sections (SALE DETAILS has money +
      requests; PERFORMANCE DETAILS has hours + per-hour rates). We join
      by stylist name.
    """

    def __init__(self, text: str) -> None:
        self.text = text or ""
        self.flags: List[str] = []

    # ---- public -----------------------------------------------------------

    def parse(self) -> Dict[str, Any]:
        """
        Parse and return:

            {
                "platform": "zenoti",
                "location": "Andover",
                "location_raw": "888-10278-Andover",
                "week_start": "2026-04-01",
                "week_end":   "2026-04-05",
                "unclosed_days": [],            # Zenoti never reports these
                "flags": [...],
                "raw": { ... PDF-reported fields ... },
                "service_categories": {
                    "haircut":      {"qty":  81, "sales": 2547.20},
                    "color":        {"qty":  12, "sales":  944.00},
                    "wax":          {"qty":  10, "sales":  198.00},
                    "waxing":       {"qty":   0, "sales":    0.00},
                    "wax_combined": {"qty":  10, "sales":  198.00},  # ← Karissa uses this
                    "treatment":    {"qty":   4, "sales":  104.00},
                    "style":        {"qty":   2, "sales":   56.00},
                    "styling":      {"qty":   1, "sales":   27.00},
                    ...
                },
                "karissa": { ... canonical KPIs ... },
                "employees": [ {...merged from both tables...}, ... ],
            }
        """
        self.flags = []

        location, location_raw = self._extract_location()
        week_start, week_end = self._extract_period()

        raw = self._extract_raw_fields()
        service_categories = self._extract_service_categories()
        employees = self._extract_employees()
        karissa = self._compute_karissa_kpis(raw, service_categories)

        result: Dict[str, Any] = {
            "platform": "zenoti",
            "location": location,
            "location_raw": location_raw,
            "week_start": week_start,
            "week_end": week_end,
            "unclosed_days": [],   # Zenoti doesn't surface unclosed-days markers
            "flags": list(self.flags),
            "raw": raw,
            "service_categories": service_categories,
            "karissa": karissa,
            "employees": employees,
        }
        return result

    # ---- location ---------------------------------------------------------

    def _extract_location(self) -> Tuple[Optional[str], Optional[str]]:
        """
        Returns (canonical_name, raw_header_fragment).

        Uses `extract_zenoti_location` from pdf_location_normalizer, which
        understands all three cluster prefixes (888-, 887-, 910-) AND the
        "FS Roseville, MN" variant.
        """
        canonical = extract_zenoti_location(self.text)

        # Capture raw header line for audit (the "888-10278-Andover" bit).
        raw = None
        m = re.search(
            r"^\s*(\d+\-\d+\-[^\n]+)\s*$",
            self.text[:2000],
            re.MULTILINE,
        )
        if m:
            raw = m.group(1).strip()

        if not canonical:
            self.flags.append(FLAG_NO_LOCATION)
            logger.warning("ZenotiV2Parser: could not identify location. Raw=%r", raw)

        return canonical, raw

    # ---- period -----------------------------------------------------------

    def _extract_period(self) -> Tuple[Optional[str], Optional[str]]:
        """Returns (start_iso, end_iso) or (None, None)."""
        try:
            start, end = parse_date_range(self.text)
            return start, end
        except ValueError:
            self.flags.append(FLAG_NO_PERIOD)
            logger.warning("ZenotiV2Parser: could not parse period")
            return None, None

    # ---- raw fields -------------------------------------------------------

    def _extract_raw_fields(self) -> Dict[str, Any]:
        """Extract the top SALES block, STATISTICS block, and tips total."""
        svc = self._match(_RE_SERVICE_SALES)
        prod = self._match(_RE_PRODUCT_SALES)
        subtot = self._match(_RE_SERVICE_PRODUCT_SUBTOTAL)
        item_total = self._match(_RE_ITEM_TOTAL)

        service_net = safe_parse_money(svc.group(3)) if svc else None
        service_gross = safe_parse_money(svc.group(2)) if svc else None
        service_count = safe_parse_int(svc.group(1)) if svc else None

        product_net = safe_parse_money(prod.group(3)) if prod else None
        product_gross = safe_parse_money(prod.group(2)) if prod else None
        product_count = safe_parse_int(prod.group(1)) if prod else None

        item_total_net = safe_parse_money(item_total.group(3)) if item_total else None
        item_total_gross = safe_parse_money(item_total.group(2)) if item_total else None
        item_total_count = safe_parse_int(item_total.group(1)) if item_total else None

        subtotal_net = safe_parse_money(subtot.group(3)) if subtot else None

        if service_net is None:
            self.flags.append(FLAG_NO_SERVICE_NET)
        if product_net is None:
            self.flags.append(FLAG_NO_PRODUCT_NET)

        # Invoice count — Karissa's guest_count
        invoice_count = self._find_int(_RE_INVOICE_COUNT)
        if invoice_count is None:
            self.flags.append(FLAG_NO_GUEST_COUNT)

        # Tips total — anchored on "TIPS COLLECTED" section
        tips_total = self._find_money(_RE_TIPS_SECTION_TOTAL)
        if tips_total is None:
            self.flags.append(FLAG_NO_TIPS)

        # Production hours — from HOURLY WORK DETAILS total
        production_hours = self._extract_production_hours_total()
        if production_hours is None:
            self.flags.append(FLAG_NO_PRODUCTION_HOURS)

        # Statistics (audit-only; most rejected)
        rebooked_match = self._match(_RE_REBOOKED)
        rebooked_count = safe_parse_int(rebooked_match.group(1)) if rebooked_match else None
        rebooked_pct = (
            safe_parse_percent(rebooked_match.group(2), as_fraction=False)
            if rebooked_match else None
        )

        return {
            # Sales block
            "service_count": service_count,
            "service_gross": service_gross,
            "service_net": service_net,
            "product_count": product_count,
            "product_gross": product_gross,
            "product_net": product_net,
            "item_total_count": item_total_count,
            "item_total_gross": item_total_gross,
            "item_total_net": item_total_net,
            "subtotal_net": subtotal_net,
            "sales_inc_tax": self._find_money(_RE_SALES_INC_TAX),
            "tax": self._find_money(_RE_TAX),
            "tips_total": tips_total,

            # Invoice / guest count canonical
            "invoice_count": invoice_count,

            # Production hours (location total)
            "production_hours": production_hours,

            # Statistics (rejected by Karissa's formulas — stored for audit)
            "avg_invoice_value_pdf": self._find_money(_RE_AVG_INVOICE_VALUE),
            "total_guest_count_pdf": self._find_int(_RE_TOTAL_GUEST_COUNT_PDF),
            "net_product_per_guest_pdf": self._find_money(_RE_NET_PRODUCT_PER_GUEST),
            "net_product_per_invoice_pdf": self._find_money(_RE_NET_PRODUCT_PER_INVOICE),
            "net_service_per_invoice_pdf": self._find_money(_RE_NET_SERVICE_PER_INVOICE),
            "net_sales_per_hr_pdf": self._find_money(_RE_NET_SALES_PER_HR),
            "net_item_sales_center_hour": self._find_money(_RE_NET_ITEM_SALES_CENTER_HR),
            "net_service_sales_center_hour": self._find_money(_RE_NET_SERVICE_SALES_CENTER_HR),
            "net_product_sales_center_hour": self._find_money(_RE_NET_PRODUCT_SALES_CENTER_HR),

            # Operational counts
            "days_with_no_invoices": self._find_int(_RE_DAYS_WITH_NO_INVOICES),
            "refund_invoices": self._find_int(_RE_REFUND_INVOICES),
            "no_show_count": self._find_int(_RE_NO_SHOW_COUNT),
            "voids_count": self._find_int(_RE_VOIDS_COUNT),
            "rebooked_count": rebooked_count,
            "rebooked_pct": rebooked_pct,
        }

    # ---- production hours helper -----------------------------------------

    def _extract_production_hours_total(self) -> Optional[float]:
        """
        Pull the location-level production hours from the HOURLY WORK DETAILS
        'Production Hours' line. The final number on that line is the total.

        Line shape (Andover):
            "Production Hours  0 0 0 0 0 0 0 0 0.47 10.13 11.8 12.8 15.73
            15.18 12.9 7.82 7 5.07 5 1.98 105.88"

        We deliberately anchor on the HOURLY WORK section (not the employee
        PERFORMANCE table) because the hourly section has a single clean
        location total and no name-wrap risk.

        Fallback: if the HOURLY section can't be found, we look for the
        EMPLOYEE PERFORMANCE "Total" row and pull its production_hours field.
        """
        # Locate HOURLY WORK section
        m_start = _RE_HOURLY_START.search(self.text)
        if m_start:
            section = self.text[m_start.start():m_start.start() + 2000]
            m = _RE_HOURLY_PRODUCTION_HOURS.search(section)
            if m:
                tail = m.group(1)
                # Last float in the tail is the location total
                nums = re.findall(r"-?\d+(?:\.\d+)?", tail)
                if nums:
                    try:
                        return round(float(nums[-1]), 4)
                    except ValueError:
                        pass

        # Fallback: EMPLOYEE PERFORMANCE Total row
        # Shape: "Total  0.20 21.16 105.88 105.88 0.00 2.00 36.61 37.80 36.61 51"
        # Column 3 (0-indexed: 2) is "actual_hours" which equals production
        # hours at the location level.
        m_perf = _RE_EMP_PERF_START.search(self.text)
        if m_perf:
            section = self.text[m_perf.start():m_perf.start() + 5000]
            m = re.search(
                r"^\s*Total\s+(-?[\d,]+\.\d+)\s+(-?[\d,]+\.\d+)\s+(-?[\d,]+\.\d+)\s+(-?[\d,]+\.\d+)",
                section,
                re.MULTILINE | re.IGNORECASE,
            )
            if m:
                # Field 4 (group 4) is production hours
                return safe_parse_money(m.group(4), default=None) or None

        return None

    # ---- service categories ----------------------------------------------

    def _extract_service_categories(self) -> Dict[str, Dict[str, float]]:
        """
        Extract top-level service category rows from the SERVICE DETAILS
        section. PyMuPDF renders this section line-per-field (no whitespace
        indentation we can anchor on) with this shape:

            Haircut                     ← category label on its own line
            81 (73.64)                  ← qty (qty_pct)
            0.60                        ← avg time
            2,547.20 (65.71)            ← sales (sales_pct)
            58.80                       ← disc
            Adult Custom Haircut (10278)  ← first sub-item, same shape
            45 (40.91)
            ...

        Sub-items always have qualifying context (an appended "(NNNNN)"
        code, a leading "FS ", or multi-word names). Top-level rows are
        the ONLY lines whose stripped content EQUALS one of the known
        category labels exactly. That's the discriminator we use.

        Crucially we sum `wax` + `waxing` into `wax_combined`, which is
        what Karissa's wax_count formula requires. If only one exists,
        the other is zero. "Hair Cut" and "Haircut" both fold into the
        `haircut` bucket with qty/sales summed.
        """
        out: Dict[str, Dict[str, float]] = {}

        # Anchor to SERVICE DETAILS section so we don't accidentally match
        # "Color" / "Wax" in the Product Details or employee sections.
        m_section = _RE_SERVICE_DETAILS_HEADER.search(self.text)
        if not m_section:
            # No section found → return empty (with wax_combined stub below).
            out["wax_combined"] = {"qty": 0, "sales": 0.0}
            return out

        section_start = m_section.end()
        # Cut section at the next major heading so PRODUCT DETAILS rows
        # can't leak in. "PRODUCT DETAILS", "Service Invoice", and the
        # employee sections (SALE DETAILS, PERFORMANCE DETAILS) all end
        # the Service Details table.
        rest = self.text[section_start:]
        stop_markers = [
            "PRODUCT DETAILS",
            "Service Invoice",
            "SALE DETAILS",
            "PERFORMANCE DETAILS",
        ]
        section_end = len(rest)
        for marker in stop_markers:
            i = rest.find(marker)
            if i != -1 and i < section_end:
                section_end = i
        section = rest[:section_end]

        # Known top-level labels — normalised to lowercase, no whitespace.
        # "Hair Cut" folds into "haircut".
        known_labels = {
            "haircut": "haircut",
            "color": "color",
            "wax": "wax",
            "waxing": "waxing",
            "treatment": "treatment",
            "styling": "styling",
            "style": "style",
            "texture": "texture",
            "perm": "perm",
            "extensions": "extensions",
            "other": "other",
            "none": "none",
        }

        # Shape of the four following lines we expect for each category row.
        qty_pct_re = re.compile(r"^\s*(\d[\d,]*)\s*\(\s*[\d\.]+\s*\)\s*$")
        sales_pct_re = re.compile(r"^\s*([\d,]+\.\d+)\s*\(\s*[\d\.]+\s*\)\s*$")
        float_re = re.compile(r"^\s*[\d,]*\.?\d+\s*$")

        lines = section.split("\n")

        def _next_nonempty(idx: int, max_idx: int) -> Tuple[int, str]:
            """Advance past blank lines. Returns (index, content) or (-1, '')."""
            j = idx
            while j < max_idx:
                l = lines[j].strip()
                if l:
                    return j, l
                j += 1
            return -1, ""

        i = 0
        n = len(lines)
        while i < n:
            raw = lines[i].strip()
            # Collapse whitespace + lowercase for label match
            label_key = re.sub(r"\s+", "", raw).lower()
            if not label_key or label_key not in known_labels:
                i += 1
                continue

            # Peek the next 4 non-empty lines and verify the shape
            j, qty_line = _next_nonempty(i + 1, n)
            if j == -1 or not qty_pct_re.match(qty_line):
                i += 1
                continue
            k, avg_line = _next_nonempty(j + 1, n)
            if k == -1 or not float_re.match(avg_line):
                i += 1
                continue
            ll, sales_line = _next_nonempty(k + 1, n)
            if ll == -1 or not sales_pct_re.match(sales_line):
                i += 1
                continue

            # Shape matches → this is a top-level category row.
            label = known_labels[label_key]
            qty = safe_parse_int(qty_pct_re.match(qty_line).group(1))
            sales = safe_parse_money(sales_pct_re.match(sales_line).group(1))
            # Fold "haircut" / "Hair Cut" together via summation.
            if label not in out:
                out[label] = {"qty": qty, "sales": sales}
            else:
                out[label]["qty"] = out[label].get("qty", 0) + qty
                out[label]["sales"] = round(
                    out[label].get("sales", 0.0) + sales, 2
                )
            # Advance past all four fields we consumed
            i = ll + 1

        # Ensure wax / waxing always have keys so downstream code can blindly
        # sum them.
        wax_row = out.get("wax", {"qty": 0, "sales": 0.0})
        waxing_row = out.get("waxing", {"qty": 0, "sales": 0.0})
        out["wax_combined"] = {
            "qty": wax_row.get("qty", 0) + waxing_row.get("qty", 0),
            "sales": round(
                wax_row.get("sales", 0.0) + waxing_row.get("sales", 0.0),
                2,
            ),
        }

        # Capture the Total row for sanity-check logging (not returned).
        # IMPORTANT: anchor to the SERVICE DETAILS section — an earlier table
        # ("Membership / Invoice Counts") also has a row matching the generic
        # "Total N (100.00)  M  X.XX (100.00)" shape and would give a false
        # positive ($4,222.20 item_total vs the $3,876.20 we want).
        m_section = _RE_SERVICE_DETAILS_HEADER.search(self.text)
        if m_section:
            section = self.text[m_section.end():m_section.end() + 15000]
            m_total = _RE_SERVICE_DETAILS_TOTAL.search(section)
            if m_total:
                total_qty = safe_parse_int(m_total.group(1))
                total_sales = safe_parse_money(m_total.group(2))
                sum_cat_sales = sum(
                    v["sales"] for k, v in out.items() if k != "wax_combined"
                )
                if abs(sum_cat_sales - total_sales) > 0.51:
                    logger.warning(
                        "ZenotiV2Parser: Service Details category sum (%.2f) "
                        "differs from table Total (%.2f) by more than $0.50",
                        sum_cat_sales, total_sales,
                    )
                del total_qty  # suppress unused warning; kept for future use

        return out

    # ---- employees -------------------------------------------------------

    def _extract_employees(self) -> List[Dict[str, Any]]:
        """
        Parse the two employee sections and merge by stylist name.

        SALE DETAILS has: net_service, comm_service, comm_disc, tips,
                          service_qty, invoice_count, net_product, comm_product,
                          net_prod_per_pi, avg_invoice_value, disc_$, disc_%.
        PERFORMANCE DETAILS has: in_service_productivity, in_service_hours,
                          actual_hours, production_hours, non_production_hours,
                          blocked_hours, net_svc_per_hr, gross_svc_per_hr,
                          svc_comm_per_hr, req_services count (+ req_pct on
                          the next line).

        Both tables use name as the join key. Group rows (MANAGER, STYLIST,
        Shift Leader, Total) are skipped for individuals but captured
        separately as `role_group` tagging on each individual.
        """
        # Locate section bounds
        sale_start = _RE_EMP_SALE_START.search(self.text)
        perf_start = _RE_EMP_PERF_START.search(self.text)
        hourly_start = _RE_HOURLY_START.search(self.text)

        sale_end = perf_start.start() if perf_start else len(self.text)
        perf_end = hourly_start.start() if hourly_start else len(self.text)

        sale_section = self.text[sale_start.start():sale_end] if sale_start else ""
        perf_section = self.text[perf_start.start():perf_end] if perf_start else ""

        sale_rows = self._parse_sale_section(sale_section)
        perf_rows = self._parse_perf_section(perf_section)

        # Merge by name (case-insensitive, whitespace-normalised)
        def _key(n: str) -> str:
            return re.sub(r"\s+", " ", (n or "").strip().lower())

        merged: Dict[str, Dict[str, Any]] = {}
        for row in sale_rows:
            k = _key(row["name"])
            merged[k] = {**row}

        for row in perf_rows:
            k = _key(row["name"])
            if k in merged:
                merged[k].update({
                    "in_service_productivity": row.get("in_service_productivity"),
                    "in_service_hours": row.get("in_service_hours"),
                    "actual_hours": row.get("actual_hours"),
                    "production_hours": row.get("production_hours"),
                    "non_production_hours": row.get("non_production_hours"),
                    "blocked_hours": row.get("blocked_hours"),
                    "net_service_per_hr": row.get("net_service_per_hr"),
                    "gross_service_per_hr": row.get("gross_service_per_hr"),
                    "service_comm_prod_per_hr": row.get("service_comm_prod_per_hr"),
                    "req_services_count": row.get("req_services_count"),
                    "req_services_pct": row.get("req_services_pct"),
                })
            else:
                # Name only in performance table — unusual but possible.
                merged[k] = {**row}

        # Sort by lowercased name so output is deterministic
        return sorted(merged.values(), key=lambda r: _key(r.get("name", "")))

    # ---- sale section walker ---------------------------------------------

    def _parse_sale_section(self, section: str) -> List[Dict[str, Any]]:
        """
        Walk the EMPLOYEE SALE DETAILS section line-by-line. Identify:
         • Role group rows (MANAGER / STYLIST / Shift Leader / Total)
         • Individual stylist rows (name + 12 numeric fields)
         • Continuation lines (just a second name chunk — merge with prior)

        The continuation-line handling mirrors the SU v2 parser: if the
        previous "name" captured ends in a single token and the next line
        is purely word-chars (no numeric tail), append it. Real example:
            "     Rebecca               192.00  ..."
            "     Follansbee"                        ← continuation
        """
        if not section:
            return []

        out: List[Dict[str, Any]] = []
        current_role = "UNKNOWN"
        prev_row_ref: Optional[Dict[str, Any]] = None

        lines = section.splitlines()
        for line in lines:
            # Role group detection: starts with 2-3 spaces + ROLE + whitespace
            m_role = re.match(
                r"^ {2,3}(MANAGER|STYLIST|Shift\s+Leader|Master\s+Stylist|Senior\s+Stylist)\b",
                line,
                re.IGNORECASE,
            )
            if m_role:
                current_role = m_role.group(1).upper().replace("  ", " ")
                prev_row_ref = None
                continue

            # Total row — end of useful data in this section
            if re.match(r"^ {2,3}Total\b", line):
                prev_row_ref = None
                continue

            # Individual row?
            m_indiv = _RE_EMPLOYEE_SALE_INDIV.match(line)
            if m_indiv:
                row = {
                    "name": re.sub(r"\s+", " ", m_indiv.group("name").strip()),
                    "role_group": current_role,
                    "net_service": safe_parse_money(m_indiv.group("net_service")),
                    "comm_service": safe_parse_money(m_indiv.group("comm_service")),
                    "comm_disc": safe_parse_money(m_indiv.group("comm_disc")),
                    "tips": safe_parse_money(m_indiv.group("tips")),
                    "service_qty": safe_parse_int(m_indiv.group("service_qty")),
                    "invoice_count": safe_parse_int(m_indiv.group("invoice_count")),
                    "net_product": safe_parse_money(m_indiv.group("net_product")),
                    "comm_product": safe_parse_money(m_indiv.group("comm_product")),
                    "net_prod_per_product_inv": safe_parse_money(m_indiv.group("net_prod_per_pi")),
                    "avg_invoice_value": safe_parse_money(m_indiv.group("avg_invoice_value")),
                    "disc_dollars": safe_parse_money(m_indiv.group("disc_dollars")),
                    "disc_pct": safe_parse_percent(m_indiv.group("disc_pct"), as_fraction=False),
                }
                out.append(row)
                prev_row_ref = row
                continue

            # Continuation line detection: only word tokens, 4+ char
            # indent matching an individual row's indent, no numeric tail.
            if prev_row_ref is not None:
                stripped = line.strip()
                if (
                    stripped
                    and not re.search(r"\d", stripped)
                    and re.match(r"^[A-Za-z][A-Za-z\.\'\-_ ]+$", stripped)
                    and re.match(r"^ {4,6}", line)
                ):
                    prev_row_ref["name"] = re.sub(
                        r"\s+", " ", f"{prev_row_ref['name']} {stripped}"
                    )

        return out

    # ---- performance section walker --------------------------------------

    def _parse_perf_section(self, section: str) -> List[Dict[str, Any]]:
        """
        Walk the EMPLOYEE PERFORMANCE DETAILS section line-by-line. The
        shape is an individual row followed by a percent-only continuation
        line holding req_services_pct, e.g.:

            "    Katelyn Kuchinski   0.05 0.83 18.30 18.30 0.00 0.00 37.92 38.96 37.92 9"
            "                                                         (0.00)"

        Multi-word names split across lines are possible (Alexandria
        Costello / Martinez in Roseville). We detect them the same way as
        the sale section — a pure-letters line before the numbers.
        """
        if not section:
            return []

        out: List[Dict[str, Any]] = []
        current_role = "UNKNOWN"
        prev_row_ref: Optional[Dict[str, Any]] = None
        pending_name_prefix: Optional[str] = None

        lines = section.splitlines()
        for line in lines:
            # Role group: 2-space indent + ROLE + numbers
            m_role = re.match(
                r"^ {1,3}(MANAGER|STYLIST|Shift\s+Leader|Master\s+Stylist|Senior\s+Stylist)\b",
                line,
                re.IGNORECASE,
            )
            if m_role:
                current_role = m_role.group(1).upper().replace("  ", " ")
                prev_row_ref = None
                pending_name_prefix = None
                continue

            # Total row — closes the section
            if re.match(r"^ {1,3}Total\b", line):
                prev_row_ref = None
                pending_name_prefix = None
                continue

            # Percent continuation line — attach to prev row
            m_pct = _RE_REQ_PCT_LINE.match(line)
            if m_pct and prev_row_ref is not None:
                prev_row_ref["req_services_pct"] = safe_parse_percent(
                    m_pct.group(1), as_fraction=False
                )
                continue

            # Individual row?
            m_indiv = _RE_EMPLOYEE_PERF_INDIV.match(line)
            if m_indiv:
                name = re.sub(r"\s+", " ", m_indiv.group("name").strip())
                if pending_name_prefix:
                    # Merge name-prefix line from before the numeric line
                    name = f"{pending_name_prefix} {name}".strip()
                    name = re.sub(r"\s+", " ", name)
                    pending_name_prefix = None

                row = {
                    "name": name,
                    "role_group": current_role,
                    "in_service_productivity": safe_parse_money(m_indiv.group("in_svc_prod")),
                    "in_service_hours": safe_parse_money(m_indiv.group("in_svc_hours")),
                    "actual_hours": safe_parse_money(m_indiv.group("actual_hours")),
                    "production_hours": safe_parse_money(m_indiv.group("production_hours")),
                    "non_production_hours": safe_parse_money(m_indiv.group("non_prod_hours")),
                    "blocked_hours": safe_parse_money(m_indiv.group("blocked_hours")),
                    "net_service_per_hr": safe_parse_money(m_indiv.group("net_svc_per_hr")),
                    "gross_service_per_hr": safe_parse_money(m_indiv.group("gross_svc_per_hr")),
                    "service_comm_prod_per_hr": safe_parse_money(m_indiv.group("svc_comm_per_hr")),
                    "req_services_count": safe_parse_int(m_indiv.group("req_services")),
                    "req_services_pct": None,  # filled by next line
                }
                out.append(row)
                prev_row_ref = row
                continue

            # Pure-name continuation line (before numeric line) — used when
            # a multi-word name wraps. Save it; the next numeric match will
            # prepend it.
            stripped = line.strip()
            if (
                stripped
                and not re.search(r"\d", stripped)
                and re.match(r"^[A-Za-z][A-Za-z\.\'\-_ ]+$", stripped)
                and re.match(r"^ {3,6}", line)
            ):
                pending_name_prefix = stripped
                continue

            # Pure-name continuation AFTER the numeric line (rarer) —
            # append to prev row. Skip if we're already holding a prefix.
            if (
                prev_row_ref is not None
                and pending_name_prefix is None
                and stripped
                and not re.search(r"\d", stripped)
                and re.match(r"^[A-Za-z][A-Za-z\.\'\-_ ]+$", stripped)
            ):
                prev_row_ref["name"] = re.sub(
                    r"\s+", " ", f"{prev_row_ref['name']} {stripped}"
                )

        return out

    # ---- karissa KPIs ----------------------------------------------------

    def _compute_karissa_kpis(
        self,
        raw: Dict[str, Any],
        service_categories: Dict[str, Dict[str, float]],
    ) -> Dict[str, Any]:
        """
        Compute Karissa-canonical KPIs from `raw` and `service_categories`.

        Cross-checks:
            • item_total_net should equal service_net + product_net.
              If it doesn't (within $0.01), flag TOTAL_SALES_MISMATCH.
            • invoice_count (canonical) vs total_guest_count_pdf (unique).
              If they match exactly, that's usually coincidence (new-guest-
              only weeks). If they differ, great — confirms we're using
              the right one. We never flag that mismatch; it's expected.
        """
        service_net = raw.get("service_net") or 0.0
        product_net = raw.get("product_net") or 0.0
        guest_count = raw.get("invoice_count")
        prod_hours = raw.get("production_hours")

        total_sales = round(service_net + product_net, 2)

        # Sanity check: total_sales should match item_total_net (± 1 cent).
        item_total = raw.get("item_total_net")
        if item_total is not None and abs(item_total - total_sales) > 0.01:
            self.flags.append(FLAG_TOTAL_SALES_MISMATCH)
            logger.warning(
                "ZenotiV2Parser: computed total_sales (%.2f) differs from "
                "PDF item_total_net (%.2f)", total_sales, item_total,
            )

        # avg_ticket / PPG / PPH — guard against missing guest_count / hours
        avg_ticket: Optional[float] = None
        ppg: Optional[float] = None
        pph: Optional[float] = None

        if guest_count and guest_count > 0:
            avg_ticket = round(total_sales / guest_count, 2)
            ppg = round(product_net / guest_count, 4)

        if prod_hours and prod_hours > 0:
            pph = round(service_net / prod_hours, 2)

        # Service-category KPIs (Karissa's formulas)
        wax = service_categories.get("wax_combined", {"qty": 0, "sales": 0.0})
        color = service_categories.get("color", {"qty": 0, "sales": 0.0})
        treatment = service_categories.get("treatment", {"qty": 0, "sales": 0.0})
        haircut = service_categories.get("haircut", {"qty": 0, "sales": 0.0})

        wax_pct: Optional[float] = None
        treatment_pct: Optional[float] = None
        color_pct: Optional[float] = None

        if guest_count and guest_count > 0:
            wax_pct = round(wax["qty"] / guest_count, 4)
            treatment_pct = round(treatment["qty"] / guest_count, 4)

        if total_sales > 0:
            color_pct = round(color["sales"] / total_sales, 4)

        # Rebook % — Karissa uses the PDF's "Rebooked (%)" when available.
        # Returned as a fraction so consumers can format consistently.
        rebook_pct_pdf = raw.get("rebooked_pct")
        rebook_pct: Optional[float] = None
        if rebook_pct_pdf is not None:
            rebook_pct = round(rebook_pct_pdf / 100.0, 4)

        return {
            "guest_count": guest_count,
            "service_net": round(service_net, 2),
            "product_net": round(product_net, 2),
            "total_sales": total_sales,
            "avg_ticket": avg_ticket,
            "pph": pph,
            "ppg": ppg,
            "tips": raw.get("tips_total"),
            "production_hours": prod_hours,
            "wax_count": wax["qty"],
            "wax_sales": round(wax["sales"], 2),
            "wax_pct": wax_pct,
            "color_sales": round(color["sales"], 2),
            "color_count": color["qty"],
            "color_pct": color_pct,
            "treatment_count": treatment["qty"],
            "treatment_sales": round(treatment["sales"], 2),
            "treatment_pct": treatment_pct,
            "haircut_count": haircut["qty"],
            "haircut_sales": round(haircut["sales"], 2),
            "rebook_pct": rebook_pct,
        }

    # ---- helpers ---------------------------------------------------------

    def _match(self, pattern: re.Pattern) -> Optional[re.Match]:
        """Find the first match of `pattern` in self.text, or return None."""
        return pattern.search(self.text)

    def _find_money(self, pattern: re.Pattern) -> Optional[float]:
        m = pattern.search(self.text)
        if not m:
            return None
        try:
            return safe_parse_money(m.group(1), default=None)
        except (IndexError, TypeError):
            return None

    def _find_int(self, pattern: re.Pattern) -> Optional[int]:
        m = pattern.search(self.text)
        if not m:
            return None
        try:
            val = safe_parse_int(m.group(1), default=None)
            return val
        except (IndexError, TypeError):
            return None

    def _find_percent(
        self, pattern: re.Pattern, as_fraction: bool = False,
    ) -> Optional[float]:
        m = pattern.search(self.text)
        if not m:
            return None
        try:
            return safe_parse_percent(m.group(1), as_fraction=as_fraction, default=None)
        except (IndexError, TypeError):
            return None


# ---------------------------------------------------------------------------
# Convenience functions
# ---------------------------------------------------------------------------

def parse_text(text: str) -> Dict[str, Any]:
    """
    Parse already-extracted PDF text. Non-raising — on any exception,
    returns a minimal dict with PARSE_CRASH flagged.
    """
    try:
        return ZenotiV2Parser(text).parse()
    except Exception as exc:  # pragma: no cover — defensive
        logger.exception("parse_text: unhandled exception: %s", exc)
        return {
            "platform": "zenoti",
            "location": None,
            "location_raw": None,
            "week_start": None,
            "week_end": None,
            "unclosed_days": [],
            "flags": [FLAG_PARSE_CRASH],
            "raw": {},
            "service_categories": {},
            "karissa": {},
            "employees": [],
            "_error": str(exc),
        }


def parse_file(pdf_path: str) -> Dict[str, Any]:
    """
    Open a Zenoti PDF, extract its text, and parse it. Non-raising.
    Uses PyMuPDF (fitz) if available; returns PARSE_CRASH flag on any I/O
    or extraction failure.
    """
    try:
        import fitz  # PyMuPDF
    except ImportError:
        logger.error("parse_file: PyMuPDF not installed")
        return {
            "platform": "zenoti",
            "location": None,
            "location_raw": None,
            "week_start": None,
            "week_end": None,
            "unclosed_days": [],
            "flags": [FLAG_PARSE_CRASH],
            "raw": {},
            "service_categories": {},
            "karissa": {},
            "employees": [],
            "_error": "PyMuPDF not installed",
        }

    try:
        doc = fitz.open(pdf_path)
        text_parts = []
        for page in doc:
            text_parts.append(page.get_text())
        doc.close()
        return parse_text("\n".join(text_parts))
    except Exception as exc:
        logger.exception("parse_file: failed to parse %s: %s", pdf_path, exc)
        return {
            "platform": "zenoti",
            "location": None,
            "location_raw": None,
            "week_start": None,
            "week_end": None,
            "unclosed_days": [],
            "flags": [FLAG_PARSE_CRASH],
            "raw": {},
            "service_categories": {},
            "karissa": {},
            "employees": [],
            "_error": str(exc),
        }


__all__ = [
    "ZenotiV2Parser",
    "parse_text",
    "parse_file",
    # Flag constants
    "FLAG_NO_LOCATION",
    "FLAG_NO_PERIOD",
    "FLAG_NO_GUEST_COUNT",
    "FLAG_NO_SERVICE_NET",
    "FLAG_NO_PRODUCT_NET",
    "FLAG_NO_PRODUCTION_HOURS",
    "FLAG_NO_TIPS",
    "FLAG_TOTAL_SALES_MISMATCH",
    "FLAG_GUEST_COUNT_MISMATCH",
    "FLAG_SERVICE_NET_MISMATCH",
    "FLAG_PARSE_CRASH",
]
