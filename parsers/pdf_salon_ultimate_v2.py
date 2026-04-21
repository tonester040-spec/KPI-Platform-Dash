"""
parsers/pdf_salon_ultimate_v2.py
────────────────────────────────
Salon Ultimate "FS Salon Dashboard" PDF parser — full extraction of every
field the Tier 2 pipeline needs to populate the V1 CURRENT tab.

This supersedes the legacy `salon_ultimate_pdf.py` which only extracted four
service-category counts. That parser is retained for the Excel+PDF merge
flow in `tier2_batch_processor.py`; new PDF-only ingestion uses THIS module.

Why a rewrite
─────────────
The legacy parser missed, per Karissa's canonical formula contract:
    • guest_count (Serviced + Retail Only)
    • service_net, product_net, total_sales
    • tips, production hours, PPH, PPG, avg ticket
    • rebook % / prebook %
    • the full Employee Summary table
    • partial-week detection (unclosed days)

All of those are now captured below, with Karissa-canonical KPIs computed
from first principles (NOT trusting the PDF's pre-computed stats).

Validated against the 2026-04-01 → 04-05 production exports:
    Apple Valley, Farmington, Lakeville   — all 3 parse cleanly.

Public API
──────────
    parser = SalonUltimateV2Parser(text)      # already-extracted text
    result = parser.parse()                   # returns dict

    # Or from a PDF file:
    parse_file("/path/to/apple_valley.pdf")   # returns dict

The returned dict structure is documented in SalonUltimateV2Parser.parse().
"""

from __future__ import annotations

import logging
import re
from typing import Any, Dict, List, Optional, Tuple

from parsers.pdf_common import (
    detect_unclosed_days,
    parse_date_range,
    safe_parse_hours,
    safe_parse_int,
    safe_parse_money,
    safe_parse_percent,
)
from parsers.pdf_location_normalizer import extract_salon_ultimate_location

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Regex: Sales / Guest Count / Statistics section anchors
# ---------------------------------------------------------------------------
# These target labels that appear in the top-of-page summary block. Each
# anchor captures one money / integer / percent / hours value on the same
# (or adjacent) line. We search the entire text — the block only ever
# appears once per PDF.

_RE_TOTAL_SERVICE  = re.compile(r"Total\s+Service\s+\$?\s*([\-\$\d,\.]+)", re.IGNORECASE)
_RE_TOTAL_RETAIL   = re.compile(r"Total\s+Retail\s+\$?\s*([\-\$\d,\.]+)", re.IGNORECASE)
_RE_TOTAL_REVENUE  = re.compile(r"Total\s+Revenue\s+\$?\s*([\-\$\d,\.]+)", re.IGNORECASE)
_RE_TIPS           = re.compile(r"(?<!Avg\s)Tips\s+\$?\s*([\-\$\d,\.]+)", re.IGNORECASE)
_RE_SALES_TAX      = re.compile(r"Sales\s+Tax\s+\$?\s*([\-\$\d,\.]+)", re.IGNORECASE)

# Guest count lines: "Serviced Guests    264   93.95%" — we only need the
# integer. Percent and the "TOTAL Guests" figure are captured separately.
_RE_SERVICED_GUESTS    = re.compile(r"Serviced\s+Guests\s+(\d[\d,]*)", re.IGNORECASE)
_RE_RETAIL_ONLY_GUESTS = re.compile(r"Retail\s+Only\s+Guests\s+(\d[\d,]*)", re.IGNORECASE)
_RE_TOTAL_GUESTS_PDF   = re.compile(r"TOTAL\s+Guests\s+(\d[\d,]*)", re.IGNORECASE)

# Rebook / Prebook % — the word Rebook/Prebook followed by a percent.
_RE_REBOOK_PCT  = re.compile(r"Rebook\s*%\s+(-?\d+(?:\.\d+)?)\s*%", re.IGNORECASE)
_RE_PREBOOK_PCT = re.compile(r"Prebook\s*%\s+(-?\d+(?:\.\d+)?)\s*%", re.IGNORECASE)

# PPH / PPG / Avg Ticket — these are PDF-reported values. Karissa does NOT
# recompute PPH (it depends on productive hours, which Karissa takes from
# the PDF), but PPG and Avg Ticket are recomputed from first principles.
_RE_PPH_PDF        = re.compile(r"(?<!R)PPH\s+\$?\s*([\-\$\d,\.]+)", re.IGNORECASE)
_RE_PPG_PDF        = re.compile(r"PPG\s+\$?\s*([\-\$\d,\.]+)", re.IGNORECASE)
_RE_AVG_TICKET_PDF = re.compile(r"TOTAL\s+Avg\s+Ticket\s+\$?\s*([\-\$\d,\.]+)", re.IGNORECASE)

# Walk-ins / online appointments — audit fields, not Karissa-canonical.
_RE_WALKIN           = re.compile(r"Walkin\s+(\d[\d,]*)", re.IGNORECASE)
_RE_ONLINE_APPTS     = re.compile(r"Online\s+Appointments\s+(\d[\d,]*)", re.IGNORECASE)
_RE_NEW_REQUEST      = re.compile(r"(?<!Male\s)New\s+Request\s+(\d[\d,]*)", re.IGNORECASE)
_RE_RETURNING_REQ    = re.compile(r"(?<!Male\s)Returning\s+Request\s+(\d[\d,]*)", re.IGNORECASE)

# Production hours / Non-production hours / Total hours.
# These values wrap across two lines in the right-most Statistics column:
#     "Production Hours              287h"
#     "                               18m"
# We use a two-step parser: first a "same-line" match, then a fallback that
# stitches the next line when we see just "XXXh" with no minutes.
_RE_PROD_HOURS_SAMELINE      = re.compile(r"Production\s+Hours\s+(\d+h\s*\d+m|\d+(?:\.\d+)?h?)", re.IGNORECASE)
_RE_NONPROD_HOURS_SAMELINE   = re.compile(r"Non-?Production\s+Hours\s+(\d+h\s*\d+m|\d+(?:\.\d+)?h?)", re.IGNORECASE)
_RE_TOTAL_HOURS_SAMELINE     = re.compile(r"Total\s+Hours\s+(\d+h\s*\d+m|\d+(?:\.\d+)?h?)", re.IGNORECASE)


# ---------------------------------------------------------------------------
# Regex: Service Categories table
# ---------------------------------------------------------------------------
# Rows we care about, matched case-insensitively at the start of a line.
# Each row shape (normalised):
#     <Label>   <Qty>   <% Qty>   <Avg Time>   <Sales>   <% Sales>   <Avg Ticket>
# e.g. "Haircut   232   52.37%   26.05   $7,076.40   44.05%   $32.91"
#
# The regex captures the integer qty (first int) and the dollar sales (the
# 4th numeric field — anchored as the first $-prefixed value). % columns
# are intentionally ignored; Karissa uses counts, not the PDF % values.

_SERVICE_CATEGORY_ROW = re.compile(
    # Label
    r"^\s*(?P<label>Haircut|Color|Wax|Waxing|Treatment|Style|Other|Perm|Extensions|None)\s+"
    # Qty
    r"(?P<qty>-?\d[\d,]*)\s+"
    # % Qty
    r"-?\d+(?:\.\d+)?\s*%\s+"
    # Avg Time (min)
    r"-?\d+(?:\.\d+)?\s+"
    # Sales ($-prefixed, may be negative like "$-80.00")
    r"\$?\s*(?P<sales>-?\$?\s*-?[\d,]+(?:\.\d+)?)\s+"
    # rest of line — don't need % sales or avg ticket
    r".*$",
    re.IGNORECASE | re.MULTILINE,
)


# ---------------------------------------------------------------------------
# Regex: Employee Summary table
# ---------------------------------------------------------------------------
# An employee DATA line matches this 13-field shape (name can have spaces):
#   <Name>  $<net_service>  $<net_retail>  <hours>  <hours>
#   $<pph>  $<retail_per_hour>  <guests>  <requests>  <req_pct>%
#   $<ppg>  <avg_service_time>  $<avg_ticket>
# Negative amounts are unusual here but the PDF may produce $0.00 widely.
# We keep the regex strict on the trailing numeric tail (which is constant
# shape) and liberal on the name head (which can be 1-4 tokens).

_EMPLOYEE_DATA_ROW = re.compile(
    # name (everything up to the first $-amount)
    r"^\s*(?P<name>[A-Za-z][A-Za-z\.\'\-_ ]+?)\s+"
    # Net Service $
    r"\$\s*(?P<net_service>-?[\d,]+(?:\.\d+)?)\s+"
    # Net Retail $
    r"\$\s*(?P<net_retail>-?[\d,]+(?:\.\d+)?)\s+"
    # Total Hours Worked (Xh Ym or decimal)
    r"(?P<total_hours>\d+h\s*\d+m|\d+(?:\.\d+)?)\s+"
    # Production Hours Worked (Xh Ym or decimal)
    r"(?P<prod_hours>\d+h\s*\d+m|\d+(?:\.\d+)?)\s+"
    # PPH $
    r"\$\s*(?P<pph>-?[\d,]+(?:\.\d+)?)\s+"
    # Retail/Hour $
    r"\$\s*(?P<retail_per_hour>-?[\d,]+(?:\.\d+)?)\s+"
    # Guests (int)
    r"(?P<guests>-?\d[\d,]*)\s+"
    # # of Requests (int)
    r"(?P<requests>-?\d[\d,]*)\s+"
    # Req % ("100.00%")
    r"(?P<req_pct>-?\d+(?:\.\d+)?)\s*%\s+"
    # PPG $
    r"\$\s*(?P<ppg>-?[\d,]+(?:\.\d+)?)\s+"
    # Avg Service Time (decimal, minutes)
    r"(?P<avg_time>-?\d+(?:\.\d+)?)\s+"
    # Avg Ticket $
    r"\$\s*(?P<avg_ticket>-?[\d,]+(?:\.\d+)?)\s*$",
    re.MULTILINE,
)

# Sentinel: the employee table ends with a TOTALS row. We find it to know
# where to stop, and also to capture location-level totals as a sanity check.
_EMPLOYEE_TOTALS_ROW = re.compile(
    r"^\s*TOTALS\s+"
    r"\$\s*(?P<net_service>-?[\d,]+(?:\.\d+)?)\s+"
    r"\$\s*(?P<net_retail>-?[\d,]+(?:\.\d+)?)\s+"
    r"(?P<total_hours>\d+h\s*\d+m|\d+(?:\.\d+)?)\s+"
    r"(?P<prod_hours>\d+h\s*\d+m|\d+(?:\.\d+)?)\s+"
    r"\$\s*(?P<pph>-?[\d,]+(?:\.\d+)?)\s+"
    r"\$\s*(?P<retail_per_hour>-?[\d,]+(?:\.\d+)?)\s+"
    r"(?P<guests>-?\d[\d,]*)\s+"
    r"(?P<requests>-?\d[\d,]*)\s+"
    r"(?P<req_pct>-?\d+(?:\.\d+)?)\s*%\s+"
    r"\$\s*(?P<ppg>-?[\d,]+(?:\.\d+)?)\s+"
    r"(?P<avg_time>-?\d+(?:\.\d+)?)\s+"
    r"\$\s*(?P<avg_ticket>-?[\d,]+(?:\.\d+)?)\s*$",
    re.MULTILINE | re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Flag constants — used in result["flags"]
# ---------------------------------------------------------------------------
FLAG_PARTIAL_WEEK   = "PARTIAL_WEEK"
FLAG_NO_LOCATION    = "LOCATION_NOT_IDENTIFIED"
FLAG_NO_PERIOD      = "PERIOD_NOT_IDENTIFIED"
FLAG_NO_GUEST_COUNT = "GUEST_COUNT_NOT_IDENTIFIED"
FLAG_NO_SERVICE_NET = "SERVICE_NET_NOT_IDENTIFIED"
FLAG_NO_PRODUCT_NET = "PRODUCT_NET_NOT_IDENTIFIED"
FLAG_GUEST_COUNT_MISMATCH = "GUEST_COUNT_MISMATCH"
FLAG_TOTAL_SALES_MISMATCH = "TOTAL_SALES_MISMATCH"


class SalonUltimateV2Parser:
    """
    Parse a Salon Ultimate FS Salon Dashboard PDF's extracted text into a
    normalised result dict.

    Design notes
    ────────────
    • Input is text (already extracted by PyMuPDF or pdftotext). Keeping the
      parser text-only makes unit testing straightforward — just feed in a
      saved .txt fixture.
    • Every parse step is non-raising: missing fields become `None` (or
      0.0 for numeric fallbacks) and a flag is appended to `result["flags"]`.
      The caller decides whether to accept or reject the partial result.
    • Karissa-canonical KPIs are recomputed from raw fields — never copied
      from the PDF's pre-computed stat rows — because Karissa's definitions
      differ subtly from the PDF (e.g. color_pct uses revenue share, PPG
      uses her guest_count denominator which already matches SU's).
    """

    def __init__(self, text: str) -> None:
        self.text = text or ""
        self.flags: List[str] = []

    # ---- public -----------------------------------------------------------

    def parse(self) -> Dict[str, Any]:
        """
        Parse the text and return a dict with the following shape:

            {
                "platform": "salon_ultimate",
                "location": "Apple Valley",       # canonical
                "location_raw": "Apple Valley Pilot Knob",
                "week_start": "2026-04-01",
                "week_end":   "2026-04-05",
                "flags": [...],                   # e.g. ["PARTIAL_WEEK"]
                "unclosed_days": ["2026-04-04"],
                "raw": { ... PDF-reported fields ... },
                "service_categories": { "haircut": {...}, ... },
                "karissa": { ... Karissa-canonical KPIs ... },
                "employees": [ {...}, ... ],
            }
        """
        self.flags = []  # reset on each parse()

        location, location_raw = self._extract_location()
        week_start, week_end = self._extract_period()
        unclosed = detect_unclosed_days(self.text)
        if unclosed:
            self.flags.append(FLAG_PARTIAL_WEEK)

        raw = self._extract_raw_fields()
        service_categories = self._extract_service_categories()
        employees = self._extract_employees()

        karissa = self._compute_karissa_kpis(raw, service_categories)

        result: Dict[str, Any] = {
            "platform": "salon_ultimate",
            "location": location,
            "location_raw": location_raw,
            "week_start": week_start,
            "week_end": week_end,
            "unclosed_days": unclosed,
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
        Returns (canonical_name, raw_captured_text).

        Uses `extract_salon_ultimate_location` (which knows about the
        "FS - <name>" header format and descriptor stripping). Also returns
        the raw fragment for audit.
        """
        canonical = extract_salon_ultimate_location(self.text)
        # Capture raw fragment for auditing (descriptor intact).
        m = re.search(
            r"Store\s+name\s*:\s*FS\s*-\s*([^\n]+)",
            self.text,
            re.IGNORECASE,
        )
        raw = None
        if m:
            # Trim the trailing "Date generated" chunk that rides the same line.
            raw_full = m.group(1).strip()
            raw = re.split(r"\s{2,}|\s+Date\s+generated", raw_full, maxsplit=1, flags=re.IGNORECASE)[0].strip()

        if not canonical:
            self.flags.append(FLAG_NO_LOCATION)
            logger.warning("SalonUltimateV2Parser: could not identify location. Raw=%r", raw)

        return canonical, raw

    # ---- period -----------------------------------------------------------

    def _extract_period(self) -> Tuple[Optional[str], Optional[str]]:
        """Returns (start_iso, end_iso) or (None, None) if not found."""
        try:
            start, end = parse_date_range(self.text)
            return start, end
        except ValueError:
            self.flags.append(FLAG_NO_PERIOD)
            logger.warning("SalonUltimateV2Parser: could not parse period")
            return None, None

    # ---- raw fields -------------------------------------------------------

    def _extract_raw_fields(self) -> Dict[str, Any]:
        """
        Extract the Sales / Guest Count / Statistics block into a flat dict.
        Every value is a float, int, or None — never a raw string.

        If a field cannot be extracted, we return None (not 0.0) so the
        downstream KPI computation can distinguish "zero value" from
        "missing field". The corresponding flag is pushed onto self.flags.
        """
        out: Dict[str, Any] = {
            "total_service": self._find_money(_RE_TOTAL_SERVICE),
            "total_retail":  self._find_money(_RE_TOTAL_RETAIL),
            "total_revenue": self._find_money(_RE_TOTAL_REVENUE),
            "tips":          self._find_money(_RE_TIPS),
            "sales_tax":     self._find_money(_RE_SALES_TAX),

            "serviced_guests":    self._find_int(_RE_SERVICED_GUESTS),
            "retail_only_guests": self._find_int(_RE_RETAIL_ONLY_GUESTS),
            "total_guests_pdf":   self._find_int(_RE_TOTAL_GUESTS_PDF),

            "rebook_pct":  self._find_percent(_RE_REBOOK_PCT),
            "prebook_pct": self._find_percent(_RE_PREBOOK_PCT),

            "pph_pdf":        self._find_money(_RE_PPH_PDF),
            "ppg_pdf":        self._find_money(_RE_PPG_PDF),
            "avg_ticket_pdf": self._find_money(_RE_AVG_TICKET_PDF),

            "walk_ins":                    self._find_int(_RE_WALKIN),
            "online_appointments_booked":  self._find_int(_RE_ONLINE_APPTS),
            "new_request":                 self._find_int(_RE_NEW_REQUEST),
            "returning_request":           self._find_int(_RE_RETURNING_REQ),

            # (?<![\-A-Za-z]) prevents "Production Hours" from matching inside
            # "Non-Production Hours" (which would otherwise hit first — 'tis
            # how we shipped 30.0167 for Apple Valley before the fix).
            "production_hours":      self._find_hours_with_wrap(r"(?<![\-A-Za-z])Production\s+Hours"),
            "non_production_hours":  self._find_hours_with_wrap(r"Non-?Production\s+Hours"),
            # Same problem lurks for "Total Hours" vs "Total Hours Worked"
            # in the Employee Summary header — but "Total Hours Worked" isn't
            # followed by digits, so the regex harmlessly falls through. Still
            # guard against "<word>Total Hours" to be safe.
            "total_hours":           self._find_hours_with_wrap(r"(?<![A-Za-z])Total\s+Hours"),
        }

        # Flags for missing critical fields.
        if out["serviced_guests"] is None and out["retail_only_guests"] is None:
            self.flags.append(FLAG_NO_GUEST_COUNT)
        if out["total_service"] is None:
            self.flags.append(FLAG_NO_SERVICE_NET)
        if out["total_retail"] is None:
            self.flags.append(FLAG_NO_PRODUCT_NET)

        return out

    def _find_money(self, pattern: re.Pattern) -> Optional[float]:
        m = pattern.search(self.text)
        if not m:
            return None
        return safe_parse_money(m.group(1), default=None) if m.group(1) else None

    def _find_int(self, pattern: re.Pattern) -> Optional[int]:
        m = pattern.search(self.text)
        if not m:
            return None
        try:
            return safe_parse_int(m.group(1))
        except Exception:
            return None

    def _find_percent(self, pattern: re.Pattern) -> Optional[float]:
        """Returns as fraction (0.1288 for 12.88%)."""
        m = pattern.search(self.text)
        if not m:
            return None
        return safe_parse_percent(m.group(1), default=None, as_fraction=True)

    def _find_hours_with_wrap(self, label_regex: str) -> Optional[float]:
        """
        Find an hours value after a label. Handles the SU Statistics-column
        layout where "287h" lands on one line and "18m" lands several dozen
        chars later (same or next line after a multi-column wrap like
        "Production Hours              287h\\nCards ... Walk outs 2 ... 18m").

        Strategy:
            1. Try a tight same-line match: "Xh Ym" or "N.M" decimal.
            2. Try the split case: "<label> <int>h" then scan ahead up to
               400 chars for "<int>m". This is needed because the PDF
               extractor interleaves OTHER column text between 287h and 18m.
            3. Return None if no hour component is found.

        Returns decimal hours or None.
        """
        # Case 1: compact form on the same line — either "Xh Ym" or a clear
        # decimal. We deliberately do NOT accept a bare integer here, because
        # bare integers are almost always just the hour part of a wrapped
        # value (e.g. "287h" → captured as "287") and case 2 owns that.
        m = re.search(
            rf"{label_regex}\s+(\d+h\s*\d+m|\d+\.\d+)",
            self.text,
            re.IGNORECASE,
        )
        if m:
            val = m.group(1).strip()
            hrs = safe_parse_hours(val, default=None)
            if hrs is not None:
                return hrs

        # Case 2: split across multi-column wrap.
        # "<label>              287h" then "...18m" up to 400 chars later.
        m = re.search(
            rf"{label_regex}\s+(\d+)\s*h\b",
            self.text,
            re.IGNORECASE,
        )
        if not m:
            return None
        hours_part = int(m.group(1))

        # Look ahead up to 400 chars for the "NNm" fragment. SU pages interleave
        # other columns (Cards / Walk outs / etc.) between the "287h" and
        # "18m" tokens, so the window has to be wide. We do NOT match any
        # "NNm" — we match the FIRST "\\bNNm\\b" that looks like minutes.
        tail = self.text[m.end(): m.end() + 400]
        m2 = re.search(r"\b(\d{1,2})\s*m\b", tail)
        minutes_part = int(m2.group(1)) if m2 else 0
        if minutes_part >= 60:
            logger.warning(
                "SalonUltimateV2Parser: wrap-parsed minutes >= 60 (%d) after %r — "
                "rejecting the minutes and returning integer hours.",
                minutes_part, label_regex,
            )
            minutes_part = 0
        return round(hours_part + minutes_part / 60.0, 4)

    # ---- service categories ----------------------------------------------

    def _extract_service_categories(self) -> Dict[str, Dict[str, float]]:
        """
        Parse the Service Categories table into:
            {
              "haircut":  {"qty": 232, "sales": 7076.40},
              "color":    {"qty": 41,  "sales": 5883.25},
              "wax":      {"qty": 38,  "sales": 703.00},
              ...
            }

        Rows we always try: Haircut, Color, Wax, Waxing, Treatment, Style,
        Other, Perm, Extensions, None. Absent rows are simply missing from
        the dict (callers that expect defaults should use .get(..., ...)).
        """
        out: Dict[str, Dict[str, float]] = {}
        for m in _SERVICE_CATEGORY_ROW.finditer(self.text):
            label = m.group("label").strip().lower()
            # Normalise a few aliases before storing
            key = label.replace(" ", "_")
            qty = safe_parse_int(m.group("qty"))
            sales_raw = m.group("sales").replace("$", "").replace(" ", "")
            sales = safe_parse_money(sales_raw)
            # If we see "Wax" and "Waxing" both (happens on Zenoti — but
            # defensive here), sum into "wax". SU hasn't been observed to do
            # this but we handle it cleanly.
            if key == "waxing":
                existing = out.get("wax", {"qty": 0, "sales": 0.0})
                out["wax"] = {
                    "qty":   existing["qty"] + qty,
                    "sales": round(existing["sales"] + sales, 2),
                }
                continue
            if key == "wax" and "wax" in out:
                # Shouldn't happen on SU, but merge just in case.
                existing = out["wax"]
                out["wax"] = {
                    "qty":   existing["qty"] + qty,
                    "sales": round(existing["sales"] + sales, 2),
                }
                continue
            out[key] = {"qty": qty, "sales": round(sales, 2)}
        return out

    # ---- employees --------------------------------------------------------

    def _extract_employees(self) -> List[Dict[str, Any]]:
        """
        Parse the Employee Summary table.

        Multi-line names (Apple Valley: "Magdalene\\nYork", Farmington:
        "Ashley Master\\nStylist Spaulding") are joined: we look for a
        trailing line with no numeric tail and treat it as a continuation.

        Returns list in the order they appear. TOTALS row is excluded.
        """
        # First, find the section boundaries to keep our continuation logic
        # from picking up stray lines from other tables.
        section = self._locate_employee_section()
        if section is None:
            return []

        lines = section.splitlines()
        employees: List[Dict[str, Any]] = []

        i = 0
        while i < len(lines):
            line = lines[i]

            # Stop at TOTALS — that ends the employee block.
            if re.match(r"^\s*TOTALS\b", line, re.IGNORECASE):
                break

            m = _EMPLOYEE_DATA_ROW.match(line)
            if m:
                name = m.group("name").strip()

                # Check the NEXT line for a continuation: any short line that
                # is not a new employee data row, not TOTALS, not blank, and
                # contains only word-like tokens.
                if i + 1 < len(lines):
                    peek = lines[i + 1]
                    if (
                        peek.strip()
                        and not _EMPLOYEE_DATA_ROW.match(peek)
                        and not re.match(r"^\s*TOTALS\b", peek, re.IGNORECASE)
                        and re.match(r"^\s*[A-Za-z][A-Za-z\.\'\-_ ]*\s*$", peek)
                    ):
                        continuation = peek.strip()
                        name = f"{name} {continuation}".strip()
                        i += 1  # consume continuation line

                # Normalise the "House _" placeholder — this is the retail-
                # only phantom bucket that appears on every SU export.
                if name.rstrip(" _").lower() == "house":
                    name = "House"

                employees.append({
                    "name": name,
                    "net_service": safe_parse_money(m.group("net_service")),
                    "net_retail":  safe_parse_money(m.group("net_retail")),
                    "total_hours": safe_parse_hours(m.group("total_hours")),
                    "production_hours": safe_parse_hours(m.group("prod_hours")),
                    "pph":              safe_parse_money(m.group("pph")),
                    "retail_per_hour":  safe_parse_money(m.group("retail_per_hour")),
                    "guests":       safe_parse_int(m.group("guests")),
                    "requests":     safe_parse_int(m.group("requests")),
                    "request_pct":  safe_parse_percent(m.group("req_pct"), default=0.0, as_fraction=True),
                    "ppg":              safe_parse_money(m.group("ppg")),
                    "avg_service_time_min": float(m.group("avg_time")),
                    "avg_ticket":   safe_parse_money(m.group("avg_ticket")),
                    "is_phantom_house": name.lower() == "house",
                })
            i += 1

        return employees

    def _locate_employee_section(self) -> Optional[str]:
        """
        Return the slice of text containing the Employee Summary table.

        Why this is trickier than it looks
        ──────────────────────────────────
        The string "Employee Summary" is VISUALLY two consecutive tokens on
        the PDF, but `pdftotext -layout` / PyMuPDF reconstruct the page by
        columns, so the real output looks like:

            Employee               Net        Net      Total Hours ...
            Summary           Service $   Retail $        Worked   ...

        The word "Summary" lands on the NEXT text line because it belongs to
        the second row of the header block. `Employee\\s+Summary` therefore
        never matches — the gap between them is "               Net ... Avg"
        which isn't whitespace-only.

        Fallback hierarchy (first match wins):
            1. Exact "Employee Summary" — if a cleaner extractor ever puts
               them on one line, we catch it here.
            2. "Summary\\s+Service\\s+\\$" — the unique 2nd-row marker.
            3. Line-start "Employee" with "Net" within 30 chars — the 1st-row
               marker; used as a last resort.

        Whichever matches, we return text from that point to EOF. The caller
        (`_extract_employees`) scans line-by-line and stops at TOTALS, so a
        little extra header slack is harmless.
        """
        # Tier 1: exact phrase (future-proof).
        m = re.search(r"Employee\s+Summary", self.text, re.IGNORECASE)
        if m:
            return self.text[m.start():]

        # Tier 2: the unique second-row marker of the employee block.
        # "Summary           Service $   Retail $" — match "Summary" followed
        # by "Service" and a dollar sign within ~50 chars.
        m = re.search(r"Summary\s+Service\s*\$", self.text, re.IGNORECASE)
        if m:
            # Back up to the preceding "Employee" line if possible so any
            # regex-sanity peeking at the header still sees it.
            rewind = self.text.rfind("Employee", 0, m.start())
            start = rewind if rewind >= 0 else m.start()
            return self.text[start:]

        # Tier 3: "Employee" line followed shortly by "Net".
        for em in re.finditer(r"(?m)^\s*Employee\b", self.text):
            window = self.text[em.start(): em.start() + 60]
            if re.search(r"\bNet\b", window):
                return self.text[em.start():]

        return None

    # ---- Karissa-canonical KPI computation --------------------------------

    def _compute_karissa_kpis(
        self,
        raw: Dict[str, Any],
        categories: Dict[str, Dict[str, float]],
    ) -> Dict[str, Any]:
        """
        Compute every KPI Karissa's contract requires from first principles.

        Contract (see CLAUDE.md, KPI formulas):
            guest_count      = serviced + retail_only            (SU definition)
            service_net      = total_service   (from Sales block)
            product_net      = total_retail
            total_sales      = service_net + product_net          (pre-tax)
            avg_ticket       = total_sales / guest_count
            ppg              = product_net / guest_count
            pph              = pass-through from PDF (NOT recomputed — depends
                               on productive hours which we take verbatim)
            wax_count        = categories["wax"]["qty"]   (+ "waxing" if present)
            wax_pct          = wax_count / guest_count
            color_sales      = categories["color"]["sales"]
            color_pct        = color_sales / total_sales          (REVENUE SHARE)
            treatment_count  = categories["treatment"]["qty"]
            treatment_pct    = treatment_count / guest_count
            haircut_count    = categories["haircut"]["qty"]

        Missing inputs fold to None (never crash). Consumers should check
        `flags` before trusting derived values.
        """
        k: Dict[str, Any] = {}

        serviced = raw.get("serviced_guests")
        retail_only = raw.get("retail_only_guests") or 0
        if serviced is None:
            guest_count = None
        else:
            guest_count = serviced + retail_only
        k["guest_count"] = guest_count

        # Cross-check against the PDF's TOTAL Guests line. If they disagree,
        # we flag (but keep computing with the Karissa-canonical value).
        total_guests_pdf = raw.get("total_guests_pdf")
        if (
            guest_count is not None
            and total_guests_pdf is not None
            and guest_count != total_guests_pdf
        ):
            logger.warning(
                "SalonUltimateV2Parser: guest_count (%s) != TOTAL Guests PDF (%s)",
                guest_count, total_guests_pdf,
            )
            self.flags.append(FLAG_GUEST_COUNT_MISMATCH)

        service_net = raw.get("total_service")
        product_net = raw.get("total_retail")
        k["service_net"] = service_net
        k["product_net"] = product_net

        if service_net is not None and product_net is not None:
            total_sales = round(service_net + product_net, 2)
        else:
            total_sales = None
        k["total_sales"] = total_sales

        # Cross-check against Total Revenue on the PDF.
        total_revenue_pdf = raw.get("total_revenue")
        if (
            total_sales is not None
            and total_revenue_pdf is not None
            and abs(total_sales - total_revenue_pdf) > 0.01
        ):
            logger.warning(
                "SalonUltimateV2Parser: total_sales (%.2f) != Total Revenue PDF (%.2f)",
                total_sales, total_revenue_pdf,
            )
            self.flags.append(FLAG_TOTAL_SALES_MISMATCH)

        # Per-guest KPIs — use Karissa's guest_count as denominator
        k["avg_ticket"] = (
            round(total_sales / guest_count, 2)
            if (total_sales is not None and guest_count)
            else None
        )
        k["ppg"] = (
            round(product_net / guest_count, 2)
            if (product_net is not None and guest_count)
            else None
        )

        # PPH — pass-through (productive hours definition stays with the POS).
        k["pph"] = raw.get("pph_pdf")

        # Service mix
        wax = categories.get("wax", {"qty": 0, "sales": 0.0})
        waxing = categories.get("waxing", {"qty": 0, "sales": 0.0})
        wax_count = wax.get("qty", 0) + waxing.get("qty", 0)
        k["wax_count"] = wax_count
        k["wax_pct"] = (
            round(wax_count / guest_count, 4) if guest_count else None
        )

        color = categories.get("color", {"qty": 0, "sales": 0.0})
        k["color_sales"] = color.get("sales", 0.0)
        k["color_pct"] = (
            round(k["color_sales"] / total_sales, 4)
            if (total_sales and total_sales > 0)
            else None
        )

        treatment = categories.get("treatment", {"qty": 0, "sales": 0.0})
        k["treatment_count"] = treatment.get("qty", 0)
        k["treatment_pct"] = (
            round(k["treatment_count"] / guest_count, 4)
            if guest_count
            else None
        )

        haircut = categories.get("haircut", {"qty": 0, "sales": 0.0})
        k["haircut_count"] = haircut.get("qty", 0)

        k["production_hours"] = raw.get("production_hours")
        k["rebook_pct"] = raw.get("rebook_pct")
        k["prebook_pct"] = raw.get("prebook_pct")

        return k


# ---------------------------------------------------------------------------
# File-level convenience entry point
# ---------------------------------------------------------------------------

def parse_text(text: str) -> Dict[str, Any]:
    """Convenience: parse already-extracted text. Never raises."""
    try:
        return SalonUltimateV2Parser(text).parse()
    except Exception as exc:
        logger.exception("SalonUltimateV2 parse_text failed: %s", exc)
        return {
            "platform": "salon_ultimate",
            "error": f"parse_text failed: {exc}",
            "flags": ["PARSE_CRASH"],
        }


def parse_file(pdf_path: str) -> Dict[str, Any]:
    """
    Extract text from a Salon Ultimate PDF and parse it.

    Uses PyMuPDF (fitz). Never raises — returns a result with a PARSE_CRASH
    flag if extraction fails. This mirrors the fail-safe pattern in
    `pdf_detect.detect_pos_from_file` so the batch processor can continue
    on bad files.
    """
    try:
        import fitz  # PyMuPDF
    except ImportError:
        logger.error("SalonUltimateV2: PyMuPDF not installed")
        return {
            "platform": "salon_ultimate",
            "error": "PyMuPDF (fitz) not installed",
            "flags": ["PARSE_CRASH"],
        }

    try:
        doc = fitz.open(pdf_path)
    except Exception as exc:
        logger.error("SalonUltimateV2: failed to open %s: %s", pdf_path, exc)
        return {
            "platform": "salon_ultimate",
            "error": f"failed to open PDF: {exc}",
            "flags": ["PARSE_CRASH"],
        }

    try:
        pages_text = []
        for page in doc:
            pages_text.append(page.get_text())
        text = "\n".join(pages_text)
    except Exception as exc:
        logger.error("SalonUltimateV2: failed to extract text from %s: %s", pdf_path, exc)
        return {
            "platform": "salon_ultimate",
            "error": f"failed to extract text: {exc}",
            "flags": ["PARSE_CRASH"],
        }
    finally:
        try:
            doc.close()
        except Exception:
            pass

    return parse_text(text)


__all__ = [
    "SalonUltimateV2Parser",
    "parse_text",
    "parse_file",
    # Flag constants
    "FLAG_PARTIAL_WEEK",
    "FLAG_NO_LOCATION",
    "FLAG_NO_PERIOD",
    "FLAG_NO_GUEST_COUNT",
    "FLAG_NO_SERVICE_NET",
    "FLAG_NO_PRODUCT_NET",
    "FLAG_GUEST_COUNT_MISMATCH",
    "FLAG_TOTAL_SALES_MISMATCH",
]
