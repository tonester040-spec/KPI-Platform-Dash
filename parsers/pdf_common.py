"""
parsers/pdf_common.py
─────────────────────
Shared PDF parsing primitives used by both the Zenoti and Salon Ultimate
parsers. Pure functions, no I/O, no PDF library imports — every function
takes a string and returns a parsed value.

Why this file exists
────────────────────
The Zenoti and Salon Ultimate PDFs encode the same concepts (dollars,
counts, hours, dates, percentages) with subtle formatting differences.
Centralising the primitives here means the platform-specific parsers
never reinvent number parsing, and bugs get fixed once.

Real-world oddities handled (all verified against the 2026-04-01 → 04-05
POS exports in /sessions/hopeful-elegant-fermi/pdf_recon/):

  • Dollars                : "$18,177.75", "$2,112.00", "$0.00"
  • Negative dollars (SU)  : "$-80.00" — dash AFTER the $ sign
  • Parenthesised negatives: "($123.45)" — standard accounting notation
  • Comma-thousands        : "$16,065.75", "2,547.20"
  • Percentages            : "88.38%", "-0.50%", "107.65"
  • Integers with commas   : "2,547"
  • Zenoti dates           : "4/1/2026" (no zero-pad)
  • SU dates               : "04/01/2026" (zero-padded)
  • Hours "Xh Ym"          : "287h 18m" → 287.3  (also "0h 0m", "30h 1m")
  • Partial week (SU)      : "This report does not include data from
                              the following unclosed days. 04/04/2026"

Contract
────────
Every `parse_*` function is **total**: it either returns the parsed
value or raises `ValueError` with a clear message. Callers decide
whether to recover or propagate. For "best-effort, never raise"
behaviour, use the matching `safe_parse_*` wrappers.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import List, Optional, Tuple


# ---------------------------------------------------------------------------
# Regex constants — compiled once at import time
# ---------------------------------------------------------------------------

# Money: captures sign + numeric body. Supports:
#   "$1,234.56", "$0.00", "$-80.00", "-$80.00", "($80.00)", "1,234.56"
# Order of alternatives matters — paren form first so we don't consume the
# inner number with the bare-number branch.
_MONEY_PAREN_RE = re.compile(r"^\s*\(\s*\$?\s*([\d,]+(?:\.\d+)?)\s*\)\s*$")
_MONEY_DOLLAR_RE = re.compile(r"^\s*-?\s*\$\s*(-?)\s*([\d,]+(?:\.\d+)?)\s*$")
_MONEY_BARE_RE = re.compile(r"^\s*(-?)\s*([\d,]+(?:\.\d+)?)\s*$")

# Percent: "88.38%", "-0.50%", " 100.00 % ".  Optional trailing %.
_PERCENT_RE = re.compile(r"^\s*(-?\d+(?:\.\d+)?)\s*%?\s*$")

# Integer with optional comma-thousands: "2,547" or "264"
_INT_RE = re.compile(r"^\s*(-?\d{1,3}(?:,\d{3})*|-?\d+)\s*$")

# Hours-minutes: "287h 18m", "11h 21m", "0h 0m", "30h 1m"
_HOURS_RE = re.compile(r"^\s*(\d+)\s*h\s*(\d+)\s*m\s*$", re.IGNORECASE)

# Decimal hours fallback: "287.3", "287"
_DECIMAL_HOURS_RE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*$")

# Dates: captures MM/DD/YYYY with or without zero-pad on the month/day.
_DATE_RE = re.compile(r"^\s*(\d{1,2})/(\d{1,2})/(\d{4})\s*$")

# Date range: handles both "04/01/2026 - 04/05/2026" and "From: 4/1/2026 To: 4/5/2026"
_DATE_RANGE_HYPHEN_RE = re.compile(
    r"(\d{1,2}/\d{1,2}/\d{4})\s*[-–—]\s*(\d{1,2}/\d{1,2}/\d{4})"
)
_DATE_RANGE_FROMTO_RE = re.compile(
    r"From:\s*(\d{1,2}/\d{1,2}/\d{4})\s*To:\s*(\d{1,2}/\d{1,2}/\d{4})",
    re.IGNORECASE,
)

# Unclosed days (Salon Ultimate partial-week flag).
# Real example from Lakeville 4/1-4/5:
#   "This report does not include data from the following unclosed days."
#   "04/04/2026"
# The date may appear on the same line or the next. We search a small
# window (200 chars) after the phrase to catch both cases.
_UNCLOSED_MARKER = "does not include data from the following unclosed days"
_UNCLOSED_DATE_SWEEP_RE = re.compile(r"(\d{1,2}/\d{1,2}/\d{4})")


# ---------------------------------------------------------------------------
# Money parsing
# ---------------------------------------------------------------------------

def parse_money(s: str) -> float:
    """
    Parse a monetary value to a float (dollars, not cents).

    Accepts:
        "$1,234.56", "1234.56", "$0.00", "$-80.00", "-$80.00",
        "($123.45)"  (parens → negative), "1,234"  (no decimal).

    Raises:
        ValueError on unparseable input. Blank strings raise.

    Returns:
        float. Always rounded to 2 decimals to avoid float-drift accumulation.
    """
    if s is None:
        raise ValueError("parse_money: got None")
    raw = str(s).strip()
    if not raw:
        raise ValueError("parse_money: empty string")

    # Parenthesised negative: "($80.00)"
    m = _MONEY_PAREN_RE.match(raw)
    if m:
        body = m.group(1).replace(",", "")
        return round(-float(body), 2)

    # Dollar form: "$X", "$-X", "-$X", "$ X"
    m = _MONEY_DOLLAR_RE.match(raw)
    if m:
        inner_sign = m.group(1)  # "-" or ""
        body = m.group(2).replace(",", "")
        outer_sign = "-" if raw.lstrip().startswith("-") else ""
        signed = f"{outer_sign}{inner_sign}{body}"
        # Collapse "--" into "" (outer + inner both negative is unusual;
        # treat as positive — no real POS export produces this)
        if signed.startswith("--"):
            signed = signed[2:]
        return round(float(signed), 2)

    # Bare number with optional sign
    m = _MONEY_BARE_RE.match(raw)
    if m:
        sign = m.group(1)
        body = m.group(2).replace(",", "")
        return round(float(f"{sign}{body}"), 2)

    raise ValueError(f"parse_money: unrecognised format {raw!r}")


def safe_parse_money(s: str, default: float = 0.0) -> float:
    """Non-raising wrapper. Returns `default` on any parse failure."""
    try:
        return parse_money(s)
    except (ValueError, TypeError):
        return default


# ---------------------------------------------------------------------------
# Integer parsing
# ---------------------------------------------------------------------------

def parse_int(s: str) -> int:
    """
    Parse an integer that may include comma-thousands separators.

    Accepts:
        "264", "2,547", "-5".
    Rejects:
        floats, empty strings, non-numeric junk.
    """
    if s is None:
        raise ValueError("parse_int: got None")
    raw = str(s).strip()
    if not raw:
        raise ValueError("parse_int: empty string")

    m = _INT_RE.match(raw)
    if not m:
        raise ValueError(f"parse_int: not an integer: {raw!r}")

    return int(m.group(1).replace(",", ""))


def safe_parse_int(s: str, default: int = 0) -> int:
    """Non-raising wrapper. Returns `default` on any parse failure."""
    try:
        return parse_int(s)
    except (ValueError, TypeError):
        return default


# ---------------------------------------------------------------------------
# Percent parsing
# ---------------------------------------------------------------------------

def parse_percent(s: str, as_fraction: bool = True) -> float:
    """
    Parse a percentage string.

    Args:
        s            : the percent text, e.g. "88.38%", "12.88", "-0.50%"
        as_fraction  : if True (default), 88.38% → 0.8838.
                       if False,          88.38% → 88.38.

    Notes:
        Trailing "%" is optional — both "88.38" and "88.38%" parse to the
        same number. We do NOT apply a fraction-vs-whole-number heuristic
        on bare numbers (that's a source of subtle bugs). Callers must
        pass raw cell text and specify the convention they want back.
    """
    if s is None:
        raise ValueError("parse_percent: got None")
    raw = str(s).strip()
    if not raw:
        raise ValueError("parse_percent: empty string")

    m = _PERCENT_RE.match(raw)
    if not m:
        raise ValueError(f"parse_percent: not a percentage: {raw!r}")

    n = float(m.group(1))
    return n / 100.0 if as_fraction else n


def safe_parse_percent(s: str, default: float = 0.0, as_fraction: bool = True) -> float:
    """Non-raising wrapper. Returns `default` on any parse failure."""
    try:
        return parse_percent(s, as_fraction=as_fraction)
    except (ValueError, TypeError):
        return default


# ---------------------------------------------------------------------------
# Hours parsing — the "Xh Ym" and decimal variants
# ---------------------------------------------------------------------------

def parse_hours(s: str) -> float:
    """
    Parse a duration string into decimal hours.

    Supported forms:
        "287h 18m"  → 287 + 18/60 = 287.3  (Salon Ultimate)
        "0h 0m"     → 0.0
        "30h 1m"    → 30.0166…
        "287.3"     → 287.3                (already decimal)
        "287"       → 287.0

    Returns:
        float, rounded to 4 decimals (keeps per-minute precision cleanly).

    Raises:
        ValueError on unparseable input.
    """
    if s is None:
        raise ValueError("parse_hours: got None")
    raw = str(s).strip()
    if not raw:
        raise ValueError("parse_hours: empty string")

    m = _HOURS_RE.match(raw)
    if m:
        hours = int(m.group(1))
        minutes = int(m.group(2))
        if minutes >= 60:
            raise ValueError(f"parse_hours: minutes >= 60 in {raw!r}")
        return round(hours + minutes / 60.0, 4)

    m = _DECIMAL_HOURS_RE.match(raw)
    if m:
        return round(float(m.group(1)), 4)

    raise ValueError(f"parse_hours: unrecognised duration: {raw!r}")


def safe_parse_hours(s: str, default: float = 0.0) -> float:
    """Non-raising wrapper. Returns `default` on any parse failure."""
    try:
        return parse_hours(s)
    except (ValueError, TypeError):
        return default


# ---------------------------------------------------------------------------
# Date parsing — single date and ranges
# ---------------------------------------------------------------------------

def parse_mmddyyyy(s: str) -> str:
    """
    Parse a single MM/DD/YYYY or M/D/YYYY date to ISO 'YYYY-MM-DD'.

    Accepts both zero-padded (SU, "04/01/2026") and non-padded (Zenoti,
    "4/1/2026"). Returns the ISO string.

    Raises:
        ValueError on unparseable input or impossible dates (e.g. 02/30).
    """
    if s is None:
        raise ValueError("parse_mmddyyyy: got None")
    raw = str(s).strip()
    if not raw:
        raise ValueError("parse_mmddyyyy: empty string")

    m = _DATE_RE.match(raw)
    if not m:
        raise ValueError(f"parse_mmddyyyy: unrecognised date: {raw!r}")

    try:
        dt = datetime.strptime(f"{m.group(1)}/{m.group(2)}/{m.group(3)}", "%m/%d/%Y")
    except ValueError as exc:
        # Impossible date like "02/30/2026"
        raise ValueError(f"parse_mmddyyyy: invalid date {raw!r}: {exc}") from exc

    return dt.strftime("%Y-%m-%d")


def parse_date_range(text: str) -> Tuple[str, str]:
    """
    Extract a reporting period (start_date, end_date) from a blob of PDF text.

    Handles both formats we see in production:
        Salon Ultimate: "Report period: 04/01/2026 - 04/05/2026"
        Zenoti:         "From: 4/1/2026 To: 4/5/2026"

    Returns:
        (start_iso, end_iso) — two 'YYYY-MM-DD' strings.

    Raises:
        ValueError if no valid range is found. Callers typically wrap this
        to downgrade to a warning.
    """
    if text is None:
        raise ValueError("parse_date_range: got None")

    m = _DATE_RANGE_FROMTO_RE.search(text)
    if not m:
        m = _DATE_RANGE_HYPHEN_RE.search(text)
    if not m:
        raise ValueError("parse_date_range: no recognisable date range found")

    start = parse_mmddyyyy(m.group(1))
    end = parse_mmddyyyy(m.group(2))

    if end < start:
        raise ValueError(
            f"parse_date_range: end ({end}) is before start ({start}) — "
            "rejecting as corrupt."
        )
    return start, end


def safe_parse_date_range(
    text: str,
    default: Tuple[Optional[str], Optional[str]] = (None, None),
) -> Tuple[Optional[str], Optional[str]]:
    """Non-raising wrapper. Returns `default` on any parse failure."""
    try:
        return parse_date_range(text)
    except (ValueError, TypeError):
        return default


# ---------------------------------------------------------------------------
# Partial-week detection (Salon Ultimate "unclosed days" warning)
# ---------------------------------------------------------------------------

def detect_unclosed_days(text: str) -> List[str]:
    """
    Return a list of ISO dates flagged as 'unclosed' in a Salon Ultimate PDF.

    Salon Ultimate PDFs sometimes include a phrase like:

        "This report does not include data from the following unclosed days."
        "04/04/2026"

    This signals the week was captured mid-close — KPIs are partial.
    Tier 2 should tag these rows with WARNING and surface them on the
    dashboard so coaches know the numbers aren't a full week.

    Args:
        text: full PDF text blob.

    Returns:
        List of ISO date strings ('YYYY-MM-DD'). Empty list if the marker
        is absent (the normal case) or no dates are found after it.

    Notes:
        - We search a 400-char window AFTER the marker. Real PDFs have the
          date on the next line (within ~40 chars), but we widen the window
          to be resilient to extractor quirks.
        - Only the first occurrence of the marker is processed; weekly
          PDFs never have more than one.
        - We DE-DUPE while preserving order.
    """
    if not text:
        return []

    idx = text.lower().find(_UNCLOSED_MARKER)
    if idx == -1:
        return []

    window = text[idx : idx + 400]
    hits = _UNCLOSED_DATE_SWEEP_RE.findall(window)

    iso_dates: List[str] = []
    seen: set = set()
    for h in hits:
        try:
            iso = parse_mmddyyyy(h)
        except ValueError:
            continue
        if iso not in seen:
            seen.add(iso)
            iso_dates.append(iso)

    return iso_dates


def has_partial_week_flag(text: str) -> bool:
    """Convenience: True if any unclosed days were detected."""
    return bool(detect_unclosed_days(text))


# ---------------------------------------------------------------------------
# __all__ — explicit public API
# ---------------------------------------------------------------------------

__all__ = [
    # Money
    "parse_money",
    "safe_parse_money",
    # Integer
    "parse_int",
    "safe_parse_int",
    # Percent
    "parse_percent",
    "safe_parse_percent",
    # Hours
    "parse_hours",
    "safe_parse_hours",
    # Dates
    "parse_mmddyyyy",
    "parse_date_range",
    "safe_parse_date_range",
    # Partial week
    "detect_unclosed_days",
    "has_partial_week_flag",
]
