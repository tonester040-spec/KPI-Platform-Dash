"""
parsers/pdf_location_normalizer.py
──────────────────────────────────
Extract a canonical location name from a raw Zenoti or Salon Ultimate PDF.

Why this file exists
────────────────────
The existing `config.locations.normalize_location()` handles aliases but
can't cope with the raw strings that actually appear in the POS exports.
Real headers seen on 2026-04-21 across all 12 files:

Zenoti (format: "CLUSTER-ID-Location")
    888-10278-Andover            → Andover
    888-9816-Blaine              → Blaine
    888-7663-Crystal             → Crystal
    887-10199-Elk River          → Elk River        ← note 887 not 888
    888-11812-Forest Lake        → Forest Lake
    910-7232-Hudson              → Hudson           ← note 910 not 888
    910-6916-New Richmond        → New Richmond     ← note 910 not 888
    888-11091-Prior Lake         → Prior Lake
    888-40098-FS Roseville, MN   → Roseville        ← "FS " prefix + state suffix

Salon Ultimate (format: "FS - Location [Descriptor]")
    FS - Apple Valley Pilot Knob → Apple Valley     ← strip "Pilot Knob"
    FS - Farmington              → Farmington
    FS - Lakeville Timbercrest   → Lakeville        ← strip "Timbercrest"

The earlier `zenoti_pdf.py` regex `888-\d+-(...)` silently ignored Elk
River (887-), Hudson (910-), and New Richmond (910-). That's a real bug
this module closes.

Public API
──────────
    extract_zenoti_location(text)         -> canonical name, or None
    extract_salon_ultimate_location(text) -> canonical name, or None
    normalize_pdf_location(raw, platform) -> canonical name fallback

All three return the canonical form from `config/locations.py` when
possible. They return `None` when they can't identify a location, so
callers can flag the PDF for manual review instead of shipping a
garbage name to Sheets.
"""

from __future__ import annotations

import logging
import re
from typing import Optional

from config.locations import LOCATION_ALIASES, LOCATION_POS_MAP, normalize_location

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Canonical-name lookup set (lowercased)
# ---------------------------------------------------------------------------
# Built from LOCATION_POS_MAP so this module stays in sync with the
# single source of truth. Each canonical name appears exactly once,
# lowercased, for substring matching.
_CANONICAL_LC = {name.lower(): name for name in LOCATION_POS_MAP.keys()}


# ---------------------------------------------------------------------------
# Zenoti — header pattern
# ---------------------------------------------------------------------------
# Matches any "CLUSTER-ID-<LocationName>" at start of a line (after
# whitespace). The extracted location may have leading prefixes
# ("FS ") or trailing state ("MN") that we strip in _clean_zenoti_location.

_ZENOTI_HEADER_RE = re.compile(
    r"(?m)^\s*\d+\-\d+\-\s*([^\n]+?)\s*$"
)


def extract_zenoti_location(text: str) -> Optional[str]:
    """
    Extract the canonical location name from a Zenoti PDF's header.

    Strategy:
        1. Find the first line matching "\\d+-\\d+-<name>" in the first
           2000 chars (header region).
        2. Clean the captured group: strip known prefixes ("FS "),
           strip trailing state codes (", MN"), collapse whitespace.
        3. Match against canonical alias table.
        4. If no match, log and return None.

    Args:
        text: full PDF text blob. Only the first 2000 chars are searched.

    Returns:
        Canonical location name (e.g. "Andover", "Roseville") or None.
    """
    if not text:
        return None

    header = text[:2000]
    best_candidate: Optional[str] = None

    for m in _ZENOTI_HEADER_RE.finditer(header):
        raw = m.group(1).strip()
        cleaned = _clean_zenoti_location(raw)
        if not cleaned:
            continue

        # Try direct alias / canonical match first
        canonical = normalize_location(cleaned)
        if canonical in LOCATION_POS_MAP:
            return canonical

        # Fallback: substring match against canonical names
        cleaned_lc = cleaned.lower()
        for lc, canonical_name in _CANONICAL_LC.items():
            if lc in cleaned_lc and LOCATION_POS_MAP.get(canonical_name) == "zenoti":
                return canonical_name

        # Remember the first candidate for logging
        if best_candidate is None:
            best_candidate = cleaned

    if best_candidate:
        logger.warning(
            "extract_zenoti_location: could not map raw name %r to a "
            "canonical Zenoti location", best_candidate,
        )
    return None


def _clean_zenoti_location(raw: str) -> str:
    """
    Strip known prefixes/suffixes from a Zenoti location fragment.

    Examples:
        "Andover"                   → "Andover"
        "FS Roseville, MN"          → "Roseville"
        "Elk River"                 → "Elk River"
        "F Sams Roseville, MN"      → "Roseville"   (historical variant)
        "Prior Lake"                → "Prior Lake"
    """
    s = raw.strip()

    # Strip leading "FS " (space) or "F Sams " prefix
    s = re.sub(r"^FS\s+", "", s, flags=re.IGNORECASE)
    s = re.sub(r"^F\s+Sams\s+", "", s, flags=re.IGNORECASE)

    # Strip trailing ", MN" or similar state codes / trailing commas
    s = re.sub(r"\s*,\s*[A-Z]{2}\s*$", "", s)
    s = s.rstrip(",").strip()

    # Collapse internal whitespace
    s = re.sub(r"\s+", " ", s)
    return s


# ---------------------------------------------------------------------------
# Salon Ultimate — "Store name: FS - <Location> [Descriptor]"
# ---------------------------------------------------------------------------

_SU_STORE_NAME_RE = re.compile(
    r"Store\s+name\s*:\s*FS\s*-\s*([^\n]+)",
    re.IGNORECASE,
)

# Known descriptors that can trail the real location name.
# Lowercased for case-insensitive matching.
_SU_DESCRIPTORS = (
    "pilot knob",
    "timbercrest",
    "suite",
    "ste",
    "#",
    "mn",     # state code
)


def extract_salon_ultimate_location(text: str) -> Optional[str]:
    """
    Extract the canonical location name from a Salon Ultimate PDF header.

    Strategy:
        1. Find the first "Store name: FS - <...>" line.
        2. Take the captured text and strip the "Date generated:"
           segment that appears on the same line in real PDFs.
        3. Strip known descriptors ("Pilot Knob", "Timbercrest", etc.).
        4. Match against canonical alias table.

    Args:
        text: full PDF text blob.

    Returns:
        Canonical location name, or None.
    """
    if not text:
        return None

    m = _SU_STORE_NAME_RE.search(text)
    if not m:
        return None

    raw = m.group(1).strip()
    cleaned = _clean_su_location(raw)
    if not cleaned:
        return None

    # Direct alias / canonical match
    canonical = normalize_location(cleaned)
    if canonical in LOCATION_POS_MAP:
        return canonical

    # Substring fallback against known SU canonical names
    cleaned_lc = cleaned.lower()
    for lc, canonical_name in _CANONICAL_LC.items():
        if lc in cleaned_lc and LOCATION_POS_MAP.get(canonical_name) == "salon_ultimate":
            return canonical_name

    logger.warning(
        "extract_salon_ultimate_location: could not map raw name %r to a "
        "canonical SU location", raw,
    )
    return None


def _clean_su_location(raw: str) -> str:
    """
    Strip trailing descriptors and sibling fields from a Salon Ultimate
    location fragment.

    Real production examples (note the "Date generated" suffix that
    rides on the same line after pdftotext extraction):

        "Apple Valley Pilot Knob         Date generated: 04/06/2026"
            → "Apple Valley"
        "Farmington                        Date generated: ..."
            → "Farmington"
        "Lakeville Timbercrest             Date generated: ..."
            → "Lakeville"
    """
    s = raw.strip()

    # Split on "Date generated" / "Date:" / any trailing numeric field
    s = re.split(r"\s{2,}|\s+Date\s+generated", s, maxsplit=1, flags=re.IGNORECASE)[0]
    s = s.strip()

    # Strip descriptor tokens from the end. We iterate until no more hits
    # because descriptors can stack: "Pilot Knob Suite 200" → "Pilot Knob"
    # → "" (after Suite match).
    changed = True
    while changed:
        changed = False
        lower = s.lower()
        for desc in _SU_DESCRIPTORS:
            # Match " <descriptor>" or " <descriptor> <junk>" at the end
            idx = lower.rfind(" " + desc)
            if idx != -1 and idx >= len(lower) - len(desc) - 10:
                s = s[:idx].strip()
                changed = True
                break

    # Strip trailing digits (suite numbers) and punctuation
    s = re.sub(r"[\s\d#,\-]+$", "", s).strip()

    # Collapse internal whitespace
    s = re.sub(r"\s+", " ", s)
    return s


# ---------------------------------------------------------------------------
# Unified entry point (for the orchestrator)
# ---------------------------------------------------------------------------

def extract_location_from_pdf_text(
    text: str,
    platform: str,
) -> Optional[str]:
    """
    Route to the right platform-specific extractor.

    Args:
        text     : full PDF text blob.
        platform : 'zenoti' or 'salon_ultimate'.

    Returns:
        Canonical location name, or None.
    """
    if platform == "zenoti":
        return extract_zenoti_location(text)
    if platform == "salon_ultimate":
        return extract_salon_ultimate_location(text)
    logger.error(
        "extract_location_from_pdf_text: unknown platform %r", platform
    )
    return None


__all__ = [
    "extract_zenoti_location",
    "extract_salon_ultimate_location",
    "extract_location_from_pdf_text",
]
