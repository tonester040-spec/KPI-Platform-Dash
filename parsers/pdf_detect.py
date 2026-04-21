"""
parsers/pdf_detect.py
─────────────────────
Detect the POS platform (Zenoti vs Salon Ultimate) for a weekly export PDF
**by content signature**, not by filename.

Why content, not filename
─────────────────────────
Elaina's exports come in with inconsistent names:
    "Salon_Summary_week_4-1.pdf"
    "Weekly Dashboard - Prior Lake.pdf"
    "Apple Valley Pilot Knob Week 04-05-26.pdf"

Any filename heuristic silently misroutes one file eventually, which ships
wrong numbers to coaches. Content signatures are the ground truth:

    Zenoti          : header contains "Salon Summary"
    Salon Ultimate  : header contains "FS Salon Dashboard"
                      (and "Store name:" as a secondary signal)

Verified across all 12 real POS exports on 2026-04-21:
    9 Zenoti  — Andover, Blaine, Crystal, Elk River, Forest Lake,
                Hudson, New Richmond, Prior Lake, Roseville
    3 Salon Ultimate — Apple Valley, Farmington, Lakeville

Public API
──────────
    detect_pos_from_text(text)    -> 'zenoti' | 'salon_ultimate' | None
    detect_pos_from_file(path)    -> same, opens the PDF and reads text

Both functions NEVER raise — on any failure they return None and let
the caller decide how to handle the unknown-platform case (log + skip
is the right move in the batch processor).
"""

from __future__ import annotations

import logging
from typing import Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Signatures
# ---------------------------------------------------------------------------
# Lowercase matches — we lowercase the header before searching.

# Primary: appears in the title/header of every Zenoti "Salon Summary" export.
_ZENOTI_SIGNATURES = (
    "salon summary",          # always present in Zenoti header
)

# Primary + secondary: "FS Salon Dashboard" in the header, plus "Store name:"
# one or two lines below. Either alone is strong; both together is unambiguous.
_SU_PRIMARY_SIGNATURES = (
    "fs salon dashboard",     # header — all 3 SU PDFs have this
)
_SU_SECONDARY_SIGNATURES = (
    "store name:",            # in the location header block
    "report period:",         # in the period header block
)

# How far into the document we look before giving up. Real PDFs have these
# tokens in the first ~2 KB of text, but we widen the window to survive
# odd PDF renderings.
_DETECTION_WINDOW_CHARS = 4000


# ---------------------------------------------------------------------------
# Platform constants — export for callers
# ---------------------------------------------------------------------------

ZENOTI = "zenoti"
SALON_ULTIMATE = "salon_ultimate"


# ---------------------------------------------------------------------------
# Core detector
# ---------------------------------------------------------------------------

def detect_pos_from_text(text: str) -> Optional[str]:
    """
    Detect POS platform from a blob of extracted PDF text.

    Args:
        text: PDF text content — full document or header region. Only the
              first 4000 chars are inspected, so long documents are cheap.

    Returns:
        'zenoti'          — Zenoti Salon Summary PDF
        'salon_ultimate'  — Salon Ultimate FS Salon Dashboard PDF
        None              — could not determine, or conflicting signatures

    Logs a WARNING if BOTH platforms' signatures are found in the same
    document (should be impossible — indicates a corrupt or stitched PDF).
    """
    if not text:
        return None

    header = text[:_DETECTION_WINDOW_CHARS].lower()

    zenoti_match = any(sig in header for sig in _ZENOTI_SIGNATURES)
    su_primary_match = any(sig in header for sig in _SU_PRIMARY_SIGNATURES)
    su_secondary_match = any(sig in header for sig in _SU_SECONDARY_SIGNATURES)

    # Salon Ultimate: primary signature is enough; secondary is defence in depth.
    is_salon_ultimate = su_primary_match or su_secondary_match

    if zenoti_match and is_salon_ultimate:
        logger.warning(
            "pdf_detect: document contains BOTH Zenoti and Salon Ultimate "
            "signatures — refusing to guess. Header excerpt: %r",
            header[:200],
        )
        return None

    if zenoti_match:
        return ZENOTI

    if is_salon_ultimate:
        return SALON_ULTIMATE

    return None


def detect_pos_from_file(file_path: str) -> Optional[str]:
    """
    Open a PDF file, extract its text, and detect the POS platform.

    This is the production entry point — `tier2_pdf_batch` uses it to
    route each inbox file to the right parser.

    Args:
        file_path: path to a .pdf file.

    Returns:
        Same values as `detect_pos_from_text`. Returns None (never raises)
        on:
          • missing PyMuPDF import
          • file-open failures
          • zero-page documents
          • any other extraction error

    Why never raise:
        The batch processor runs against potentially dozens of files per
        week. One bad file should not crash the batch — it should be
        logged, skipped, and the rest should process. Callers check the
        return value and handle None themselves.
    """
    try:
        import fitz  # PyMuPDF
    except ImportError:
        logger.error("pdf_detect: PyMuPDF (fitz) not installed; cannot detect.")
        return None

    try:
        doc = fitz.open(file_path)
    except Exception as exc:  # fitz raises several different exceptions
        logger.error("pdf_detect: failed to open %s: %s", file_path, exc)
        return None

    try:
        if doc.page_count == 0:
            logger.warning("pdf_detect: %s has zero pages", file_path)
            return None

        # First page is enough — every known signature is on page 1.
        # If a future export starts placing them later we'll revisit.
        text = doc.load_page(0).get_text()
    except Exception as exc:
        logger.error("pdf_detect: failed to extract text from %s: %s", file_path, exc)
        return None
    finally:
        try:
            doc.close()
        except Exception:
            pass

    return detect_pos_from_text(text)


# ---------------------------------------------------------------------------
# __all__
# ---------------------------------------------------------------------------

__all__ = [
    "ZENOTI",
    "SALON_ULTIMATE",
    "detect_pos_from_text",
    "detect_pos_from_file",
]
