"""
Salon Ultimate Dashboard PDF Parser
Extracts service category data from Salon Ultimate "FS Salon Dashboard" PDFs.

Typical PDF structure:
  "Store name: FS - Apple Valley Pilot Knob"
  "Report period: 04/01/2026 - 04/05/2026"
  ...
  "Service Categories  Qty  %  Qty  Avg Time (min)  Sales  %  Sales  Avg Ticket"
  "Haircut   232  52.37%  26.05  $7,076.40  44.05%  $32.91"
  "Wax        38   8.58%  11.82    $703.00   4.38%  $22.68"
  "Color      41   9.26% 107.65  $5,883.25  36.62% $150.85"
  "Treatment 113  25.51%   7.27  $2,187.10  13.61%  $28.40"

Row format after PyMuPDF text extraction:
  <Name> <QTY> <pct>% <avg_time> $<SALES> <pct>% $<avg_ticket>

We want:
  QTY    — integer immediately after the category name
  SALES  — first "$X,XXX.XX" value on the row

Notes:
  - Store name may contain a descriptor suffix: "Pilot Knob", "MN", etc.
  - PyMuPDF sometimes splits rows across lines — we search the full text blob
  - $ sign may or may not have a space before the number
  - Percentages always end with "%" so they're easy to skip
"""

import re
from datetime import datetime
from typing import Dict, List, Optional

try:
    import fitz  # PyMuPDF
except ImportError as e:
    raise ImportError(
        "PyMuPDF is required for PDF parsing. "
        "Install with: pip install PyMuPDF==1.23.26"
    ) from e

from config.locations import normalize_location


class SalonUltimatePDFParser:
    """Parse a single Salon Ultimate FS Salon Dashboard PDF file."""

    CATEGORIES = ["Haircut", "Wax", "Color", "Treatment"]

    # Extra aliases Salon Ultimate sometimes uses
    CATEGORY_ALIASES = {
        "Men's Haircut": "Haircut",
        "Mens Haircut":  "Haircut",
        "Men Haircut":   "Haircut",
    }

    def __init__(self, file_path: str):
        self.file_path = file_path
        self.doc = fitz.open(file_path)
        self._text = self._extract_text()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def parse(self) -> Dict:
        """
        Parse the full PDF.

        Returns:
            {
              'location':           str,
              'period':             {'start_date': 'YYYY-MM-DD', 'end_date': 'YYYY-MM-DD'},
              'pos_system':         'salon_ultimate',
              'service_categories': {
                  'haircut_count':    int,
                  'haircut_net':      float,
                  'color_count':      int,
                  'color_net':        float,
                  'wax_count':        int,
                  'wax_net':          float,
                  'treatment_count':  int,
                  'treatment_net':    float,
              }
            }
        """
        cats = self._extract_service_categories()

        return {
            "location":   self.extract_location(),
            "period":     self.extract_period(),
            "pos_system": "salon_ultimate",
            "service_categories": {
                "haircut_count":   cats.get("Haircut",   {}).get("qty",   0),
                "haircut_net":     cats.get("Haircut",   {}).get("sales", 0.0),
                "color_count":     cats.get("Color",     {}).get("qty",   0),
                "color_net":       cats.get("Color",     {}).get("sales", 0.0),
                "wax_count":       cats.get("Wax",       {}).get("qty",   0),
                "wax_net":         cats.get("Wax",       {}).get("sales", 0.0),
                "treatment_count": cats.get("Treatment", {}).get("qty",   0),
                "treatment_net":   cats.get("Treatment", {}).get("sales", 0.0),
            },
        }

    # ------------------------------------------------------------------
    # Extraction helpers
    # ------------------------------------------------------------------

    def _extract_text(self) -> str:
        """Concatenate text from all pages."""
        return "\n".join(page.get_text() for page in self.doc)

    def extract_location(self) -> str:
        """
        Extract location from "Store name:" field.

        Examples:
          "Store name: FS - Apple Valley Pilot Knob" → "Apple Valley"
          "Store name: FS - Prior Lake"               → "Prior Lake"

        Strategy: try canonical names first, then fallback to regex extraction.
        """
        # Try known canonical names (most reliable — avoids suffix ambiguity)
        for canonical in ["Apple Valley", "Prior Lake", "Farmington", "Lakeville"]:
            if canonical.lower() in self._text.lower():
                # Confirm it appears near "Store name" or "FS -"
                if re.search(
                    rf"(?:Store name|FS\s*-)[^\n]*{re.escape(canonical)}",
                    self._text,
                    re.IGNORECASE,
                ):
                    return canonical

        # Fallback: extract from "Store name: FS - <text>"
        match = re.search(
            r"Store\s+name\s*:\s*FS\s*-\s*([^\n]+)",
            self._text,
            re.IGNORECASE,
        )
        if match:
            raw = match.group(1).strip()
            # Strip trailing descriptor words ("Pilot Knob", "MN", suite numbers)
            raw = re.split(r"\s+(?:Pilot|MN|Suite|#|\d)", raw, maxsplit=1)[0].strip()
            return normalize_location(raw)

        return "Unknown"

    def extract_period(self) -> Dict[str, Optional[str]]:
        """
        Extract reporting period from "Report period:" field.

        Expected: "Report period: 04/01/2026 - 04/05/2026"
        """
        match = re.search(
            r"Report\s+period\s*:\s*(\d{2}/\d{2}/\d{4})\s*-\s*(\d{2}/\d{2}/\d{4})",
            self._text,
            re.IGNORECASE,
        )
        if match:
            try:
                start = datetime.strptime(match.group(1), "%m/%d/%Y").strftime("%Y-%m-%d")
                end   = datetime.strptime(match.group(2), "%m/%d/%Y").strftime("%Y-%m-%d")
                return {"start_date": start, "end_date": end}
            except ValueError:
                pass

        return {"start_date": None, "end_date": None}

    # ------------------------------------------------------------------
    # Service category extraction
    # ------------------------------------------------------------------

    def _safe_int(self, s: str) -> int:
        try:
            return int(s.replace(",", "").strip())
        except (ValueError, AttributeError):
            return 0

    def _safe_float(self, s: str) -> float:
        try:
            return float(s.replace(",", "").replace("$", "").strip())
        except (ValueError, AttributeError):
            return 0.0

    def _extract_service_categories(self) -> Dict[str, Dict]:
        """
        Parse service category rows from the Service Categories section.

        Salon Ultimate row format:
          <Name> <QTY> <pct>% <avg_time> $<SALES> <pct>% $<avg_ticket>

        We match:
          group 1 = QTY   (integer immediately after name)
          group 2 = SALES (first "$" number on the row)
        """
        categories: Dict[str, Dict] = {}

        all_names: List[str] = list(self.CATEGORIES) + list(self.CATEGORY_ALIASES.keys())

        for name in all_names:
            # Pattern breakdown:
            #   <name>                    — literal (word boundary before)
            #   \s+(\d[\d,]*)             — QTY
            #   \s+[\d.]+%                — % of qty — skip
            #   (?:\s+[\d.]+)?            — optional avg time — skip
            #   \s+\$([\d,]+\.\d{2})      — SALES ($X,XXX.XX)
            pattern = (
                rf"(?<!\w){re.escape(name)}"
                r"\s+(\d[\d,]*)"           # QTY
                r"\s+[\d.]+%"              # pct qty — skip
                r"(?:\s+[\d.]+)?"          # avg time — optional, skip
                r"\s+\$([\d,]+\.\d{2})"    # SALES
            )
            m = re.search(pattern, self._text, re.IGNORECASE)
            if m:
                qty   = self._safe_int(m.group(1))
                sales = self._safe_float(m.group(2))

                canonical = self.CATEGORY_ALIASES.get(name, name)

                if canonical in categories:
                    categories[canonical]["qty"]   += qty
                    categories[canonical]["sales"] += sales
                else:
                    categories[canonical] = {"qty": qty, "sales": round(sales, 2)}

        return categories

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------

    def __del__(self):
        try:
            if hasattr(self, "doc"):
                self.doc.close()
        except Exception:
            pass
