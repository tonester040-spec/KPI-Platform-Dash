"""
Zenoti Salon Summary PDF Parser
Extracts service category data from Zenoti "Salon Summary" PDFs.

Typical PDF structure:
  Page header : "888-10278-Andover Salon Summary From: 4/1/2026 To: 4/5/2026"
  Section     : "SERVICE DETAILS ITEM QTY (%)"
  Data rows   : Haircut / Color / Wax / Treatment

Data row format (raw text after PyMuPDF extraction):
  "Haircut 81 (73.64) 0.60 2,547.20 (65.71) 58.80"
   ^^^^^^^ ^^ ^       ^^^^  ^^^^^^^^          ─────
   name    qty%      avg   net_sales           ...

Column positions we care about:
  Col 1 : category name
  Col 2 : QTY (count)      ← we want this
  Col 3 : (pct of total)   ← skip
  Col 4 : avg service      ← skip
  Col 5 : NET SALES ($)    ← we want this

Notes:
  - Non-standard Roseville header: "888-40098-F Sams Roseville, MN" — handled via fallback regex
  - Some PDFs show "Men's Haircut" as a separate row — lumped into haircut bucket
  - All $ amounts may include commas  e.g. "2,547.20" — strip before float()

Karissa's service category definitions:
  Haircut   = standard cuts (men's + women's combined at location level)
  Color     = full-color services
  Wax       = all wax services
  Treatment = all treatment services (e.g., Olaplex)
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


class ZenotiPDFParser:
    """Parse a single Zenoti Salon Summary PDF file."""

    # Category names as they appear in the Zenoti PDF.
    # Order matters — we try each against the extracted text.
    CATEGORIES = ["Haircut", "Color", "Wax", "Treatment"]

    # Additional aliases that Zenoti sometimes uses.
    # CRITICAL: "Waxing" is a separate row in some Zenoti PDFs — MUST be summed
    # with "Wax" per Karissa's explicit rule. Both map to canonical "Wax".
    CATEGORY_ALIASES = {
        "Men's Haircut": "Haircut",
        "Mens Haircut":  "Haircut",
        "Men Haircut":   "Haircut",
        "Waxing":        "Wax",     # ← Karissa's rule: sum Wax + Waxing
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
              'pos_system':         'zenoti',
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
            "pos_system": "zenoti",
            "service_categories": {
                "haircut_count":   cats.get("Haircut",   {}).get("qty",       0),
                "haircut_net":     cats.get("Haircut",   {}).get("net_sales",  0.0),
                "color_count":     cats.get("Color",     {}).get("qty",       0),
                "color_net":       cats.get("Color",     {}).get("net_sales",  0.0),
                "wax_count":       cats.get("Wax",       {}).get("qty",       0),
                "wax_net":         cats.get("Wax",       {}).get("net_sales",  0.0),
                "treatment_count": cats.get("Treatment", {}).get("qty",       0),
                "treatment_net":   cats.get("Treatment", {}).get("net_sales",  0.0),
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
        Extract location name from the PDF header.

        Standard  : "888-10278-Andover Salon Summary"  → "Andover"
        Roseville : "888-40098-F Sams Roseville, MN Salon Summary"
                    → fallback "Sams <City>" match → "Roseville"
        """
        # Standard pattern: "888-NNNNN-<LocationName> Salon Summary"
        std = re.search(
            r"888-\d+-([A-Za-z][A-Za-z\s]+?)\s+Salon Summary",
            self._text,
        )
        if std:
            raw = std.group(1).strip()
            # Reject IDs that got captured (e.g. "F Sams ...")
            if not re.search(r"\d", raw):
                return normalize_location(raw)

        # Roseville / non-standard: "Sams <City>, <State>"
        sams = re.search(r"Sams\s+([A-Za-z][A-Za-z\s]+?)(?:,|\s+Salon)", self._text)
        if sams:
            return normalize_location(sams.group(1).strip())

        return "Unknown"

    def extract_period(self) -> Dict[str, Optional[str]]:
        """
        Extract reporting period.

        Expected: "From: 4/1/2026 To: 4/5/2026"
        Also handles zero-padded: "From: 04/01/2026 To: 04/05/2026"
        """
        match = re.search(
            r"From:\s*(\d{1,2}/\d{1,2}/\d{4})\s+To:\s*(\d{1,2}/\d{1,2}/\d{4})",
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
        Parse the SERVICE DETAILS section for Haircut / Color / Wax / Treatment.

        Zenoti row format (space-separated after text extraction):
          <Name> <QTY> (<pct>) <avg_per_hour?> <NET_SALES,> (<net_pct>) <something>

        We target:
          group 1 = QTY   (integer before first parenthesis group)
          group 2 = NET SALES (first dollar-style number after the first paren group)

        We search the full text rather than isolating a section, because
        PyMuPDF text extraction order can vary.
        """
        categories: Dict[str, Dict] = {}

        # Build a combined set of names to try
        all_names: List[str] = list(self.CATEGORIES) + list(self.CATEGORY_ALIASES.keys())

        for name in all_names:
            # Pattern explanation:
            #   <name>                     — literal category name
            #   \s+(\d[\d,]*)              — QTY: one or more digits
            #   \s+\([^)]+\)               — skip the (pct) group
            #   \s+[\d.]+                  — skip avg value
            #   \s+([\d,]+\.\d{2})         — NET SALES (decimal number with optional commas)
            pattern = (
                rf"(?<!\w){re.escape(name)}"
                r"\s+(\d[\d,]*)"           # QTY
                r"\s+\([^)]+\)"            # (pct) — skip
                r"\s+[\d.]+"               # avg — skip
                r"\s+([\d,]+\.\d{2})"      # NET SALES
            )
            m = re.search(pattern, self._text, re.IGNORECASE)
            if m:
                qty      = self._safe_int(m.group(1))
                net_sale = self._safe_float(m.group(2))

                # Resolve alias → canonical
                canonical = self.CATEGORY_ALIASES.get(name, name)

                # Accumulate (Men's Haircut + Haircut → total Haircut)
                if canonical in categories:
                    categories[canonical]["qty"]       += qty
                    categories[canonical]["net_sales"] += net_sale
                else:
                    categories[canonical] = {"qty": qty, "net_sales": round(net_sale, 2)}

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
