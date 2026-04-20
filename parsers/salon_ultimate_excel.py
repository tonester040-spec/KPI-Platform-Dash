"""
Salon Ultimate Stylist Tracking Excel Parser
Parses .xls files from Salon Ultimate "Stylist Tracking Report".

Schema:
  File extension : .xls  (legacy Excel format — requires LibreOffice conversion)
  Sheet name     : "Worksheet"
  Cell B1        : Store name  e.g. "FS - Apple Valley Pilot Knob"
  Cell B2        : Report period  e.g. "03/01/2026 - 03/31/2026"
  Row 5          : Column headers
  Row 6+         : Stylist data rows
  Last data row  : "Totals:" label in column A

Column mapping (Row 5 headers, 1-based):
  Col A (1)  : Stylist Name
  Col C (3)  : Service Clients
  Col D (4)  : Retail Clients
  Col G (7)  : Service Net Sales
  Col J (10) : Product Net Sales

Known edge cases (from spec):
  - "Salon Ultimate" phantom rows may appear — skip them
  - Some .xls files already have a .xlsx counterpart — skip re-conversion if present
  - LibreOffice must be installed: soffice --headless --convert-to xlsx ...

Karissa's authoritative formulas:
  guest_count = service_clients + retail_clients
  total_sales = service_net + product_net
  ppg_net     = product_net / guest_count   (0 when guest_count == 0)
"""

import os
import re
import subprocess
from datetime import datetime
from typing import Dict, List, Optional

import openpyxl

from config.locations import normalize_location


class SalonUltimateExcelParser:
    """Parse a single Salon Ultimate Stylist Tracking .xls/.xlsx file."""

    SHEET_NAME  = "Worksheet"
    HEADER_ROW  = 5
    DATA_START  = 6

    # 1-based column indices
    COL_STYLIST_NAME    = 1   # A
    COL_SERVICE_CLIENTS = 3   # C
    COL_RETAIL_CLIENTS  = 4   # D
    COL_SERVICE_NET     = 7   # G
    COL_PRODUCT_NET     = 10  # J

    def __init__(self, file_path: str):
        self.file_path       = file_path
        self._converted_path = None

        wb_path = self._resolve_workbook_path(file_path)
        self.wb = openpyxl.load_workbook(wb_path, data_only=True)

        # Worksheet sheet is required
        if self.SHEET_NAME not in self.wb.sheetnames:
            raise ValueError(
                f"Expected sheet '{self.SHEET_NAME}' not found in {wb_path}. "
                f"Available sheets: {self.wb.sheetnames}"
            )
        self.sheet = self.wb[self.SHEET_NAME]

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def parse(self) -> Dict:
        """
        Parse the full file.

        Returns:
            {
              'location':   str,
              'period':     {'start_date': 'YYYY-MM-DD', 'end_date': 'YYYY-MM-DD'},
              'pos_system': 'salon_ultimate',
              'stylists':   [ <stylist dict>, ... ]
            }
        """
        location = self.extract_location()
        period   = self.extract_period()
        stylists = self._parse_stylists(location, period)

        return {
            "location":   location,
            "period":     period,
            "pos_system": "salon_ultimate",
            "stylists":   stylists,
        }

    # ------------------------------------------------------------------
    # .xls → .xlsx conversion
    # ------------------------------------------------------------------

    def _resolve_workbook_path(self, file_path: str) -> str:
        """
        Return the path to an .xlsx workbook, converting from .xls if necessary.

        Strategy:
          1. If the path already ends with .xlsx — use it directly.
          2. If a corresponding .xlsx already exists on disk — reuse it.
          3. Otherwise — run LibreOffice conversion and return the new path.
        """
        if file_path.endswith(".xlsx"):
            return file_path

        xlsx_path = re.sub(r"\.xls$", ".xlsx", file_path, flags=re.IGNORECASE)

        if os.path.exists(xlsx_path):
            # Already converted from a previous run
            return xlsx_path

        # Run LibreOffice headless conversion
        xlsx_path = self._convert_with_libreoffice(file_path)
        self._converted_path = xlsx_path   # remember for optional cleanup
        return xlsx_path

    def _convert_with_libreoffice(self, xls_path: str) -> str:
        """
        Convert .xls → .xlsx using LibreOffice in headless mode.

        Command:
            soffice --headless --convert-to xlsx --outdir <dir> <file>

        Raises RuntimeError if LibreOffice is not installed or conversion fails.
        """
        output_dir = os.path.dirname(os.path.abspath(xls_path))
        expected_xlsx = os.path.join(
            output_dir,
            os.path.splitext(os.path.basename(xls_path))[0] + ".xlsx",
        )

        soffice = os.environ.get("SOFFICE_PATH", "soffice")
        try:
            result = subprocess.run(
                [soffice, "--headless", "--convert-to", "xlsx",
                 "--outdir", output_dir, xls_path],
                check=True,
                capture_output=True,
                text=True,
            )
        except FileNotFoundError:
            raise RuntimeError(
                "LibreOffice (soffice) is not installed or not on PATH. "
                "Install with: sudo apt-get install libreoffice  "
                "or brew install --cask libreoffice"
            )
        except subprocess.CalledProcessError as e:
            raise RuntimeError(
                f"LibreOffice conversion failed for {xls_path}.\n"
                f"stdout: {e.stdout}\nstderr: {e.stderr}"
            )

        if not os.path.exists(expected_xlsx):
            raise RuntimeError(
                f"LibreOffice conversion appeared to succeed but output file "
                f"not found: {expected_xlsx}"
            )

        return expected_xlsx

    # ------------------------------------------------------------------
    # Extraction helpers
    # ------------------------------------------------------------------

    def extract_location(self) -> str:
        """
        Extract location from Cell B1.

        Examples:
          "FS - Apple Valley Pilot Knob"  → "Apple Valley"
          "FS - Prior Lake"               → "Prior Lake"
          "FS - Farmington"               → "Farmington"
          "FS - Lakeville"                → "Lakeville"

        Strategy: match "FS - <text>" then test against known location names.
        """
        b1 = self.sheet.cell(row=1, column=2).value or ""
        store_name = str(b1).strip()

        # Try known canonical names first (most reliable)
        for canonical in ["Apple Valley", "Prior Lake", "Farmington", "Lakeville"]:
            if canonical.lower() in store_name.lower():
                return canonical

        # Fallback: extract the segment after "FS - " and normalize
        match = re.search(r"FS\s*-\s*(.+)", store_name, re.IGNORECASE)
        if match:
            raw = match.group(1).strip()
            # Strip trailing descriptor words (e.g., "Pilot Knob", "MN")
            raw = re.split(r"\s+(Pilot|MN|Suite|#)", raw, maxsplit=1)[0].strip()
            return normalize_location(raw)

        return "Unknown"

    def extract_period(self) -> Dict[str, Optional[str]]:
        """
        Extract reporting period from Cell B2.

        Expected pattern: "03/01/2026 - 03/31/2026"
        """
        b2 = self.sheet.cell(row=2, column=2).value or ""
        period_str = str(b2).strip()

        match = re.search(
            r"(\d{2}/\d{2}/\d{4})\s*-\s*(\d{2}/\d{2}/\d{4})",
            period_str,
        )

        if match:
            try:
                start_date = datetime.strptime(match.group(1), "%m/%d/%Y").strftime("%Y-%m-%d")
                end_date   = datetime.strptime(match.group(2), "%m/%d/%Y").strftime("%Y-%m-%d")
                return {"start_date": start_date, "end_date": end_date}
            except ValueError:
                pass

        return {"start_date": None, "end_date": None}

    # ------------------------------------------------------------------
    # Stylist parsing
    # ------------------------------------------------------------------

    def _safe_float(self, value) -> float:
        """Convert a cell value to float, stripping commas and $ signs."""
        if value is None:
            return 0.0
        try:
            return float(str(value).replace(",", "").replace("$", "").strip())
        except (ValueError, AttributeError):
            return 0.0

    def _find_totals_row(self) -> Optional[int]:
        """Return the 1-based row number of the 'Totals:' row, or None."""
        for row_idx in range(self.DATA_START, self.sheet.max_row + 1):
            cell_val = self.sheet.cell(row=row_idx, column=self.COL_STYLIST_NAME).value
            if cell_val and "Totals:" in str(cell_val):
                return row_idx
        return None

    def _parse_stylists(self, location: str, period: Dict) -> List[Dict]:
        """
        Parse stylist rows from DATA_START up to (but not including) 'Totals:'.

        Skips:
          - Empty stylist name
          - Rows where name contains "Salon Ultimate" (phantom rows — known spec edge case)
          - The "Totals:" summary row
        """
        totals_row = self._find_totals_row()
        end_row    = totals_row if totals_row else self.sheet.max_row + 1

        stylists = []

        for row_idx in range(self.DATA_START, end_row):
            stylist_name = self.sheet.cell(row=row_idx, column=self.COL_STYLIST_NAME).value

            # Skip empty
            if not stylist_name:
                continue

            name_str = str(stylist_name).strip()

            # Skip phantom "Salon Ultimate" rows (known edge case from spec)
            if "Salon Ultimate" in name_str:
                continue

            # Extract raw values (1-based column access)
            service_clients = self._safe_float(
                self.sheet.cell(row=row_idx, column=self.COL_SERVICE_CLIENTS).value
            )
            retail_clients  = self._safe_float(
                self.sheet.cell(row=row_idx, column=self.COL_RETAIL_CLIENTS).value
            )
            service_net     = self._safe_float(
                self.sheet.cell(row=row_idx, column=self.COL_SERVICE_NET).value
            )
            product_net     = self._safe_float(
                self.sheet.cell(row=row_idx, column=self.COL_PRODUCT_NET).value
            )

            # Skip rows that are entirely zero (likely blank spacer rows)
            if service_clients == 0 and retail_clients == 0 and service_net == 0 and product_net == 0:
                continue

            # ---- Karissa's authoritative formulas ----
            guest_count = service_clients + retail_clients
            total_sales = service_net + product_net
            ppg_net     = (product_net / guest_count) if guest_count > 0 else 0.0

            stylists.append({
                # Identity
                "location":      location,
                "stylist_name":  name_str,
                "period_start":  period["start_date"],
                "period_end":    period["end_date"],
                "pos_system":    "salon_ultimate",

                # Raw values preserved from Excel
                "service_clients": service_clients,
                "retail_clients":  retail_clients,
                "service_net":     round(service_net, 2),
                "product_net":     round(product_net, 2),

                # Derived per Karissa's formulas
                "guest_count":  round(guest_count, 0),
                "total_sales":  round(total_sales, 2),
                "ppg_net":      round(ppg_net, 2),
            })

        return stylists

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------

    def cleanup(self):
        """Remove temp .xlsx file created during .xls conversion (optional call)."""
        if self._converted_path and os.path.exists(self._converted_path):
            os.remove(self._converted_path)
            self._converted_path = None

    def __del__(self):
        """Best-effort cleanup on garbage collection."""
        try:
            self.cleanup()
        except Exception:
            pass
