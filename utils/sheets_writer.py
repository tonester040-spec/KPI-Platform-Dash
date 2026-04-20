"""
Google Sheets Writer — Tier 2 Normalized Stylist Data
Writes parsed stylist data to Google Sheets.

Target sheet schema ("Stylist Data") — 22 columns:
   1. location          12. ppg_net
   2. location_id       13. avg_ticket
   3. location_name     14. product_pct
   4. stylist_name      15. pph_net            (SU only)
   5. period_start      16. productive_hours   (SU only)
   6. period_end        17. wax_count          (Phase 2)
   7. pos_system        18. wax_pct            (Phase 2)
   8. guest_count       19. color_net          (Phase 2)
   9. service_net       20. color_pct          (Phase 2)
  10. product_net       21. treatment_count    (Phase 2)
  11. total_sales       22. treatment_pct      (Phase 2)

Phase 1 (Excel-only) rows write empty string "" for PDF-sourced columns.
Phase 2 (merged) rows populate all columns.

Write modes:
  write_stylists()  — Full overwrite including header row (use for first load or full refresh)
  append_stylists() — Append rows only, no header (use for incremental updates)

Authentication:
  Requires a Google service-account credentials JSON file.
  Env var: GOOGLE_SHEETS_CREDENTIALS (path to JSON)
  Env var: GOOGLE_SHEETS_ID          (spreadsheet ID)
"""

import os
from typing import List, Dict

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


# The minimum OAuth scope needed for read+write
_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# Normalized column order — 22 columns, matches header row written by write_stylists()
# Phase 1 (Excel-only) rows: PDF-sourced columns are empty string ""
# Phase 2 (merged)     rows: all 22 columns populated
_COLUMNS = [
    "location",           # 1
    "location_id",        # 2
    "location_name",      # 3
    "stylist_name",       # 4
    "period_start",       # 5
    "period_end",         # 6
    "pos_system",         # 7
    "guest_count",        # 8
    "service_net",        # 9
    "product_net",        # 10
    "total_sales",        # 11
    "ppg_net",            # 12
    "avg_ticket",         # 13 — computed from Excel data
    "product_pct",        # 14 — computed from Excel data
    "pph_net",            # 15 — SU only (from location_totals); None for Zenoti
    "productive_hours",   # 16 — SU only (from location_totals); None for Zenoti
    "wax_count",          # 17 — Phase 2: distributed from PDF location total
    "wax_pct",            # 18 — Phase 2: wax_count / guest_count × 100
    "color_net",          # 19 — Phase 2: distributed from PDF location total
    "color_pct",          # 20 — Phase 2: color_net / service_net × 100
    "treatment_count",    # 21 — Phase 2: distributed from PDF location total
    "treatment_pct",      # 22 — Phase 2: treatment_count / guest_count × 100
]


class GoogleSheetsWriter:
    """Write parsed stylist data to a Google Sheets tab."""

    def __init__(self, credentials_path: str, spreadsheet_id: str):
        """
        Args:
            credentials_path: Absolute path to a Google service-account JSON key file.
            spreadsheet_id:   The Sheets document ID (from the URL).
        """
        self.spreadsheet_id = spreadsheet_id

        creds = Credentials.from_service_account_file(
            credentials_path, scopes=_SCOPES
        )
        service     = build("sheets", "v4", credentials=creds)
        self._sheet = service.spreadsheets()

    # ------------------------------------------------------------------
    # Public write methods
    # ------------------------------------------------------------------

    def write_stylists(
        self,
        stylists: List[Dict],
        sheet_name: str = "Stylist Data",
    ) -> int:
        """
        Full overwrite: write header + all stylist rows starting at A1.

        Use for initial load or complete weekly refresh.

        Args:
            stylists:   List of stylist dicts from the parsers.
            sheet_name: Name of the destination sheet tab.

        Returns:
            Number of stylist rows written (excludes header row).
        """
        if not stylists:
            print(f"⚠️  write_stylists: no stylists provided — nothing written to '{sheet_name}'")
            return 0

        rows = [_COLUMNS]  # Header row first
        for stylist in stylists:
            rows.append(self._stylist_to_row(stylist))

        self._update_range(f"{sheet_name}!A1", rows)

        count = len(stylists)
        print(f"✅ Wrote {count} stylist rows to '{sheet_name}' (including header)")
        return count

    def append_stylists(
        self,
        stylists: List[Dict],
        sheet_name: str = "Stylist Data",
    ) -> int:
        """
        Append-only: add stylist rows after the last populated row.

        Use for incremental updates (new weeks or new locations).
        Does NOT write a header row — assumes the sheet was seeded by write_stylists().

        Args:
            stylists:   List of stylist dicts from the parsers.
            sheet_name: Name of the destination sheet tab.

        Returns:
            Number of rows appended.
        """
        if not stylists:
            print(f"⚠️  append_stylists: no stylists provided — nothing appended to '{sheet_name}'")
            return 0

        rows = [self._stylist_to_row(s) for s in stylists]

        self._append_range(f"{sheet_name}!A:A", rows)

        count = len(stylists)
        print(f"✅ Appended {count} stylist rows to '{sheet_name}'")
        return count

    # ------------------------------------------------------------------
    # Row serialisation
    # ------------------------------------------------------------------

    @staticmethod
    def _stylist_to_row(stylist: Dict) -> List:
        """
        Serialize a stylist dict to a flat list matching _COLUMNS order (22 columns).

        Phase 1 (Excel-only) rows have "" for PDF-sourced columns (wax/color/treatment).
        Phase 2 (merged) rows have numeric values for all columns.
        pph_net / productive_hours are None for Zenoti rows (serialized as "").
        """
        def _val(key, default=""):
            """Return value or default. None → default."""
            v = stylist.get(key)
            return default if v is None else v

        return [
            _val("location",          ""),   # 1
            _val("location_id",       ""),   # 2
            _val("location_name",     ""),   # 3
            _val("stylist_name",      ""),   # 4
            _val("period_start",      ""),   # 5
            _val("period_end",        ""),   # 6
            _val("pos_system",        ""),   # 7
            _val("guest_count",       0),    # 8
            _val("service_net",       0),    # 9
            _val("product_net",       0),    # 10
            _val("total_sales",       0),    # 11
            _val("ppg_net",           0),    # 12
            _val("avg_ticket",        0),    # 13
            _val("product_pct",       0),    # 14
            _val("pph_net",           ""),   # 15 — SU only; None → "" for Zenoti
            _val("productive_hours",  ""),   # 16 — SU only; None → "" for Zenoti
            _val("wax_count",         ""),   # 17 — Phase 2
            _val("wax_pct",           ""),   # 18 — Phase 2
            _val("color_net",         ""),   # 19 — Phase 2
            _val("color_pct",         ""),   # 20 — Phase 2
            _val("treatment_count",   ""),   # 21 — Phase 2
            _val("treatment_pct",     ""),   # 22 — Phase 2
        ]

    # ------------------------------------------------------------------
    # Sheets API helpers
    # ------------------------------------------------------------------

    def _update_range(self, range_name: str, values: List[List]) -> None:
        """Overwrite a range with the provided values (RAW input)."""
        body = {"values": values}
        try:
            self._sheet.values().update(
                spreadsheetId=self.spreadsheet_id,
                range=range_name,
                valueInputOption="RAW",
                body=body,
            ).execute()
        except HttpError as e:
            raise RuntimeError(f"Sheets API update failed for range '{range_name}': {e}") from e

    def _append_range(self, range_name: str, values: List[List]) -> None:
        """Append rows below the last populated row in the range."""
        body = {"values": values}
        try:
            self._sheet.values().append(
                spreadsheetId=self.spreadsheet_id,
                range=range_name,
                valueInputOption="RAW",
                insertDataOption="INSERT_ROWS",
                body=body,
            ).execute()
        except HttpError as e:
            raise RuntimeError(f"Sheets API append failed for range '{range_name}': {e}") from e

    # ------------------------------------------------------------------
    # Factory helper — build from environment variables
    # ------------------------------------------------------------------

    @classmethod
    def from_env(cls) -> "GoogleSheetsWriter":
        """
        Construct a GoogleSheetsWriter using environment variables.

        Required env vars:
            GOOGLE_SHEETS_CREDENTIALS  — path to service-account JSON key
            GOOGLE_SHEETS_ID           — target spreadsheet ID

        Raises:
            EnvironmentError if either variable is missing.
        """
        creds_path      = os.getenv("GOOGLE_SHEETS_CREDENTIALS")
        spreadsheet_id  = os.getenv("GOOGLE_SHEETS_ID")

        missing = []
        if not creds_path:
            missing.append("GOOGLE_SHEETS_CREDENTIALS")
        if not spreadsheet_id:
            missing.append("GOOGLE_SHEETS_ID")

        if missing:
            raise EnvironmentError(
                f"Missing required environment variable(s): {', '.join(missing)}\n"
                "Copy .env.example → .env and fill in values."
            )

        return cls(credentials_path=creds_path, spreadsheet_id=spreadsheet_id)
