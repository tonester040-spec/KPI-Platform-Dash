"""
Google Sheets Writer — Tier 2 Normalized Stylist Data
Writes parsed stylist data to Google Sheets.

Target sheet schema ("Stylist Data"):
  location        | stylist_name  | period_start | period_end | pos_system
  guest_count     | service_net   | product_net  | total_sales | ppg_net
  wax_count (PDF) | color_net (PDF) | treatment_count (PDF)

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

# Normalized column order — matches header row written by write_stylists()
_COLUMNS = [
    "location",
    "stylist_name",
    "period_start",
    "period_end",
    "pos_system",
    "guest_count",
    "service_net",
    "product_net",
    "total_sales",
    "ppg_net",
    "wax_count",        # Phase 2 — populated by PDF parsers
    "color_net",        # Phase 2 — populated by PDF parsers
    "treatment_count",  # Phase 2 — populated by PDF parsers
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
        Serialize a stylist dict to a flat list matching _COLUMNS order.

        PDF-only fields (wax_count, color_net, treatment_count) default to ""
        and will be back-filled by Phase 2 PDF parsers.
        """
        return [
            stylist.get("location",     ""),
            stylist.get("stylist_name", ""),
            stylist.get("period_start", ""),
            stylist.get("period_end",   ""),
            stylist.get("pos_system",   ""),
            stylist.get("guest_count",  0),
            stylist.get("service_net",  0),
            stylist.get("product_net",  0),
            stylist.get("total_sales",  0),
            stylist.get("ppg_net",      0),
            stylist.get("wax_count",    ""),   # Phase 2
            stylist.get("color_net",    ""),   # Phase 2
            stylist.get("treatment_count", ""),  # Phase 2
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
