"""
tests/test_sheets_writer_data_monthly.py
────────────────────────────────────────
Tests for core/sheets_writer.py::append_to_data_monthly() and its supporting
schema + tab-bootstrap logic.

All tests use unittest.mock to stand in for the Google Sheets API service —
no real network calls, no real sheet writes.

Test surface:
  - DATA_MONTHLY_HEADERS column ordering matches the 23-column contract
  - dry_run=True suppresses every service call
  - Empty input is a no-op
  - Idempotency: rows whose (loc_name, year_month) key already exists are skipped
  - Happy path: all-new rows get appended with the correct column order
  - Auto-creation: missing DATA_MONTHLY tab is created with a header row
  - Tab-exists path: when DATA_MONTHLY already exists, no batchUpdate fires
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from core.sheets_writer import (  # noqa: E402
    DATA_MONTHLY_HEADERS,
    _ensure_data_monthly_tab,
    append_to_data_monthly,
)


SHEET_ID = "test_sheet_id_xyz"
CONFIG = {"sheet_id": SHEET_ID}


def _build_service(*, tab_exists: bool = True, existing_keys: list[tuple[str, str]] | None = None):
    """Return a MagicMock that mimics the Google Sheets service chain.

    Controls:
      tab_exists      — toggles whether DATA_MONTHLY appears in spreadsheets().get() metadata
      existing_keys   — list of (loc_name, year_month) pairs returned by the idempotency read

    Important: configures return_value on each method WITHOUT invoking the
    method (which would record an extra call). After this helper runs, the
    only recorded calls on the service chain are the navigation calls
    (spreadsheets(), values()) — no get/append/batchUpdate/update calls.
    Tests can then assert against the real production-code calls cleanly.
    """
    existing_keys = existing_keys or []
    service = MagicMock()

    # spreadsheets().get(spreadsheetId=...).execute() — metadata for tab existence
    titles = [{"properties": {"title": "DATA"}}, {"properties": {"title": "STYLISTS_DATA"}}]
    if tab_exists:
        titles.append({"properties": {"title": "DATA_MONTHLY"}})
    service.spreadsheets.return_value.get.return_value.execute.return_value = {"sheets": titles}

    # spreadsheets().values().get(...).execute() — idempotency read returns existing keys
    existing_rows = [list(k) for k in existing_keys]
    service.spreadsheets.return_value.values.return_value.get.return_value.execute.return_value = {
        "values": existing_rows,
    }

    # batchUpdate / update / append all return a no-op dict on .execute()
    service.spreadsheets.return_value.batchUpdate.return_value.execute.return_value = {}
    service.spreadsheets.return_value.values.return_value.update.return_value.execute.return_value = {}
    service.spreadsheets.return_value.values.return_value.append.return_value.execute.return_value = {}

    # Reset the call log so navigation calls during setup don't pollute test assertions
    service.reset_mock()
    return service


def _sample_row(loc: str = "Blaine", year_month: str = "2026-03", **overrides) -> dict:
    base = {
        "loc_name": loc,
        "year_month": year_month,
        "platform": "zenoti",
        "guests": 850,
        "total_sales": 50000.0,
        "service": 45000.0,
        "product": 5000.0,
        "product_pct": 0.10,
        "ppg": 5.88,
        "pph": 38.5,
        "avg_ticket": 58.82,
        "prod_hours": 1100.0,
        "wax_count": 100,
        "wax": 2000.0,
        "wax_pct": 0.1176,
        "color": 15000.0,
        "color_pct": 0.3333,
        "treat_count": 150,
        "treat": 3000.0,
        "treat_pct": 0.1765,
        "source": "tracker",
        "period_start": "2026-03-01",
        "period_end": "2026-03-31",
    }
    base.update(overrides)
    return base


class TestSchemaContract(unittest.TestCase):
    """The 23-col header order is the contract — verify shape and key names."""

    def test_exactly_23_columns(self):
        self.assertEqual(len(DATA_MONTHLY_HEADERS), 23)

    def test_first_two_cols_are_idempotency_key(self):
        # (loc_name, year_month) is the unique key the idempotency check relies on
        self.assertEqual(DATA_MONTHLY_HEADERS[0], "loc_name")
        self.assertEqual(DATA_MONTHLY_HEADERS[1], "year_month")

    def test_last_three_cols_are_provenance(self):
        self.assertEqual(DATA_MONTHLY_HEADERS[-3:], ["source", "period_start", "period_end"])

    def test_kpi_columns_match_data_tab_order(self):
        # Cols D-T should match DATA tab order (guests..treat_pct) so the two
        # tabs stay schema-cousins. If you renumber, update both at once.
        expected_kpi_order = [
            "guests", "total_sales", "service", "product", "product_pct",
            "ppg", "pph", "avg_ticket", "prod_hours",
            "wax_count", "wax", "wax_pct",
            "color", "color_pct",
            "treat_count", "treat", "treat_pct",
        ]
        self.assertEqual(DATA_MONTHLY_HEADERS[3:20], expected_kpi_order)


class TestDryRun(unittest.TestCase):
    def test_dry_run_makes_no_service_calls(self):
        service = MagicMock()
        rows = [_sample_row()]
        append_to_data_monthly(service, CONFIG, rows, dry_run=True)
        # Nothing on the service chain should have been invoked at .execute() level
        service.spreadsheets.return_value.values.return_value.append.assert_not_called()
        service.spreadsheets.return_value.values.return_value.get.assert_not_called()
        service.spreadsheets.return_value.batchUpdate.assert_not_called()


class TestEmptyInput(unittest.TestCase):
    def test_empty_rows_is_noop(self):
        service = MagicMock()
        append_to_data_monthly(service, CONFIG, [], dry_run=False)
        service.spreadsheets.return_value.values.return_value.append.assert_not_called()
        # _ensure_data_monthly_tab shouldn't even be reached
        service.spreadsheets.return_value.get.assert_not_called()


class TestIdempotency(unittest.TestCase):
    def test_skips_rows_with_existing_loc_yearmonth(self):
        service = _build_service(
            tab_exists=True,
            existing_keys=[("Blaine", "2026-03"), ("Hudson", "2026-03")],
        )
        rows = [
            _sample_row(loc="Blaine", year_month="2026-03"),  # skip
            _sample_row(loc="Hudson", year_month="2026-03"),  # skip
            _sample_row(loc="Andover FS", year_month="2026-03"),  # write
        ]
        append_to_data_monthly(service, CONFIG, rows, dry_run=False)

        # Append should have been called once with exactly 1 row (Andover)
        append_calls = service.spreadsheets().values().append.call_args_list
        # Filter out the call recorded during _build_service setup (the MagicMock auto-tracks accesses)
        write_calls = [
            c for c in append_calls
            if c.kwargs.get("range") == "DATA_MONTHLY!A2:W"
            and "body" in c.kwargs
        ]
        self.assertEqual(len(write_calls), 1)
        appended_values = write_calls[0].kwargs["body"]["values"]
        self.assertEqual(len(appended_values), 1)
        self.assertEqual(appended_values[0][0], "Andover FS")
        self.assertEqual(appended_values[0][1], "2026-03")

    def test_all_rows_already_present_skips_append(self):
        service = _build_service(
            tab_exists=True,
            existing_keys=[("Blaine", "2026-03")],
        )
        rows = [_sample_row(loc="Blaine", year_month="2026-03")]
        append_to_data_monthly(service, CONFIG, rows, dry_run=False)
        write_calls = [
            c for c in service.spreadsheets().values().append.call_args_list
            if c.kwargs.get("range") == "DATA_MONTHLY!A2:W"
            and "body" in c.kwargs
        ]
        self.assertEqual(len(write_calls), 0)


class TestHappyPath(unittest.TestCase):
    def test_all_new_rows_get_appended_with_correct_column_order(self):
        service = _build_service(tab_exists=True, existing_keys=[])
        rows = [
            _sample_row(loc="Blaine", year_month="2026-03"),
            _sample_row(
                loc="Apple Valley", year_month="2026-04", platform="salon_ultimate",
                source="su_monthly_pdf", period_start="2026-04-01", period_end="2026-04-30",
            ),
        ]
        append_to_data_monthly(service, CONFIG, rows, dry_run=False)

        write_calls = [
            c for c in service.spreadsheets().values().append.call_args_list
            if c.kwargs.get("range") == "DATA_MONTHLY!A2:W"
            and "body" in c.kwargs
        ]
        self.assertEqual(len(write_calls), 1)
        values = write_calls[0].kwargs["body"]["values"]
        self.assertEqual(len(values), 2)

        # First row column-by-column
        first = values[0]
        self.assertEqual(len(first), 23)
        self.assertEqual(first[0], "Blaine")             # loc_name
        self.assertEqual(first[1], "2026-03")            # year_month
        self.assertEqual(first[2], "zenoti")             # platform
        self.assertEqual(first[3], 850)                  # guests
        self.assertEqual(first[20], "tracker")           # source
        self.assertEqual(first[21], "2026-03-01")        # period_start
        self.assertEqual(first[22], "2026-03-31")        # period_end

        # Second row provenance/platform differ
        second = values[1]
        self.assertEqual(second[0], "Apple Valley")
        self.assertEqual(second[1], "2026-04")
        self.assertEqual(second[2], "salon_ultimate")
        self.assertEqual(second[20], "su_monthly_pdf")
        self.assertEqual(second[21], "2026-04-01")
        self.assertEqual(second[22], "2026-04-30")

    def test_missing_optional_fields_default_to_zero_or_empty(self):
        service = _build_service(tab_exists=True, existing_keys=[])
        rows = [{"loc_name": "X", "year_month": "2026-03", "platform": "zenoti"}]
        append_to_data_monthly(service, CONFIG, rows, dry_run=False)
        write_calls = [
            c for c in service.spreadsheets().values().append.call_args_list
            if c.kwargs.get("range") == "DATA_MONTHLY!A2:W"
            and "body" in c.kwargs
        ]
        values = write_calls[0].kwargs["body"]["values"]
        row = values[0]
        self.assertEqual(row[0], "X")
        self.assertEqual(row[1], "2026-03")
        self.assertEqual(row[2], "zenoti")
        # Numeric defaults
        for col_idx in range(3, 20):
            self.assertEqual(row[col_idx], 0, f"col {col_idx} should default to 0")
        # Provenance defaults
        self.assertEqual(row[20], "")
        self.assertEqual(row[21], "")
        self.assertEqual(row[22], "")


class TestTabBootstrap(unittest.TestCase):
    def test_existing_tab_skips_batchupdate(self):
        service = _build_service(tab_exists=True)
        _ensure_data_monthly_tab(service, SHEET_ID)
        service.spreadsheets().batchUpdate.assert_not_called()

    def test_missing_tab_triggers_addSheet_and_header_write(self):
        service = _build_service(tab_exists=False)
        _ensure_data_monthly_tab(service, SHEET_ID)

        # batchUpdate called with addSheet for DATA_MONTHLY
        bu_calls = service.spreadsheets().batchUpdate.call_args_list
        self.assertTrue(any(
            (c.kwargs or {}).get("body", {}).get("requests", [{}])[0]
            .get("addSheet", {}).get("properties", {}).get("title") == "DATA_MONTHLY"
            for c in bu_calls
        ), "expected addSheet request for DATA_MONTHLY")

        # update() called for the header row
        update_calls = service.spreadsheets().values().update.call_args_list
        header_writes = [
            c for c in update_calls
            if c.kwargs.get("range") == "DATA_MONTHLY!A1:W1"
            and "body" in c.kwargs
        ]
        self.assertEqual(len(header_writes), 1)
        written_headers = header_writes[0].kwargs["body"]["values"][0]
        self.assertEqual(written_headers, DATA_MONTHLY_HEADERS)


if __name__ == "__main__":
    unittest.main()
