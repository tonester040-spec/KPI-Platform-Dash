"""
tests/test_sheets_writer_stylists_data_monthly.py
─────────────────────────────────────────────────
Tests for core/sheets_writer.py::append_to_stylists_data_monthly().
Mirrors the DATA_MONTHLY test file with the 3-column (year_month, name,
loc_name) idempotency key.
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
    STYLISTS_DATA_MONTHLY_HEADERS,
    _ensure_stylists_data_monthly_tab,
    append_to_stylists_data_monthly,
)


SHEET_ID = "test_sheet_id_xyz"
CONFIG = {"sheet_id": SHEET_ID}


def _build_service(*, tab_exists: bool = True, existing_keys: list[tuple[str, str, str]] | None = None):
    """Mock service with no spurious recorded calls during setup."""
    existing_keys = existing_keys or []
    service = MagicMock()

    titles = [{"properties": {"title": "DATA_MONTHLY"}}]
    if tab_exists:
        titles.append({"properties": {"title": "STYLISTS_DATA_MONTHLY"}})
    service.spreadsheets.return_value.get.return_value.execute.return_value = {"sheets": titles}

    existing_rows = [list(k) for k in existing_keys]
    service.spreadsheets.return_value.values.return_value.get.return_value.execute.return_value = {
        "values": existing_rows,
    }

    service.spreadsheets.return_value.batchUpdate.return_value.execute.return_value = {}
    service.spreadsheets.return_value.values.return_value.update.return_value.execute.return_value = {}
    service.spreadsheets.return_value.values.return_value.append.return_value.execute.return_value = {}

    service.reset_mock()
    return service


def _sample_stylist(name: str = "Heaven Hobbs", loc: str = "Andover FS", ym: str = "2026-03", **kw) -> dict:
    base = {
        "year_month": ym, "name": name, "loc_name": loc, "loc_id": "z001",
        "platform": "zenoti",
        "invoices": 75, "guests": 74,
        "net_service": 3088.50, "net_product": 84.25,
        "avg_ticket": 42.30, "pph": 35.0, "ppg": 1.14,
        "production_hours": 88.25,
        "source": "zenoti_xlsx", "period_start": "2026-03-01", "period_end": "2026-03-31",
    }
    base.update(kw)
    return base


class TestSchema(unittest.TestCase):
    def test_exactly_18_columns(self):
        # 16 original + 2 added 2026-05-27 (req_pct, avg_service_time_min)
        self.assertEqual(len(STYLISTS_DATA_MONTHLY_HEADERS), 18)

    def test_first_three_cols_are_idempotency_key(self):
        # Idempotency = (year_month, name, loc_name) — cols A/B/C
        self.assertEqual(STYLISTS_DATA_MONTHLY_HEADERS[:3], ["year_month", "name", "loc_name"])

    def test_new_per_stylist_engagement_cols_at_end(self):
        # 2026-05-27: req_pct + avg_service_time_min appended at the end
        # so existing rows pad with empty trailing cells (no migration).
        self.assertEqual(STYLISTS_DATA_MONTHLY_HEADERS[-2:], ["req_pct", "avg_service_time_min"])
        # Provenance trio shifted from -3: to -5:-2 by the append
        self.assertEqual(
            STYLISTS_DATA_MONTHLY_HEADERS[-5:-2],
            ["source", "period_start", "period_end"],
        )


class TestDryRun(unittest.TestCase):
    def test_dry_run_makes_no_service_calls(self):
        service = MagicMock()
        append_to_stylists_data_monthly(service, CONFIG, [_sample_stylist()], dry_run=True)
        service.spreadsheets.return_value.values.return_value.append.assert_not_called()
        service.spreadsheets.return_value.values.return_value.get.assert_not_called()
        service.spreadsheets.return_value.batchUpdate.assert_not_called()


class TestEmpty(unittest.TestCase):
    def test_empty_rows_noop(self):
        service = MagicMock()
        append_to_stylists_data_monthly(service, CONFIG, [], dry_run=False)
        service.spreadsheets.return_value.values.return_value.append.assert_not_called()


class TestIdempotency(unittest.TestCase):
    def test_three_col_key_skip(self):
        service = _build_service(
            tab_exists=True,
            existing_keys=[("2026-03", "Heaven Hobbs", "Andover FS")],
        )
        rows = [
            _sample_stylist(name="Heaven Hobbs", loc="Andover FS", ym="2026-03"),   # skip
            _sample_stylist(name="Heaven Hobbs", loc="Blaine",      ym="2026-03"),   # different loc - write
            _sample_stylist(name="Other Stylist", loc="Andover FS", ym="2026-03"),   # different name - write
            _sample_stylist(name="Heaven Hobbs", loc="Andover FS", ym="2026-04"),   # different ym - write
        ]
        append_to_stylists_data_monthly(service, CONFIG, rows, dry_run=False)

        write_calls = [
            c for c in service.spreadsheets().values().append.call_args_list
            if c.kwargs.get("range") == "STYLISTS_DATA_MONTHLY!A2:R"
            and "body" in c.kwargs
        ]
        self.assertEqual(len(write_calls), 1)
        appended = write_calls[0].kwargs["body"]["values"]
        self.assertEqual(len(appended), 3)


class TestHappyPath(unittest.TestCase):
    def test_columns_in_order(self):
        service = _build_service(tab_exists=True, existing_keys=[])
        append_to_stylists_data_monthly(service, CONFIG, [_sample_stylist()], dry_run=False)
        write_calls = [
            c for c in service.spreadsheets().values().append.call_args_list
            if c.kwargs.get("range") == "STYLISTS_DATA_MONTHLY!A2:R"
            and "body" in c.kwargs
        ]
        row = write_calls[0].kwargs["body"]["values"][0]
        self.assertEqual(len(row), 18)  # +2 for req_pct, avg_service_time_min
        self.assertEqual(row[0], "2026-03")
        self.assertEqual(row[1], "Heaven Hobbs")
        self.assertEqual(row[2], "Andover FS")
        self.assertEqual(row[3], "z001")
        self.assertEqual(row[4], "zenoti")
        self.assertEqual(row[13], "zenoti_xlsx")
        # New cols default to 0 when not supplied — _sample_stylist doesn't set them
        self.assertEqual(row[16], 0)  # req_pct
        self.assertEqual(row[17], 0)  # avg_service_time_min


class TestBootstrap(unittest.TestCase):
    def test_missing_tab_creates_with_headers(self):
        service = _build_service(tab_exists=False)
        _ensure_stylists_data_monthly_tab(service, SHEET_ID)
        # Verify the header write happened with correct content
        update_calls = service.spreadsheets().values().update.call_args_list
        header_writes = [
            c for c in update_calls
            if c.kwargs.get("range") == "STYLISTS_DATA_MONTHLY!A1:P1"
            and "body" in c.kwargs
        ]
        self.assertEqual(len(header_writes), 1)
        self.assertEqual(header_writes[0].kwargs["body"]["values"][0], STYLISTS_DATA_MONTHLY_HEADERS)


if __name__ == "__main__":
    unittest.main()
