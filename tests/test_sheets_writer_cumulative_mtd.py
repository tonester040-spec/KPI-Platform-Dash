"""
tests/test_sheets_writer_cumulative_mtd.py
──────────────────────────────────────────
Tests for the CUMULATIVE_MTD and STYLISTS_CUMULATIVE_MTD writers in
core/sheets_writer.py. Mirrors the DATA_MONTHLY test pattern with a
three-column (loc_name, year_month, week_ending) and four-column
(year_month, week_ending, name, loc_name) idempotency keys.
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
    CUMULATIVE_MTD_HEADERS,
    STYLISTS_CUMULATIVE_MTD_HEADERS,
    _ensure_cumulative_mtd_tab,
    _ensure_stylists_cumulative_mtd_tab,
    append_to_cumulative_mtd,
    append_to_stylists_cumulative_mtd,
)


SHEET_ID = "test_sheet_id_xyz"
CONFIG = {"sheet_id": SHEET_ID}


def _build_service(
    *,
    cumulative_tab_exists: bool = True,
    stylists_cumulative_tab_exists: bool = True,
    existing_cumulative_keys: list[tuple] | None = None,
    existing_stylists_keys: list[tuple] | None = None,
):
    """Mock service with no spurious recorded calls during setup."""
    existing_cumulative_keys = existing_cumulative_keys or []
    existing_stylists_keys = existing_stylists_keys or []
    service = MagicMock()

    titles = [{"properties": {"title": "DATA"}}]
    if cumulative_tab_exists:
        titles.append({"properties": {"title": "CUMULATIVE_MTD"}})
    if stylists_cumulative_tab_exists:
        titles.append({"properties": {"title": "STYLISTS_CUMULATIVE_MTD"}})
    service.spreadsheets.return_value.get.return_value.execute.return_value = {"sheets": titles}

    # values().get() — return appropriate existing-keys based on the range requested
    def _values_get_side_effect(spreadsheetId=None, range=None):
        m = MagicMock()
        if range == "CUMULATIVE_MTD!A2:C":
            m.execute.return_value = {"values": [list(k) for k in existing_cumulative_keys]}
        elif range == "STYLISTS_CUMULATIVE_MTD!A2:D":
            m.execute.return_value = {"values": [list(k) for k in existing_stylists_keys]}
        else:
            m.execute.return_value = {"values": []}
        return m
    service.spreadsheets.return_value.values.return_value.get.side_effect = _values_get_side_effect

    service.spreadsheets.return_value.batchUpdate.return_value.execute.return_value = {}
    service.spreadsheets.return_value.values.return_value.update.return_value.execute.return_value = {}
    service.spreadsheets.return_value.values.return_value.append.return_value.execute.return_value = {}

    service.reset_mock()
    # Re-set the side_effect because reset_mock clears it
    service.spreadsheets.return_value.values.return_value.get.side_effect = _values_get_side_effect
    return service


def _sample_cum_row(loc: str = "Blaine", ym: str = "2026-04", we: str = "2026-04-05", **kw) -> dict:
    base = {
        "loc_name": loc, "year_month": ym, "week_ending": we, "platform": "zenoti",
        "guests": 280, "total_sales": 16500.0, "service": 15800.0, "product": 700.0,
        "product_pct": 0.042, "ppg": 2.5, "pph": 55.0, "avg_ticket": 58.9,
        "prod_hours": 287.0,
        "wax_count": 40, "wax": 720.0, "wax_pct": 0.143,
        "color": 5300.0, "color_pct": 0.335,
        "treat_count": 50, "treat": 900.0, "treat_pct": 0.179,
        "source": "zenoti_weekly_pdf",
    }
    base.update(kw)
    return base


def _sample_stylist_cum_row(
    name: str = "Heaven Hobbs",
    loc: str = "Andover FS",
    ym: str = "2026-04",
    we: str = "2026-04-05",
    **kw,
) -> dict:
    base = {
        "year_month": ym, "week_ending": we, "name": name, "loc_name": loc,
        "loc_id": "z001", "platform": "zenoti",
        "invoices": 18, "guests": 18,
        "net_service": 850.0, "net_product": 40.0,
        "avg_ticket": 47.22, "pph": 35.0, "ppg": 2.22,
        "production_hours": 24.5,
        "source": "zenoti_weekly_pdf",
    }
    base.update(kw)
    return base


# ────────────────────────────────────────────────────────────────────────
# CUMULATIVE_MTD tests
# ────────────────────────────────────────────────────────────────────────

class TestCumulativeMtdSchema(unittest.TestCase):
    def test_exactly_22_columns(self):
        self.assertEqual(len(CUMULATIVE_MTD_HEADERS), 22)

    def test_first_three_cols_are_idempotency_key(self):
        self.assertEqual(CUMULATIVE_MTD_HEADERS[:3], ["loc_name", "year_month", "week_ending"])

    def test_source_is_last(self):
        self.assertEqual(CUMULATIVE_MTD_HEADERS[-1], "source")

    def test_kpi_columns_match_data_monthly_order(self):
        # Cols E-U (indices 4-20) should match DATA_MONTHLY's KPI order
        from core.sheets_writer import DATA_MONTHLY_HEADERS
        cum_kpi = CUMULATIVE_MTD_HEADERS[4:21]
        data_monthly_kpi = DATA_MONTHLY_HEADERS[3:20]
        self.assertEqual(cum_kpi, data_monthly_kpi)


class TestCumulativeMtdDryRun(unittest.TestCase):
    def test_dry_run_makes_no_service_calls(self):
        service = MagicMock()
        append_to_cumulative_mtd(service, CONFIG, [_sample_cum_row()], dry_run=True)
        service.spreadsheets.return_value.values.return_value.append.assert_not_called()
        service.spreadsheets.return_value.values.return_value.get.assert_not_called()
        service.spreadsheets.return_value.batchUpdate.assert_not_called()


class TestCumulativeMtdEmpty(unittest.TestCase):
    def test_empty_rows_noop(self):
        service = MagicMock()
        append_to_cumulative_mtd(service, CONFIG, [], dry_run=False)
        service.spreadsheets.return_value.values.return_value.append.assert_not_called()


class TestCumulativeMtdIdempotency(unittest.TestCase):
    def test_three_col_key_skip(self):
        service = _build_service(
            cumulative_tab_exists=True,
            existing_cumulative_keys=[
                ("Blaine", "2026-04", "2026-04-05"),
                ("Hudson", "2026-04", "2026-04-05"),
            ],
        )
        rows = [
            _sample_cum_row(loc="Blaine", ym="2026-04", we="2026-04-05"),   # skip
            _sample_cum_row(loc="Hudson", ym="2026-04", we="2026-04-05"),   # skip
            _sample_cum_row(loc="Blaine", ym="2026-04", we="2026-04-12"),   # diff week -> write
            _sample_cum_row(loc="Andover FS", ym="2026-04", we="2026-04-05"),  # diff loc -> write
        ]
        append_to_cumulative_mtd(service, CONFIG, rows, dry_run=False)
        write_calls = [
            c for c in service.spreadsheets().values().append.call_args_list
            if c.kwargs.get("range") == "CUMULATIVE_MTD!A2:V"
            and "body" in c.kwargs
        ]
        self.assertEqual(len(write_calls), 1)
        appended = write_calls[0].kwargs["body"]["values"]
        self.assertEqual(len(appended), 2)

    def test_negative_values_pass_through(self):
        """Karissa's Q5: refunds can produce decreasing cumulative values.
        Writer must not reject negative numbers."""
        service = _build_service(cumulative_tab_exists=True, existing_cumulative_keys=[])
        row = _sample_cum_row(service=-200.0, guests=-3)  # implausible but valid
        append_to_cumulative_mtd(service, CONFIG, [row], dry_run=False)
        write_calls = [
            c for c in service.spreadsheets().values().append.call_args_list
            if c.kwargs.get("range") == "CUMULATIVE_MTD!A2:V"
            and "body" in c.kwargs
        ]
        self.assertEqual(len(write_calls), 1)
        appended = write_calls[0].kwargs["body"]["values"][0]
        self.assertEqual(appended[6], -200.0)  # service column
        self.assertEqual(appended[4], -3)       # guests column


class TestCumulativeMtdHappyPath(unittest.TestCase):
    def test_all_new_rows_appended_with_correct_columns(self):
        service = _build_service(cumulative_tab_exists=True, existing_cumulative_keys=[])
        rows = [
            _sample_cum_row(loc="Blaine", ym="2026-04", we="2026-04-05"),
            _sample_cum_row(loc="Hudson", ym="2026-04", we="2026-04-05"),
        ]
        append_to_cumulative_mtd(service, CONFIG, rows, dry_run=False)
        write_calls = [
            c for c in service.spreadsheets().values().append.call_args_list
            if c.kwargs.get("range") == "CUMULATIVE_MTD!A2:V"
            and "body" in c.kwargs
        ]
        self.assertEqual(len(write_calls), 1)
        values = write_calls[0].kwargs["body"]["values"]
        self.assertEqual(len(values), 2)

        first = values[0]
        self.assertEqual(len(first), 22)
        self.assertEqual(first[0], "Blaine")
        self.assertEqual(first[1], "2026-04")
        self.assertEqual(first[2], "2026-04-05")
        self.assertEqual(first[3], "zenoti")
        self.assertEqual(first[21], "zenoti_weekly_pdf")  # source last


class TestCumulativeMtdBootstrap(unittest.TestCase):
    def test_existing_tab_skips_batchupdate(self):
        service = _build_service(cumulative_tab_exists=True)
        _ensure_cumulative_mtd_tab(service, SHEET_ID)
        service.spreadsheets().batchUpdate.assert_not_called()

    def test_missing_tab_creates_with_headers(self):
        service = _build_service(cumulative_tab_exists=False)
        _ensure_cumulative_mtd_tab(service, SHEET_ID)
        bu_calls = service.spreadsheets().batchUpdate.call_args_list
        self.assertTrue(any(
            (c.kwargs or {}).get("body", {}).get("requests", [{}])[0]
            .get("addSheet", {}).get("properties", {}).get("title") == "CUMULATIVE_MTD"
            for c in bu_calls
        ))
        header_writes = [
            c for c in service.spreadsheets().values().update.call_args_list
            if c.kwargs.get("range") == "CUMULATIVE_MTD!A1:V1"
            and "body" in c.kwargs
        ]
        self.assertEqual(len(header_writes), 1)
        self.assertEqual(
            header_writes[0].kwargs["body"]["values"][0],
            CUMULATIVE_MTD_HEADERS,
        )


# ────────────────────────────────────────────────────────────────────────
# STYLISTS_CUMULATIVE_MTD tests
# ────────────────────────────────────────────────────────────────────────

class TestStylistsCumulativeMtdSchema(unittest.TestCase):
    def test_exactly_15_columns(self):
        self.assertEqual(len(STYLISTS_CUMULATIVE_MTD_HEADERS), 15)

    def test_first_four_cols_are_idempotency_key(self):
        self.assertEqual(
            STYLISTS_CUMULATIVE_MTD_HEADERS[:4],
            ["year_month", "week_ending", "name", "loc_name"],
        )

    def test_source_is_last(self):
        self.assertEqual(STYLISTS_CUMULATIVE_MTD_HEADERS[-1], "source")


class TestStylistsCumulativeMtdDryRun(unittest.TestCase):
    def test_dry_run_makes_no_service_calls(self):
        service = MagicMock()
        append_to_stylists_cumulative_mtd(service, CONFIG, [_sample_stylist_cum_row()], dry_run=True)
        service.spreadsheets.return_value.values.return_value.append.assert_not_called()


class TestStylistsCumulativeMtdIdempotency(unittest.TestCase):
    def test_four_col_key_skip(self):
        service = _build_service(
            stylists_cumulative_tab_exists=True,
            existing_stylists_keys=[
                ("2026-04", "2026-04-05", "Heaven Hobbs", "Andover FS"),
            ],
        )
        rows = [
            _sample_stylist_cum_row(name="Heaven Hobbs", loc="Andover FS", ym="2026-04", we="2026-04-05"),  # skip
            _sample_stylist_cum_row(name="Heaven Hobbs", loc="Andover FS", ym="2026-04", we="2026-04-12"),  # diff we -> write
            _sample_stylist_cum_row(name="Heaven Hobbs", loc="Blaine", ym="2026-04", we="2026-04-05"),     # diff loc -> write
            _sample_stylist_cum_row(name="Other Stylist", loc="Andover FS", ym="2026-04", we="2026-04-05"),  # diff name -> write
        ]
        append_to_stylists_cumulative_mtd(service, CONFIG, rows, dry_run=False)
        write_calls = [
            c for c in service.spreadsheets().values().append.call_args_list
            if c.kwargs.get("range") == "STYLISTS_CUMULATIVE_MTD!A2:O"
            and "body" in c.kwargs
        ]
        self.assertEqual(len(write_calls), 1)
        appended = write_calls[0].kwargs["body"]["values"]
        self.assertEqual(len(appended), 3)


class TestStylistsCumulativeMtdHappyPath(unittest.TestCase):
    def test_columns_in_order(self):
        service = _build_service(stylists_cumulative_tab_exists=True, existing_stylists_keys=[])
        append_to_stylists_cumulative_mtd(service, CONFIG, [_sample_stylist_cum_row()], dry_run=False)
        write_calls = [
            c for c in service.spreadsheets().values().append.call_args_list
            if c.kwargs.get("range") == "STYLISTS_CUMULATIVE_MTD!A2:O"
            and "body" in c.kwargs
        ]
        row = write_calls[0].kwargs["body"]["values"][0]
        self.assertEqual(len(row), 15)
        self.assertEqual(row[0], "2026-04")  # year_month
        self.assertEqual(row[1], "2026-04-05")  # week_ending
        self.assertEqual(row[2], "Heaven Hobbs")  # name
        self.assertEqual(row[3], "Andover FS")  # loc_name
        self.assertEqual(row[4], "z001")  # loc_id
        self.assertEqual(row[5], "zenoti")  # platform
        self.assertEqual(row[14], "zenoti_weekly_pdf")  # source


class TestStylistsCumulativeMtdBootstrap(unittest.TestCase):
    def test_missing_tab_creates_with_headers(self):
        service = _build_service(stylists_cumulative_tab_exists=False)
        _ensure_stylists_cumulative_mtd_tab(service, SHEET_ID)
        header_writes = [
            c for c in service.spreadsheets().values().update.call_args_list
            if c.kwargs.get("range") == "STYLISTS_CUMULATIVE_MTD!A1:O1"
            and "body" in c.kwargs
        ]
        self.assertEqual(len(header_writes), 1)
        self.assertEqual(
            header_writes[0].kwargs["body"]["values"][0],
            STYLISTS_CUMULATIVE_MTD_HEADERS,
        )


if __name__ == "__main__":
    unittest.main()
