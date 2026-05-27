"""
tests/test_data_source_cumulative_mtd.py
────────────────────────────────────────
Tests for the cumulative-MTD reader helpers added to core/data_source.py:
  - read_cumulative_mtd_snapshots
  - read_stylists_cumulative_mtd_snapshots
  - derive_year_month
  - find_latest_priors_by_location
  - find_latest_priors_by_stylist

Tab-missing case is critical (first-ever pipeline run) — must return [] not raise.
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from core.data_source import (  # noqa: E402
    derive_year_month,
    find_latest_priors_by_location,
    find_latest_priors_by_stylist,
    read_cumulative_mtd_snapshots,
    read_stylists_cumulative_mtd_snapshots,
)


CONFIG = {"sheet_id": "test_sheet"}


# ────────────────────────────────────────────────────────────────────────
# derive_year_month
# ────────────────────────────────────────────────────────────────────────

class TestDeriveYearMonth(unittest.TestCase):
    def test_typical(self):
        self.assertEqual(derive_year_month("2026-04-12"), "2026-04")

    def test_month_end(self):
        self.assertEqual(derive_year_month("2026-04-30"), "2026-04")

    def test_year_end(self):
        self.assertEqual(derive_year_month("2026-12-31"), "2026-12")

    def test_empty(self):
        self.assertEqual(derive_year_month(""), "")

    def test_already_year_month(self):
        # 7 chars is already YYYY-MM; pass through
        self.assertEqual(derive_year_month("2026-04"), "2026-04")

    def test_too_short_returns_empty(self):
        self.assertEqual(derive_year_month("2026"), "")


# ────────────────────────────────────────────────────────────────────────
# read_cumulative_mtd_snapshots
# ────────────────────────────────────────────────────────────────────────

class TestReadCumulativeMtdSnapshots(unittest.TestCase):
    def test_missing_tab_returns_empty(self):
        """First-ever pipeline run: tab doesn't exist; reader must not crash."""
        service = MagicMock()
        # Simulate API exception (tab not found)
        service.spreadsheets().values().get(
            spreadsheetId=CONFIG["sheet_id"], range="CUMULATIVE_MTD!A2:V",
        ).execute.side_effect = Exception("Unable to parse range")
        result = read_cumulative_mtd_snapshots(service, CONFIG, "2026-04")
        self.assertEqual(result, [])

    def test_empty_tab_returns_empty(self):
        service = MagicMock()
        service.spreadsheets.return_value.values.return_value.get.return_value.execute.return_value = {
            "values": [],
        }
        result = read_cumulative_mtd_snapshots(service, CONFIG, "2026-04")
        self.assertEqual(result, [])

    def test_filters_by_year_month(self):
        service = MagicMock()
        service.spreadsheets.return_value.values.return_value.get.return_value.execute.return_value = {
            "values": [
                ["Blaine", "2026-04", "2026-04-05", "zenoti", "280", "16500.0",
                 "15800.0", "700.0", "0.042", "2.5", "55.0", "58.9", "287.0",
                 "40", "720.0", "0.143", "5300.0", "0.335", "50", "900.0", "0.179",
                 "zenoti_weekly_pdf"],
                ["Blaine", "2026-03", "2026-03-15", "zenoti", "200", "12000.0",
                 "11000.0", "1000.0", "0.083", "5.0", "40.0", "60.0", "275.0",
                 "30", "600.0", "0.15", "3500.0", "0.32", "40", "800.0", "0.20",
                 "zenoti_weekly_pdf"],
                ["Hudson", "2026-04", "2026-04-05", "zenoti", "350", "20000.0",
                 "19000.0", "1000.0", "0.05", "2.85", "55.0", "57.0", "350.0",
                 "50", "900.0", "0.143", "6500.0", "0.342", "60", "1100.0", "0.171",
                 "zenoti_weekly_pdf"],
            ],
        }
        result = read_cumulative_mtd_snapshots(service, CONFIG, "2026-04")
        self.assertEqual(len(result), 2)
        self.assertEqual({r["loc_name"] for r in result}, {"Blaine", "Hudson"})
        # Ensure values parsed correctly
        blaine = next(r for r in result if r["loc_name"] == "Blaine")
        self.assertEqual(blaine["guests"], 280)
        self.assertAlmostEqual(blaine["service"], 15800.0, places=2)
        self.assertEqual(blaine["week_ending"], "2026-04-05")

    def test_skips_rows_with_empty_loc(self):
        service = MagicMock()
        service.spreadsheets.return_value.values.return_value.get.return_value.execute.return_value = {
            "values": [
                ["", "2026-04", "2026-04-05", "zenoti"],  # empty loc, skip
                ["Blaine", "2026-04", "2026-04-05", "zenoti", "280", "16500",
                 "15800", "700", "0.042", "2.5", "55", "58.9", "287",
                 "40", "720", "0.143", "5300", "0.335", "50", "900", "0.179",
                 "zenoti_weekly_pdf"],
            ],
        }
        result = read_cumulative_mtd_snapshots(service, CONFIG, "2026-04")
        self.assertEqual(len(result), 1)


# ────────────────────────────────────────────────────────────────────────
# read_stylists_cumulative_mtd_snapshots
# ────────────────────────────────────────────────────────────────────────

class TestReadStylistsCumulativeMtdSnapshots(unittest.TestCase):
    def test_missing_tab_returns_empty(self):
        service = MagicMock()
        service.spreadsheets().values().get(
            spreadsheetId=CONFIG["sheet_id"], range="STYLISTS_CUMULATIVE_MTD!A2:O",
        ).execute.side_effect = Exception("Unable to parse range")
        result = read_stylists_cumulative_mtd_snapshots(service, CONFIG, "2026-04")
        self.assertEqual(result, [])

    def test_filters_by_year_month(self):
        service = MagicMock()
        service.spreadsheets.return_value.values.return_value.get.return_value.execute.return_value = {
            "values": [
                ["2026-04", "2026-04-05", "Heaven Hobbs", "Andover FS", "z001", "zenoti",
                 "18", "18", "850.0", "40.0", "49.4", "35.0", "2.22", "24.5", "zenoti_weekly_pdf"],
                ["2026-03", "2026-03-29", "Heaven Hobbs", "Andover FS", "z001", "zenoti",
                 "70", "70", "3200.0", "150.0", "48.0", "33.0", "2.14", "97.0", "zenoti_weekly_pdf"],
            ],
        }
        result = read_stylists_cumulative_mtd_snapshots(service, CONFIG, "2026-04")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["name"], "Heaven Hobbs")
        self.assertEqual(result[0]["invoices"], 18)
        self.assertAlmostEqual(result[0]["net_service"], 850.0, places=2)


# ────────────────────────────────────────────────────────────────────────
# find_latest_priors_by_location
# ────────────────────────────────────────────────────────────────────────

class TestFindLatestPriorsByLocation(unittest.TestCase):
    def test_picks_latest_strict_prior(self):
        snapshots = [
            {"loc_name": "Blaine", "week_ending": "2026-04-05"},
            {"loc_name": "Blaine", "week_ending": "2026-04-12"},
            {"loc_name": "Blaine", "week_ending": "2026-04-19"},  # current
            {"loc_name": "Hudson", "week_ending": "2026-04-12"},
        ]
        priors = find_latest_priors_by_location(snapshots, current_week_ending="2026-04-19")
        # Blaine's latest prior is 2026-04-12 (NOT 2026-04-19 which is current)
        self.assertEqual(priors["Blaine"]["week_ending"], "2026-04-12")
        # Hudson's only prior is 2026-04-12
        self.assertEqual(priors["Hudson"]["week_ending"], "2026-04-12")

    def test_no_prior_returns_empty(self):
        """Week 1: only the current snapshot exists, no priors."""
        snapshots = [
            {"loc_name": "Blaine", "week_ending": "2026-04-05"},
        ]
        priors = find_latest_priors_by_location(snapshots, current_week_ending="2026-04-05")
        self.assertEqual(priors, {})

    def test_skips_future_snapshots(self):
        """Should never return a snapshot dated >= current_week_ending."""
        snapshots = [
            {"loc_name": "Blaine", "week_ending": "2026-04-12"},
            {"loc_name": "Blaine", "week_ending": "2026-04-19"},
            {"loc_name": "Blaine", "week_ending": "2026-04-26"},
        ]
        priors = find_latest_priors_by_location(snapshots, current_week_ending="2026-04-19")
        # Only 2026-04-12 should be returned (not 04-19 = current, not 04-26 = future)
        self.assertEqual(priors["Blaine"]["week_ending"], "2026-04-12")

    def test_skips_snapshots_with_empty_fields(self):
        snapshots = [
            {"loc_name": "", "week_ending": "2026-04-05"},
            {"loc_name": "Blaine", "week_ending": ""},
            {"loc_name": "Blaine", "week_ending": "2026-04-05"},
        ]
        priors = find_latest_priors_by_location(snapshots, current_week_ending="2026-04-12")
        self.assertEqual(set(priors.keys()), {"Blaine"})


# ────────────────────────────────────────────────────────────────────────
# find_latest_priors_by_stylist
# ────────────────────────────────────────────────────────────────────────

class TestFindLatestPriorsByStylist(unittest.TestCase):
    def test_keyed_by_name_and_loc(self):
        snapshots = [
            {"name": "A", "loc_name": "Hudson", "week_ending": "2026-04-05"},
            {"name": "A", "loc_name": "Andover FS", "week_ending": "2026-04-05"},
            {"name": "A", "loc_name": "Hudson", "week_ending": "2026-04-12"},
            {"name": "B", "loc_name": "Hudson", "week_ending": "2026-04-12"},
        ]
        priors = find_latest_priors_by_stylist(snapshots, current_week_ending="2026-04-19")
        # 3 stylist-location pairs: (A, Hudson) → 2026-04-12, (A, Andover) → 2026-04-05, (B, Hudson) → 2026-04-12
        self.assertEqual(len(priors), 3)
        self.assertEqual(priors[("A", "Hudson")]["week_ending"], "2026-04-12")
        self.assertEqual(priors[("A", "Andover FS")]["week_ending"], "2026-04-05")
        self.assertEqual(priors[("B", "Hudson")]["week_ending"], "2026-04-12")


if __name__ == "__main__":
    unittest.main()
