"""
tests/test_cumulative_pipeline.py
─────────────────────────────────
Integration tests for core/cumulative_pipeline.py — the orchestration
that ties together the cumulative-MTD writer, snapshot reader, and
differencing math.

Each test mocks the Google Sheets API service and verifies:
  - Snapshot writes happen (or are skipped under dry_run)
  - Prior-snapshot reads return the right rows
  - Differencing produces correct weekly records (Week 1 vs Week 2+)
  - loc_id is preserved end-to-end
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from core.cumulative_pipeline import snapshot_and_difference


CONFIG = {"sheet_id": "test_sheet"}


def _build_cumulative_location(
    *, loc_name: str = "Blaine", loc_id: str = "z002",
    week_ending: str = "2026-04-12", guests: int = 562,
    service: float = 32132.5, product: float = 4225.0, prod_hours: float = 574.0,
    wax_count: int = 80, wax: float = 1440.0, color: float = 10866.0,
    treat_count: int = 100, treat: float = 1800.0,
    platform: str = "zenoti",
) -> dict:
    """Build a CURRENT-tab style location dict (post-data_source.load_location_data)."""
    total = service + product
    return {
        "loc_name": loc_name, "loc_id": loc_id,
        "week_ending": week_ending, "platform": platform,
        "guests": guests, "total_sales": total,
        "service": service, "product": product,
        "product_pct": product / total if total else 0,
        "ppg": product / guests if guests else 0,
        "pph": service / prod_hours if prod_hours else 0,
        "avg_ticket": total / guests if guests else 0,
        "prod_hours": prod_hours,
        "wax_count": wax_count, "wax": wax,
        "wax_pct": wax_count / guests if guests else 0,
        "color": color, "color_pct": color / service if service else 0,
        "treat_count": treat_count, "treat": treat,
        "treat_pct": treat_count / guests if guests else 0,
    }


class TestSnapshotAndDifferenceWeek1(unittest.TestCase):
    """Week 1 of month — no prior snapshots — weekly should equal current."""

    def test_week_1_no_priors(self):
        cur_loc = _build_cumulative_location(week_ending="2026-04-05")  # Sun of Week 1
        # Mock service: no existing CUMULATIVE_MTD rows for this month
        with patch("core.cumulative_pipeline.append_to_cumulative_mtd") as m_append_loc, \
             patch("core.cumulative_pipeline.append_to_stylists_cumulative_mtd") as m_append_styl, \
             patch("core.cumulative_pipeline.read_cumulative_mtd_snapshots", return_value=[]) as m_read_loc, \
             patch("core.cumulative_pipeline.read_stylists_cumulative_mtd_snapshots", return_value=[]) as m_read_styl:
            weekly_locs, weekly_styls = snapshot_and_difference(
                MagicMock(), CONFIG,
                cumulative_locations=[cur_loc], cumulative_stylists=[],
                source_label="current_tab", dry_run=False,
            )

        # Snapshot write was attempted
        self.assertEqual(m_append_loc.call_count, 1)
        # Read for differencing happened
        m_read_loc.assert_called_once_with(unittest.mock.ANY, CONFIG, "2026-04")
        # Weekly should equal current (no prior → Week 1)
        self.assertEqual(len(weekly_locs), 1)
        w = weekly_locs[0]
        self.assertEqual(w["loc_name"], "Blaine")
        self.assertEqual(w["loc_id"], "z002")  # preserved from original
        self.assertEqual(w["week_ending"], "2026-04-05")
        self.assertEqual(w["guests"], 562)
        self.assertAlmostEqual(w["service"], 32132.5, places=2)


class TestSnapshotAndDifferenceWeek2(unittest.TestCase):
    """Week 2 — has Week 1 prior — should produce differenced weekly."""

    def test_week_2_diffs_against_prior(self):
        cur_loc = _build_cumulative_location(
            week_ending="2026-04-12", guests=562, service=32132.5, prod_hours=574.0,
            product=4225.0, wax_count=80, wax=1440.0, color=10866.0,
            treat_count=100, treat=1800.0,
        )
        # Existing CUMULATIVE_MTD row for Week 1
        prior_row = {
            "loc_name": "Blaine", "year_month": "2026-04", "week_ending": "2026-04-05",
            "platform": "zenoti",
            "guests": 280, "total_sales": 18000.0, "service": 16000.0,
            "product": 2000.0, "product_pct": 0.111, "ppg": 7.14,
            "pph": 57.14, "avg_ticket": 64.29, "prod_hours": 280.0,
            "wax_count": 40, "wax": 720.0, "wax_pct": 0.143,
            "color": 5300.0, "color_pct": 0.331,
            "treat_count": 50, "treat": 900.0, "treat_pct": 0.179,
            "source": "current_tab",
        }
        with patch("core.cumulative_pipeline.append_to_cumulative_mtd"), \
             patch("core.cumulative_pipeline.append_to_stylists_cumulative_mtd"), \
             patch("core.cumulative_pipeline.read_cumulative_mtd_snapshots", return_value=[prior_row]), \
             patch("core.cumulative_pipeline.read_stylists_cumulative_mtd_snapshots", return_value=[]):
            weekly_locs, _ = snapshot_and_difference(
                MagicMock(), CONFIG,
                cumulative_locations=[cur_loc], cumulative_stylists=[],
                source_label="current_tab", dry_run=False,
            )

        self.assertEqual(len(weekly_locs), 1)
        w = weekly_locs[0]
        # Week 2 alone = current(562) - prior(280) = 282
        self.assertEqual(w["guests"], 282)
        self.assertAlmostEqual(w["service"], 32132.5 - 16000.0, places=2)
        self.assertAlmostEqual(w["product"], 4225.0 - 2000.0, places=2)
        self.assertAlmostEqual(w["prod_hours"], 574.0 - 280.0, places=2)
        # KPIs recomputed
        self.assertAlmostEqual(w["pph"], (32132.5 - 16000.0) / (574.0 - 280.0), places=2)
        # Provenance + identity preserved
        self.assertEqual(w["loc_name"], "Blaine")
        self.assertEqual(w["loc_id"], "z002")
        self.assertEqual(w["week_ending"], "2026-04-12")


class TestSnapshotAndDifferenceDryRun(unittest.TestCase):
    def test_dry_run_skips_snapshot_writes(self):
        cur_loc = _build_cumulative_location(week_ending="2026-04-05")
        with patch("core.cumulative_pipeline.append_to_cumulative_mtd") as m_append_loc, \
             patch("core.cumulative_pipeline.append_to_stylists_cumulative_mtd"), \
             patch("core.cumulative_pipeline.read_cumulative_mtd_snapshots", return_value=[]), \
             patch("core.cumulative_pipeline.read_stylists_cumulative_mtd_snapshots", return_value=[]):
            weekly_locs, _ = snapshot_and_difference(
                MagicMock(), CONFIG,
                cumulative_locations=[cur_loc], cumulative_stylists=[],
                source_label="current_tab", dry_run=True,
            )

        # append_to_cumulative_mtd is still called but with dry_run=True
        self.assertEqual(m_append_loc.call_count, 1)
        self.assertTrue(m_append_loc.call_args.kwargs.get("dry_run", False))
        # Differencing still produces output
        self.assertEqual(len(weekly_locs), 1)


class TestSnapshotAndDifferenceEmpty(unittest.TestCase):
    def test_empty_locations_returns_empty(self):
        weekly_locs, weekly_styls = snapshot_and_difference(
            MagicMock(), CONFIG,
            cumulative_locations=[], cumulative_stylists=[],
            source_label="current_tab", dry_run=False,
        )
        self.assertEqual(weekly_locs, [])
        self.assertEqual(weekly_styls, [])


class TestSnapshotAndDifferenceMultipleLocations(unittest.TestCase):
    def test_per_location_prior_lookup(self):
        """3 locations, only 1 has a prior — others treated as Week 1."""
        locs = [
            _build_cumulative_location(loc_name="Andover FS", loc_id="z001",
                                        week_ending="2026-04-12", guests=300, service=18000, prod_hours=300),
            _build_cumulative_location(loc_name="Blaine", loc_id="z002",
                                        week_ending="2026-04-12", guests=562, service=32132.5, prod_hours=574),
            _build_cumulative_location(loc_name="Hudson", loc_id="z007",
                                        week_ending="2026-04-12", guests=600, service=35000, prod_hours=550),
        ]
        # Only Blaine has a prior
        priors = [
            {"loc_name": "Blaine", "year_month": "2026-04", "week_ending": "2026-04-05",
             "platform": "zenoti", "guests": 280, "service": 16000, "prod_hours": 280,
             "total_sales": 18000, "product": 2000, "product_pct": 0.11,
             "ppg": 7.14, "pph": 57.14, "avg_ticket": 64.29,
             "wax_count": 40, "wax": 720, "wax_pct": 0.143,
             "color": 5300, "color_pct": 0.331,
             "treat_count": 50, "treat": 900, "treat_pct": 0.179,
             "source": "current_tab"},
        ]
        with patch("core.cumulative_pipeline.append_to_cumulative_mtd"), \
             patch("core.cumulative_pipeline.append_to_stylists_cumulative_mtd"), \
             patch("core.cumulative_pipeline.read_cumulative_mtd_snapshots", return_value=priors), \
             patch("core.cumulative_pipeline.read_stylists_cumulative_mtd_snapshots", return_value=[]):
            weekly_locs, _ = snapshot_and_difference(
                MagicMock(), CONFIG,
                cumulative_locations=locs, cumulative_stylists=[],
                source_label="current_tab", dry_run=False,
            )

        self.assertEqual(len(weekly_locs), 3)
        # Order preserved
        self.assertEqual([w["loc_name"] for w in weekly_locs], ["Andover FS", "Blaine", "Hudson"])

        andover = weekly_locs[0]
        self.assertEqual(andover["guests"], 300)  # no prior → current
        self.assertAlmostEqual(andover["service"], 18000, places=2)

        blaine = weekly_locs[1]
        self.assertEqual(blaine["guests"], 562 - 280)  # differenced
        self.assertAlmostEqual(blaine["service"], 32132.5 - 16000, places=2)

        hudson = weekly_locs[2]
        self.assertEqual(hudson["guests"], 600)  # no prior → current
        self.assertAlmostEqual(hudson["service"], 35000, places=2)


class TestSnapshotAndDifferenceMissingWeekEnding(unittest.TestCase):
    def test_no_week_ending_returns_passthrough(self):
        """If week_ending is missing/malformed, can't derive year_month → pass through unchanged."""
        cur_loc = _build_cumulative_location(week_ending="")  # no week_ending
        with patch("core.cumulative_pipeline.append_to_cumulative_mtd") as m_append, \
             patch("core.cumulative_pipeline.read_cumulative_mtd_snapshots") as m_read:
            weekly_locs, _ = snapshot_and_difference(
                MagicMock(), CONFIG,
                cumulative_locations=[cur_loc], cumulative_stylists=[],
                source_label="current_tab", dry_run=False,
            )
        # Should NOT have attempted any reads/writes
        m_append.assert_not_called()
        m_read.assert_not_called()
        # Should return current as-is
        self.assertEqual(len(weekly_locs), 1)
        self.assertEqual(weekly_locs[0]["loc_name"], "Blaine")


if __name__ == "__main__":
    unittest.main()
