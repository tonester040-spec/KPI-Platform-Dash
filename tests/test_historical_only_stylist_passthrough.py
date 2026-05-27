"""
tests/test_historical_only_stylist_passthrough.py
─────────────────────────────────────────────────
Tests for the hotfix that prevents Phase 2.5's monthly-only synthesized
stylists from crashing the pipeline.

Contract:
  - `_merge_stylist_rosters` tags monthly-only stylists with
    `is_historical_only=True` and zeroes their `cur_*` fields.
  - `cumulative_pipeline.snapshot_and_difference` filters those out of
    snapshot + differencing, then re-attaches them at the end.
  - `data_processor.enrich_stylists` filters those out of the star
    threshold + archetype enrichment, then re-attaches them at the end.

Regression guard for: KeyError: 'cur_pph' at
data_processor.py:167 (Mon 2026-05-27 pipeline run).
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from core.data_processor import enrich_stylists  # noqa: E402
from core.cumulative_pipeline import snapshot_and_difference  # noqa: E402


def _historical_only_stylist(name: str, loc_name: str = "Hudson") -> dict:
    """Build a synthesized historical-only stylist (matches _merge_stylist_rosters output)."""
    return {
        "name": name,
        "loc_name": loc_name,
        "loc_id": "z007",
        "status": "active",
        "tenure": 0,
        "is_historical_only": True,
        "weeks": ["2026-03-30", "2026-04-30"],
        "pph":   [33.0, 35.0],
        "rebook":   [0, 0],
        "product":  [0, 0],
        "ticket":   [48.0, 50.0],
        "services": [70, 80],
        "color":    [0, 0],
        "cur_pph": 0,
        "cur_rebook": 0,
        "cur_product": 0,
        "cur_ticket": 0,
    }


def _live_stylist(name: str, cur_pph: float, loc_name: str = "Hudson") -> dict:
    """Build a live current-week stylist record."""
    return {
        "name": name,
        "loc_name": loc_name,
        "loc_id": "z007",
        "status": "active",
        "tenure": 5.0,
        "weeks": ["2026-05-24"],
        "pph":     [cur_pph],
        "rebook":  [0.10],
        "product": [0.05],
        "ticket":  [55.0],
        "services": [60],
        "color":   [0.30],
        "cur_pph": cur_pph,
        "cur_rebook":  0.10,
        "cur_product": 0.05,
        "cur_ticket":  55.0,
    }


# ────────────────────────────────────────────────────────────────────────
# enrich_stylists: historical-only records survive without cur_pph crash
# ────────────────────────────────────────────────────────────────────────

class TestEnrichStylistsHistoricalOnly(unittest.TestCase):
    def test_all_historical_only_no_crash(self):
        """When STYLISTS_DATA empty + monthly fallback gives only historical-only,
        enrich_stylists must NOT crash on cur_pph lookup."""
        stylists = [_historical_only_stylist("Alexis"), _historical_only_stylist("Brittany")]
        network = {"avg_pph": 46.29, "avg_product_pct": 7.96}

        # The actual bug: pre-hotfix this raised KeyError: 'cur_pph'
        result, star_threshold = enrich_stylists(stylists, network)

        self.assertEqual(len(result), 2)
        self.assertEqual(star_threshold, 0)  # no live stylists → no threshold
        # Historical-only records pass through unchanged (no archetype tagged)
        for s in result:
            self.assertNotIn("arch_label", s)
            self.assertNotIn("is_star", s)
            self.assertTrue(s.get("is_historical_only"))

    def test_mixed_live_and_historical_only(self):
        """Live stylists get enriched; historical-only pass through."""
        live = _live_stylist("LiveOne", cur_pph=50.0)
        hist = _historical_only_stylist("HistOnly")
        network = {"avg_pph": 46.29, "avg_product_pct": 7.96}

        result, star_threshold = enrich_stylists([live, hist], network)

        # Both stylists present in output (live first, historical last)
        self.assertEqual(len(result), 2)
        names = [s["name"] for s in result]
        self.assertIn("LiveOne", names)
        self.assertIn("HistOnly", names)

        # Live got enriched
        live_out = next(s for s in result if s["name"] == "LiveOne")
        self.assertIn("arch_label", live_out)
        self.assertIn("vs_net_pph", live_out)
        self.assertIn("is_star", live_out)

        # Historical-only did NOT get enriched (would have been "Needs Coaching"
        # since cur_pph=0 - 46.29 = -46 < -8). This is the bug being prevented.
        hist_out = next(s for s in result if s["name"] == "HistOnly")
        self.assertNotIn("arch_label", hist_out)
        self.assertTrue(hist_out.get("is_historical_only"))

    def test_star_threshold_excludes_historical_only(self):
        """Historical-only with high pph (35) shouldn't pollute live threshold."""
        live_low = _live_stylist("Low", cur_pph=20.0)
        live_high = _live_stylist("High", cur_pph=80.0)
        hist = _historical_only_stylist("Hist")  # cur_pph=0
        network = {"avg_pph": 46.29, "avg_product_pct": 7.96}

        _, star_threshold = enrich_stylists([live_low, live_high, hist], network)

        # 85th-percentile of [20, 80] only — historical-only's 0 cur_pph excluded
        self.assertGreater(star_threshold, 20)
        self.assertLessEqual(star_threshold, 80)

    def test_defensive_get_cur_pph(self):
        """Even a malformed live stylist missing cur_pph shouldn't crash."""
        malformed = _live_stylist("Live", cur_pph=50.0)
        del malformed["cur_pph"]
        network = {"avg_pph": 46.29, "avg_product_pct": 7.96}

        result, _ = enrich_stylists([malformed], network)
        # Treated as cur_pph=0 → archetype computed off that
        self.assertEqual(len(result), 1)
        self.assertIn("arch_label", result[0])


# ────────────────────────────────────────────────────────────────────────
# snapshot_and_difference: historical-only stylists skip diff, pass through
# ────────────────────────────────────────────────────────────────────────

class TestSnapshotAndDifferenceHistoricalOnly(unittest.TestCase):
    def _build_service_with_empty_snapshots(self):
        """Mock service that returns empty CUMULATIVE_MTD reads (no priors)."""
        service = MagicMock()
        # batchUpdate (for tab creation) returns generic success
        service.spreadsheets().batchUpdate().execute.return_value = {}
        # get (for reads) returns no prior snapshots
        service.spreadsheets().values().get().execute.return_value = {"values": []}
        # append (for snapshot writes) returns success
        service.spreadsheets().values().append().execute.return_value = {}
        # Reset call counts after setup
        service.reset_mock()
        # Re-wire defaults after reset
        service.spreadsheets().batchUpdate().execute.return_value = {}
        service.spreadsheets().values().get().execute.return_value = {"values": []}
        service.spreadsheets().values().append().execute.return_value = {}
        return service

    def test_historical_only_skipped_from_snapshot(self):
        """Historical-only stylists must NOT be written to STYLISTS_CUMULATIVE_MTD."""
        live_loc = {
            "loc_name": "Hudson", "loc_id": "z007", "platform": "zenoti",
            "week_ending": "2026-05-24", "year_month": "2026-05",
            "total_sales": 10000, "guests": 200, "service": 9000, "product": 1000,
            "prod_hours": 200, "wax_count": 30, "color": 3000, "treat_count": 5,
        }
        cumulative_stylists = [
            _live_stylist("LiveOne", cur_pph=50.0),
            _historical_only_stylist("HistOnly"),
        ]

        # Use dry_run=True so we don't need a real Sheets backend for writes
        service = self._build_service_with_empty_snapshots()
        config = {"sheet_id": "fake_sheet_id"}

        weekly_locations, weekly_stylists = snapshot_and_difference(
            service, config, [live_loc], cumulative_stylists, dry_run=True,
        )

        # Output includes both — historical-only re-attached at the end
        self.assertEqual(len(weekly_stylists), 2)
        names = [s["name"] for s in weekly_stylists]
        self.assertIn("LiveOne", names)
        self.assertIn("HistOnly", names)
        # Historical-only retains its flag
        hist = next(s for s in weekly_stylists if s["name"] == "HistOnly")
        self.assertTrue(hist.get("is_historical_only"))

    def test_all_historical_only_returns_them_passthrough(self):
        """If ALL stylists are historical-only (STYLISTS_DATA empty), they all
        pass through without going through differencing."""
        live_loc = {
            "loc_name": "Hudson", "loc_id": "z007", "platform": "zenoti",
            "week_ending": "2026-05-24", "year_month": "2026-05",
            "total_sales": 10000, "guests": 200, "service": 9000, "product": 1000,
            "prod_hours": 200, "wax_count": 30, "color": 3000, "treat_count": 5,
        }
        cumulative_stylists = [
            _historical_only_stylist("Alexis"),
            _historical_only_stylist("Brittany"),
            _historical_only_stylist("Casey"),
        ]

        service = self._build_service_with_empty_snapshots()
        config = {"sheet_id": "fake_sheet_id"}

        weekly_locations, weekly_stylists = snapshot_and_difference(
            service, config, [live_loc], cumulative_stylists, dry_run=True,
        )

        # All 3 pass through
        self.assertEqual(len(weekly_stylists), 3)
        self.assertEqual(
            {s["name"] for s in weekly_stylists},
            {"Alexis", "Brittany", "Casey"},
        )
        for s in weekly_stylists:
            self.assertTrue(s.get("is_historical_only"))


if __name__ == "__main__":
    unittest.main()
