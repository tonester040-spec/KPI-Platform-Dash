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


class TestSnapshotAndDifferenceLiveStylistReshape(unittest.TestCase):
    """Regression: differenced live-stylist records must keep history-array
    + cur_* shape so data_processor.enrich_stylists doesn't crash on
    len(pph)."""

    def _live_stylist(self, name="Alice", loc="Blaine", loc_id="z002"):
        """A live stylist dict as produced by data_source.load_stylist_data
        — arrays for history, cur_* for current-week scalars."""
        return {
            "name": name, "loc_name": loc, "loc_id": loc_id,
            "status": "active", "tenure": 3.0,
            "weeks":    ["2026-04-05", "2026-04-12"],
            "pph":      [55.0, 62.0],          # latest entry = cumulative-MTD
            "rebook":   [40.0, 45.0],
            "product":  [12.5, 13.5],
            "ticket":   [60.0, 70.0],
            "services": [10, 12],
            "color":    [0.30, 0.32],
            "cur_pph":     62.0,
            "cur_rebook":  45.0,
            "cur_product": 13.5,
            "cur_ticket":  70.0,
            # invoices/guests/net_service/net_product/production_hours are
            # not always present on the live shape, but the snapshot row
            # converter pulls them via .get() with 0 default — that's fine.
            "invoices": 25, "guests": 25, "net_service": 1500.0,
            "net_product": 300.0, "production_hours": 35.0,
            "platform": "zenoti",
        }

    def test_live_stylist_keeps_history_arrays_and_cur_fields(self):
        """After differencing, pph + ticket must still be lists (latest entry
        replaced with the differenced true-weekly), and cur_pph / cur_ticket
        must be scalars set to the differenced value."""
        cur_loc = _build_cumulative_location(week_ending="2026-04-12")
        live = self._live_stylist()
        # Prior stylist snapshot for Week 1 (cumulative-MTD as of 2026-04-05)
        prior_styl = {
            "year_month": "2026-04", "week_ending": "2026-04-05",
            "name": "Alice", "loc_name": "Blaine", "loc_id": "z002",
            "platform": "zenoti",
            "invoices": 12, "guests": 12, "net_service": 600.0,
            "net_product": 120.0, "production_hours": 15.0,
            "avg_ticket": 60.0, "pph": 40.0, "ppg": 10.0,
            "source": "current_tab",
        }
        with patch("core.cumulative_pipeline.append_to_cumulative_mtd"), \
             patch("core.cumulative_pipeline.append_to_stylists_cumulative_mtd"), \
             patch("core.cumulative_pipeline.read_cumulative_mtd_snapshots", return_value=[]), \
             patch(
                 "core.cumulative_pipeline.read_stylists_cumulative_mtd_snapshots",
                 return_value=[prior_styl],
             ):
            _, weekly_styls = snapshot_and_difference(
                MagicMock(), CONFIG,
                cumulative_locations=[cur_loc],
                cumulative_stylists=[live],
                source_label="current_tab", dry_run=False,
            )

        self.assertEqual(len(weekly_styls), 1)
        s = weekly_styls[0]

        # Shape contract — these are what enrich_stylists / sheets_writer /
        # dashboard_builder all rely on:
        self.assertIsInstance(s["pph"], list, "pph must remain a list")
        self.assertIsInstance(s["rebook"], list, "rebook must remain a list")
        self.assertIsInstance(s["ticket"], list, "ticket must remain a list")
        self.assertIsInstance(s.get("cur_pph"), (int, float))
        self.assertIsInstance(s.get("cur_ticket"), (int, float))
        self.assertIsInstance(s.get("cur_product"), (int, float))
        self.assertIsInstance(s.get("cur_rebook"), (int, float))

        # History preserved (length-2 arrays)
        self.assertEqual(len(s["pph"]), 2)
        self.assertEqual(len(s["ticket"]), 2)
        self.assertEqual(len(s["product"]), 2)
        # Latest pph entry replaced with differenced true-weekly
        # ((1500 - 600) / (35 - 15) = 900 / 20 = 45.0)
        self.assertAlmostEqual(s["pph"][-1], 45.0, places=2)
        # Older entry untouched
        self.assertEqual(s["pph"][0], 55.0)
        # cur_pph mirrors the differenced value
        self.assertAlmostEqual(s["cur_pph"], 45.0, places=2)
        # cur_product comes from the differenced product_pct, scaled to percent
        # (Karissa golden rule: product_pct = net_product / (net_service + net_product))
        # Differenced: net_service=900, net_product=180 → product_pct = 180/1080 ≈ 0.1667
        # Scaled to percent: ≈ 16.67
        self.assertAlmostEqual(s["cur_product"], 180.0 / 1080.0 * 100, places=2)
        # product array's latest entry replaced with the differenced value, older entry untouched
        self.assertAlmostEqual(s["product"][-1], 180.0 / 1080.0 * 100, places=2)
        self.assertEqual(s["product"][0], 12.5)
        # cur_rebook preserved from original input (no per-stylist rebook source)
        self.assertEqual(s["cur_rebook"], 45.0)

        # Static identity preserved
        self.assertEqual(s["name"], "Alice")
        self.assertEqual(s["loc_name"], "Blaine")
        self.assertEqual(s["loc_id"], "z002")
        # is_historical_only must NOT be set on a live stylist — otherwise
        # enrich_stylists would skip it.
        self.assertFalse(s.get("is_historical_only", False))

    def test_live_stylist_week_one_no_prior(self):
        """Week 1 of month: no prior snapshot → differenced = current as-is.
        Shape contract still applies."""
        cur_loc = _build_cumulative_location(week_ending="2026-04-05")
        live = self._live_stylist()
        # Trim history to just the current entry so we exercise the
        # single-entry array path
        live["weeks"] = ["2026-04-05"]
        live["pph"] = [62.0]
        live["rebook"] = [45.0]
        live["product"] = [13.5]
        live["ticket"] = [70.0]
        live["services"] = [12]
        live["color"] = [0.32]

        with patch("core.cumulative_pipeline.append_to_cumulative_mtd"), \
             patch("core.cumulative_pipeline.append_to_stylists_cumulative_mtd"), \
             patch("core.cumulative_pipeline.read_cumulative_mtd_snapshots", return_value=[]), \
             patch(
                 "core.cumulative_pipeline.read_stylists_cumulative_mtd_snapshots",
                 return_value=[],
             ):
            _, weekly_styls = snapshot_and_difference(
                MagicMock(), CONFIG,
                cumulative_locations=[cur_loc],
                cumulative_stylists=[live],
                source_label="current_tab", dry_run=False,
            )

        self.assertEqual(len(weekly_styls), 1)
        s = weekly_styls[0]
        self.assertIsInstance(s["pph"], list)
        self.assertEqual(len(s["pph"]), 1)
        # Week 1: no prior → differenced pph = net_service / production_hours
        # = 1500 / 35 = ~42.86
        self.assertAlmostEqual(s["pph"][-1], 1500.0 / 35.0, places=2)
        self.assertAlmostEqual(s["cur_pph"], 1500.0 / 35.0, places=2)

    def test_empty_placeholder_weekly_falls_back_to_monthly_history(self):
        """Bug fix (2026-05-27): when STYLISTS_DATA contains all-zero placeholder
        rows (Karissa's team hasn't entered cumulative-MTD per-stylist data yet),
        differencing gives all-zero "weekly" values. We must NOT overwrite the
        last array entry with these zeros — the dashboard's
        `s.cur_pph = s.pph[s.pph.length-1]` would collapse to 0 and the whole
        stylist table would render as $0.00.

        Expected behavior: drop the trailing zero-placeholder entry from each
        array so cur_pph falls back to the most recent monthly value
        (e.g. May $42.16 for Aleksis), and the dashboard shows real data.
        """
        cur_loc = _build_cumulative_location(week_ending="2026-04-12")
        # Stylist with monthly history backfilled BUT placeholder weekly
        # (services tracked, every other metric zero — matches the actual
        # STYLISTS_DATA state on 2026-05-27).
        live_with_placeholder = {
            "name": "Aleksis", "loc_name": "Blaine", "loc_id": "z002",
            "status": "active", "tenure": 0,
            "weeks":    ["2026-03-31", "2026-04-12"],   # monthly + placeholder weekly
            "pph":      [42.16, 0.0],                    # monthly + zero placeholder
            "rebook":   [0, 0.0],
            "product":  [6.78, 0.0],
            "ticket":   [55.78, 0.0],
            "services": [15, 15],                        # services has values both weeks
            "color":    [0, 0.0],
            "cur_pph": 0.0, "cur_rebook": 0.0,
            "cur_product": 0.0, "cur_ticket": 0.0,
            # No net_service/net_product on placeholder row → differencing yields zeros
            "invoices": 0, "guests": 0, "net_service": 0.0,
            "net_product": 0.0, "production_hours": 0.0,
            "platform": "zenoti",
        }

        with patch("core.cumulative_pipeline.append_to_cumulative_mtd"), \
             patch("core.cumulative_pipeline.append_to_stylists_cumulative_mtd"), \
             patch("core.cumulative_pipeline.read_cumulative_mtd_snapshots", return_value=[]), \
             patch(
                 "core.cumulative_pipeline.read_stylists_cumulative_mtd_snapshots",
                 return_value=[],
             ):
            _, weekly_styls = snapshot_and_difference(
                MagicMock(), CONFIG,
                cumulative_locations=[cur_loc],
                cumulative_stylists=[live_with_placeholder],
                source_label="current_tab", dry_run=False,
            )

        s = weekly_styls[0]
        # Trailing zero-placeholder dropped; arrays back to monthly-only length
        self.assertEqual(len(s["pph"]), 1)
        self.assertEqual(len(s["ticket"]), 1)
        self.assertEqual(len(s["product"]), 1)
        self.assertEqual(len(s["weeks"]), 1)
        # cur_* fall back to the last historical (monthly) value — NOT zero
        self.assertEqual(s["cur_pph"], 42.16)
        self.assertEqual(s["cur_ticket"], 55.78)
        self.assertEqual(s["cur_product"], 6.78)
        # Static identity preserved
        self.assertEqual(s["name"], "Aleksis")

    def test_mixed_live_and_historical_only_stylists(self):
        """Historical-only stylists pass through; live ones get reshaped.
        Both should be present in output without crashing downstream."""
        cur_loc = _build_cumulative_location(week_ending="2026-04-12")
        live = self._live_stylist(name="Alice")
        hist = {
            "name": "Bob", "loc_name": "Blaine", "loc_id": "z002",
            "status": "active", "tenure": 0,
            "is_historical_only": True,
            "weeks":    ["2026-03-31"],
            "pph":      [50.0],
            "rebook":   [0],
            "product":  [0],
            "ticket":   [55.0],
            "services": [8],
            "color":    [0],
            "cur_pph": 0, "cur_rebook": 0, "cur_product": 0, "cur_ticket": 0,
        }

        with patch("core.cumulative_pipeline.append_to_cumulative_mtd"), \
             patch("core.cumulative_pipeline.append_to_stylists_cumulative_mtd"), \
             patch("core.cumulative_pipeline.read_cumulative_mtd_snapshots", return_value=[]), \
             patch(
                 "core.cumulative_pipeline.read_stylists_cumulative_mtd_snapshots",
                 return_value=[],
             ):
            _, weekly_styls = snapshot_and_difference(
                MagicMock(), CONFIG,
                cumulative_locations=[cur_loc],
                cumulative_stylists=[live, hist],
                source_label="current_tab", dry_run=False,
            )

        names = sorted(s["name"] for s in weekly_styls)
        self.assertEqual(names, ["Alice", "Bob"])

        alice = next(s for s in weekly_styls if s["name"] == "Alice")
        bob = next(s for s in weekly_styls if s["name"] == "Bob")
        self.assertFalse(alice.get("is_historical_only", False))
        self.assertTrue(bob.get("is_historical_only", False))
        # Both have list-shaped pph (alice from reshape, bob unchanged)
        self.assertIsInstance(alice["pph"], list)
        self.assertIsInstance(bob["pph"], list)


class TestSnapshotAndDifferenceDataProcessorContract(unittest.TestCase):
    """End-to-end: differenced live stylists must survive a full
    data_processor.enrich_stylists pass without raising."""

    def test_enrich_stylists_does_not_crash_on_reshaped_records(self):
        from core import data_processor

        cur_loc = _build_cumulative_location(week_ending="2026-04-12")
        live = {
            "name": "Alice", "loc_name": "Blaine", "loc_id": "z002",
            "status": "active", "tenure": 3.0,
            "weeks": ["2026-04-05", "2026-04-12"],
            "pph":     [55.0, 62.0],
            "rebook":  [40.0, 45.0],
            "product": [12.5, 13.5],
            "ticket":  [60.0, 70.0],
            "services": [10, 12],
            "color":   [0.30, 0.32],
            "cur_pph": 62.0, "cur_rebook": 45.0,
            "cur_product": 13.5, "cur_ticket": 70.0,
            "invoices": 25, "guests": 25, "net_service": 1500.0,
            "net_product": 300.0, "production_hours": 35.0,
            "platform": "zenoti",
        }
        with patch("core.cumulative_pipeline.append_to_cumulative_mtd"), \
             patch("core.cumulative_pipeline.append_to_stylists_cumulative_mtd"), \
             patch("core.cumulative_pipeline.read_cumulative_mtd_snapshots", return_value=[]), \
             patch(
                 "core.cumulative_pipeline.read_stylists_cumulative_mtd_snapshots",
                 return_value=[],
             ):
            _, weekly_styls = snapshot_and_difference(
                MagicMock(), CONFIG,
                cumulative_locations=[cur_loc],
                cumulative_stylists=[live],
                source_label="current_tab", dry_run=False,
            )

        # The exact crash that production hit: len(pph) on a scalar float.
        # This call should now complete cleanly.
        network_summary = {"avg_pph": 50.0, "avg_product_pct": 12.0}
        enriched, threshold = data_processor.enrich_stylists(weekly_styls, network_summary)

        self.assertEqual(len(enriched), 1)
        self.assertIn("pph_delta", enriched[0])
        self.assertIn("rebook_delta", enriched[0])
        self.assertIn("arch_label", enriched[0])


if __name__ == "__main__":
    unittest.main()
