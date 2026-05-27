"""
tests/test_data_source_monthly_fallback.py
──────────────────────────────────────────
Tests for the Phase 2.5 monthly fallback in core/data_source.py:
  - load_data_monthly_history  — reads DATA_MONTHLY tab
  - load_stylists_data_monthly_history — reads STYLISTS_DATA_MONTHLY
  - merge_history — combines weekly + monthly location history
  - _merge_stylist_rosters — combines weekly + monthly stylist rosters

Critical case: STYLISTS_DATA tab empty + STYLISTS_DATA_MONTHLY has rows
→ load_stylist_data must return monthly-only stylists, not [].
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
    _merge_stylist_rosters,
    load_data_monthly_history,
    load_historical_data,
    load_stylist_data,
    load_stylists_data_monthly_history,
    merge_history,
)


CONFIG = {"sheet_id": "test_sheet"}


def _make_data_monthly_row(loc: str, year_month: str, **kw) -> list:
    """Build a 23-col DATA_MONTHLY row (matches DATA_MONTHLY_HEADERS)."""
    defaults = {
        "platform": "zenoti", "guests": 500, "total_sales": 25000.0, "service": 23000.0,
        "product": 2000.0, "product_pct": 0.08, "ppg": 4.0, "pph": 40.0, "avg_ticket": 50.0,
        "prod_hours": 575.0, "wax_count": 70, "wax": 1260.0, "wax_pct": 0.14,
        "color": 6900.0, "color_pct": 0.30, "treat_count": 80, "treat": 1440.0, "treat_pct": 0.16,
        "source": "tracker", "period_start": f"{year_month}-01",
        "period_end": f"{year_month}-30",
    }
    defaults.update(kw)
    return [
        loc, year_month, defaults["platform"], defaults["guests"], defaults["total_sales"],
        defaults["service"], defaults["product"], defaults["product_pct"],
        defaults["ppg"], defaults["pph"], defaults["avg_ticket"], defaults["prod_hours"],
        defaults["wax_count"], defaults["wax"], defaults["wax_pct"],
        defaults["color"], defaults["color_pct"],
        defaults["treat_count"], defaults["treat"], defaults["treat_pct"],
        defaults["source"], defaults["period_start"], defaults["period_end"],
    ]


def _make_stylist_monthly_row(name: str, loc: str, year_month: str, **kw) -> list:
    """Build a 16-col STYLISTS_DATA_MONTHLY row."""
    defaults = {
        "loc_id": "z001", "platform": "zenoti", "invoices": 50, "guests": 50,
        "net_service": 2500.0, "net_product": 100.0, "avg_ticket": 50.0,
        "pph": 35.0, "ppg": 2.0, "production_hours": 70.0, "source": "tracker",
        "period_start": f"{year_month}-01", "period_end": f"{year_month}-30",
    }
    defaults.update(kw)
    return [
        year_month, name, loc, defaults["loc_id"], defaults["platform"],
        defaults["invoices"], defaults["guests"], defaults["net_service"], defaults["net_product"],
        defaults["avg_ticket"], defaults["pph"], defaults["ppg"], defaults["production_hours"],
        defaults["source"], defaults["period_start"], defaults["period_end"],
    ]


def _service_with_data_monthly(rows: list[list]) -> MagicMock:
    """MagicMock service returning given rows for the DATA_MONTHLY read."""
    service = MagicMock()
    service.spreadsheets.return_value.values.return_value.get.return_value.execute.return_value = {
        "values": rows,
    }
    return service


# ────────────────────────────────────────────────────────────────────────
# load_data_monthly_history
# ────────────────────────────────────────────────────────────────────────

class TestLoadDataMonthlyHistory(unittest.TestCase):
    def test_missing_tab_returns_empty(self):
        service = MagicMock()
        service.spreadsheets().values().get(
            spreadsheetId="test_sheet", range="DATA_MONTHLY!A2:W",
        ).execute.side_effect = Exception("Unable to parse range")
        result = load_data_monthly_history(service, CONFIG)
        self.assertEqual(result, {})

    def test_returns_sorted_chronological(self):
        # Insert out of order — load should sort by year_month
        rows = [
            _make_data_monthly_row("Blaine", "2026-05"),
            _make_data_monthly_row("Blaine", "2026-03"),
            _make_data_monthly_row("Blaine", "2026-04"),
        ]
        result = load_data_monthly_history(_service_with_data_monthly(rows), CONFIG)
        self.assertIn("Blaine", result)
        weeks = result["Blaine"]["weeks"]
        self.assertEqual(weeks, ["2026-03-30", "2026-04-30", "2026-05-30"])

    def test_kpi_arrays_aligned_with_weeks(self):
        rows = [
            _make_data_monthly_row("Blaine", "2026-03", total_sales=63049.40, product_pct=0.08),
            _make_data_monthly_row("Blaine", "2026-04", total_sales=55872.37, product_pct=0.04),
        ]
        result = load_data_monthly_history(_service_with_data_monthly(rows), CONFIG)
        b = result["Blaine"]
        self.assertEqual(b["total_sales"], [63049.40, 55872.37])
        self.assertEqual(b["product_pct"], [0.08, 0.04])


# ────────────────────────────────────────────────────────────────────────
# load_stylists_data_monthly_history
# ────────────────────────────────────────────────────────────────────────

class TestLoadStylistsDataMonthlyHistory(unittest.TestCase):
    def test_groups_by_stylist_name(self):
        rows = [
            _make_stylist_monthly_row("Alexis", "Hudson", "2026-03"),
            _make_stylist_monthly_row("Alexis", "Hudson", "2026-04"),
            _make_stylist_monthly_row("Brittany", "Hudson", "2026-04"),
        ]
        result = load_stylists_data_monthly_history(_service_with_data_monthly(rows), CONFIG)
        self.assertEqual(set(result.keys()), {"Alexis", "Brittany"})
        # Alexis has 2 entries chronologically
        self.assertEqual(result["Alexis"]["weeks"], ["2026-03-30", "2026-04-30"])
        # Brittany has 1
        self.assertEqual(len(result["Brittany"]["weeks"]), 1)


# ────────────────────────────────────────────────────────────────────────
# merge_history
# ────────────────────────────────────────────────────────────────────────

class TestMergeHistory(unittest.TestCase):
    def test_monthly_prepended_to_weekly(self):
        weekly = {"Blaine": {
            "weeks": ["2026-05-24"],
            "pph":   [50.73],
        }}
        monthly = {"Blaine": {
            "weeks": ["2026-03-30", "2026-04-30"],
            "pph":   [39.57, 45.75],
        }}
        merged = merge_history(weekly, monthly, weeks=12)
        self.assertEqual(merged["Blaine"]["weeks"], ["2026-03-30", "2026-04-30", "2026-05-24"])
        self.assertEqual(merged["Blaine"]["pph"], [39.57, 45.75, 50.73])

    def test_weeks_limit_keeps_last_N(self):
        weekly = {"Blaine": {"weeks": ["w1", "w2", "w3"], "pph": [1, 2, 3]}}
        monthly = {"Blaine": {"weeks": ["m1", "m2"], "pph": [10, 20]}}
        merged = merge_history(weekly, monthly, weeks=3)
        # Combined = [m1, m2, w1, w2, w3] → take last 3 (most recent) = [w1, w2, w3]
        self.assertEqual(merged["Blaine"]["weeks"], ["w1", "w2", "w3"])
        self.assertEqual(merged["Blaine"]["pph"], [1, 2, 3])

    def test_weeks_limit_blends_monthly_and_weekly(self):
        """If weekly < N entries, blend in tail of monthly to fill up to N."""
        weekly = {"Blaine": {"weeks": ["w1"], "pph": [3]}}
        monthly = {"Blaine": {"weeks": ["m1", "m2", "m3"], "pph": [10, 20, 30]}}
        merged = merge_history(weekly, monthly, weeks=3)
        # Combined = [m1, m2, m3, w1] → take last 3 = [m2, m3, w1]
        self.assertEqual(merged["Blaine"]["weeks"], ["m2", "m3", "w1"])
        self.assertEqual(merged["Blaine"]["pph"], [20, 30, 3])

    def test_locations_only_in_monthly_pass_through(self):
        weekly = {}
        monthly = {"Hudson": {"weeks": ["2026-04-30"], "pph": [60.0]}}
        merged = merge_history(weekly, monthly, weeks=12)
        self.assertIn("Hudson", merged)
        self.assertEqual(merged["Hudson"]["pph"], [60.0])


# ────────────────────────────────────────────────────────────────────────
# _merge_stylist_rosters
# ────────────────────────────────────────────────────────────────────────

class TestMergeStylistRosters(unittest.TestCase):
    def test_monthly_only_stylist_synthesized(self):
        weekly = []
        monthly = {"Alexis": {
            "name": "Alexis", "loc_name": "Hudson", "loc_id": "z007",
            "weeks": ["2026-03-30", "2026-04-30"],
            "pph":   [33.0, 35.0],
            "ticket": [48.0, 50.0],
            "services": [70, 80],
        }}
        result = _merge_stylist_rosters(weekly, monthly)
        self.assertEqual(len(result), 1)
        s = result[0]
        self.assertEqual(s["name"], "Alexis")
        self.assertEqual(s["loc_name"], "Hudson")
        self.assertEqual(s["status"], "active")  # synthesized default
        self.assertEqual(s["weeks"], ["2026-03-30", "2026-04-30"])
        self.assertEqual(s["pph"], [33.0, 35.0])
        self.assertEqual(s["services"], [70, 80])
        # Missing fields get zero arrays of matching length
        self.assertEqual(s["rebook"], [0, 0])
        self.assertEqual(s["color"], [0, 0])
        # Historical-only stylists get cur_* zeroed AND the is_historical_only
        # flag set — downstream pipeline filters skip them. See hotfix-phase-
        # 2-5-cur-pph-keyerror PR for the contract.
        self.assertTrue(s.get("is_historical_only"))
        self.assertEqual(s["cur_pph"], 0)
        self.assertEqual(s["cur_ticket"], 0)
        self.assertEqual(s["cur_rebook"], 0)
        self.assertEqual(s["cur_product"], 0)

    def test_overlap_prepends_monthly_to_weekly(self):
        weekly = [{
            "name": "Alexis", "loc_name": "Hudson", "loc_id": "z007",
            "status": "active", "tenure": 5.0,
            "weeks": ["2026-05-24"], "pph": [40.0], "ticket": [55.0], "services": [60],
            "rebook": [0.10], "product": [0.05], "color": [0.30],
            "cur_pph": 40.0, "cur_rebook": 0.10, "cur_product": 0.05, "cur_ticket": 55.0,
        }]
        monthly = {"Alexis": {
            "name": "Alexis", "loc_name": "Hudson", "loc_id": "z007",
            "weeks": ["2026-03-30", "2026-04-30"],
            "pph":   [33.0, 35.0], "ticket": [48.0, 50.0], "services": [70, 80],
        }}
        result = _merge_stylist_rosters(weekly, monthly)
        self.assertEqual(len(result), 1)
        s = result[0]
        # Monthly prepended (oldest first), weekly last
        self.assertEqual(s["weeks"], ["2026-03-30", "2026-04-30", "2026-05-24"])
        self.assertEqual(s["pph"], [33.0, 35.0, 40.0])
        # Static fields preserved from weekly
        self.assertEqual(s["status"], "active")
        self.assertEqual(s["tenure"], 5.0)
        # cur_* preserved from weekly (latest week)
        self.assertEqual(s["cur_pph"], 40.0)
        # NOT historical-only — this stylist has a live week
        self.assertFalse(s.get("is_historical_only", False))

    def test_stable_ordering(self):
        weekly = [
            {"name": "Zoe", "loc_name": "Hudson", "loc_id": "z007", "status": "a", "tenure": 0,
             "weeks": [], "pph": [], "rebook": [], "product": [], "ticket": [], "services": [], "color": [],
             "cur_pph": 0, "cur_rebook": 0, "cur_product": 0, "cur_ticket": 0},
            {"name": "Amy", "loc_name": "Hudson", "loc_id": "z007", "status": "a", "tenure": 0,
             "weeks": [], "pph": [], "rebook": [], "product": [], "ticket": [], "services": [], "color": [],
             "cur_pph": 0, "cur_rebook": 0, "cur_product": 0, "cur_ticket": 0},
        ]
        monthly = {"Beth": {"name": "Beth", "loc_name": "Blaine", "loc_id": "z002",
                            "weeks": [], "pph": [], "ticket": [], "services": []}}
        result = _merge_stylist_rosters(weekly, monthly)
        # Weekly order preserved (Zoe, Amy), then monthly-only alphabetical (Beth)
        self.assertEqual([s["name"] for s in result], ["Zoe", "Amy", "Beth"])


# ────────────────────────────────────────────────────────────────────────
# load_stylist_data with monthly fallback — critical scenario
# ────────────────────────────────────────────────────────────────────────

class TestLoadStylistDataWithMonthlyFallback(unittest.TestCase):
    def test_empty_weekly_tab_falls_back_to_monthly(self):
        """If STYLISTS_DATA is empty, return monthly-only stylists from STYLISTS_DATA_MONTHLY."""
        service = MagicMock()
        # First call (STYLISTS_DATA!A2:L) returns empty
        # Second call (STYLISTS_DATA_MONTHLY!A2:P) returns monthly rows
        responses = [
            {"values": []},                                         # STYLISTS_DATA empty
            {"values": [_make_stylist_monthly_row("Alexis", "Hudson", "2026-04")]},  # monthly rows
        ]
        call_count = [0]
        def side_effect(**kwargs):
            m = MagicMock()
            idx = call_count[0]
            call_count[0] += 1
            m.execute.return_value = responses[idx] if idx < len(responses) else {"values": []}
            return m
        service.spreadsheets.return_value.values.return_value.get.side_effect = side_effect

        result = load_stylist_data(service, CONFIG)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["name"], "Alexis")
        self.assertEqual(result[0]["loc_name"], "Hudson")

    def test_disable_fallback_returns_empty(self):
        service = MagicMock()
        service.spreadsheets().values().get().execute.return_value = {"values": []}
        result = load_stylist_data(service, CONFIG, include_monthly_fallback=False)
        self.assertEqual(result, [])


# ────────────────────────────────────────────────────────────────────────
# load_historical_data with monthly fallback
# ────────────────────────────────────────────────────────────────────────

class TestLoadHistoricalDataWithMonthlyFallback(unittest.TestCase):
    def test_disable_fallback_returns_weekly_only(self):
        service = MagicMock()
        service.spreadsheets().values().get().execute.return_value = {"values": []}
        result = load_historical_data(service, CONFIG, include_monthly_fallback=False)
        self.assertEqual(result, {})


if __name__ == "__main__":
    unittest.main()
