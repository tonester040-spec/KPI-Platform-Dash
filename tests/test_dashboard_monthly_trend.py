"""
tests/test_dashboard_monthly_trend.py
─────────────────────────────────────
Tests for the MONTHLY_TREND_DATA JS-constant builder in
core/dashboard_builder.py — the Phase 2 dashboard view that addresses
Karissa's Q7: "weeks in a column then at the end of the report having
the monthly adding up as the month goes on."
"""

from __future__ import annotations

import json
import re
import sys
import unittest
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from core.dashboard_builder import _build_monthly_trend_data  # noqa: E402


def _parse_constant(js: str) -> dict:
    """Extract the dict from `const MONTHLY_TREND_DATA = {...};`"""
    match = re.search(r"const MONTHLY_TREND_DATA\s*=\s*(\{.*\});?\s*$", js, re.DOTALL)
    assert match, f"could not find MONTHLY_TREND_DATA in:\n{js[:200]}"
    return json.loads(match.group(1))


def _make_loc(
    name: str = "Blaine",
    *,
    weeks: list[str] | None = None,
    sales: list[float] | None = None,
) -> dict:
    return {
        "loc_name": name,
        "hist": {
            "weeks": weeks or [],
            "total_sales": sales or [],
        },
    }


class TestMonthlyTrendBuilder(unittest.TestCase):
    def test_current_month_filtering(self):
        """Only weeks in the same year-month as the latest week should appear."""
        loc = _make_loc(
            weeks=[
                "2026-03-22", "2026-03-29", "2026-03-31",   # March (should be excluded)
                "2026-04-05", "2026-04-12", "2026-04-19",   # April
            ],
            sales=[7000, 8000, 2500, 18000, 18500, 17000],
        )
        out = _parse_constant(_build_monthly_trend_data([loc]))
        b = out["Blaine"]
        self.assertEqual(b["current_month"], "2026-04")
        self.assertEqual(b["weeks"], ["2026-04-05", "2026-04-12", "2026-04-19"])
        self.assertEqual(b["sales"], [18000.0, 18500.0, 17000.0])
        self.assertAlmostEqual(b["mtd_total"], 53500.0, places=2)

    def test_only_one_week(self):
        """Week 1 of a new month — only one entry, MTD = that week."""
        loc = _make_loc(weeks=["2026-04-05"], sales=[18000])
        out = _parse_constant(_build_monthly_trend_data([loc]))
        self.assertEqual(out["Blaine"]["weeks"], ["2026-04-05"])
        self.assertEqual(out["Blaine"]["sales"], [18000.0])
        self.assertAlmostEqual(out["Blaine"]["mtd_total"], 18000.0, places=2)

    def test_no_history_returns_empty_arrays(self):
        loc = _make_loc(weeks=[], sales=[])
        out = _parse_constant(_build_monthly_trend_data([loc]))
        self.assertEqual(out["Blaine"]["weeks"], [])
        self.assertEqual(out["Blaine"]["sales"], [])
        self.assertEqual(out["Blaine"]["mtd_total"], 0)

    def test_missing_hist_dict_is_safe(self):
        loc = {"loc_name": "Andover FS"}  # no hist key at all
        out = _parse_constant(_build_monthly_trend_data([loc]))
        self.assertIn("Andover FS", out)
        self.assertEqual(out["Andover FS"]["mtd_total"], 0)

    def test_empty_loc_name_skipped(self):
        locs = [_make_loc(name=""), _make_loc(name="Blaine", weeks=["2026-04-05"], sales=[18000])]
        out = _parse_constant(_build_monthly_trend_data(locs))
        self.assertEqual(set(out.keys()), {"Blaine"})

    def test_multiple_locations_independent(self):
        locs = [
            _make_loc(name="Andover FS",
                      weeks=["2026-04-05", "2026-04-12"],
                      sales=[6000, 6500]),
            _make_loc(name="Blaine",
                      weeks=["2026-04-05", "2026-04-12", "2026-04-19"],
                      sales=[18000, 18500, 17000]),
        ]
        out = _parse_constant(_build_monthly_trend_data(locs))
        self.assertEqual(len(out["Andover FS"]["weeks"]), 2)
        self.assertEqual(len(out["Blaine"]["weeks"]), 3)
        self.assertAlmostEqual(out["Andover FS"]["mtd_total"], 12500.0, places=2)
        self.assertAlmostEqual(out["Blaine"]["mtd_total"], 53500.0, places=2)

    def test_handles_none_in_sales_array(self):
        """Defensive: a None in the sales array shouldn't crash."""
        loc = _make_loc(
            weeks=["2026-04-05", "2026-04-12"],
            sales=[18000, None],
        )
        out = _parse_constant(_build_monthly_trend_data([loc]))
        self.assertEqual(out["Blaine"]["sales"], [18000.0, 0.0])
        self.assertAlmostEqual(out["Blaine"]["mtd_total"], 18000.0, places=2)

    def test_mtd_total_is_rounded_to_cents(self):
        loc = _make_loc(
            weeks=["2026-04-05", "2026-04-12"],
            sales=[18000.123, 12000.456],
        )
        out = _parse_constant(_build_monthly_trend_data([loc]))
        self.assertEqual(out["Blaine"]["mtd_total"], 30000.58)  # rounded


if __name__ == "__main__":
    unittest.main()
