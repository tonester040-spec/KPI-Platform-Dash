"""
tests/test_dashboard_monthly_trend.py
─────────────────────────────────────
Tests for the MONTHLY_TREND_DATA JS-constant builder in
core/dashboard_builder.py.

Reframed 2026-05-27: switched from per-week breakdown (current month
only) to per-MONTH aggregation across all history. The old "weekly
within current month" view was nonsensical with monthly-only backfill
data — every week cell got the same monthly total and the MTD summed
that single value 6 times. New shape exposes one entry per year_month
present in the location's history.

New output shape:
  {
    "Blaine": {
      "months":        ["2026-03", "2026-04", "2026-05"],
      "month_labels":  ["Mar 2026", "Apr 2026", "May 2026"],
      "sales":         [12000.0, 18000.0, 42000.0],
      "current_month": "2026-05",
      "current_total": 42000.0,
    },
    ...
  }

Per-month aggregation uses MAX when multiple weekly entries exist for
the same year_month (monthly-backfill entries hold the full-month total;
partial weekly entries are smaller). Once full weekly history exists we
can switch to SUM without changing the output shape.
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
    def test_aggregates_by_year_month(self):
        """Multiple weekly entries roll up into one entry per year_month."""
        loc = _make_loc(
            weeks=[
                "2026-03-22", "2026-03-29", "2026-03-31",   # March
                "2026-04-05", "2026-04-12", "2026-04-19",   # April
            ],
            sales=[7000, 8000, 28500, 18000, 18500, 17000],
        )
        out = _parse_constant(_build_monthly_trend_data([loc]))
        b = out["Blaine"]
        # Two months should be present, in chronological order
        self.assertEqual(b["months"], ["2026-03", "2026-04"])
        self.assertEqual(b["month_labels"], ["Mar 2026", "Apr 2026"])
        # MAX aggregation: 28500 wins for March (largest, presumed full-month total)
        # and 18500 wins for April
        self.assertEqual(b["sales"], [28500.0, 18500.0])
        # Current month + total
        self.assertEqual(b["current_month"], "2026-04")
        self.assertEqual(b["current_total"], 18500.0)

    def test_only_one_month(self):
        """Single month of history — one entry."""
        loc = _make_loc(weeks=["2026-04-05"], sales=[18000])
        out = _parse_constant(_build_monthly_trend_data([loc]))
        b = out["Blaine"]
        self.assertEqual(b["months"], ["2026-04"])
        self.assertEqual(b["sales"], [18000.0])
        self.assertEqual(b["current_month"], "2026-04")
        self.assertEqual(b["current_total"], 18000.0)

    def test_no_history_returns_empty_arrays(self):
        loc = _make_loc(weeks=[], sales=[])
        out = _parse_constant(_build_monthly_trend_data([loc]))
        b = out["Blaine"]
        self.assertEqual(b["months"], [])
        self.assertEqual(b["month_labels"], [])
        self.assertEqual(b["sales"], [])
        self.assertEqual(b["current_total"], 0.0)

    def test_missing_hist_dict_is_safe(self):
        loc = {"loc_name": "Andover FS"}  # no hist key at all
        out = _parse_constant(_build_monthly_trend_data([loc]))
        self.assertIn("Andover FS", out)
        self.assertEqual(out["Andover FS"]["current_total"], 0.0)
        self.assertEqual(out["Andover FS"]["months"], [])

    def test_empty_loc_name_skipped(self):
        locs = [_make_loc(name=""), _make_loc(name="Blaine", weeks=["2026-04-05"], sales=[18000])]
        out = _parse_constant(_build_monthly_trend_data(locs))
        self.assertEqual(set(out.keys()), {"Blaine"})

    def test_multiple_locations_independent(self):
        locs = [
            _make_loc(name="Andover FS",
                      weeks=["2026-03-31", "2026-04-30"],
                      sales=[12000, 14500]),
            _make_loc(name="Blaine",
                      weeks=["2026-03-31", "2026-04-30", "2026-05-24"],
                      sales=[40000, 45000, 42000]),
        ]
        out = _parse_constant(_build_monthly_trend_data(locs))
        self.assertEqual(out["Andover FS"]["months"], ["2026-03", "2026-04"])
        self.assertEqual(out["Blaine"]["months"], ["2026-03", "2026-04", "2026-05"])
        self.assertEqual(out["Andover FS"]["current_total"], 14500.0)
        self.assertEqual(out["Blaine"]["current_total"], 42000.0)

    def test_handles_none_in_sales_array(self):
        """Defensive: a None in the sales array shouldn't crash."""
        loc = _make_loc(
            weeks=["2026-04-05", "2026-04-12"],
            sales=[18000, None],
        )
        out = _parse_constant(_build_monthly_trend_data([loc]))
        # Both entries are April; MAX picks 18000
        self.assertEqual(out["Blaine"]["months"], ["2026-04"])
        self.assertEqual(out["Blaine"]["sales"], [18000.0])

    def test_sales_rounded_to_cents(self):
        loc = _make_loc(
            weeks=["2026-04-05", "2026-04-12"],
            sales=[18000.123, 12000.456],
        )
        out = _parse_constant(_build_monthly_trend_data([loc]))
        # MAX aggregation; 18000.123 wins. Rounded to 2 places.
        self.assertEqual(out["Blaine"]["sales"], [18000.12])

    def test_dedup_collision_picks_max(self):
        """Polluted DATA tab scenario: same week_ending listed multiple
        times. With MAX aggregation we get the largest value (typically
        the monthly-backfill row which holds the full-month total)."""
        loc = _make_loc(
            weeks=["2026-05-24", "2026-05-24", "2026-05-24"],
            sales=[10000, 42143.70, 10000],
        )
        out = _parse_constant(_build_monthly_trend_data([loc]))
        self.assertEqual(out["Blaine"]["months"], ["2026-05"])
        self.assertEqual(out["Blaine"]["sales"], [42143.70])
        self.assertEqual(out["Blaine"]["current_total"], 42143.70)


if __name__ == "__main__":
    unittest.main()
