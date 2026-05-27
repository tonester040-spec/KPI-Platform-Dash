"""
tests/test_cumulative_to_weekly.py
──────────────────────────────────
Tests for core/cumulative_to_weekly.py — the pure-function differencing
math that converts cumulative-MTD snapshots to true weekly values.

Heavy edge-case coverage because this is the heart of the pipeline:
  - Happy path: two consecutive cumulative snapshots → weekly diff
  - Week 1 (no prior): weekly = current as-is
  - Negative differences (refunds): pass through, don't reject
  - Division by zero in derived KPIs: returns 0 (not NaN, not error)
  - Recomputed KPIs match what you'd compute from the differenced primitives
  - Missing fields default to 0
  - Stylist mirrors location pattern
  - Batch helpers preserve order and apply lookups correctly
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from core.cumulative_to_weekly import (  # noqa: E402
    ADDITIVE_LOCATION_FIELDS,
    ADDITIVE_STYLIST_FIELDS,
    _additive_diff,
    _recompute_location_kpis,
    _recompute_stylist_kpis,
    _safe_div,
    difference_location_batch,
    difference_location_record,
    difference_stylist_batch,
    difference_stylist_record,
)


def _make_location_mtd(
    *, week_ending: str = "2026-04-12", guests: int = 562, service: float = 32132.5,
    product: float = 4225.0, prod_hours: float = 574.0,
    wax_count: int = 80, wax: float = 1440.0,
    color: float = 10866.0, treat_count: int = 100, treat: float = 1800.0,
    loc_name: str = "Blaine", platform: str = "zenoti",
    year_month: str = "2026-04", source: str = "zenoti_weekly_pdf",
) -> dict:
    total = service + product
    return {
        "loc_name": loc_name, "year_month": year_month, "week_ending": week_ending,
        "platform": platform,
        "guests": guests, "total_sales": total, "service": service, "product": product,
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
        "source": source,
    }


def _make_stylist_mtd(
    *, week_ending: str = "2026-04-12", name: str = "Heaven Hobbs",
    loc_name: str = "Andover FS", loc_id: str = "z001", platform: str = "zenoti",
    invoices: int = 36, guests: int = 36,
    net_service: float = 1700.0, net_product: float = 80.0,
    production_hours: float = 49.0,
    year_month: str = "2026-04", source: str = "zenoti_weekly_pdf",
) -> dict:
    return {
        "year_month": year_month, "week_ending": week_ending,
        "name": name, "loc_name": loc_name, "loc_id": loc_id, "platform": platform,
        "invoices": invoices, "guests": guests,
        "net_service": net_service, "net_product": net_product,
        "avg_ticket": (net_service + net_product) / invoices if invoices else 0,
        "pph": net_service / production_hours if production_hours else 0,
        "ppg": net_product / invoices if invoices else 0,
        "production_hours": production_hours,
        "source": source,
    }


# ────────────────────────────────────────────────────────────────────────
# Helper tests
# ────────────────────────────────────────────────────────────────────────

class TestSafeDiv(unittest.TestCase):
    def test_normal(self):
        self.assertAlmostEqual(_safe_div(10, 2), 5.0)

    def test_zero_denominator(self):
        self.assertEqual(_safe_div(10, 0), 0.0)

    def test_zero_over_zero(self):
        self.assertEqual(_safe_div(0, 0), 0.0)

    def test_negative_numerator(self):
        self.assertAlmostEqual(_safe_div(-10, 2), -5.0)


class TestAdditiveDiff(unittest.TestCase):
    def test_basic_subtraction(self):
        cur = {"guests": 100, "service": 5000.0}
        prior = {"guests": 60, "service": 3000.0}
        out = _additive_diff(cur, prior, ("guests", "service"))
        self.assertEqual(out, {"guests": 40, "service": 2000.0})

    def test_prior_none_returns_current(self):
        cur = {"guests": 100, "service": 5000.0}
        out = _additive_diff(cur, None, ("guests", "service"))
        self.assertEqual(out, {"guests": 100, "service": 5000.0})

    def test_missing_field_treated_as_zero(self):
        cur = {"guests": 100}
        prior = {"guests": 60, "service": 3000.0}
        out = _additive_diff(cur, prior, ("guests", "service"))
        self.assertEqual(out["guests"], 40)
        self.assertEqual(out["service"], -3000.0)  # missing in current → treated as 0

    def test_negative_result_passes_through(self):
        """Karissa's Q5: refunds can cause cumulative to decrease."""
        cur = {"service": 4990.0}  # refund applied
        prior = {"service": 5000.0}
        out = _additive_diff(cur, prior, ("service",))
        self.assertEqual(out["service"], -10.0)


# ────────────────────────────────────────────────────────────────────────
# Location differencing
# ────────────────────────────────────────────────────────────────────────

class TestDifferenceLocationRecord(unittest.TestCase):
    def test_week_2_normal_diff(self):
        """Apr Week 2 cumulative = Week 1 + Week 2 standalone. Subtract Week 1."""
        # Week 1 cumulative: 280 guests, $16k service
        prior = _make_location_mtd(
            week_ending="2026-04-05",
            guests=280, service=16000.0, product=2000.0, prod_hours=280.0,
            wax_count=40, wax=720.0, color=5300.0, treat_count=50, treat=900.0,
        )
        # Week 1+2 cumulative: 562 guests, $32k service
        current = _make_location_mtd(
            week_ending="2026-04-12",
            guests=562, service=32132.5, product=4225.0, prod_hours=574.0,
            wax_count=80, wax=1440.0, color=10866.0, treat_count=100, treat=1800.0,
        )

        weekly = difference_location_record(current, prior)

        # Identity carried from current
        self.assertEqual(weekly["loc_name"], "Blaine")
        self.assertEqual(weekly["year_month"], "2026-04")
        self.assertEqual(weekly["week_ending"], "2026-04-12")
        self.assertEqual(weekly["platform"], "zenoti")
        self.assertEqual(weekly["source"], "zenoti_weekly_pdf")

        # Additive primitives differenced
        self.assertEqual(weekly["guests"], 282)
        self.assertAlmostEqual(weekly["service"], 16132.5, places=2)
        self.assertAlmostEqual(weekly["product"], 2225.0, places=2)
        self.assertAlmostEqual(weekly["prod_hours"], 294.0, places=2)
        self.assertEqual(weekly["wax_count"], 40)
        self.assertAlmostEqual(weekly["wax"], 720.0, places=2)
        self.assertAlmostEqual(weekly["color"], 5566.0, places=2)
        self.assertEqual(weekly["treat_count"], 50)
        self.assertAlmostEqual(weekly["treat"], 900.0, places=2)

        # Derived KPIs recomputed from the diffed primitives — NOT subtracted
        self.assertAlmostEqual(weekly["total_sales"], 18357.5, places=2)  # 16132.5 + 2225
        self.assertAlmostEqual(weekly["product_pct"], 2225.0 / 18357.5, places=4)
        self.assertAlmostEqual(weekly["ppg"], 2225.0 / 282, places=4)
        self.assertAlmostEqual(weekly["pph"], 16132.5 / 294.0, places=2)
        self.assertAlmostEqual(weekly["avg_ticket"], 18357.5 / 282, places=2)
        self.assertAlmostEqual(weekly["wax_pct"], 40 / 282, places=4)
        self.assertAlmostEqual(weekly["color_pct"], 5566.0 / 16132.5, places=4)
        self.assertAlmostEqual(weekly["treat_pct"], 50 / 282, places=4)

    def test_week_1_no_prior_returns_current_as_weekly(self):
        """Week 1 of month has no prior cumulative — current IS the weekly."""
        current = _make_location_mtd(
            week_ending="2026-04-05",
            guests=280, service=16000.0, product=2000.0, prod_hours=280.0,
        )
        weekly = difference_location_record(current, None)
        self.assertEqual(weekly["guests"], 280)
        self.assertAlmostEqual(weekly["service"], 16000.0, places=2)
        # Recomputed from current as-is
        self.assertAlmostEqual(weekly["pph"], 16000.0 / 280.0, places=2)

    def test_refund_produces_negative_weekly_value(self):
        """Q5: if a refund happens, cumulative can decrease. Pass through."""
        prior = _make_location_mtd(week_ending="2026-04-05", service=16000.0, guests=280, prod_hours=280)
        # This week: $10 refund means cumulative dropped to $15,990, with no new business
        current = _make_location_mtd(week_ending="2026-04-12", service=15990.0, guests=280, prod_hours=280)
        weekly = difference_location_record(current, prior)
        self.assertEqual(weekly["service"], -10.0)
        # PPH recomputed from negative service and zero new hours → -10/0 = 0 (safe_div)
        self.assertEqual(weekly["pph"], 0.0)

    def test_zero_guests_safe_div(self):
        """Holiday-closed week: zero guests, zero everything. KPIs all 0."""
        prior = _make_location_mtd(week_ending="2026-04-05", guests=280, service=16000)
        current = _make_location_mtd(week_ending="2026-04-12", guests=280, service=16000)
        # No new activity at all — differenced everything is 0
        weekly = difference_location_record(current, prior)
        self.assertEqual(weekly["guests"], 0)
        self.assertEqual(weekly["service"], 0)
        self.assertEqual(weekly["ppg"], 0)
        self.assertEqual(weekly["pph"], 0)
        self.assertEqual(weekly["avg_ticket"], 0)

    def test_missing_loc_name_defaults_to_empty(self):
        weekly = difference_location_record({"week_ending": "2026-04-12"}, None)
        self.assertEqual(weekly["loc_name"], "")

    def test_all_additive_fields_are_present_in_output(self):
        weekly = difference_location_record(_make_location_mtd(), None)
        for f in ADDITIVE_LOCATION_FIELDS:
            self.assertIn(f, weekly, f"missing additive field: {f}")


class TestRecomputeLocationKpis(unittest.TestCase):
    def test_typical_values(self):
        diffed = {
            "guests": 282, "total_sales": 18357.5, "service": 16132.5,
            "product": 2225.0, "prod_hours": 294.0,
            "wax_count": 40, "color": 5566.0, "treat_count": 50,
        }
        out = _recompute_location_kpis(diffed)
        self.assertAlmostEqual(out["product_pct"], 2225.0 / 18357.5, places=4)
        self.assertAlmostEqual(out["ppg"], 2225.0 / 282, places=4)
        self.assertAlmostEqual(out["pph"], 16132.5 / 294.0, places=4)
        self.assertAlmostEqual(out["avg_ticket"], 18357.5 / 282, places=4)
        self.assertAlmostEqual(out["wax_pct"], 40 / 282, places=4)
        self.assertAlmostEqual(out["color_pct"], 5566.0 / 16132.5, places=4)
        self.assertAlmostEqual(out["treat_pct"], 50 / 282, places=4)

    def test_color_pct_uses_service_denominator(self):
        """CLAUDE.md spec: color_pct = color / service (NOT total_sales)."""
        diffed = {"guests": 100, "total_sales": 10000, "service": 9000,
                  "product": 1000, "prod_hours": 100, "wax_count": 10,
                  "color": 3000, "treat_count": 10}
        out = _recompute_location_kpis(diffed)
        # 3000 / 9000 = 0.3333 (service denom), NOT 3000/10000 = 0.30 (total denom)
        self.assertAlmostEqual(out["color_pct"], 1/3, places=4)


# ────────────────────────────────────────────────────────────────────────
# Stylist differencing
# ────────────────────────────────────────────────────────────────────────

class TestDifferenceStylistRecord(unittest.TestCase):
    def test_week_2_normal_diff(self):
        prior = _make_stylist_mtd(
            week_ending="2026-04-05",
            invoices=18, guests=18, net_service=850.0, net_product=40.0,
            production_hours=24.5,
        )
        current = _make_stylist_mtd(
            week_ending="2026-04-12",
            invoices=36, guests=36, net_service=1700.0, net_product=80.0,
            production_hours=49.0,
        )
        weekly = difference_stylist_record(current, prior)
        self.assertEqual(weekly["name"], "Heaven Hobbs")
        self.assertEqual(weekly["week_ending"], "2026-04-12")
        self.assertEqual(weekly["invoices"], 18)
        self.assertEqual(weekly["guests"], 18)
        self.assertAlmostEqual(weekly["net_service"], 850.0, places=2)
        self.assertAlmostEqual(weekly["net_product"], 40.0, places=2)
        self.assertAlmostEqual(weekly["production_hours"], 24.5, places=2)
        # Recomputed: avg_ticket = (850+40)/18 = 49.44
        self.assertAlmostEqual(weekly["avg_ticket"], 890.0 / 18, places=2)
        # pph = 850/24.5
        self.assertAlmostEqual(weekly["pph"], 850.0 / 24.5, places=2)
        # ppg = 40/18
        self.assertAlmostEqual(weekly["ppg"], 40.0 / 18, places=4)

    def test_week_1_no_prior(self):
        current = _make_stylist_mtd(week_ending="2026-04-05")
        weekly = difference_stylist_record(current, None)
        self.assertEqual(weekly["invoices"], 36)
        self.assertEqual(weekly["net_service"], 1700.0)

    def test_zero_invoices_safe(self):
        """Stylist has zero new invoices this week (vacation, etc.) — KPIs go to 0."""
        prior = _make_stylist_mtd(week_ending="2026-04-05", invoices=18, net_service=850)
        current = _make_stylist_mtd(week_ending="2026-04-12", invoices=18, net_service=850)
        weekly = difference_stylist_record(current, prior)
        self.assertEqual(weekly["invoices"], 0)
        self.assertEqual(weekly["net_service"], 0)
        self.assertEqual(weekly["avg_ticket"], 0)
        self.assertEqual(weekly["pph"], 0)
        self.assertEqual(weekly["ppg"], 0)

    def test_identity_fields_preserved(self):
        current = _make_stylist_mtd(name="Stylist X", loc_id="z007", loc_name="Hudson")
        weekly = difference_stylist_record(current, None)
        self.assertEqual(weekly["name"], "Stylist X")
        self.assertEqual(weekly["loc_id"], "z007")
        self.assertEqual(weekly["loc_name"], "Hudson")


# ────────────────────────────────────────────────────────────────────────
# Batch helpers
# ────────────────────────────────────────────────────────────────────────

class TestLocationBatch(unittest.TestCase):
    def test_preserves_order_and_applies_priors(self):
        cur = [
            _make_location_mtd(loc_name="Andover FS", week_ending="2026-04-12",
                               guests=200, service=10000, prod_hours=200),
            _make_location_mtd(loc_name="Blaine", week_ending="2026-04-12",
                               guests=562, service=32132.5, prod_hours=574),
            _make_location_mtd(loc_name="Crystal FS", week_ending="2026-04-12",
                               guests=400, service=20000, prod_hours=300),
        ]
        # Only Blaine has a prior (Andover and Crystal treated as Week 1)
        priors = {
            "Blaine": _make_location_mtd(loc_name="Blaine", week_ending="2026-04-05",
                                          guests=280, service=16000, prod_hours=280),
        }
        out = difference_location_batch(cur, priors)
        self.assertEqual([r["loc_name"] for r in out], ["Andover FS", "Blaine", "Crystal FS"])

        # Andover treated as Week 1 — weekly = current
        self.assertEqual(out[0]["guests"], 200)
        self.assertAlmostEqual(out[0]["service"], 10000.0, places=2)

        # Blaine differenced
        self.assertEqual(out[1]["guests"], 282)
        self.assertAlmostEqual(out[1]["service"], 16132.5, places=2)

        # Crystal treated as Week 1 — weekly = current
        self.assertEqual(out[2]["guests"], 400)
        self.assertAlmostEqual(out[2]["service"], 20000.0, places=2)


class TestStylistBatch(unittest.TestCase):
    def test_lookup_by_name_and_loc(self):
        cur = [
            _make_stylist_mtd(name="A", loc_name="Hudson", invoices=20, net_service=1000),
            _make_stylist_mtd(name="A", loc_name="Andover FS", invoices=30, net_service=1500),
            _make_stylist_mtd(name="B", loc_name="Hudson", invoices=15, net_service=750),
        ]
        priors = {
            ("A", "Hudson"): _make_stylist_mtd(name="A", loc_name="Hudson",
                                                 invoices=10, net_service=500),
            # No prior for ("A", "Andover FS") or ("B", "Hudson")
        }
        out = difference_stylist_batch(cur, priors)
        self.assertEqual(out[0]["invoices"], 10)            # A@Hudson differenced
        self.assertAlmostEqual(out[0]["net_service"], 500.0)
        self.assertEqual(out[1]["invoices"], 30)            # A@Andover as-is (week 1)
        self.assertEqual(out[2]["invoices"], 15)            # B@Hudson as-is (week 1)


if __name__ == "__main__":
    unittest.main()
