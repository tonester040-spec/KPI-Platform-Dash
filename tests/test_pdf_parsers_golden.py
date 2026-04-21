"""
PDF Parsers — Golden Fixture Test Suite
────────────────────────────────────────
Pins the current, validated output of `parsers.pdf_zenoti_v2.parse_file`,
`parsers.pdf_salon_ultimate_v2.parse_file`, and `parsers.pdf_detect`
against all 12 real production fixtures in `data/inbox/`.

Why this suite exists
─────────────────────
The Zenoti Service Details extractor was quietly broken — it was returning
zeros for wax / color / treatment across all 9 Zenoti fixtures because the
original regex assumed 3 leading spaces of indentation (legacy pdftotext),
but PyMuPDF extracts the section line-per-field with zero indent. That bug
shipped into an earlier "validated" session summary that claimed numbers
matched when they didn't. We never want that to happen again.

This suite locks in the numbers Tony hand-verified on 2026-04-21 against
Karissa's canonical KPI contract (see CLAUDE.md). If someone touches
`_extract_service_categories`, the header regex, the date parser, the
summing rule for `Wax` + `Waxing`, or anything that affects these numbers,
this test will fail loudly.

Fixtures live in: data/inbox/*.pdf (12 files — 9 Zenoti, 3 Salon Ultimate).
Do NOT delete them. If the fixtures change, regenerate the golden values
in this file from a fresh hand-audit — do not blindly rubber-stamp new
numbers.

Run with:
    python -m pytest tests/test_pdf_parsers_golden.py -v
Or without pytest:
    python tests/test_pdf_parsers_golden.py
"""

from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path
from typing import Any, Dict, List

# Make the repo root importable when run directly (not via pytest)
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from parsers.pdf_detect import detect_pos_from_file, ZENOTI, SALON_ULTIMATE
from parsers.pdf_zenoti_v2 import parse_file as zenoti_parse_file
from parsers.pdf_salon_ultimate_v2 import parse_file as su_parse_file
from parsers.tier2_pdf_batch import (
    transform_to_current_row,
    _build_location_lookup,
    _load_customer_config,
)


# ---------------------------------------------------------------------------
# Tolerances
# ---------------------------------------------------------------------------
# Money values are pre-rounded by the parser to 2 decimals, so 0.01 is
# tight but correct. Ratios are pre-rounded to 4 decimals; 0.0005 leaves
# half a unit at the last decimal for floating-point slop.
MONEY_TOL = 0.01
RATIO_TOL = 0.0005
HOURS_TOL = 0.01


# ---------------------------------------------------------------------------
# Golden values — hand-verified 2026-04-21 against Karissa-canonical KPI spec
# ---------------------------------------------------------------------------
# Each record is the output we CURRENTLY produce AND have confirmed matches
# the PDF's hand-audit values. Flags list is the exact `flags` field we
# expect from the parser (NOT including any Tier-2 flags that the
# orchestrator appends downstream — those are tested separately).
#
# Keys that we assert exactly (integers): guest_count, wax_count,
# treatment_count.
# Keys that we assert within MONEY_TOL: service_net, product_net,
# total_sales, avg_ticket, pph, color_sales.
# Keys that we assert within RATIO_TOL: ppg, wax_pct, color_pct, treatment_pct.
# Keys that we assert within HOURS_TOL: production_hours.

GOLDEN_ZENOTI: List[Dict[str, Any]] = [
    {
        "fixture":          "1232b9_Andover.pdf",
        "location":         "Andover",
        "week_start":       "2026-04-01",
        "week_end":         "2026-04-05",
        "guest_count":      95,
        "service_net":      3876.20,
        "product_net":      346.00,
        "total_sales":      4222.20,
        "avg_ticket":       44.44,
        "pph":              36.61,
        "ppg":              3.6421,
        "production_hours": 105.88,
        "wax_count":        10,
        "wax_pct":          0.1053,
        "color_sales":      944.00,
        "color_pct":        0.2236,
        "treatment_count":  4,
        "treatment_pct":    0.0421,
        "flags":            [],
    },
    {
        "fixture":          "6b53de_Blaine.pdf",
        "location":         "Blaine",
        "week_start":       "2026-04-01",
        "week_end":         "2026-04-05",
        "guest_count":      158,
        "service_net":      9708.80,
        "product_net":      417.75,
        "total_sales":      10126.55,
        "avg_ticket":       64.09,
        "pph":              51.17,
        "ppg":              2.6440,
        "production_hours": 189.73,
        "wax_count":        27,
        "wax_pct":          0.1709,
        "color_sales":      3917.75,
        "color_pct":        0.3869,
        "treatment_count":  35,
        "treatment_pct":    0.2215,
        "flags":            [],
    },
    {
        "fixture":          "33ff41_Crystal.pdf",
        "location":         "Crystal",
        "week_start":       "2026-04-01",
        "week_end":         "2026-04-05",
        "guest_count":      180,
        "service_net":      9265.90,
        "product_net":      314.00,
        "total_sales":      9579.90,
        "avg_ticket":       53.22,
        "pph":              48.96,
        "ppg":              1.7444,
        "production_hours": 189.27,
        "wax_count":        18,
        "wax_pct":          0.1000,
        "color_sales":      2239.50,
        "color_pct":        0.2338,
        "treatment_count":  37,
        "treatment_pct":    0.2056,
        "flags":            [],
    },
    {
        "fixture":          "73928f_Elk River.pdf",
        "location":         "Elk River",
        "week_start":       "2026-04-01",
        "week_end":         "2026-04-05",
        "guest_count":      132,
        "service_net":      5428.70,
        "product_net":      272.05,
        "total_sales":      5700.75,
        "avg_ticket":       43.19,
        "pph":              37.23,
        "ppg":              2.0610,
        "production_hours": 145.83,
        "wax_count":        18,
        "wax_pct":          0.1364,
        "color_sales":      1473.45,
        "color_pct":        0.2585,
        "treatment_count":  4,
        "treatment_pct":    0.0303,
        "flags":            [],
    },
    {
        "fixture":          "b6c35f_Forest Lake.pdf",
        "location":         "Forest Lake",
        "week_start":       "2026-04-01",
        "week_end":         "2026-04-05",
        "guest_count":      112,
        "service_net":      5887.10,
        "product_net":      612.50,
        "total_sales":      6499.60,
        "avg_ticket":       58.03,
        "pph":              50.97,
        "ppg":              5.4688,
        "production_hours": 115.50,
        "wax_count":        13,
        "wax_pct":          0.1161,
        "color_sales":      2035.50,
        "color_pct":        0.3132,
        "treatment_count":  19,
        "treatment_pct":    0.1696,
        "flags":            [],
    },
    {
        "fixture":          "ba3f13_Hudson.pdf",
        "location":         "Hudson",
        "week_start":       "2026-04-01",
        "week_end":         "2026-04-05",
        "guest_count":      191,
        "service_net":      9120.75,
        "product_net":      706.10,
        "total_sales":      9826.85,
        "avg_ticket":       51.45,
        "pph":              67.60,
        "ppg":              3.6969,
        "production_hours": 134.93,
        "wax_count":        24,
        "wax_pct":          0.1257,
        "color_sales":      2976.08,
        "color_pct":        0.3029,
        "treatment_count":  19,
        "treatment_pct":    0.0995,
        "flags":            [],
    },
    {
        # New Richmond: exercises the Haircut + "Hair Cut" dual-row folding
        # and a real-world low-volume store.
        "fixture":          "ef7fe8_New Richmond.pdf",
        "location":         "New Richmond",
        "week_start":       "2026-04-01",
        "week_end":         "2026-04-05",
        "guest_count":      67,
        "service_net":      3677.60,
        "product_net":      226.00,
        "total_sales":      3903.60,
        "avg_ticket":       58.26,
        "pph":              29.11,
        "ppg":              3.3731,
        "production_hours": 126.35,
        "wax_count":        6,
        "wax_pct":          0.0896,
        "color_sales":      1461.00,
        "color_pct":        0.3743,
        "treatment_count":  9,
        "treatment_pct":    0.1343,
        "flags":            [],
    },
    {
        # Prior Lake: regression test for BUG-1 — must be routed as Zenoti,
        # NOT Salon Ultimate. See CLAUDE.md ("DO NOT re-introduce
        # 'Prior Lake': 'salon_ultimate'").
        "fixture":          "e80f78_Prior Lake.pdf",
        "location":         "Prior Lake",
        "week_start":       "2026-04-01",
        "week_end":         "2026-04-05",
        "guest_count":      137,
        "service_net":      7551.95,
        "product_net":      637.00,
        "total_sales":      8188.95,
        "avg_ticket":       59.77,
        "pph":              37.37,
        "ppg":              4.6496,
        "production_hours": 202.10,
        "wax_count":        5,
        "wax_pct":          0.0365,
        "color_sales":      2684.00,
        "color_pct":        0.3278,
        "treatment_count":  41,
        "treatment_pct":    0.2993,
        "flags":            [],
    },
    {
        # Roseville: exercises the Waxing-only case — this PDF has a
        # "Waxing" row with NO "Wax" row at all. Parser must sum
        # wax + waxing into wax_count via wax_combined without crashing
        # when Wax is absent. Also: Roseville's pph is legitimately high
        # (404.88) because production_hours is only 15.17 — verified
        # against the raw PDF, not a parser bug.
        "fixture":          "e7b9a5_Roseville.pdf",
        "location":         "Roseville",
        "week_start":       "2026-04-01",
        "week_end":         "2026-04-05",
        "guest_count":      159,
        "service_net":      6142.00,
        "product_net":      63.99,
        "total_sales":      6205.99,
        "avg_ticket":       39.03,
        "pph":              404.88,
        "ppg":              0.4025,
        "production_hours": 15.17,
        "wax_count":        5,
        "wax_pct":          0.0314,
        "color_sales":      1695.00,
        "color_pct":        0.2731,
        "treatment_count":  14,
        "treatment_pct":    0.0881,
        "flags":            [],
    },
]

GOLDEN_SU: List[Dict[str, Any]] = [
    {
        "fixture":          "c73c34_Apple Valley.pdf",
        "location":         "Apple Valley",
        "week_start":       "2026-04-01",
        "week_end":         "2026-04-05",
        "guest_count":      281,
        "service_net":      16065.75,
        "product_net":      2112.00,
        "total_sales":      18177.75,
        "avg_ticket":       64.69,
        "pph":              55.92,
        "ppg":              7.52,
        "production_hours": 287.30,
        "wax_count":        38,
        "wax_pct":          0.1352,
        "color_sales":      5883.25,
        "color_pct":        0.3237,
        "treatment_count":  113,
        "treatment_pct":    0.4021,
        "flags":            [],
    },
    {
        "fixture":          "3f7bca_Farmington.pdf",
        "location":         "Farmington",
        "week_start":       "2026-04-01",
        "week_end":         "2026-04-05",
        "guest_count":      227,
        "service_net":      9688.50,
        "product_net":      1000.85,
        "total_sales":      10689.35,
        "avg_ticket":       47.09,
        "pph":              58.80,
        "ppg":              4.41,
        "production_hours": 164.7667,
        "wax_count":        20,
        "wax_pct":          0.0881,
        "color_sales":      1847.00,
        "color_pct":        0.1728,
        "treatment_count":  39,
        "treatment_pct":    0.1718,
        "flags":            [],
    },
    {
        # Lakeville: the only fixture that carries a legitimate parser
        # flag. The real PDF contains a partial-week indicator. This
        # fixture also guards against anyone "cleaning up" the parser
        # flag set.
        "fixture":          "7071b3_Lakeville.pdf",
        "location":         "Lakeville",
        "week_start":       "2026-04-01",
        "week_end":         "2026-04-05",
        "guest_count":      148,
        "service_net":      4636.30,
        "product_net":      534.50,
        "total_sales":      5170.80,
        "avg_ticket":       34.94,
        "pph":              38.05,
        "ppg":              3.61,
        "production_hours": 172.5667,
        "wax_count":        16,
        "wax_pct":          0.1081,
        "color_sales":      1507.00,
        "color_pct":        0.2914,
        "treatment_count":  21,
        "treatment_pct":    0.1419,
        "flags":            ["PARTIAL_WEEK"],
    },
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fixture_path(filename: str) -> str:
    """Resolve a fixture filename to an absolute path under data/inbox/."""
    return str(_REPO_ROOT / "data" / "inbox" / filename)


def _kpi_assertions(tc: unittest.TestCase, golden: Dict[str, Any], parsed: Dict[str, Any]) -> None:
    """Assert every Karissa-canonical KPI field matches the golden record."""
    karissa = parsed.get("karissa") or {}
    ctx = f"{golden['fixture']} ({golden['location']})"

    # Integers — exact match
    tc.assertEqual(karissa.get("guest_count"), golden["guest_count"],
                   f"{ctx}: guest_count")
    tc.assertEqual(karissa.get("wax_count"), golden["wax_count"],
                   f"{ctx}: wax_count")
    tc.assertEqual(karissa.get("treatment_count"), golden["treatment_count"],
                   f"{ctx}: treatment_count")

    # Money — within MONEY_TOL
    for field in ("service_net", "product_net", "total_sales",
                  "avg_ticket", "pph", "color_sales"):
        tc.assertAlmostEqual(
            karissa.get(field), golden[field], delta=MONEY_TOL,
            msg=f"{ctx}: {field} — got {karissa.get(field)} expected {golden[field]}",
        )

    # Ratios — within RATIO_TOL
    for field in ("ppg", "wax_pct", "color_pct", "treatment_pct"):
        tc.assertAlmostEqual(
            karissa.get(field), golden[field], delta=RATIO_TOL,
            msg=f"{ctx}: {field} — got {karissa.get(field)} expected {golden[field]}",
        )

    # Hours — within HOURS_TOL
    tc.assertAlmostEqual(
        karissa.get("production_hours"), golden["production_hours"], delta=HOURS_TOL,
        msg=f"{ctx}: production_hours — got {karissa.get('production_hours')} expected {golden['production_hours']}",
    )


# ---------------------------------------------------------------------------
# Test cases
# ---------------------------------------------------------------------------

class TestFixturesExist(unittest.TestCase):
    """All 12 golden fixtures must be present on disk before anything else runs."""

    def test_all_fixtures_present(self):
        missing = []
        for g in GOLDEN_ZENOTI + GOLDEN_SU:
            p = _fixture_path(g["fixture"])
            if not os.path.exists(p):
                missing.append(p)
        self.assertEqual(missing, [],
                         f"Missing fixtures: {missing}. "
                         f"Restore from data/archive/ or rebuild from the source inbox.")


class TestPlatformDetection(unittest.TestCase):
    """`detect_pos_from_file` must route every fixture to the right parser."""

    def test_all_zenoti_detected(self):
        for g in GOLDEN_ZENOTI:
            with self.subTest(fixture=g["fixture"]):
                platform = detect_pos_from_file(_fixture_path(g["fixture"]))
                self.assertEqual(
                    platform, ZENOTI,
                    f"{g['fixture']}: expected ZENOTI, got {platform!r}",
                )

    def test_all_su_detected(self):
        for g in GOLDEN_SU:
            with self.subTest(fixture=g["fixture"]):
                platform = detect_pos_from_file(_fixture_path(g["fixture"]))
                self.assertEqual(
                    platform, SALON_ULTIMATE,
                    f"{g['fixture']}: expected SALON_ULTIMATE, got {platform!r}",
                )

    def test_prior_lake_is_zenoti_regression(self):
        # BUG-1 regression guard. See CLAUDE.md.
        platform = detect_pos_from_file(_fixture_path("e80f78_Prior Lake.pdf"))
        self.assertEqual(
            platform, ZENOTI,
            "Prior Lake MUST be detected as Zenoti (z006). If this fails, "
            "check config/locations.py — do NOT re-introduce "
            "'Prior Lake': 'salon_ultimate'.",
        )


class TestZenotiGolden(unittest.TestCase):
    """Every Zenoti fixture must parse to its pinned golden KPI values."""

    def test_all_zenoti_parse_to_golden(self):
        for g in GOLDEN_ZENOTI:
            with self.subTest(fixture=g["fixture"]):
                parsed = zenoti_parse_file(_fixture_path(g["fixture"]))
                ctx = f"{g['fixture']} ({g['location']})"

                # Platform + location + dates
                self.assertEqual(parsed.get("platform"), "zenoti",
                                 f"{ctx}: platform")
                self.assertEqual(parsed.get("location"), g["location"],
                                 f"{ctx}: location")
                self.assertEqual(parsed.get("week_start"), g["week_start"],
                                 f"{ctx}: week_start")
                self.assertEqual(parsed.get("week_end"), g["week_end"],
                                 f"{ctx}: week_end")
                self.assertEqual(parsed.get("flags"), g["flags"],
                                 f"{ctx}: flags — expected {g['flags']} got {parsed.get('flags')}")

                _kpi_assertions(self, g, parsed)


class TestSalonUltimateGolden(unittest.TestCase):
    """Every Salon Ultimate fixture must parse to its pinned golden KPI values."""

    def test_all_su_parse_to_golden(self):
        for g in GOLDEN_SU:
            with self.subTest(fixture=g["fixture"]):
                parsed = su_parse_file(_fixture_path(g["fixture"]))
                ctx = f"{g['fixture']} ({g['location']})"

                self.assertEqual(parsed.get("platform"), "salon_ultimate",
                                 f"{ctx}: platform")
                self.assertEqual(parsed.get("location"), g["location"],
                                 f"{ctx}: location")
                self.assertEqual(parsed.get("week_start"), g["week_start"],
                                 f"{ctx}: week_start")
                self.assertEqual(parsed.get("week_end"), g["week_end"],
                                 f"{ctx}: week_end")
                self.assertEqual(parsed.get("flags"), g["flags"],
                                 f"{ctx}: flags — expected {g['flags']} got {parsed.get('flags')}")

                _kpi_assertions(self, g, parsed)


class TestTransformToCurrentRow(unittest.TestCase):
    """
    `transform_to_current_row` must map the parser's Karissa block into the
    20-column V1 CURRENT tab schema (A–T). We spot-check one Zenoti and one
    SU fixture end-to-end — ordering, display-name resolution, and value
    pass-through.
    """

    @classmethod
    def setUpClass(cls):
        cls.customer_cfg = _load_customer_config("karissa_001")
        cls.assertIsNotNone = unittest.TestCase.assertIsNotNone  # alias for mypy
        if cls.customer_cfg is None:
            raise unittest.SkipTest(
                "config/customers/karissa_001.json not found — "
                "transform tests require the customer config."
            )
        cls.lookup = _build_location_lookup(cls.customer_cfg)

    # Full schema keys for the 20-column V1 CURRENT tab dict returned by
    # transform_to_current_row(). Keeping this list co-located with the
    # assertion helper gives us a single place to notice drift.
    CURRENT_ROW_KEYS = {
        "loc_name", "week_ending", "platform", "guests", "total_sales",
        "service", "product", "product_pct", "ppg", "pph",
        "avg_ticket", "prod_hours", "wax_count", "wax", "wax_pct",
        "color", "color_pct", "treat_count", "treat", "treat_pct",
    }

    def _assert_row_matches(self, golden: Dict[str, Any], row: Dict[str, Any],
                            display_name: str, platform: str) -> None:
        """Compare a CURRENT-tab dict row against the golden parser output."""
        ctx = golden["fixture"]

        # All 20 keys must be present
        self.assertEqual(
            set(row.keys()), self.CURRENT_ROW_KEYS,
            f"{ctx}: CURRENT row keys must match the 20-column V1 schema. "
            f"Missing: {self.CURRENT_ROW_KEYS - set(row.keys())}. "
            f"Extra: {set(row.keys()) - self.CURRENT_ROW_KEYS}.",
        )

        self.assertEqual(row["loc_name"], display_name, f"{ctx}: loc_name")
        self.assertEqual(row["week_ending"], golden["week_end"], f"{ctx}: week_ending")
        self.assertEqual(row["platform"], platform, f"{ctx}: platform")
        self.assertEqual(row["guests"], golden["guest_count"], f"{ctx}: guests")
        self.assertAlmostEqual(row["total_sales"], golden["total_sales"], delta=MONEY_TOL,
                               msg=f"{ctx}: total_sales")
        self.assertAlmostEqual(row["service"], golden["service_net"], delta=MONEY_TOL,
                               msg=f"{ctx}: service")
        self.assertAlmostEqual(row["product"], golden["product_net"], delta=MONEY_TOL,
                               msg=f"{ctx}: product")
        self.assertAlmostEqual(row["pph"], golden["pph"], delta=MONEY_TOL,
                               msg=f"{ctx}: pph")
        self.assertAlmostEqual(row["avg_ticket"], golden["avg_ticket"], delta=MONEY_TOL,
                               msg=f"{ctx}: avg_ticket")
        self.assertAlmostEqual(row["prod_hours"], golden["production_hours"], delta=HOURS_TOL,
                               msg=f"{ctx}: prod_hours")
        self.assertEqual(row["wax_count"], golden["wax_count"], f"{ctx}: wax_count")
        self.assertAlmostEqual(row["wax_pct"], golden["wax_pct"], delta=RATIO_TOL,
                               msg=f"{ctx}: wax_pct")
        self.assertAlmostEqual(row["color"], golden["color_sales"], delta=MONEY_TOL,
                               msg=f"{ctx}: color_sales")
        self.assertAlmostEqual(row["color_pct"], golden["color_pct"], delta=RATIO_TOL,
                               msg=f"{ctx}: color_pct")
        self.assertEqual(row["treat_count"], golden["treatment_count"],
                         f"{ctx}: treat_count")
        self.assertAlmostEqual(row["treat_pct"], golden["treatment_pct"], delta=RATIO_TOL,
                               msg=f"{ctx}: treat_pct")

        # Cross-check: product_pct must be product / total_sales, rounded 4dp.
        if row["total_sales"] > 0:
            expected_pp = round(row["product"] / row["total_sales"], 4)
            self.assertAlmostEqual(
                row["product_pct"], expected_pp, delta=RATIO_TOL,
                msg=f"{ctx}: product_pct must be product / total_sales",
            )

    def test_zenoti_transform_andover(self):
        g = next(x for x in GOLDEN_ZENOTI if x["location"] == "Andover")
        parsed = zenoti_parse_file(_fixture_path(g["fixture"]))
        # Display name for Andover is "Andover FS" per the customer config.
        entry = self.lookup.get("andover")
        self.assertIsNotNone(entry, "Andover must resolve via the location lookup")
        row = transform_to_current_row(
            parsed,
            platform=entry["platform"],
            loc_display_name=entry["display"],
        )
        self._assert_row_matches(g, row, entry["display"], entry["platform"])

    def test_su_transform_apple_valley(self):
        g = next(x for x in GOLDEN_SU if x["location"] == "Apple Valley")
        parsed = su_parse_file(_fixture_path(g["fixture"]))
        entry = self.lookup.get("apple valley")
        self.assertIsNotNone(entry, "Apple Valley must resolve via the location lookup")
        row = transform_to_current_row(
            parsed,
            platform=entry["platform"],
            loc_display_name=entry["display"],
        )
        self._assert_row_matches(g, row, entry["display"], entry["platform"])


class TestKarissaCanonicalContracts(unittest.TestCase):
    """
    Narrow, high-signal checks on the KPI contract itself — the rules
    we care about even if someone "fixes" one of the golden numbers.
    """

    def test_color_pct_is_revenue_share_not_penetration(self):
        # Andover: color_sales=944, total_sales=4222.20 → 0.2236
        # If someone ever changed color_pct to color_count/guest_count,
        # the number would be 12/95 = 0.1263 — fail.
        parsed = zenoti_parse_file(_fixture_path("1232b9_Andover.pdf"))
        k = parsed["karissa"]
        expected = round(k["color_sales"] / k["total_sales"], 4)
        self.assertAlmostEqual(k["color_pct"], expected, delta=RATIO_TOL,
                               msg="color_pct must be color_sales / total_sales (revenue share)")

    def test_wax_pct_is_count_over_guests(self):
        # Blaine: 27 wax guests / 158 total = 0.1709
        parsed = zenoti_parse_file(_fixture_path("6b53de_Blaine.pdf"))
        k = parsed["karissa"]
        expected = round(k["wax_count"] / k["guest_count"], 4)
        self.assertAlmostEqual(k["wax_pct"], expected, delta=RATIO_TOL,
                               msg="wax_pct must be wax_count / guest_count (penetration)")

    def test_treatment_pct_is_count_over_guests(self):
        parsed = zenoti_parse_file(_fixture_path("ba3f13_Hudson.pdf"))
        k = parsed["karissa"]
        expected = round(k["treatment_count"] / k["guest_count"], 4)
        self.assertAlmostEqual(k["treatment_pct"], expected, delta=RATIO_TOL,
                               msg="treatment_pct must be treatment_count / guest_count")

    def test_roseville_waxing_only_did_not_zero_out(self):
        # Roseville has NO "Wax" row — only "Waxing". Regression: parser
        # must still report a non-zero wax_count by summing wax + waxing.
        parsed = zenoti_parse_file(_fixture_path("e7b9a5_Roseville.pdf"))
        k = parsed["karissa"]
        self.assertGreater(k["wax_count"], 0,
                           "Roseville Waxing-only case: wax_count must not be zero. "
                           "Regression in wax + waxing summing logic.")

    def test_total_sales_equals_service_plus_product(self):
        # Spot-check on Hudson — total_sales must match the sum, not any
        # tax-inclusive figure from the PDF.
        parsed = zenoti_parse_file(_fixture_path("ba3f13_Hudson.pdf"))
        k = parsed["karissa"]
        self.assertAlmostEqual(
            k["total_sales"], k["service_net"] + k["product_net"], delta=MONEY_TOL,
            msg="total_sales must equal service_net + product_net (pre-tax). "
                "If this drifts, someone is reading the 'Sales(Inc. Tax)' field.",
        )


# ---------------------------------------------------------------------------
# Script-mode entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    unittest.main(verbosity=2)
