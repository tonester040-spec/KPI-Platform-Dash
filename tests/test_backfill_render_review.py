"""
tests/test_backfill_render_review.py
────────────────────────────────────
Smoke tests for the human-readable renderers in
scripts/backfill/render_review.py. Output is for eyeballs, not parsers —
we just verify it contains the right shape and the right key data points.
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from scripts.backfill.render_review import (  # noqa: E402
    render_cross_check,
    render_kpi_table,
    render_validation_summary,
)


SAMPLE_ROWS = [
    {
        "loc_name": "Andover FS", "year_month": "2026-03", "platform": "zenoti",
        "guests": 578, "service": 31534.55, "product": 1757.25, "total_sales": 33291.80,
        "ppg": 3.04, "pph": 41.51, "avg_ticket": 57.60,
        "wax_pct": 0.139, "color_pct": 0.341, "treat_pct": 0.102,
    },
    {
        "loc_name": "Apple Valley", "year_month": "2026-03", "platform": "salon_ultimate",
        "guests": 1796, "service": 97223.30, "product": 8253.82, "total_sales": 105477.12,
        "ppg": 4.60, "pph": 53.74, "avg_ticket": 58.73,
        "wax_pct": 0.153, "color_pct": 0.296, "treat_pct": 0.339,
    },
]


class TestRenderKpiTable(unittest.TestCase):
    def test_includes_each_location(self):
        out = render_kpi_table(SAMPLE_ROWS, title="March 2026")
        self.assertIn("Andover FS", out)
        self.assertIn("Apple Valley", out)
        self.assertIn("March 2026", out)

    def test_includes_totals(self):
        out = render_kpi_table(SAMPLE_ROWS)
        # 578 + 1796 = 2374
        self.assertIn("2374", out)
        # 31534.55 + 97223.30 = 128757.85
        self.assertIn("128,757.85", out)
        self.assertIn("TOTAL", out)

    def test_platform_marker(self):
        out = render_kpi_table(SAMPLE_ROWS)
        # Z for Zenoti, S for SU
        self.assertIn(" Z ", out)
        self.assertIn(" S ", out)

    def test_empty_rows(self):
        out = render_kpi_table([], title="empty")
        self.assertIn("no rows", out)


class TestRenderValidationSummary(unittest.TestCase):
    def test_all_clean_message(self):
        out = render_validation_summary([])
        self.assertIn("All clean", out)

    def test_groups_by_severity(self):
        issues = [
            {"severity": "error", "code": "BAD", "location": "Loc1", "message": "boom"},
            {"severity": "warning", "code": "EH", "location": "Loc2", "message": "meh"},
            {"severity": "info", "code": "FYI", "location": "Loc3", "message": "fyi"},
        ]
        out = render_validation_summary(issues)
        # Headings present
        self.assertIn("ERRORS", out)
        self.assertIn("WARNINGS", out)
        self.assertIn("INFO", out)
        # Counts
        self.assertIn("1 error(s)", out)
        self.assertIn("1 warning(s)", out)
        self.assertIn("1 info", out)
        # Messages
        self.assertIn("BAD", out)
        self.assertIn("Loc1", out)


class TestRenderCrossCheck(unittest.TestCase):
    def test_passes_through_findings(self):
        findings = [
            {"severity": "info", "code": "GUEST_COUNT_DIFF", "location": "Blaine",
             "message": "tracker=1050 vs xlsx=1199"},
        ]
        out = render_cross_check(findings)
        self.assertIn("Cross-check", out)
        self.assertIn("Blaine", out)
        self.assertIn("1199", out)


if __name__ == "__main__":
    unittest.main()
