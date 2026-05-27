"""
tests/test_data_source_date_normalize.py
────────────────────────────────────────
Tests for core/data_source.py::_to_date_str — the helper that normalizes
date cell values from Sheets API reads.

Sheets stores dates entered via USER_ENTERED as serial numbers. With
valueRenderOption=UNFORMATTED_VALUE, the API returns those as integers
(e.g., 46166 for 2026-05-24). dateTimeRenderOption=FORMATTED_STRING only
helps when the cell has a date *format* — not just date *type*. So we
normalize at the data_source layer.
"""

from __future__ import annotations

import datetime
import sys
import unittest
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from core.data_source import _to_date_str  # noqa: E402


class TestToDateStr(unittest.TestCase):
    def test_excel_serial_int(self):
        """Sheets serial 46166 should decode to 2026-05-24 (the date the pipeline wrote)."""
        self.assertEqual(_to_date_str(46166), "2026-05-24")

    def test_excel_serial_float(self):
        """Excel serial 1.5 = 1899-12-30 + 1 day = 1899-12-31 (intval-truncated)."""
        self.assertEqual(_to_date_str(1.5), "1899-12-31")

    def test_iso_string_passes_through(self):
        self.assertEqual(_to_date_str("2026-05-24"), "2026-05-24")

    def test_us_slash_date(self):
        self.assertEqual(_to_date_str("5/24/2026"), "2026-05-24")

    def test_us_slash_short_year(self):
        self.assertEqual(_to_date_str("5/24/26"), "2026-05-24")

    def test_iso_slash_date(self):
        self.assertEqual(_to_date_str("2026/05/24"), "2026-05-24")

    def test_python_date(self):
        self.assertEqual(_to_date_str(datetime.date(2026, 5, 24)), "2026-05-24")

    def test_python_datetime(self):
        # datetime is a subclass of date — isoformat would give 'YYYY-MM-DDTHH:MM:SS'
        # We accept this; downstream code uses [:7] which still works
        result = _to_date_str(datetime.datetime(2026, 5, 24, 12, 30, 0))
        self.assertTrue(result.startswith("2026-05-24"))

    def test_empty_string(self):
        self.assertEqual(_to_date_str(""), "")

    def test_none(self):
        self.assertEqual(_to_date_str(None), "")

    def test_unparseable_passes_through(self):
        """If we can't parse it, return as-is — caller decides what to do."""
        self.assertEqual(_to_date_str("not a date"), "not a date")


class TestDateStrUsageContract(unittest.TestCase):
    """Verifies the output works downstream — specifically derive_year_month
    and len() comparisons that broke on the raw int return."""

    def test_output_is_always_string(self):
        for val in (46166, "2026-05-24", "5/24/2026", datetime.date(2026, 5, 24)):
            self.assertIsInstance(_to_date_str(val), str)

    def test_output_indexable_to_7_chars(self):
        """derive_year_month does week_ending[:7] — must work on output."""
        result = _to_date_str(46166)
        self.assertEqual(result[:7], "2026-05")


if __name__ == "__main__":
    unittest.main()
