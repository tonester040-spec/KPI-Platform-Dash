"""
tests/test_dashboard_loc_hist_pct.py
────────────────────────────────────
Verify the decimal→percent conversion for pct fields in
core/dashboard_builder.py::_build_loc_hist().

Pipeline convention: pct fields stored as DECIMAL (0.0796 = 7.96%) in
DATA and CURRENT — matches parser output and CLAUDE.md KPI formulas.
Dashboard JS expects PERCENT scale (7.96) for `.toFixed(1) + '%'`
rendering. The conversion happens at JS emission time.
"""

from __future__ import annotations

import re
import sys
import unittest
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from core.dashboard_builder import _build_loc_hist  # noqa: E402


class TestPctConversion(unittest.TestCase):
    def test_product_pct_multiplied_by_100_in_emission(self):
        loc = {
            "loc_name": "Blaine",
            "platform": "zenoti",
            "hist": {
                "product_pct": [0.05, 0.075, 0.10],  # decimal scale
                "wax_pct":     [0.15, 0.16, 0.18],
            },
        }
        js = _build_loc_hist([loc])
        # Find the 'p:' array (product_pct) and 'w:' (wax_pct) — values should be ×100
        # The emission format is `'Blaine': { plat:'zenoti', s:..., h:..., p:[5.0,7.5,10.0], ...}`
        m_p = re.search(r"p:\s*\[([\d.,\s-]+)\]", js)
        self.assertIsNotNone(m_p, f"could not find p: array in:\n{js}")
        p_values = [float(v) for v in m_p.group(1).split(",")]
        self.assertEqual(p_values, [5.0, 7.5, 10.0])  # 0.05 → 5.0, etc.

        m_w = re.search(r"w:\s*\[([\d.,\s-]+)\]", js)
        self.assertIsNotNone(m_w)
        w_values = [float(v) for v in m_w.group(1).split(",")]
        self.assertEqual(w_values, [15.0, 16.0, 18.0])

    def test_non_pct_fields_not_multiplied(self):
        """Sales, PPH, guests, avg_ticket are dollar/count fields — leave as-is."""
        loc = {
            "loc_name": "Blaine",
            "platform": "zenoti",
            "hist": {
                "total_sales": [18000.0],
                "pph":         [55.92],
                "guests":      [280],
                "avg_ticket":  [64.69],
                "product_pct": [0.10],
                "wax_pct":     [0.15],
            },
        }
        js = _build_loc_hist([loc])
        m_s = re.search(r"s:\s*\[([\d.,\s-]+)\]", js)
        self.assertEqual([float(v) for v in m_s.group(1).split(",")], [18000.0])
        m_h = re.search(r"h:\s*\[([\d.,\s-]+)\]", js)
        self.assertAlmostEqual(float(m_h.group(1)), 55.92, places=2)
        m_g = re.search(r"g:\s*\[([\d.,\s-]+)\]", js)
        self.assertEqual([float(v) for v in m_g.group(1).split(",")], [280.0])


if __name__ == "__main__":
    unittest.main()
