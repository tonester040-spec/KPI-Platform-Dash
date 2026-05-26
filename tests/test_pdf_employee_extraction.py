"""
PDF Parsers — Employee Extraction Tests
────────────────────────────────────────
Pins per-stylist extraction from `_extract_employees` for both parsers.

Why this suite exists
─────────────────────
Stylist extraction was silently broken from the v2 parser commit (7abe5c3,
2026-04-21) until 2026-05-26 — for ~5 weeks. The golden suite asserts
location-level KPIs but never asserted `parsed["employees"]`. Tony found the
bug when he checked `len(parsed["employees"])` and got 0 on all 12 fixtures.

Root cause: PyMuPDF renders the per-stylist tables in vertical column order
(one field per line). Both parsers' per-row regexes were written for a
columnar layout (pdftotext -layout style) and walked `splitlines()` with
`.match()` per line, which can never see a full row. See
STYLIST_EXTRACTION_ROOT_CAUSE_2026-05-26.md for the full diagnosis.

These tests are written to FAIL on main and PASS on the fix branch. The
key assertion is `assertGreater(len(employees), 0)` per fixture — any
employee at all proves the walker now works. Spot-checks pin specific
stylist names so a regression that drops to a single nameless dict still
fails. Field-shape tests guard against accidental key removal.

Fixtures live in: data/inbox/*.pdf (12 files — 9 Zenoti, 3 Salon Ultimate).
Do NOT delete them — they are the same fixtures the golden suite uses.

Run with:
    python -m pytest tests/test_pdf_employee_extraction.py -v
Or without pytest:
    python tests/test_pdf_employee_extraction.py
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from typing import Any, Dict, List

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from parsers.pdf_zenoti_v2 import parse_file as zenoti_parse_file
from parsers.pdf_salon_ultimate_v2 import parse_file as su_parse_file


def _fixture_path(name: str) -> str:
    return str(_REPO_ROOT / "data" / "inbox" / name)


# ---------------------------------------------------------------------------
# Expected presence (lower bounds + spot-check names)
# ---------------------------------------------------------------------------
# Lower bounds are conservative — they reflect the minimum number of
# individuals we know each location had on 2026-04-01 to 2026-04-05.
# Spot-check names are stylists confirmed visually in the raw PDF text.
# We do NOT pin exact counts here — that's a separate, tighter contract
# once the field shapes settle (TODO once fix is verified).

ZENOTI_EXPECTED: List[Dict[str, Any]] = [
    {"fixture": "1232b9_Andover.pdf",       "min": 3,  "names": ["Katelyn Kuchinski", "Mackenzie Literski"]},
    {"fixture": "6b53de_Blaine.pdf",        "min": 1,  "names": []},
    {"fixture": "33ff41_Crystal.pdf",       "min": 1,  "names": []},
    {"fixture": "73928f_Elk River.pdf",     "min": 1,  "names": []},
    {"fixture": "b6c35f_Forest Lake.pdf",   "min": 1,  "names": []},
    {"fixture": "ba3f13_Hudson.pdf",        "min": 1,  "names": []},
    {"fixture": "ef7fe8_New Richmond.pdf",  "min": 1,  "names": []},
    {"fixture": "e80f78_Prior Lake.pdf",    "min": 1,  "names": []},
    {"fixture": "e7b9a5_Roseville.pdf",     "min": 1,  "names": []},
]

SU_EXPECTED: List[Dict[str, Any]] = [
    {"fixture": "c73c34_Apple Valley.pdf",  "min": 8, "names": ["Ari Spainhower", "Chloe Benage", "Kristin Martin"]},
    {"fixture": "3f7bca_Farmington.pdf",    "min": 1, "names": []},
    {"fixture": "7071b3_Lakeville.pdf",     "min": 1, "names": []},
]


# Required keys on every employee dict. Field names are taken from the
# existing _extract_employees implementations so the contract matches the
# current schema (see parsers/pdf_zenoti_v2.py:_extract_employees docstring
# and parsers/pdf_salon_ultimate_v2.py:_extract_employees).
ZENOTI_REQUIRED_KEYS = {"name", "role_group"}
SU_REQUIRED_KEYS     = {"name", "net_service", "net_retail", "guests"}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
class TestZenotiEmployeeExtraction(unittest.TestCase):
    def test_each_fixture_has_employees(self):
        """Every Zenoti fixture must extract at least one employee dict."""
        for spec in ZENOTI_EXPECTED:
            with self.subTest(fixture=spec["fixture"]):
                parsed = zenoti_parse_file(_fixture_path(spec["fixture"]))
                employees = parsed.get("employees") or []
                self.assertGreaterEqual(
                    len(employees), spec["min"],
                    f"{spec['fixture']}: expected >= {spec['min']} employees, "
                    f"got {len(employees)}"
                )

    def test_spot_check_known_stylist_names(self):
        """Known stylist names from raw PDF inspection must appear in output."""
        for spec in ZENOTI_EXPECTED:
            if not spec["names"]:
                continue
            with self.subTest(fixture=spec["fixture"]):
                parsed = zenoti_parse_file(_fixture_path(spec["fixture"]))
                names = {e.get("name", "") for e in (parsed.get("employees") or [])}
                for expected in spec["names"]:
                    self.assertIn(
                        expected, names,
                        f"{spec['fixture']}: expected stylist {expected!r} not "
                        f"found in employees. Got names: {sorted(names)}"
                    )

    def test_each_employee_has_required_keys(self):
        """Every Zenoti employee dict must have the contract's required keys."""
        for spec in ZENOTI_EXPECTED:
            with self.subTest(fixture=spec["fixture"]):
                parsed = zenoti_parse_file(_fixture_path(spec["fixture"]))
                employees = parsed.get("employees") or []
                if not employees:
                    self.skipTest("no employees extracted (separate test catches this)")
                for emp in employees:
                    missing = ZENOTI_REQUIRED_KEYS - set(emp.keys())
                    self.assertFalse(
                        missing,
                        f"{spec['fixture']}: employee {emp.get('name')!r} is "
                        f"missing required keys: {missing}"
                    )

    def test_no_employee_has_blank_name(self):
        """Blank or whitespace-only names indicate the walker captured a
        continuation line as a standalone row, or vice versa."""
        for spec in ZENOTI_EXPECTED:
            with self.subTest(fixture=spec["fixture"]):
                parsed = zenoti_parse_file(_fixture_path(spec["fixture"]))
                for emp in (parsed.get("employees") or []):
                    name = (emp.get("name") or "").strip()
                    self.assertTrue(
                        name,
                        f"{spec['fixture']}: found employee with blank name: {emp!r}"
                    )


class TestSalonUltimateEmployeeExtraction(unittest.TestCase):
    def test_each_fixture_has_employees(self):
        for spec in SU_EXPECTED:
            with self.subTest(fixture=spec["fixture"]):
                parsed = su_parse_file(_fixture_path(spec["fixture"]))
                employees = parsed.get("employees") or []
                self.assertGreaterEqual(
                    len(employees), spec["min"],
                    f"{spec['fixture']}: expected >= {spec['min']} employees, "
                    f"got {len(employees)}"
                )

    def test_spot_check_known_stylist_names(self):
        for spec in SU_EXPECTED:
            if not spec["names"]:
                continue
            with self.subTest(fixture=spec["fixture"]):
                parsed = su_parse_file(_fixture_path(spec["fixture"]))
                names = {e.get("name", "") for e in (parsed.get("employees") or [])}
                for expected in spec["names"]:
                    self.assertIn(
                        expected, names,
                        f"{spec['fixture']}: expected stylist {expected!r} not "
                        f"found. Got names: {sorted(names)}"
                    )

    def test_each_employee_has_required_keys(self):
        for spec in SU_EXPECTED:
            with self.subTest(fixture=spec["fixture"]):
                parsed = su_parse_file(_fixture_path(spec["fixture"]))
                employees = parsed.get("employees") or []
                if not employees:
                    self.skipTest("no employees extracted (separate test catches this)")
                for emp in employees:
                    missing = SU_REQUIRED_KEYS - set(emp.keys())
                    self.assertFalse(
                        missing,
                        f"{spec['fixture']}: employee {emp.get('name')!r} is "
                        f"missing required keys: {missing}"
                    )

    def test_phantom_house_is_flagged(self):
        """SU exports include a 'House' phantom row for retail-only sales.
        The parser tags it with is_phantom_house=True so downstream filters
        can exclude it from stylist KPIs."""
        for spec in SU_EXPECTED:
            with self.subTest(fixture=spec["fixture"]):
                parsed = su_parse_file(_fixture_path(spec["fixture"]))
                employees = parsed.get("employees") or []
                house_rows = [e for e in employees if (e.get("name", "").lower() == "house")]
                if house_rows:
                    self.assertTrue(
                        all(e.get("is_phantom_house") for e in house_rows),
                        f"{spec['fixture']}: 'House' row exists but "
                        f"is_phantom_house was not set: {house_rows}"
                    )


if __name__ == "__main__":
    unittest.main(verbosity=2)
