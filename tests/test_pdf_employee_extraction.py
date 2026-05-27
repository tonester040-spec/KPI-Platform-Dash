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
from parsers.pdf_zenoti_v2 import _unwrap_pymupdf_number_wraps
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


class TestZenotiColumnWrapUnwrap(unittest.TestCase):
    """Regression coverage for the PyMuPDF column-overflow wrap bug.

    Bug discovered 2026-05-26 in fresh-PDF validation: when a Zenoti report
    has a numeric value too wide for its visual column (typically money
    >= $1,000.00 in narrow money columns), PyMuPDF wraps the trailing digit
    to the next line. The parser's row regexes expect one numeric field per
    line, so the wrap injects an extra "field," shifts the field count by
    one, and the entire row silently fails to match. Production impact at
    discovery: 3 Hudson stylists + 1 Blaine stylist dropped from
    STYLISTS_CURRENT ($30,644.85 of service revenue silently zeroed), and
    Crystal's location-level production_hours flagged as
    PRODUCTION_HOURS_NOT_IDENTIFIED.

    Apr 21 fixtures never triggered the bug because weekly tips don't
    reach $1,000. Tests here use synthetic strings so they don't depend on
    any specific fixture wrap pattern.
    """

    def test_unwraps_tips_value_in_sale_row(self):
        """Hudson 2026-05-26 pattern: Jessica Stoik's $1,490.48 tips."""
        section = (
            "Jessica Stoik\n"
            "9,929.50\n"
            "9,929.50\n"
            "10.00\n"
            "1,490.4\n"
            "8\n"
            "253\n"
            "154\n"
        )
        out = _unwrap_pymupdf_number_wraps(section)
        self.assertIn("1,490.48", out)
        self.assertNotIn("1,490.4\n8\n", out)
        # Other lines untouched
        self.assertIn("9,929.50", out)
        self.assertIn("253", out)

    def test_unwraps_hours_value_in_total_row(self):
        """Crystal 2026-05-26 pattern: PERF Total row actual_hours 1011.22."""
        section = "Total\n0.40 382.38 1011.2\n2\n950.88\n"
        out = _unwrap_pymupdf_number_wraps(section)
        self.assertIn("1011.22", out)
        self.assertNotIn("1011.2\n2\n", out)
        self.assertIn("950.88", out)

    def test_leaves_legitimate_two_decimal_values_alone(self):
        """A two-decimal money value like `602.41` on its own line followed
        by a count is the normal case and must not be merged."""
        section = "Crystal Riley\n2,601.00\n2,601.00\n0.00\n602.41\n104\n86\n"
        out = _unwrap_pymupdf_number_wraps(section)
        self.assertEqual(out, section)

    def test_leaves_value_followed_by_multi_digit_count_alone(self):
        """`1,047.8` followed by `67` (multi-digit) is NOT a wrap signature;
        the lookahead requires the next line to be a single digit only."""
        section = "1,047.8\n67\n"
        out = _unwrap_pymupdf_number_wraps(section)
        self.assertEqual(out, section)

    def test_blaine_heather_murto_row_now_parses(self):
        """End-to-end against the synthetic Blaine row that previously failed.
        After unwrap, the 12-field row matches the SALE row regex."""
        from parsers.pdf_zenoti_v2 import _RE_EMPLOYEE_SALE_INDIV
        section = (
            "Heather Murto\n"
            "6,516.20\n"     # net_service
            "6,539.60\n"     # comm_service
            "193.40\n"       # comm_disc
            "1,047.8\n"      # tips (wrapped)
            "6\n"            # wrapped tail
            "183\n"          # service_qty
            "115\n"          # invoice_count
            "169.00\n"       # net_product
            "169.00\n"       # comm_product
            "1.47\n"         # net_prod_per_pi
            "58.13\n"        # avg_invoice_value
            "94.80\n"        # disc_$
            "14.51\n"        # disc_pct
        )
        unwrapped = _unwrap_pymupdf_number_wraps(section)
        m = _RE_EMPLOYEE_SALE_INDIV.search(unwrapped)
        self.assertIsNotNone(m, f"Row should match after unwrap. Unwrapped section: {unwrapped!r}")
        self.assertEqual(m.group("name"), "Heather Murto")
        self.assertEqual(m.group("tips"), "1,047.86")
        self.assertEqual(m.group("service_qty"), "183")


class TestZenotiEmployeeMergeAcrossSections(unittest.TestCase):
    """Regression coverage for two name-mismatch bugs found 2026-05-27 against
    the April monthly PDFs. Both bugs produced two half-records per stylist
    (one with money, one with hours) instead of one merged record, leaving
    `production_hours = None` on the money half. STYLISTS_DATA_MONTHLY then
    received zero hours for the stylist and PPH was reported as 0.

    The fixtures here use synthetic Zenoti SALE + PERF sections so the
    assertions don't depend on whether the bug fixtures get refreshed.
    """

    def _parse(self, text: str):
        from parsers.pdf_zenoti_v2 import ZenotiV2Parser
        return ZenotiV2Parser(text).parse()["employees"]

    def test_hyphenated_name_merges_when_wrapped_in_one_section(self):
        """Crystal Apr 2026 — Kamia Guy-Robinson case. SALE wraps the name
        across two lines at the hyphen ('Kamia Guy-' + 'Robinson'); PERF
        renders it on one line. Merge must produce ONE record carrying both
        revenue and hours."""
        text = (
            "888-10100-Crystal\n"
            "From Apr 1 2026 To Apr 30 2026\n"
            "EMPLOYEE SALE DETAILS\n"
            "STYLIST\n"
            "0.00 0.00 0.00 0.00 0 0.00 0.00 0.00 0.00\n"
            "Existing Stylist\n"
            "100.00\n100.00\n0.00\n10.00\n5\n5\n0.00\n0.00\n0.00\n20.00\n0.00\n0.00\n"
            "Kamia Guy-\n"
            "Robinson\n"
            "2,915.00\n2,915.00\n26.35\n514.52\n92\n68\n279.00\n279.00\n4.10\n46.97\n95.00\n5.70\n"
            "EMPLOYEE PERFORMANCE DETAILS\n"
            "STYLIST\n"
            "0.00 0.00 0.00 0.00 0.00 0.00 0.00 0.00 0.00 0\n"
            "Existing Stylist\n"
            "0.10\n2.00\n8.00\n8.00\n0.00\n0.00\n12.50\n12.50\n12.50\n3\n"
            "Kamia Guy-Robinson\n"
            "0.15\n16.50\n110.28\n110.28\n0.00\n12.42\n26.43\n27.29\n26.43\n6\n"
            "(0.00)\n"
        )
        emps = self._parse(text)
        kamia = [e for e in emps if "kamia" in (e.get("name") or "").lower()]
        self.assertEqual(
            len(kamia), 1,
            f"expected ONE merged Kamia row, got {len(kamia)}: {kamia!r}"
        )
        row = kamia[0]
        self.assertEqual(row["name"], "Kamia Guy-Robinson")
        self.assertEqual(row["net_service"], 2915.0)
        self.assertEqual(row["production_hours"], 110.28)
        self.assertEqual(row["invoice_count"], 68)

    def test_first_stylist_gets_multi_line_name_lookback(self):
        """Roseville Apr 2026 — Alexandria Costello Martinez case. She's the
        first stylist in the SALE section and her name spans three lines
        ('Alexandria Costello' + 'Martinez'). PERF renders all three names
        on one line. Before the fix, SALE captured only 'Martinez' because
        lookback was disabled for the first match, and the two halves never
        merged. After the fix, lookback runs for the first match too."""
        text = (
            "888-40098-FS Roseville, MN\n"
            "From Apr 1 2026 To Apr 30 2026\n"
            "EMPLOYEE SALE DETAILS\n"
            "MANAGER\n"
            "3,614.50\n3,621.50\n69.99\n694.06\n102\n91.99\n89.06\n232.50\n8.36\n"
            "Alexandria Costello\n"
            "Martinez\n"
            "3,614.50\n3,621.50\n69.99\n694.06\n102\n82\n91.99\n89.06\n1.12\n45.20\n232.50\n8.36\n"
            "EMPLOYEE PERFORMANCE DETAILS\n"
            "MANAGER\n"
            "0.37 38.73 103.58 103.58 0.00 0.50 34.89 37.14 34.96 59\n"
            "(11.07)\n"
            "Alexandria Costello Martinez\n"
            "0.37\n38.73\n103.58\n103.58\n0.00\n0.50\n34.89\n37.14\n34.96\n59\n"
            "(0.00)\n"
        )
        emps = self._parse(text)
        martinez = [e for e in emps if "martinez" in (e.get("name") or "").lower()]
        self.assertEqual(
            len(martinez), 1,
            f"expected ONE merged Martinez row, got {len(martinez)}: {martinez!r}"
        )
        row = martinez[0]
        self.assertEqual(row["name"], "Alexandria Costello Martinez")
        self.assertEqual(row["net_service"], 3614.5)
        self.assertEqual(row["production_hours"], 103.58)

    def test_zero_hours_stylist_stays_zero(self):
        """Elk River Apr 2026 — Jen Watson case. Her PERF row in the PDF is
        all zeros (she's classified as 'Business Coach', not a service role),
        so production_hours = 0.0 is the correct extraction. This negative
        test guards against an over-eager future fix that synthesizes hours."""
        text = (
            "910-10500-Elk River\n"
            "From Apr 1 2026 To Apr 30 2026\n"
            "EMPLOYEE SALE DETAILS\n"
            "Business Coach\n"
            "118.00\n118.00\n0.00\n25.00\n4\n0.00\n0.00\n6.00\n0.42\n"
            "Jen Watson\n"
            "118.00\n118.00\n0.00\n25.00\n4\n4\n0.00\n0.00\n0.00\n29.50\n6.00\n0.42\n"
            "EMPLOYEE PERFORMANCE DETAILS\n"
            "Business Coach\n"
            "0.00 0.00 0.00 0.00 0.00 0.00 0.00 0.00 0.00 0\n"
            "(0.00)\n"
            "Jen Watson\n"
            "0.00\n0.00\n0.00\n0.00\n0.00\n0.00\n0.00\n0.00\n0.00\n0\n"
            "(0.00)\n"
        )
        emps = self._parse(text)
        jen = [e for e in emps if (e.get("name") or "").lower() == "jen watson"]
        self.assertEqual(
            len(jen), 1,
            f"expected ONE Jen Watson row, got {len(jen)}: {jen!r}"
        )
        row = jen[0]
        self.assertEqual(row["net_service"], 118.0)
        self.assertEqual(row["production_hours"], 0.0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
