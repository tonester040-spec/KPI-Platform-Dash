"""
tests/test_backfill_monthly_pdf_loader.py
─────────────────────────────────────────
Tests for scripts/backfill/monthly_pdf_loader.py.

Three layers:
  1. Unit tests for the pure helpers (_validate_parse_result, _build_row,
     _build_bare_to_config) — fast, no I/O.
  2. discover_pdfs tests using a temp directory of empty .pdf files.
  3. load_monthly_pdfs end-to-end test with patched parsers.
  4. Optional integration test against the real April PDF folder — skipped
     if the folder isn't present.
"""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from scripts.backfill.monthly_pdf_loader import (  # noqa: E402
    DEFAULT_RECONCILIATION_TOLERANCE,
    INFORMATIONAL_FLAGS,
    ValidationIssue,
    _build_bare_to_config,
    _build_row,
    _build_stylist_rows,
    _validate_parse_result,
    discover_pdfs,
    load_monthly_pdfs,
)


SAMPLE_CONFIG = {
    "customer_id": "test_001",
    "sheet_id": "test_sheet",
    "locations": [
        {"id": "z001", "name": "Andover FS",   "platform": "zenoti"},
        {"id": "z002", "name": "Blaine",       "platform": "zenoti"},
        {"id": "z010", "name": "Apple Valley", "platform": "salon_ultimate"},
    ],
}


def _clean_zenoti_parsed(loc_name: str = "Andover", *, service_net: float = 28917.40, emp_sum: float = None) -> dict:
    """Build a synthetic Zenoti parse result that mirrors the real output shape."""
    if emp_sum is None:
        emp_sum = service_net  # reconciles by default

    # Split emp_sum across 2 stylists
    half = round(emp_sum / 2, 2)
    other = round(emp_sum - half, 2)
    return {
        "platform": "zenoti",
        "location": loc_name,
        "location_raw": f"888-10278-{loc_name}",
        "week_start": "2026-04-01",
        "week_end": "2026-04-30",
        "unclosed_days": [],
        "flags": [],
        "raw": {
            "service_net": service_net,
            "product_net": 2378.44,
        },
        "karissa": {
            "guest_count": 574,
            "service_net": service_net,
            "product_net": 2378.44,
            "total_sales": service_net + 2378.44,
            "avg_ticket": 54.52,
            "ppg": 4.14,
            "pph": 37.38,
            "wax_count": 62,
            "wax_sales": 1170.15,
            "wax_pct": 0.108,
            "color_sales": 10816.99,
            "color_count": 200,
            "color_pct": 0.3741,
            "treatment_count": 40,
            "treatment_sales": 1076.75,
            "treatment_pct": 0.0697,
            "production_hours": 773.62,
        },
        "service_categories": ["haircut", "color", "wax", "treatment"],
        "employees": [
            {"name": "Stylist A", "net_service": half},
            {"name": "Stylist B", "net_service": other},
        ],
    }


def _clean_su_parsed(loc_name: str = "Apple Valley", *, service_net: float = 89440.50) -> dict:
    """Synthetic SU parse result."""
    half = round(service_net / 2, 2)
    other = round(service_net - half, 2)
    return {
        "platform": "salon_ultimate",
        "location": loc_name,
        "location_raw": f"FS - {loc_name} Pilot Knob",
        "week_start": "2026-04-01",
        "week_end": "2026-04-30",
        "unclosed_days": [],
        "flags": [],
        "raw": {
            "total_service": service_net,
            "total_retail": 9363.44,
        },
        "karissa": {
            "guest_count": 1643,
            "service_net": service_net,
            "product_net": 9363.44,
            "total_sales": service_net + 9363.44,
            "avg_ticket": 60.14,
            "ppg": 5.70,
            "pph": 51.61,
            "wax_count": 241,
            "wax_pct": 0.1467,
            "color_sales": 27327.0,
            "color_pct": 0.3055,
            "treatment_count": 573,
            "treatment_pct": 0.3488,
            "production_hours": 1733.02,
            # SU intentionally lacks wax_sales / treatment_sales / color_count
        },
        "service_categories": ["haircut", "treatment", "wax", "color"],
        "employees": [
            {"name": "Stylist X", "net_service": half},
            {"name": "Stylist Y", "net_service": other},
        ],
    }


class TestBuildBareToConfig(unittest.TestCase):
    def test_canonical_and_bare_both_indexed(self):
        idx = _build_bare_to_config(SAMPLE_CONFIG)
        # "Andover FS" canonical → also indexed as bare "Andover"
        self.assertIn("Andover FS", idx)
        self.assertIn("Andover", idx)
        self.assertEqual(idx["Andover"]["id"], "z001")
        self.assertEqual(idx["Andover FS"]["id"], "z001")

    def test_non_fs_names_only_canonical_form(self):
        idx = _build_bare_to_config(SAMPLE_CONFIG)
        self.assertIn("Blaine", idx)
        self.assertIn("Apple Valley", idx)


class TestDiscoverPdfs(unittest.TestCase):
    def _touch_pdf(self, dirpath: Path, name: str) -> None:
        (dirpath / name).write_bytes(b"%PDF-1.4 stub")

    def test_happy_path(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            self._touch_pdf(tmp_path, "Andover.pdf")
            self._touch_pdf(tmp_path, "Blaine.pdf")
            self._touch_pdf(tmp_path, "Apple Valley.pdf")

            found = discover_pdfs(tmp_path, SAMPLE_CONFIG)
            self.assertEqual(set(found.keys()), {"Andover FS", "Blaine", "Apple Valley"})

    def test_canonical_name_with_fs_suffix_also_works(self):
        """If the file is named 'Andover FS.pdf' (with the FS), still maps."""
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            self._touch_pdf(tmp_path, "Andover FS.pdf")
            self._touch_pdf(tmp_path, "Blaine.pdf")
            self._touch_pdf(tmp_path, "Apple Valley.pdf")

            found = discover_pdfs(tmp_path, SAMPLE_CONFIG)
            self.assertIn("Andover FS", found)

    def test_missing_location_raises(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            self._touch_pdf(tmp_path, "Andover.pdf")
            # Missing Blaine and Apple Valley
            with self.assertRaises(ValueError) as cm:
                discover_pdfs(tmp_path, SAMPLE_CONFIG)
            self.assertIn("Blaine", str(cm.exception))
            self.assertIn("Apple Valley", str(cm.exception))

    def test_unmapped_file_raises(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            self._touch_pdf(tmp_path, "Andover.pdf")
            self._touch_pdf(tmp_path, "Blaine.pdf")
            self._touch_pdf(tmp_path, "Apple Valley.pdf")
            self._touch_pdf(tmp_path, "Mystery Salon.pdf")

            with self.assertRaises(ValueError) as cm:
                discover_pdfs(tmp_path, SAMPLE_CONFIG)
            self.assertIn("Mystery Salon", str(cm.exception))

    def test_missing_directory_raises(self):
        with self.assertRaises(FileNotFoundError):
            discover_pdfs("/no/such/dir/anywhere", SAMPLE_CONFIG)


class TestValidateParseResult(unittest.TestCase):
    def test_clean_result_no_issues(self):
        loc = {"name": "Andover FS"}
        parsed = _clean_zenoti_parsed(service_net=28917.40, emp_sum=28917.40)
        issues = _validate_parse_result(parsed, loc, tolerance=DEFAULT_RECONCILIATION_TOLERANCE)
        self.assertEqual(issues, [])

    def test_non_informational_flag_is_error(self):
        loc = {"name": "Andover FS"}
        parsed = _clean_zenoti_parsed()
        parsed["flags"] = ["UNCLOSED_DAYS", "BAD_PARSE"]
        issues = _validate_parse_result(parsed, loc, tolerance=0.01)
        errors = [i for i in issues if i.severity == "error"]
        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0].code, "PARSER_FLAGS")

    def test_informational_flag_is_info_not_error(self):
        loc = {"name": "Apple Valley"}
        parsed = _clean_su_parsed()
        parsed["flags"] = ["GUEST_COUNT_MISMATCH"]
        issues = _validate_parse_result(parsed, loc, tolerance=0.01)
        errors = [i for i in issues if i.severity == "error"]
        self.assertEqual(errors, [])
        infos = [i for i in issues if i.severity == "info"]
        self.assertEqual(len(infos), 1)
        self.assertEqual(infos[0].code, "GUEST_COUNT_MISMATCH")

    def test_stylist_sum_mismatch_outside_tolerance_is_error(self):
        loc = {"name": "Andover FS"}
        # service_net = 28917.40, emp_sum = 28910.00 → delta = -7.40, > 0.01 tolerance
        parsed = _clean_zenoti_parsed(service_net=28917.40, emp_sum=28910.00)
        issues = _validate_parse_result(parsed, loc, tolerance=0.01)
        errors = [i for i in issues if i.severity == "error"]
        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0].code, "STYLIST_SUM_MISMATCH")
        self.assertAlmostEqual(errors[0].details["delta"], -7.40, places=2)

    def test_stylist_sum_within_tolerance_passes(self):
        loc = {"name": "Andover FS"}
        # delta = 0.005, under 0.01 tolerance
        parsed = _clean_zenoti_parsed(service_net=28917.40, emp_sum=28917.405)
        issues = _validate_parse_result(parsed, loc, tolerance=0.01)
        errors = [i for i in issues if i.severity == "error"]
        self.assertEqual(errors, [])

    def test_no_employees_skips_reconciliation(self):
        loc = {"name": "Apple Valley"}
        parsed = _clean_su_parsed()
        parsed["employees"] = []
        issues = _validate_parse_result(parsed, loc, tolerance=0.01)
        self.assertEqual(issues, [])


class TestBuildRow(unittest.TestCase):
    def test_zenoti_row_maps_all_fields(self):
        loc = {"name": "Andover FS", "platform": "zenoti", "id": "z001"}
        parsed = _clean_zenoti_parsed(service_net=28917.40)
        row = _build_row(
            parsed, loc,
            year_month="2026-04",
            period_start="2026-04-01",
            period_end="2026-04-30",
            source="zenoti_monthly_pdf",
        )
        self.assertEqual(row["loc_name"], "Andover FS")
        self.assertEqual(row["year_month"], "2026-04")
        self.assertEqual(row["platform"], "zenoti")
        self.assertEqual(row["guests"], 574)
        self.assertAlmostEqual(row["service"], 28917.40, places=2)
        self.assertAlmostEqual(row["product"], 2378.44, places=2)
        self.assertAlmostEqual(row["total_sales"], 31295.84, places=2)
        # Derived: product / total
        self.assertAlmostEqual(row["product_pct"], 2378.44 / 31295.84, places=4)
        self.assertEqual(row["wax_count"], 62)
        self.assertAlmostEqual(row["wax"], 1170.15, places=2)
        self.assertEqual(row["source"], "zenoti_monthly_pdf")
        self.assertEqual(row["period_start"], "2026-04-01")
        self.assertEqual(row["period_end"], "2026-04-30")

    def test_su_row_handles_missing_wax_sales_treat_sales(self):
        loc = {"name": "Apple Valley", "platform": "salon_ultimate", "id": "z010"}
        parsed = _clean_su_parsed()
        row = _build_row(
            parsed, loc,
            year_month="2026-04", period_start="2026-04-01", period_end="2026-04-30",
            source="su_monthly_pdf",
        )
        self.assertEqual(row["loc_name"], "Apple Valley")
        self.assertEqual(row["platform"], "salon_ultimate")
        self.assertEqual(row["guests"], 1643)
        # SU doesn't supply wax_sales / treatment_sales — should default to 0
        self.assertEqual(row["wax"], 0)
        self.assertEqual(row["treat"], 0)
        # But pct fields still come through
        self.assertAlmostEqual(row["wax_pct"], 0.1467, places=4)
        self.assertAlmostEqual(row["treat_pct"], 0.3488, places=4)
        self.assertEqual(row["source"], "su_monthly_pdf")


class TestBuildStylistRows(unittest.TestCase):
    def test_zenoti_employees_mapped_with_invoice_count_as_guests(self):
        loc = {"name": "Andover FS", "platform": "zenoti", "id": "z001"}
        parsed = _clean_zenoti_parsed()
        rows = _build_stylist_rows(
            parsed, loc,
            year_month="2026-04", period_start="2026-04-01", period_end="2026-04-30",
            source="zenoti_monthly_pdf",
        )
        self.assertEqual(len(rows), 2)
        first = rows[0]
        self.assertEqual(first["year_month"], "2026-04")
        self.assertEqual(first["loc_name"], "Andover FS")
        self.assertEqual(first["loc_id"], "z001")
        self.assertEqual(first["platform"], "zenoti")
        self.assertEqual(first["source"], "zenoti_monthly_pdf")
        # Zenoti convention: invoices count == guests count (Karissa formula)
        self.assertEqual(first["invoices"], first["guests"])

    def test_su_employees_use_guests_field(self):
        loc = {"name": "Apple Valley", "platform": "salon_ultimate", "id": "z010"}
        parsed = _clean_su_parsed()
        rows = _build_stylist_rows(
            parsed, loc,
            year_month="2026-04", period_start="2026-04-01", period_end="2026-04-30",
            source="su_monthly_pdf",
        )
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["platform"], "salon_ultimate")
        self.assertEqual(rows[0]["source"], "su_monthly_pdf")

    def test_empty_name_employees_filtered(self):
        loc = {"name": "Andover FS", "platform": "zenoti", "id": "z001"}
        parsed = _clean_zenoti_parsed()
        parsed["employees"] = [
            {"name": "  ", "net_service": 100},
            {"name": "Real Person", "net_service": 200},
        ]
        rows = _build_stylist_rows(
            parsed, loc,
            year_month="2026-04", period_start="2026-04-01", period_end="2026-04-30",
            source="zenoti_monthly_pdf",
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["name"], "Real Person")


class TestLoadMonthlyPdfsEndToEnd(unittest.TestCase):
    """Patch the parser entry points so we don't need real PDFs."""

    def _touch_pdfs(self, dirpath: Path) -> None:
        for name in ("Andover.pdf", "Blaine.pdf", "Apple Valley.pdf"):
            (dirpath / name).write_bytes(b"%PDF-1.4 stub")

    def test_all_clean_rows_no_errors(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            self._touch_pdfs(tmp_path)

            def _z(path: str):
                # Distinguish the two zenoti locations by filename
                loc = Path(path).stem
                return _clean_zenoti_parsed(loc_name=loc, service_net=28917.40)

            def _s(path: str):
                return _clean_su_parsed(loc_name="Apple Valley")

            with patch("scripts.backfill.monthly_pdf_loader._parse_zenoti", side_effect=lambda p: _z(str(p))), \
                 patch("scripts.backfill.monthly_pdf_loader._parse_su", side_effect=lambda p: _s(str(p))):
                rows, _stylists, issues = load_monthly_pdfs(
                    tmp_path, SAMPLE_CONFIG,
                    year_month="2026-04", period_start="2026-04-01", period_end="2026-04-30",
                )

            self.assertEqual(len(rows), 3)
            self.assertEqual([r["loc_name"] for r in rows],
                             ["Andover FS", "Blaine", "Apple Valley"])
            errors = [i for i in issues if i["severity"] == "error"]
            self.assertEqual(errors, [])

    def test_parse_exception_yields_error_issue_and_zero_row(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            self._touch_pdfs(tmp_path)

            def _z_explode(path):
                raise RuntimeError("simulated parser crash")

            def _s(path):
                return _clean_su_parsed()

            with patch("scripts.backfill.monthly_pdf_loader._parse_zenoti", side_effect=_z_explode), \
                 patch("scripts.backfill.monthly_pdf_loader._parse_su", side_effect=lambda p: _s(str(p))):
                rows, _stylists, issues = load_monthly_pdfs(
                    tmp_path, SAMPLE_CONFIG,
                    year_month="2026-04", period_start="2026-04-01", period_end="2026-04-30",
                )

            # Both zenoti locs explode; SU one succeeds → 3 rows total, 2 errors
            self.assertEqual(len(rows), 3)
            errors = [i for i in issues if i["severity"] == "error"]
            self.assertEqual(len(errors), 2)
            self.assertEqual({e["code"] for e in errors}, {"PARSE_EXCEPTION"})


# ─── Optional integration test against real April folder ───────────────────────

REAL_APRIL_DIR = Path(r"C:\Users\TonyGrant\Downloads\KPI Historical Data\4-1-26 - 4-30-26")
REAL_CONFIG_PATH = _REPO_ROOT / "config" / "customers" / "karissa_001.json"


@unittest.skipUnless(
    REAL_APRIL_DIR.exists() and REAL_CONFIG_PATH.exists(),
    f"Real April folder not present at {REAL_APRIL_DIR} — skipping integration",
)
class TestRealAprilIntegration(unittest.TestCase):
    def test_all_12_april_pdfs_emit_rows(self):
        """All 12 April PDFs should parse and emit a row, even Prior Lake which
        has a known $34 stylist-sum anomaly (caught as STYLIST_SUM_MISMATCH error).
        """
        config = json.loads(REAL_CONFIG_PATH.read_text())
        rows, _stylists, issues = load_monthly_pdfs(
            REAL_APRIL_DIR, config,
            year_month="2026-04", period_start="2026-04-01", period_end="2026-04-30",
        )
        self.assertEqual(len(rows), 12, f"Expected 12 rows, got {len(rows)}")

    def test_april_known_anomaly_prior_lake_34_stylist_gap(self):
        """Prior Lake's April PDF has sum(emp)=41827 vs service_net=41861 (delta=-34).
        The validator MUST flag this so the orchestrator stops before writing.
        Decision pending Tony review whether to accept the $34 gap as immaterial.
        """
        config = json.loads(REAL_CONFIG_PATH.read_text())
        _, _stylists, issues = load_monthly_pdfs(
            REAL_APRIL_DIR, config,
            year_month="2026-04", period_start="2026-04-01", period_end="2026-04-30",
        )
        errors = [i for i in issues if i["severity"] == "error"]
        # Exactly one known error: Prior Lake stylist sum
        self.assertEqual(len(errors), 1, f"Expected exactly 1 known anomaly, got: {errors}")
        e = errors[0]
        self.assertEqual(e["location"], "Prior Lake")
        self.assertEqual(e["code"], "STYLIST_SUM_MISMATCH")
        self.assertAlmostEqual(e["details"]["delta"], -34.0, places=2)

    def test_april_clean_locations(self):
        """Spot-check the 11 locations that DO reconcile cleanly."""
        config = json.loads(REAL_CONFIG_PATH.read_text())
        rows, _stylists, _ = load_monthly_pdfs(
            REAL_APRIL_DIR, config,
            year_month="2026-04", period_start="2026-04-01", period_end="2026-04-30",
        )
        andover = next(r for r in rows if r["loc_name"] == "Andover FS")
        self.assertEqual(andover["platform"], "zenoti")
        self.assertEqual(andover["source"], "zenoti_monthly_pdf")
        self.assertAlmostEqual(andover["service"], 28917.40, places=2)

        apple = next(r for r in rows if r["loc_name"] == "Apple Valley")
        self.assertEqual(apple["platform"], "salon_ultimate")
        self.assertEqual(apple["source"], "su_monthly_pdf")
        self.assertEqual(apple["guests"], 1643)


REAL_MAY_DIR = Path(r"C:\Users\TonyGrant\Downloads\KPI Historical Data\5-1-24 - 5-24-26")


@unittest.skipUnless(
    REAL_MAY_DIR.exists() and REAL_CONFIG_PATH.exists() and any(REAL_MAY_DIR.glob("*.pdf")),
    f"Real May folder not present at {REAL_MAY_DIR} — skipping integration",
)
class TestRealMayIntegration(unittest.TestCase):
    def test_all_12_may_pdfs_reconcile_cleanly(self):
        """May (partial month 5/1-5/24) — all 12 should parse with zero errors.
        Only informational flag is Apple Valley's GUEST_COUNT_MISMATCH."""
        config = json.loads(REAL_CONFIG_PATH.read_text())
        rows, _stylists, issues = load_monthly_pdfs(
            REAL_MAY_DIR, config,
            year_month="2026-05", period_start="2026-05-01", period_end="2026-05-24",
        )
        self.assertEqual(len(rows), 12)
        errors = [i for i in issues if i["severity"] == "error"]
        self.assertEqual(errors, [], f"Unexpected errors on real May PDFs: {errors}")

        hudson = next(r for r in rows if r["loc_name"] == "Hudson")
        self.assertAlmostEqual(hudson["service"], 41916.65, places=2)
        self.assertEqual(hudson["period_end"], "2026-05-24")  # partial month preserved


if __name__ == "__main__":
    unittest.main()
