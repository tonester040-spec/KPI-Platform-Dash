"""
tests/test_truth_mediation_log.py
─────────────────────────────────
Tests for trust_layer/truth_mediation_log.py — the on-disk audit log of
reconciliation events per FINAL_SPEC v1.0.0 §10 and v1.0.1 addendum §J
(Branch 3).

Also tests the tier2_pdf_batch wiring helper (`_write_truth_mediation_events`)
that bridges parsed-flag detection to log writes.

All tests use temp directories via tempfile — no real log writes, no
network. Each test gets its own log file via the `log_path` parameter
or the TRUTH_MEDIATION_LOG_PATH env var.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

# Make the repo root importable
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from trust_layer.truth_mediation_log import (
    write_event,
    read_events,
    RULE_SALON_LEVEL_SUPREMACY,
    RULE_PRODUCT_TOTAL_MISMATCH,
    RULE_PARTIAL_WEEK_DETECTED,
    RULE_CROSS_FILE_RECONCILED,
)
from parsers.tier2_pdf_batch import _write_truth_mediation_events


# ---------------------------------------------------------------------------
# Schema constants — locked to FINAL_SPEC v1.0.0 §10
# ---------------------------------------------------------------------------

SPEC_REQUIRED_KEYS = {
    "timestamp",
    "location_id",
    "week_start",
    "rule_applied",
    "field",
    "salon_level_value",
    "stylist_sum_before",
    "drift_amount",
    "drift_pct",
    "hypothesis",
    "action",
    "human_review_required",
}


# ---------------------------------------------------------------------------
# Module unit tests
# ---------------------------------------------------------------------------

class TestTruthMediationLogModule(unittest.TestCase):
    """Unit tests for write_event + read_events in isolation."""

    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.tmp_path = Path(self.tmp_dir) / "test_log.json"

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def test_write_event_creates_file_with_one_record(self):
        ok = write_event(
            rule_applied=RULE_SALON_LEVEL_SUPREMACY,
            location_id="z007",
            week_start="2026-04-01",
            field="service_net_stylist_allocation",
            salon_level_value=9120.75,
            stylist_sum_before=9087.25,
            drift_amount=33.50,
            drift_pct=0.0037,
            hypothesis="Likely refund exclusion at stylist level",
            action="proportional_adjustment_applied",
            human_review_required=False,
            log_path=self.tmp_path,
        )
        self.assertTrue(ok)
        self.assertTrue(self.tmp_path.exists())
        events = read_events(log_path=self.tmp_path)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["rule_applied"], RULE_SALON_LEVEL_SUPREMACY)
        self.assertEqual(events[0]["location_id"], "z007")
        self.assertEqual(events[0]["salon_level_value"], 9120.75)

    def test_event_schema_matches_FINAL_SPEC_section_10(self):
        """Every field in FINAL_SPEC §10 must be present in every event."""
        write_event(
            rule_applied=RULE_SALON_LEVEL_SUPREMACY,
            location_id="z007",
            week_start="2026-04-01",
            field="x",
            salon_level_value=1.0,
            stylist_sum_before=1.0,
            drift_amount=0.0,
            drift_pct=0.0,
            hypothesis="x",
            action="logged",
            human_review_required=False,
            log_path=self.tmp_path,
        )
        events = read_events(log_path=self.tmp_path)
        self.assertEqual(set(events[0].keys()), SPEC_REQUIRED_KEYS,
                         "Event schema must exactly match FINAL_SPEC v1.0.0 §10 — "
                         "missing or extra keys break downstream consumers.")

    def test_multiple_events_append_as_ndjson(self):
        """Each event is on its own line, parseable independently."""
        for i in range(3):
            write_event(
                rule_applied=RULE_PARTIAL_WEEK_DETECTED,
                location_id=f"loc_{i}",
                week_start="2026-04-01",
                field="test",
                log_path=self.tmp_path,
            )
        events = read_events(log_path=self.tmp_path)
        self.assertEqual(len(events), 3)
        # Verify NDJSON: count physical lines and parse each independently
        with self.tmp_path.open(encoding="utf-8") as fh:
            lines = [l.strip() for l in fh if l.strip()]
        self.assertEqual(len(lines), 3)
        for line in lines:
            json.loads(line)  # raises if any line isn't valid JSON

    def test_rule_filter_returns_only_matching_events(self):
        write_event(rule_applied=RULE_PRODUCT_TOTAL_MISMATCH, location_id="A",
                    week_start="2026-04-01", field="x", log_path=self.tmp_path)
        write_event(rule_applied=RULE_PARTIAL_WEEK_DETECTED, location_id="B",
                    week_start="2026-04-01", field="y", log_path=self.tmp_path)
        write_event(rule_applied=RULE_PRODUCT_TOTAL_MISMATCH, location_id="C",
                    week_start="2026-04-01", field="z", log_path=self.tmp_path)
        mismatch_events = read_events(
            log_path=self.tmp_path, rule_filter=RULE_PRODUCT_TOTAL_MISMATCH,
        )
        self.assertEqual(len(mismatch_events), 2)
        for e in mismatch_events:
            self.assertEqual(e["rule_applied"], RULE_PRODUCT_TOTAL_MISMATCH)

    def test_location_filter_returns_only_matching_events(self):
        write_event(rule_applied=RULE_PARTIAL_WEEK_DETECTED, location_id="Lakeville",
                    week_start="2026-04-01", field="x", log_path=self.tmp_path)
        write_event(rule_applied=RULE_PARTIAL_WEEK_DETECTED, location_id="Andover FS",
                    week_start="2026-04-01", field="y", log_path=self.tmp_path)
        lakeville_events = read_events(
            log_path=self.tmp_path, location_filter="Lakeville",
        )
        self.assertEqual(len(lakeville_events), 1)
        self.assertEqual(lakeville_events[0]["location_id"], "Lakeville")

    def test_read_events_returns_empty_when_file_missing(self):
        """No file = empty list, not an exception."""
        missing = Path(self.tmp_dir) / "does_not_exist.json"
        self.assertEqual(read_events(log_path=missing), [])

    def test_read_events_skips_malformed_lines(self):
        """A garbage line should be skipped, not crash the reader."""
        write_event(rule_applied=RULE_SALON_LEVEL_SUPREMACY, location_id="A",
                    week_start="2026-04-01", field="x", log_path=self.tmp_path)
        with self.tmp_path.open("a", encoding="utf-8") as fh:
            fh.write("not valid json at all\n")
            fh.write('{"valid": "but-incomplete-schema"}\n')
        events = read_events(log_path=self.tmp_path)
        # First (valid full) + third (valid JSON, partial schema) = 2.
        # Second (not JSON) skipped.
        self.assertEqual(len(events), 2)

    def test_optional_fields_default_to_none_or_safe_defaults(self):
        """write_event with only required args produces a complete schema record."""
        write_event(
            rule_applied="custom_event_type",
            location_id="x",
            week_start="2026-04-01",
            field="y",
            log_path=self.tmp_path,
        )
        events = read_events(log_path=self.tmp_path)
        e = events[0]
        self.assertIsNone(e["salon_level_value"])
        self.assertIsNone(e["stylist_sum_before"])
        self.assertIsNone(e["drift_amount"])
        self.assertIsNone(e["drift_pct"])
        self.assertEqual(e["hypothesis"], "")
        self.assertEqual(e["action"], "logged")
        self.assertEqual(e["human_review_required"], False)

    def test_timestamp_is_iso_utc(self):
        """timestamp ends with Z and parses as ISO-8601."""
        import datetime as dt
        write_event(rule_applied="x", location_id="y", week_start="2026-04-01",
                    field="z", log_path=self.tmp_path)
        events = read_events(log_path=self.tmp_path)
        ts = events[0]["timestamp"]
        self.assertTrue(ts.endswith("Z"), f"timestamp should end with Z, got {ts!r}")
        # Strip trailing Z and verify ISO parses
        dt.datetime.fromisoformat(ts[:-1])


# ---------------------------------------------------------------------------
# Tier 2 wiring tests
# ---------------------------------------------------------------------------

class TestTier2TruthMediationWiring(unittest.TestCase):
    """
    Tests for parsers/tier2_pdf_batch._write_truth_mediation_events —
    the bridge between parsed-PDF flags and the truth-mediation log.
    """

    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.tmp_path = Path(self.tmp_dir) / "tier2_wiring_log.json"
        # Redirect log writes via env var
        os.environ["TRUTH_MEDIATION_LOG_PATH"] = str(self.tmp_path)

    def tearDown(self):
        os.environ.pop("TRUTH_MEDIATION_LOG_PATH", None)
        import shutil
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def test_product_total_mismatch_writes_event_with_correct_fields(self):
        """PRODUCT_TOTAL_MISMATCH flag → event with header / line-sum / drift."""
        parsed = {
            "location": "Lakeville",
            "week_end": "2026-04-05",
            "unclosed_days": [],
            "raw": {
                "total_retail": 534.50,
                "product_lines_sum": 623.25,
            },
        }
        _write_truth_mediation_events(
            parsed=parsed,
            flags=["PRODUCT_TOTAL_MISMATCH"],
            display_name="Lakeville",
        )
        events = read_events(log_path=self.tmp_path)
        self.assertEqual(len(events), 1)
        e = events[0]
        self.assertEqual(e["rule_applied"], RULE_PRODUCT_TOTAL_MISMATCH)
        self.assertEqual(e["location_id"], "Lakeville")
        self.assertEqual(e["week_start"], "2026-04-05")
        self.assertAlmostEqual(e["salon_level_value"], 534.50)
        self.assertAlmostEqual(e["stylist_sum_before"], 623.25)
        self.assertAlmostEqual(e["drift_amount"], 88.75, places=2)
        # drift_pct = 88.75 / 534.50 ≈ 0.1660
        self.assertAlmostEqual(e["drift_pct"], 0.1660, places=3)
        self.assertFalse(e["human_review_required"])
        self.assertIn("FINAL_SPEC", e["action"])

    def test_partial_week_writes_event_with_unclosed_days_in_hypothesis(self):
        """PARTIAL_WEEK flag → event with human_review_required=True + unclosed dates."""
        parsed = {
            "location": "Lakeville",
            "week_end": "2026-04-05",
            "unclosed_days": ["2026-04-04"],
            "raw": {},
        }
        _write_truth_mediation_events(
            parsed=parsed,
            flags=["PARTIAL_WEEK"],
            display_name="Lakeville",
        )
        events = read_events(log_path=self.tmp_path)
        self.assertEqual(len(events), 1)
        e = events[0]
        self.assertEqual(e["rule_applied"], RULE_PARTIAL_WEEK_DETECTED)
        self.assertEqual(e["location_id"], "Lakeville")
        self.assertTrue(e["human_review_required"])
        self.assertIn("2026-04-04", e["hypothesis"])
        # Reconciliation-numeric fields stay None for PARTIAL_WEEK
        self.assertIsNone(e["salon_level_value"])
        self.assertIsNone(e["stylist_sum_before"])

    def test_both_flags_write_two_separate_events(self):
        """A PDF with both flags → two log entries, one per flag."""
        parsed = {
            "location": "Lakeville",
            "week_end": "2026-04-05",
            "unclosed_days": ["2026-04-04"],
            "raw": {"total_retail": 534.50, "product_lines_sum": 623.25},
        }
        _write_truth_mediation_events(
            parsed=parsed,
            flags=["PARTIAL_WEEK", "PRODUCT_TOTAL_MISMATCH"],
            display_name="Lakeville",
        )
        events = read_events(log_path=self.tmp_path)
        self.assertEqual(len(events), 2)
        rules = {e["rule_applied"] for e in events}
        self.assertEqual(rules, {RULE_PARTIAL_WEEK_DETECTED, RULE_PRODUCT_TOTAL_MISMATCH})

    def test_unrecognized_flag_writes_no_event(self):
        """Flags that aren't in the dispatch list don't produce events (silent skip)."""
        parsed = {
            "location": "Andover",
            "week_end": "2026-04-05",
            "unclosed_days": [],
            "raw": {},
        }
        _write_truth_mediation_events(
            parsed=parsed,
            flags=["SOME_UNRECOGNIZED_FLAG"],
            display_name="Andover",
        )
        events = read_events(log_path=self.tmp_path)
        self.assertEqual(events, [])

    def test_missing_raw_fields_skips_product_mismatch_safely(self):
        """If raw is missing total_retail or product_lines_sum, don't crash — just skip."""
        parsed = {
            "location": "Andover",
            "week_end": "2026-04-05",
            "unclosed_days": [],
            "raw": {},  # missing both fields
        }
        # Should NOT raise
        _write_truth_mediation_events(
            parsed=parsed,
            flags=["PRODUCT_TOTAL_MISMATCH"],
            display_name="Andover",
        )
        events = read_events(log_path=self.tmp_path)
        self.assertEqual(events, [])  # nothing written because data was missing


# ---------------------------------------------------------------------------
# Script-mode entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    unittest.main(verbosity=2)
