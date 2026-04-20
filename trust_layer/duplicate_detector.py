"""
Trust Layer — Duplicate Detector
Prevents the same reporting period from being loaded multiple times.

Generates stable record keys and batch fingerprints.
Checks new batch against existing data before writing.

Note on _query_existing_records:
  The Sheets query is implemented as a stub. When GoogleSheetsWriter.read_stylists()
  is added (Phase 4), replace the stub body with a real read.  All detection logic
  above the stub is fully functional and tested.
"""

import hashlib
import logging
from typing import Dict, List, Optional, Set

from trust_layer.severity import CompletenessCheck

logger = logging.getLogger(__name__)


class DuplicateDetector:
    """
    Detects duplicate data ingestion.
    Prevents the same period from being loaded multiple times.
    """

    # Overlap thresholds that determine action
    WARN_THRESHOLD    = 0.20   # >20% overlap → warn
    CONFIRM_THRESHOLD = 0.50   # >50% overlap → require confirmation
    BLOCK_THRESHOLD   = 0.80   # >80% overlap → hard block

    def generate_record_key(self, record: Dict) -> str:
        """
        Generate a stable unique key for a single stylist record.

        Format: location_id|period_start|period_end|stylist_name
        """
        return (
            f"{record.get('location_id', record.get('location', ''))}|"
            f"{record.get('period_start', '')}|"
            f"{record.get('period_end', '')}|"
            f"{record.get('stylist_name', record.get('employee_name', ''))}"
        )

    def generate_batch_fingerprint(self, records: List[Dict]) -> str:
        """
        Generate a SHA-256 fingerprint for the entire batch.

        Fingerprint is deterministic: same records in any order produce
        the same hash (records are sorted before hashing).

        Args:
            records: List of stylist dicts.

        Returns:
            64-character hex string.
        """
        keys = sorted(self.generate_record_key(r) for r in records)
        content = "\n".join(keys).encode("utf-8")
        return hashlib.sha256(content).hexdigest()

    def check_duplicates(
        self,
        new_records:      List[Dict],
        existing_records: List[Dict],
    ) -> Dict:
        """
        Compare new batch against existing records.

        Returns:
            {
              'is_duplicate'   : bool,
              'overlap_pct'    : float (0–1),
              'duplicate_keys' : set of overlapping record keys,
              'new_keys'       : set of genuinely new record keys,
              'action_required': 'PROCEED' | 'WARN' | 'CONFIRM' | 'BLOCK',
            }
        """
        new_keys      = {self.generate_record_key(r) for r in new_records}
        existing_keys = {self.generate_record_key(r) for r in existing_records}

        overlap     = new_keys & existing_keys
        overlap_pct = len(overlap) / len(new_keys) if new_keys else 0.0

        if overlap_pct == 0:
            action = "PROCEED"
        elif overlap_pct < self.WARN_THRESHOLD:
            action = "WARN"
        elif overlap_pct < self.CONFIRM_THRESHOLD:
            action = "CONFIRM"
        else:
            action = "BLOCK"

        return {
            "is_duplicate":    overlap_pct >= self.CONFIRM_THRESHOLD,
            "overlap_pct":     overlap_pct,
            "duplicate_keys":  overlap,
            "new_keys":        new_keys - overlap,
            "action_required": action,
        }

    def validate_batch(
        self,
        new_batch: List[Dict],
        existing_records: Optional[List[Dict]] = None,
    ) -> List[CompletenessCheck]:
        """
        Validate new batch for duplicates before writing.

        Args:
            new_batch:        Stylist records about to be written.
            existing_records: Pre-fetched existing records from Sheets
                              (pass None to trigger a live query).

        Returns:
            List of CompletenessCheck results.

        Raises:
            ValueError: If overlap exceeds BLOCK_THRESHOLD.
        """
        if not new_batch:
            return [CompletenessCheck(
                name="Duplicate Detection",
                status="pass",
                message="Empty batch — nothing to check",
                severity=0,
            )]

        # Get period info from first record
        sample       = new_batch[0]
        period_start = sample.get("period_start", "")
        period_end   = sample.get("period_end",   "")

        # Fetch existing records if not provided
        if existing_records is None:
            existing_records = self._query_existing_records(period_start, period_end)

        if not existing_records:
            return [CompletenessCheck(
                name="Duplicate Detection",
                status="pass",
                message=(
                    f"No existing data for period {period_start} → {period_end} "
                    f"(first upload for this period)"
                ),
                severity=0,
            )]

        result = self.check_duplicates(new_batch, existing_records)

        if result["action_required"] == "PROCEED":
            return [CompletenessCheck(
                name="Duplicate Detection",
                status="pass",
                message="No duplicate records detected",
                severity=0,
            )]

        if result["action_required"] == "WARN":
            return [CompletenessCheck(
                name="Duplicate Detection",
                status="warn",
                message=(
                    f"{result['overlap_pct']:.0%} of records already exist — "
                    f"may be a partial re-upload"
                ),
                severity=5,
            )]

        if result["action_required"] == "CONFIRM":
            return [CompletenessCheck(
                name="Duplicate Detection",
                status="warn",
                message=(
                    f"⚠️ {result['overlap_pct']:.0%} DUPLICATE DATA DETECTED\n"
                    f"  Period: {period_start} → {period_end}\n"
                    f"  {len(result['duplicate_keys'])} of "
                    f"{len(new_batch)} records already exist.\n"
                    f"  Possible accidental re-upload — review before proceeding."
                ),
                severity=20,
            )]

        # BLOCK
        raise ValueError(
            f"❌ DUPLICATE BATCH DETECTED\n\n"
            f"  Period   : {period_start} → {period_end}\n"
            f"  Overlap  : {result['overlap_pct']:.0%} of records already exist\n"
            f"  Duplicate records : {len(result['duplicate_keys'])}\n"
            f"  New records       : {len(result['new_keys'])}\n\n"
            f"This batch is a near-complete duplicate of existing data.\n\n"
            f"OPTIONS:\n"
            f"  1. SKIP   — Keep existing data, discard this upload\n"
            f"  2. REPLACE — Delete existing data for this period, load new batch\n"
            f"  3. CANCEL — Stop and review manually\n\n"
            f"Processing stopped to prevent data duplication."
        )

    # ------------------------------------------------------------------
    # Sheets query stub (to be implemented when read interface is ready)
    # ------------------------------------------------------------------

    def _query_existing_records(
        self,
        period_start: str,
        period_end:   str,
    ) -> List[Dict]:
        """
        Query Google Sheets for existing records in the given period.

        Currently a stub — returns empty list (no duplicates detected).
        Replace body with GoogleSheetsReader.read_stylists_for_period() call
        when the read interface is implemented.

        Args:
            period_start: ISO date string 'YYYY-MM-DD'.
            period_end:   ISO date string 'YYYY-MM-DD'.

        Returns:
            List of existing stylist dicts (empty until Sheets reader is wired).
        """
        logger.debug(
            "DuplicateDetector: Sheets query stub — returning empty for "
            "period %s → %s (no duplicate check performed)",
            period_start, period_end,
        )
        return []
