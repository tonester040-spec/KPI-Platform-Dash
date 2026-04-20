"""
Trust Layer — Schema Fingerprinter  (Phase 3B)
Stores a hash of each file's header row after every successful upload.
On subsequent uploads, compares the current header hash against the stored one.
If the hash changes → POS export format changed → hard fail before parsing.

This catches format drift BETWEEN uploads (Schema Validator only catches it
at upload time; Fingerprinter catches it if the format changes between uploads).

Storage: 'Schema_Fingerprints' sheet tab
  Columns: location_id | pos_system | fingerprint | last_updated | header_text
"""

import hashlib
import logging
from datetime import datetime
from typing import Dict, List, Optional

from trust_layer.severity import CompletenessCheck

logger = logging.getLogger(__name__)


class SchemaFingerprinter:
    """
    Records and validates schema fingerprints (header row hashes) per location.

    Phase 3B — requires Sheets read/write access to persist fingerprints.
    All Sheets I/O is stubbed pending implementation.
    """

    SHEET_NAME = "Schema_Fingerprints"

    def compute_fingerprint(self, worksheet, header_row: int, num_cols: int) -> str:
        """
        Compute SHA-256 hash of the header row content.

        Args:
            worksheet:  openpyxl worksheet object.
            header_row: 1-based row index of the header row.
            num_cols:   Number of columns to include in the hash.

        Returns:
            64-character hex string (deterministic).
        """
        headers = [
            str(worksheet.cell(row=header_row, column=c).value or "").strip().lower()
            for c in range(1, num_cols + 1)
        ]
        content = "|".join(headers).encode("utf-8")
        return hashlib.sha256(content).hexdigest()

    def validate_fingerprint(
        self,
        location_id:    str,
        pos_system:     str,
        current_fp:     str,
        header_preview: str = "",
    ) -> List[CompletenessCheck]:
        """
        Compare current fingerprint against the stored fingerprint for this location.

        Args:
            location_id:    Snake-case location identifier.
            pos_system:     'zenoti' | 'salon_ultimate'.
            current_fp:     Fingerprint just computed from the uploaded file.
            header_preview: First few header values (for error context).

        Returns:
            List of CompletenessCheck results.

        Raises:
            ValueError: If fingerprint changed (schema drift detected).
        """
        stored = self._load_fingerprint(location_id, pos_system)

        if stored is None:
            # First upload — store fingerprint and pass
            self._save_fingerprint(location_id, pos_system, current_fp, header_preview)
            return [CompletenessCheck(
                name="Schema Fingerprint",
                status="pass",
                message=f"Initial fingerprint stored for {location_id} ({pos_system})",
                severity=0,
            )]

        if stored["fingerprint"] == current_fp:
            return [CompletenessCheck(
                name="Schema Fingerprint",
                status="pass",
                message=f"Schema unchanged since {stored.get('last_updated', '?')}",
                severity=0,
            )]

        # Fingerprint changed — schema drift detected
        raise ValueError(
            f"❌ SCHEMA CHANGE DETECTED FOR {location_id.upper()}\n\n"
            f"  POS system     : {pos_system}\n"
            f"  Previous hash  : {stored['fingerprint'][:12]}…\n"
            f"  Current hash   : {current_fp[:12]}…\n"
            f"  Last confirmed : {stored.get('last_updated', 'unknown')}\n\n"
            f"  The export format for this location has changed since the "
            f"last successful upload.\n\n"
            f"  Possible causes:\n"
            f"    • {pos_system.replace('_', ' ').title()} updated its export format\n"
            f"    • File was opened and re-saved (column order changed)\n"
            f"    • Wrong file version uploaded\n\n"
            f"  ACTION REQUIRED:\n"
            f"    1. Verify the correct file was uploaded\n"
            f"    2. If the POS format changed, update parser column constants\n"
            f"    3. After verifying the new format is intentional, run "
            f"reset_fingerprint('{location_id}', '{pos_system}') to accept the new schema\n\n"
            f"  Processing stopped to prevent silent data corruption."
        )

    def reset_fingerprint(self, location_id: str, pos_system: str) -> None:
        """
        Clear the stored fingerprint for a location.

        Call this after manually verifying a new POS export format is intentional.
        The next upload will store a fresh fingerprint.
        """
        logger.info(
            "Resetting schema fingerprint for %s (%s)", location_id, pos_system
        )
        self._delete_fingerprint(location_id, pos_system)

    # ------------------------------------------------------------------
    # Sheets I/O stubs (Phase 4 implementation hooks)
    # ------------------------------------------------------------------

    def _load_fingerprint(
        self, location_id: str, pos_system: str
    ) -> Optional[Dict]:
        """
        Load stored fingerprint from Schema_Fingerprints sheet.

        Stub — returns None until Sheets read interface is implemented.
        Returns: {'fingerprint': str, 'last_updated': str} or None.
        """
        logger.debug(
            "SchemaFingerprinter: load stub for %s/%s → None", location_id, pos_system
        )
        return None

    def _save_fingerprint(
        self,
        location_id:    str,
        pos_system:     str,
        fingerprint:    str,
        header_preview: str,
    ) -> None:
        """
        Save fingerprint to Schema_Fingerprints sheet.

        Stub — no-op until Sheets write interface is implemented.
        """
        logger.debug(
            "SchemaFingerprinter: save stub for %s/%s = %s…",
            location_id, pos_system, fingerprint[:12],
        )

    def _delete_fingerprint(self, location_id: str, pos_system: str) -> None:
        """
        Delete fingerprint from Schema_Fingerprints sheet.

        Stub — no-op until Sheets write interface is implemented.
        """
        logger.debug(
            "SchemaFingerprinter: delete stub for %s/%s", location_id, pos_system
        )
