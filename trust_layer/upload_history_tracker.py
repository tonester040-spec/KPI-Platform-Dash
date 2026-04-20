"""
Trust Layer — Upload History Tracker  (Phase 3B)
Records the timestamp and period of every successful upload per location.
Surfaces staleness information before each batch ("Andover: 12 days stale").

Storage: 'Upload_History' sheet tab
  Columns: location_id | last_upload_timestamp | last_period_end | status

Status categories:
  current  — last upload < STALE_THRESHOLD_DAYS days ago
  stale    — last upload ≥ STALE_THRESHOLD_DAYS days ago
  never    — no successful upload on record
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

from trust_layer.severity import CompletenessCheck

logger = logging.getLogger(__name__)


class UploadHistoryTracker:
    """
    Records and surfaces per-location upload history.

    Phase 3B — Sheets I/O is stubbed.  All display and decision logic
    is fully functional once wired.
    """

    SHEET_NAME         = "Upload_History"
    STALE_THRESHOLD    = 10   # days — locations older than this are marked stale

    # ------------------------------------------------------------------
    # Pre-upload status display
    # ------------------------------------------------------------------

    def display_status(self, location_ids: List[str]) -> str:
        """
        Generate a human-readable staleness report for a set of locations.

        Display this BEFORE processing a batch so Karissa can see
        which locations are current, stale, or never uploaded.

        Args:
            location_ids: List of snake_case location identifiers.

        Returns:
            Formatted multi-line string.
        """
        lines = [
            "CURRENT DATA STATUS",
            "=" * 68,
        ]

        now = datetime.now(tz=timezone.utc)

        for loc_id in sorted(location_ids):
            history = self._load_history(loc_id)

            if history is None:
                lines.append(f"  ❌  {loc_id:<20} Never uploaded")
                continue

            last_ts = datetime.fromisoformat(history["last_upload_timestamp"])
            age_days = (now - last_ts).days
            period_end = history.get("last_period_end", "?")

            if age_days < self.STALE_THRESHOLD:
                status_icon = "✅"
                status_text = f"{age_days} day(s) old (week ending {period_end})"
            else:
                status_icon = "⚠️ "
                status_text = (
                    f"{age_days} days old (week ending {period_end}) — STALE"
                )

            lines.append(f"  {status_icon}  {loc_id:<20} {status_text}")

        lines.append("=" * 68)
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Post-upload recording
    # ------------------------------------------------------------------

    def record_success(
        self,
        location_id: str,
        period_end:  str,
    ) -> None:
        """
        Record a successful upload for a location.

        Args:
            location_id: Snake-case location identifier.
            period_end:  ISO date string 'YYYY-MM-DD' of the period end date.
        """
        timestamp = datetime.now(tz=timezone.utc).isoformat()
        logger.info(
            "Recording successful upload: %s, period end %s", location_id, period_end
        )
        self._save_history(location_id, timestamp, period_end, status="success")

    def validate_batch_freshness(
        self, location_ids: List[str]
    ) -> List[CompletenessCheck]:
        """
        Check if any locations in the batch are overdue for an upload.

        This is informational — stale data generates a warning but doesn't
        block the current batch (the current batch IS the update).

        Args:
            location_ids: Locations included in the current batch.

        Returns:
            List of CompletenessCheck results.
        """
        now = datetime.now(tz=timezone.utc)
        stale: List[str] = []
        never: List[str] = []

        for loc_id in location_ids:
            history = self._load_history(loc_id)
            if history is None:
                never.append(loc_id)
                continue
            last_ts  = datetime.fromisoformat(history["last_upload_timestamp"])
            age_days = (now - last_ts).days
            if age_days >= self.STALE_THRESHOLD:
                stale.append(f"{loc_id} ({age_days}d)")

        checks: List[CompletenessCheck] = []

        if never:
            checks.append(CompletenessCheck(
                name="Upload History",
                status="pass",
                message=f"First upload for: {', '.join(never)}",
                severity=0,
            ))

        if stale:
            checks.append(CompletenessCheck(
                name="Stale Locations",
                status="warn",
                message=(
                    f"Locations overdue for upload (≥{self.STALE_THRESHOLD}d): "
                    f"{', '.join(stale)}"
                ),
                severity=5,
            ))

        if not stale and not never:
            checks.append(CompletenessCheck(
                name="Upload History",
                status="pass",
                message=f"All {len(location_ids)} locations have recent upload history",
                severity=0,
            ))

        return checks

    # ------------------------------------------------------------------
    # Sheets I/O stubs
    # ------------------------------------------------------------------

    def _load_history(self, location_id: str) -> Optional[Dict]:
        """
        Load upload history for a location from Upload_History sheet.

        Stub — returns None until Sheets read interface is implemented.
        Returns: {'last_upload_timestamp': str, 'last_period_end': str} or None.
        """
        logger.debug(
            "UploadHistoryTracker: load stub for %s → None", location_id
        )
        return None

    def _save_history(
        self,
        location_id: str,
        timestamp:   str,
        period_end:  str,
        status:      str,
    ) -> None:
        """
        Write or update history record in Upload_History sheet.

        Stub — no-op until Sheets write interface is implemented.
        """
        logger.debug(
            "UploadHistoryTracker: save stub — %s at %s (period end %s)",
            location_id, timestamp, period_end,
        )
