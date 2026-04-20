"""
Trust Layer — Transfer Validator  (Phase 3B Addendum)
Validates transfer events as part of the Trust Layer pipeline.

Plugs into the existing CompletenessCheck + confidence scoring system.
Call validate_transfers() after TransferDetector.detect() and append
the returned checks to the location's trust_checks list before scoring.

Anomaly thresholds:
  >3  transfers from a single location  → WARN (severity 15)
  >10 transfers in a batch              → WARN (severity 20)
  Any low-confidence transfers          → WARN (severity 10)
"""

import logging
from typing import Dict, List

from trust_layer.severity import CompletenessCheck

logger = logging.getLogger(__name__)


class TransferValidator:
    """
    Validates a batch of detected transfer events for anomalies.

    Usage:
        validator = TransferValidator()
        checks = validator.validate_transfers(confirmed_transfers)
        # Append checks to location trust_checks before confidence scoring
    """

    MAX_TRANSFERS_PER_LOCATION = 3    # >3 from one location → warn
    MAX_TRANSFERS_PER_BATCH    = 10   # >10 total → warn
    CONFIDENCE_THRESHOLD       = 0.80 # Below this → flagged as low-confidence

    def validate_transfers(self, transfers: List[Dict]) -> List[CompletenessCheck]:
        """
        Validate a list of confirmed (or pending) transfer events.

        Args:
            transfers: List of transfer dicts (from TransferDetector.detect()).
                       Each dict should contain 'from_location', 'to_location',
                       and optionally 'confidence'.

        Returns:
            List[CompletenessCheck] — empty only if transfers list is empty and
            we emit a pass, otherwise warns on anomalous counts.
        """
        checks: List[CompletenessCheck] = []

        if not transfers:
            checks.append(CompletenessCheck(
                name="Transfer Validation",
                status="pass",
                message="No transfers detected this batch",
                severity=0,
            ))
            return checks

        # Count transfers by from_location
        from_counts: Dict[str, int] = {}
        for t in transfers:
            loc = t.get("from_location", "unknown")
            from_counts[loc] = from_counts.get(loc, 0) + 1

        # Anomaly 1: High volume from a single location
        for location, count in from_counts.items():
            if count > self.MAX_TRANSFERS_PER_LOCATION:
                checks.append(CompletenessCheck(
                    name="Transfer Volume",
                    status="warn",
                    message=(
                        f"⚠️  HIGH TRANSFER VOLUME: {count} transfers out of {location} "
                        f"(max expected: {self.MAX_TRANSFERS_PER_LOCATION}) — "
                        f"verify data integrity before using these records."
                    ),
                    severity=15,
                ))
                logger.warning(
                    "Transfer volume anomaly: %d transfers from %s", count, location
                )

        # Anomaly 2: Unusually high total
        if len(transfers) > self.MAX_TRANSFERS_PER_BATCH:
            checks.append(CompletenessCheck(
                name="Total Transfers",
                status="warn",
                message=(
                    f"⚠️  UNUSUAL TRANSFER COUNT: {len(transfers)} transfers in this batch "
                    f"(max expected: {self.MAX_TRANSFERS_PER_BATCH}) — "
                    f"review for false positives before coaching decisions."
                ),
                severity=20,
            ))
            logger.warning(
                "Total transfer count anomaly: %d transfers in batch", len(transfers)
            )

        # Anomaly 3: Low-confidence transfers
        low_confidence = [
            t for t in transfers
            if t.get("confidence", 1.0) < self.CONFIDENCE_THRESHOLD
        ]
        if low_confidence:
            names = ", ".join(
                t.get("canonical_name", "?") for t in low_confidence
            )
            checks.append(CompletenessCheck(
                name="Transfer Confidence",
                status="warn",
                message=(
                    f"{len(low_confidence)} low-confidence transfer(s) require manual review: "
                    f"{names}"
                ),
                severity=10,
            ))
            logger.warning(
                "%d low-confidence transfers: %s", len(low_confidence), names
            )

        # Pass if no anomalies found
        if not checks:
            checks.append(CompletenessCheck(
                name="Transfer Validation",
                status="pass",
                message=(
                    f"{len(transfers)} transfer(s) validated — "
                    f"volumes normal, confidence OK"
                ),
                severity=0,
            ))

        return checks

    def validate_pending_volume(self, pending: List[Dict]) -> List[CompletenessCheck]:
        """
        Warn if there's an unusually large pending transfer queue.

        A growing pending queue may indicate systemic name-resolution issues
        or unusual scheduling patterns across the network.

        Args:
            pending: List of pending transfer dicts (from TransferDetector stubs).

        Returns:
            List[CompletenessCheck]
        """
        checks: List[CompletenessCheck] = []

        if len(pending) > 20:
            checks.append(CompletenessCheck(
                name="Pending Transfer Queue",
                status="warn",
                message=(
                    f"⚠️  LARGE PENDING QUEUE: {len(pending)} transfers awaiting "
                    f"grace-period confirmation — review for name-resolution issues."
                ),
                severity=5,
            ))
        else:
            checks.append(CompletenessCheck(
                name="Pending Transfer Queue",
                status="pass",
                message=f"{len(pending)} transfer(s) in grace-period queue",
                severity=0,
            ))

        return checks
