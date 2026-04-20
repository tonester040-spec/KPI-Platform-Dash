"""
Trust Layer — Public API
Import from here to avoid coupling to internal module structure.

Phase 3A (Core):
    from trust_layer import (
        CompletenessCheck, Severity,
        CompletenessValidator, CrossFileVerifier,
        SchemaValidator, DuplicateDetector,
        ConfidenceScore, ConfidenceScorer,
        AnomalyDetector, IntegrityReporter,
        BatchProcessingError, AtomicProcessor,
        run_trust_validation,
    )

Phase 3B (Sheets-dependent — stubs active):
    from trust_layer import (
        SchemaFingerprinter, StylistIdentityResolver,
        HistoricalBaselineValidator, UploadHistoryTracker,
    )

Phase 3B Addendum (Transfer detection + org intelligence — stubs active):
    from trust_layer import (
        TransferDetector, TransferConfidence,
        TransferValidator,
        TransferImpactTracker,
    )

Phase 3B Final Polish (Immutability + Confidence 2.0 + Talent Graph — stubs active):
    from trust_layer import (
        TransferEventLog,
        LocationEffectScore, LocationEffectScorer,
    )
"""

from trust_layer.severity                      import Severity, CompletenessCheck
from trust_layer.completeness_validator        import CompletenessValidator
from trust_layer.cross_file_verifier           import CrossFileVerifier
from trust_layer.schema_validator              import SchemaValidator, SchemaVersion
from trust_layer.duplicate_detector            import DuplicateDetector
from trust_layer.confidence_scorer             import ConfidenceScore, ConfidenceScorer
from trust_layer.anomaly_detector              import AnomalyDetector
from trust_layer.integrity_reporter            import IntegrityReporter
from trust_layer.atomic_processor              import AtomicProcessor, BatchProcessingError

# Phase 3B
from trust_layer.schema_fingerprinter          import SchemaFingerprinter
from trust_layer.stylist_identity_resolver     import StylistIdentityResolver
from trust_layer.historical_baseline_validator import HistoricalBaselineValidator
from trust_layer.upload_history_tracker        import UploadHistoryTracker

# Phase 3B Addendum — transfer detection + organizational intelligence
from trust_layer.transfer_detector             import TransferDetector, TransferConfidence
from trust_layer.transfer_validator            import TransferValidator
from trust_layer.transfer_impact_tracker       import TransferImpactTracker

# Phase 3B Final Polish — immutable event log + Talent Graph
from trust_layer.transfer_event_log            import TransferEventLog
from trust_layer.location_effect_scorer        import LocationEffectScore, LocationEffectScorer


def run_trust_validation(excel_data: dict, pdf_data: dict = None) -> list:
    """
    Convenience function: run all Phase 3A trust checks for one location.

    Runs in order:
      1. Cross-file verification  (raises ValueError on hard mismatch)
      2. Completeness validation
      3. Anomaly detection        (against historical data stub)

    Args:
        excel_data: Parsed Excel result dict.
        pdf_data:   Parsed PDF result dict, or None for Excel-only.

    Returns:
        List of CompletenessCheck results from all validators.
    """
    checks = []

    # Cross-file verification (only when both files present)
    if pdf_data:
        verifier = CrossFileVerifier()
        checks.extend(verifier.verify(excel_data, pdf_data))

    # Merge data for completeness checks
    merged = excel_data.copy()
    if pdf_data and "service_categories" in pdf_data:
        merged["service_categories"] = pdf_data["service_categories"]
        loc_totals = merged.setdefault("location_totals", {})
        loc_totals.update(pdf_data.get("location_totals", {}))

    # Completeness validation
    validator = CompletenessValidator()
    checks.extend(validator.validate(merged))

    # Historical anomaly detection (stub → skips gracefully if no history)
    anomaly_detector = AnomalyDetector()
    historical_data  = anomaly_detector.get_historical_data(
        excel_data.get("location_id", excel_data.get("location", ""))
    )
    checks.extend(anomaly_detector.detect_anomalies(merged, historical_data))

    return checks


__all__ = [
    # Core types
    "Severity", "CompletenessCheck",
    # Phase 3A validators
    "CompletenessValidator", "CrossFileVerifier",
    "SchemaValidator", "SchemaVersion",
    "DuplicateDetector",
    "ConfidenceScore", "ConfidenceScorer",
    "AnomalyDetector",
    "IntegrityReporter",
    "AtomicProcessor", "BatchProcessingError",
    # Phase 3B validators
    "SchemaFingerprinter",
    "StylistIdentityResolver",
    "HistoricalBaselineValidator",
    "UploadHistoryTracker",
    # Phase 3B Addendum — transfer detection + org intelligence
    "TransferDetector", "TransferConfidence",
    "TransferValidator",
    "TransferImpactTracker",
    # Phase 3B Final Polish — immutable event log + Talent Graph
    "TransferEventLog",
    "LocationEffectScore", "LocationEffectScorer",
    # Convenience
    "run_trust_validation",
]
