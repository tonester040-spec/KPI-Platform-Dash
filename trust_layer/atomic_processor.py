"""
Trust Layer — Atomic Batch Processor
Ensures batch processing is all-or-nothing: either every location succeeds,
or nothing is written.

Prevents mixed-week data where some locations show the current week
and others still show last week's numbers.

Staging workflow (Phase 4 — currently a pass-through):
  Stage 1: Write to staging (separate sheet tab)
  Stage 2: Validate staged data
  Stage 3: Atomic swap to production

Until GoogleSheetsWriter.write_to_staging() exists, the processor writes
directly.  The _write_to_staging / _validate_staging / _promote_to_production
stubs are structured as placeholder hooks.
"""

import logging
import traceback
from typing import Any, Dict, List, Optional, Tuple

from trust_layer.severity import CompletenessCheck

logger = logging.getLogger(__name__)


class BatchProcessingError(Exception):
    """
    Raised when a batch fails validation.

    The message is human-readable and includes per-location error details.
    Catching code should display the message directly to the user.
    """
    def __init__(self, message: str, errors: Optional[List[Dict]] = None):
        super().__init__(message)
        self.errors = errors or []


class AtomicProcessor:
    """
    Orchestrates atomic batch processing.

    Usage:
        processor = AtomicProcessor()
        try:
            result = processor.process_batch_atomic(
                parse_results=parse_results,    # list of (excel, pdf) tuples
                merge_fn=DataMerger.merge_location_data,
                trust_fn=run_trust_validation,
                write_fn=writer.write_stylists,
            )
        except BatchProcessingError as e:
            print(str(e))   # human-readable failure report
    """

    def process_batch_atomic(
        self,
        parse_results:  List[Tuple[Dict, Optional[Dict]]],
        merge_fn:       Any,   # (excel_data, pdf_data) → merged_dict
        trust_fn:       Any,   # (excel_data, pdf_data) → List[CompletenessCheck]
        write_fn:       Any,   # (stylists) → None
        dry_run:        bool = False,
    ) -> Dict:
        """
        Process a complete weekly batch atomically.

        Args:
            parse_results: List of (excel_data, pdf_data) tuples.
                           pdf_data may be None for Excel-only locations.
            merge_fn:      Callable that merges Excel + PDF data.
            trust_fn:      Callable that returns trust layer checks.
            write_fn:      Callable that writes stylist rows to Sheets.
            dry_run:       If True, parse and validate but skip the write.

        Returns:
            {
              'success':              bool,
              'locations_processed':  int,
              'total_stylists':       int,
            }

        Raises:
            BatchProcessingError: If any location fails validation.
        """
        results: List[Dict] = []
        errors:  List[Dict] = []

        # ── Phase 1: Parse, validate, and merge all locations ───────────
        for excel_data, pdf_data in parse_results:
            location_id = excel_data.get("location_id", excel_data.get("location", "?"))
            try:
                # Run trust validation
                trust_checks = trust_fn(excel_data, pdf_data)

                # Merge data
                merged = merge_fn(excel_data, pdf_data) if pdf_data else excel_data.copy()
                merged["trust_checks"] = trust_checks

                results.append(merged)

            except ValueError as e:
                # Hard fail from a validator (location mismatch, schema drift, etc.)
                errors.append({
                    "location":  location_id,
                    "error":     str(e),
                    "traceback": "",   # ValueError from trust layer is message-only
                })
                logger.error("Validation hard-fail for %s: %s", location_id, e)

            except Exception as e:
                errors.append({
                    "location":  location_id,
                    "error":     str(e),
                    "traceback": traceback.format_exc(),
                })
                logger.error("Unexpected error for %s: %s", location_id, e)

        # ── Phase 2: All-or-nothing decision ────────────────────────────
        if errors:
            self._handle_batch_failure(
                success_count=len(results),
                total_count=len(parse_results),
                errors=errors,
            )

        if not results:
            raise BatchProcessingError("No locations were successfully processed.")

        # ── Phase 3: Write (atomic) ──────────────────────────────────────
        from utils.data_merger import DataMerger
        all_stylists = DataMerger.flatten_stylists(results)

        if not dry_run:
            staging_id = self._write_to_staging(results)
            self._validate_staging(staging_id, all_stylists)
            self._promote_to_production(staging_id, all_stylists, write_fn)
        else:
            logger.info(
                "AtomicProcessor DRY RUN — would write %d stylist rows "
                "from %d locations",
                len(all_stylists), len(results),
            )

        return {
            "success":             True,
            "locations_processed": len(results),
            "total_stylists":      len(all_stylists),
        }

    # ------------------------------------------------------------------
    # Failure reporting
    # ------------------------------------------------------------------

    def _handle_batch_failure(
        self,
        success_count: int,
        total_count:   int,
        errors:        List[Dict],
    ) -> None:
        """Build a human-readable failure report and raise BatchProcessingError."""
        error_blocks: List[str] = []
        for err in errors:
            block = f"\n  🔴 {err['location'].upper()}\n     {err['error']}"
            if err.get("traceback"):
                # Include first line of traceback for technical context
                first_line = err["traceback"].strip().split("\n")[-1]
                block += f"\n     ({first_line})"
            error_blocks.append(block)

        message = (
            f"\n{'=' * 60}\n"
            f"❌  BATCH PROCESSING FAILED\n"
            f"{'=' * 60}\n\n"
            f"  Successfully processed : {success_count}/{total_count} locations\n"
            f"  Failed                 : {len(errors)} locations\n\n"
            f"  🚨  NO DATA HAS BEEN WRITTEN\n\n"
            f"  To maintain data integrity, this batch was rejected.\n"
            f"  All locations must pass validation before data is written.\n\n"
            f"  ERRORS:{''.join(error_blocks)}\n\n"
            f"{'=' * 60}\n"
            f"  ACTION REQUIRED:\n"
            f"    1. Review and fix the errors above\n"
            f"    2. Verify all files are correct and properly paired\n"
            f"    3. Re-upload the complete batch\n"
            f"{'=' * 60}\n"
        )
        raise BatchProcessingError(message, errors=errors)

    # ------------------------------------------------------------------
    # Staging hooks (Phase 4 stubs — currently pass-through to direct write)
    # ------------------------------------------------------------------

    def _write_to_staging(self, results: List[Dict]) -> str:
        """
        Write results to a staging area before promoting to production.

        Stub — currently returns a placeholder staging ID.
        Phase 4: Create a timestamped tab in Google Sheets.
        """
        from datetime import datetime
        staging_id = f"staging_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        logger.debug("Staging placeholder: %s (direct write in use)", staging_id)
        return staging_id

    def _validate_staging(
        self, staging_id: str, stylists: List[Dict]
    ) -> None:
        """
        Validate data in staging before promotion.

        Stub — currently a no-op.
        Phase 4: Re-read from the staging tab and cross-check row counts.
        """
        logger.debug(
            "Staging validation stub for %s (%d rows)", staging_id, len(stylists)
        )

    def _promote_to_production(
        self,
        staging_id: str,
        stylists:   List[Dict],
        write_fn:   Any,
    ) -> None:
        """
        Atomically promote staging data to the production sheet.

        Currently delegates directly to write_fn (no separate staging tab yet).
        Phase 4: Atomic tab swap.
        """
        logger.debug(
            "Promoting %d rows from %s to production", len(stylists), staging_id
        )
        if stylists:
            write_fn(stylists)
