"""
Tier 2 Batch Processor
Processes a weekly batch of Zenoti and Salon Ultimate files, merges Excel + PDF data,
and writes normalized stylist rows to Google Sheets.

Typical weekly workflow:
  1. Drop all Excel + PDF files for the week into a single directory
  2. Run:
       python -m parsers.tier2_batch_processor --input-dir /path/to/weekly-files
     Or import and call process_weekly_batch() from your orchestrator

File detection rules:
  - Zenoti Excel     : .xlsx files (not converted from .xls), name contains no "Dashboard"
  - Zenoti PDF       : .pdf files, filename or content suggests "Salon Summary"
  - Salon Ultimate Excel : .xls files (legacy format), or .xlsx converted from .xls
  - Salon Ultimate PDF   : .pdf files, filename or content suggests "Dashboard"

  Because filenames can vary, the processor uses both filename hints AND POS-system detection
  inside each parser (parse() returns 'pos_system' key). Files that fail to parse are logged
  and skipped — the batch continues.

Matching strategy (Excel ↔ PDF):
  Matched on (location_name, period_start, period_end).
  Excel without a matching PDF → written as Phase 1 row (Excel only).
  PDF without a matching Excel → logged as warning, not written (no stylist rows to distribute into).

Output:
  - Calls sheets_writer.write_stylists() for first-run / full refresh
  - Calls sheets_writer.append_stylists() for incremental weekly runs

Usage (CLI):
  python -m parsers.tier2_batch_processor \\
      --input-dir ./weekly-files \\
      --mode append   # or overwrite (default: append)

Usage (Python):
  from parsers.tier2_batch_processor import Tier2BatchProcessor
  processor = Tier2BatchProcessor(input_dir="./weekly-files")
  summary = processor.process()
  print(summary)
"""

import argparse
import logging
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Trust layer — imported lazily inside methods so the batch processor can still
# run (with trust checks disabled) if the trust_layer package isn't importable.
_TRUST_LAYER_AVAILABLE = True
try:
    from trust_layer import (
        run_trust_validation,
        ConfidenceScorer,
        IntegrityReporter,
        BatchProcessingError,
    )
except ImportError:
    _TRUST_LAYER_AVAILABLE = False

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Parser imports — wrapped so import errors are surfaced clearly
# ---------------------------------------------------------------------------

def _import_parsers():
    """Import all parsers and return them as a namespace dict."""
    try:
        from parsers.zenoti_excel       import ZenotiExcelParser
        from parsers.zenoti_pdf         import ZenotiPDFParser
        from parsers.salon_ultimate_excel import SalonUltimateExcelParser
        from parsers.salon_ultimate_pdf   import SalonUltimatePDFParser
        from utils.data_merger          import DataMerger
    except ImportError as exc:
        raise ImportError(
            f"Failed to import a required parser: {exc}\n"
            "Ensure you are running from the project root and all dependencies are installed."
        ) from exc

    return {
        "ZenotiExcel":   ZenotiExcelParser,
        "ZenotiPDF":     ZenotiPDFParser,
        "SUExcel":       SalonUltimateExcelParser,
        "SUPDF":         SalonUltimatePDFParser,
        "DataMerger":    DataMerger,
    }


# ---------------------------------------------------------------------------
# File classification
# ---------------------------------------------------------------------------

def detect_system(file_path: str) -> Optional[str]:
    """
    Classify a file as one of:
      'zenoti_excel', 'zenoti_pdf', 'su_excel', 'su_pdf', or None (skip).

    Classification uses filename heuristics.
    The parsers themselves are the authoritative source — if a file misclassified here
    it will raise an exception at parse time (caught and logged by the batch processor).

    Rules:
      .xls              → 'su_excel'   (SU uses legacy .xls)
      .xlsx (no 'dash') → ambiguous; try zenoti_excel first
      .pdf with 'dash'  → 'su_pdf'
      .pdf without      → 'zenoti_pdf'
    """
    p = Path(file_path)
    suffix = p.suffix.lower()
    name   = p.name.lower()

    if suffix == ".xls":
        return "su_excel"

    if suffix == ".xlsx":
        # SU Excel files are originally .xls; if we see a raw .xlsx it's Zenoti
        # unless it's a converted SU file (LibreOffice produces same base name).
        # Heuristic: converted SU files share the directory with the source .xls.
        xls_sibling = p.with_suffix(".xls")
        if xls_sibling.exists():
            return "su_excel"
        return "zenoti_excel"

    if suffix == ".pdf":
        # Salon Ultimate PDFs typically have "dashboard" in the name
        if "dashboard" in name:
            return "su_pdf"
        # Zenoti PDFs typically have "salon summary" or just salon naming
        return "zenoti_pdf"

    return None   # .csv, .txt, etc. — skip


def group_files_by_location(
    files: List[str],
) -> Dict[str, Dict[str, Optional[str]]]:
    """
    Group file paths by their detected type.

    Returns a dict:
      {
        'zenoti_excel': [path, ...],
        'zenoti_pdf':   [path, ...],
        'su_excel':     [path, ...],
        'su_pdf':       [path, ...],
        'skipped':      [path, ...],
      }
    """
    groups: Dict[str, List[str]] = {
        "zenoti_excel": [],
        "zenoti_pdf":   [],
        "su_excel":     [],
        "su_pdf":       [],
        "skipped":      [],
    }

    for f in files:
        category = detect_system(f)
        if category:
            groups[category].append(f)
        else:
            groups["skipped"].append(f)

    return groups


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

REQUIRED_STYLIST_KEYS = {
    "location", "location_id", "stylist_name",
    "period_start", "period_end", "pos_system",
    "guest_count", "service_net", "product_net",
    "total_sales", "ppg_net",
}


def validate_stylist_data(stylists: List[Dict]) -> Tuple[List[Dict], List[str]]:
    """
    Validate a list of stylist dicts.

    Returns:
        (valid_stylists, error_messages)

    Checks:
      - Required keys are present
      - guest_count >= 0
      - service_net >= 0
      - product_net >= 0
      - total_sales ≈ service_net + product_net (within $0.02 rounding tolerance)
    """
    valid = []
    errors = []

    for i, s in enumerate(stylists):
        tag = f"[{s.get('stylist_name', f'row {i}')} @ {s.get('location', '?')}]"

        # Required key check
        missing = REQUIRED_STYLIST_KEYS - set(s.keys())
        if missing:
            errors.append(f"{tag} Missing keys: {sorted(missing)}")
            continue

        # Non-negative checks
        for field in ("guest_count", "service_net", "product_net"):
            if s[field] < 0:
                errors.append(f"{tag} Negative value for {field}: {s[field]}")

        # total_sales sanity
        expected_total = round(float(s["service_net"]) + float(s["product_net"]), 2)
        actual_total   = round(float(s["total_sales"]), 2)
        if abs(expected_total - actual_total) > 0.02:
            errors.append(
                f"{tag} total_sales mismatch: "
                f"service_net({s['service_net']}) + product_net({s['product_net']}) "
                f"= {expected_total}, but total_sales = {actual_total}"
            )

        valid.append(s)

    return valid, errors


# ---------------------------------------------------------------------------
# Core batch processor
# ---------------------------------------------------------------------------

class Tier2BatchProcessor:
    """
    Orchestrate parsing, merging, validation, and writing for a weekly batch.

    Args:
        input_dir    : Directory containing all Excel + PDF files for the week.
        dry_run      : If True, parse and validate but do not write to Sheets.
        sheets_writer: Pre-constructed GoogleSheetsWriter (or None for dry run / auto-build).
    """

    def __init__(
        self,
        input_dir: str,
        dry_run: bool = False,
        sheets_writer=None,
    ):
        self.input_dir     = input_dir
        self.dry_run       = dry_run
        self._writer       = sheets_writer
        self._parsers      = _import_parsers()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def process(self, write_mode: str = "append") -> Dict:
        """
        Run the full batch: detect → parse → merge → validate → write.

        Args:
            write_mode: 'append' (default) or 'overwrite'.

        Returns:
            Summary dict with counts and any errors.
        """
        summary = {
            "input_dir":        self.input_dir,
            "files_found":      0,
            "excel_parsed":     0,
            "pdf_parsed":       0,
            "merged":           0,
            "excel_only":       0,
            "stylists_total":   0,
            "validation_errors":[],
            "parse_errors":     [],
            "write_mode":       write_mode,
            "dry_run":          self.dry_run,
            # Trust layer (populated in step 6a)
            "trust_score":      None,   # int 0–100, or None if skipped
            "trust_tier":       None,   # 'high' | 'moderate' | 'low' | None
            "trust_report":     None,   # full IntegrityReporter output string
            "trust_error":      None,   # set on hard fail
        }

        # 1. Discover files
        all_files = self._discover_files()
        summary["files_found"] = len(all_files)

        if not all_files:
            logger.warning("No files found in %s", self.input_dir)
            return summary

        # 2. Classify
        groups = group_files_by_location(all_files)
        for skipped in groups["skipped"]:
            logger.debug("Skipped (unrecognised type): %s", skipped)

        # 3. Parse Excel files
        excel_results = []
        for path in groups["zenoti_excel"]:
            result = self._safe_parse("ZenotiExcel", path, summary)
            if result:
                excel_results.append(result)

        for path in groups["su_excel"]:
            result = self._safe_parse("SUExcel", path, summary)
            if result:
                excel_results.append(result)

        summary["excel_parsed"] = len(excel_results)

        # 4. Parse PDF files
        pdf_results = []
        for path in groups["zenoti_pdf"]:
            result = self._safe_parse("ZenotiPDF", path, summary)
            if result:
                pdf_results.append(result)

        for path in groups["su_pdf"]:
            result = self._safe_parse("SUPDF", path, summary)
            if result:
                pdf_results.append(result)

        summary["pdf_parsed"] = len(pdf_results)

        if not excel_results:
            logger.warning("No Excel files successfully parsed — nothing to write.")
            return summary

        # 5. Build PDF lookup for trust validation (keyed by normalised location name)
        pdf_by_location: Dict[str, Dict] = {
            r.get("location", "").lower().strip(): r
            for r in pdf_results
        }

        # 6. Merge Excel + PDF
        DataMerger = self._parsers["DataMerger"]
        merged_locations = DataMerger.merge_multiple_locations(excel_results, pdf_results)

        for loc in merged_locations:
            if "service_categories" in loc:
                summary["merged"] += 1
            else:
                summary["excel_only"] += 1

        # 6a. Trust-layer validation
        # Runs per-location; attaches 'trust_checks' to each merged dict.
        # Hard fails (ValueError from cross-file verifier or historical baseline)
        # propagate up and abort the batch — nothing is written.
        # Low-confidence batches also abort (BatchProcessingError).
        if _TRUST_LAYER_AVAILABLE:
            try:
                trust_report, batch_score = self._run_trust_layer(
                    excel_results, merged_locations, pdf_by_location
                )
                summary["trust_score"]  = batch_score.score
                summary["trust_tier"]   = batch_score.tier
                summary["trust_report"] = trust_report
                logger.info(
                    "Trust layer: score=%d%% tier=%s — %s",
                    batch_score.score, batch_score.tier, batch_score.summary,
                )

                if batch_score.tier == "low":
                    raise BatchProcessingError(
                        f"Batch confidence score is LOW ({batch_score.score}%) — "
                        "manual review required before writing to Sheets.",
                        errors=[batch_score.summary],
                    )

                if batch_score.tier == "moderate":
                    logger.warning(
                        "Batch confidence is MODERATE (%d%%) — review warnings "
                        "before treating this data as authoritative.",
                        batch_score.score,
                    )

            except BatchProcessingError:
                summary["trust_error"] = (
                    f"LOW confidence ({summary.get('trust_score', '?')}%) — "
                    "batch blocked. Review warnings and re-upload."
                )
                logger.error(summary["trust_error"])
                raise

            except ValueError as exc:
                # Hard fail from CrossFileVerifier or HistoricalBaselineValidator
                summary["trust_error"] = str(exc)
                logger.error("Trust layer HARD FAIL — batch aborted:\n%s", exc)
                raise BatchProcessingError(
                    "Trust layer hard fail — batch aborted.",
                    errors=[str(exc)],
                )
        else:
            logger.warning(
                "trust_layer package not found — skipping trust validation. "
                "Install it or check your PYTHONPATH."
            )

        # 7. Flatten to stylist rows
        all_stylists = DataMerger.flatten_stylists(merged_locations)

        # 8. Validate
        valid_stylists, val_errors = validate_stylist_data(all_stylists)
        summary["validation_errors"] = val_errors
        summary["stylists_total"]    = len(valid_stylists)

        if val_errors:
            for err in val_errors:
                logger.warning("Validation: %s", err)

        # 9. Write (unless dry run)
        if not self.dry_run and valid_stylists:
            writer = self._get_writer()
            if write_mode == "overwrite":
                writer.write_stylists(valid_stylists)
            else:
                writer.append_stylists(valid_stylists)
        elif self.dry_run:
            logger.info(
                "DRY RUN — would write %d stylist rows (mode: %s)",
                len(valid_stylists), write_mode,
            )

        return summary

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _discover_files(self) -> List[str]:
        """Return all files in input_dir (non-recursive)."""
        if not os.path.isdir(self.input_dir):
            raise NotADirectoryError(f"Input directory not found: {self.input_dir}")

        return [
            os.path.join(self.input_dir, f)
            for f in os.listdir(self.input_dir)
            if os.path.isfile(os.path.join(self.input_dir, f))
        ]

    def _safe_parse(
        self,
        parser_key: str,
        file_path: str,
        summary: Dict,
    ) -> Optional[Dict]:
        """
        Parse one file with the named parser, catching and logging any exception.

        Returns the parsed dict on success, or None on failure.
        """
        ParserClass = self._parsers[parser_key]
        try:
            parser = ParserClass(file_path)
            result = parser.parse()
            logger.info(
                "Parsed [%s] %s → location=%s period=%s",
                parser_key,
                os.path.basename(file_path),
                result.get("location", "?"),
                result.get("period", {}).get("start_date", "?"),
            )
            return result
        except Exception as exc:
            msg = f"{parser_key} failed on {os.path.basename(file_path)}: {exc}"
            logger.error(msg)
            summary["parse_errors"].append(msg)
            return None

    def _get_writer(self):
        """Return the sheets writer (build from env if not injected)."""
        if self._writer is not None:
            return self._writer

        from utils.sheets_writer import GoogleSheetsWriter
        return GoogleSheetsWriter.from_env()

    def _run_trust_layer(
        self,
        excel_results:    List[Dict],
        merged_locations: List[Dict],
        pdf_by_location:  Dict[str, Dict],
    ):
        """
        Run trust-layer validation for every location in the batch.

        Strategy:
          1. Build a fast lookup of Excel results by normalised location name.
          2. For each merged location, find the original Excel + PDF parsed dicts.
          3. Call run_trust_validation(excel_data, pdf_data) → List[CompletenessCheck].
          4. Attach the checks to the merged dict as 'trust_checks' (IntegrityReporter reads this key).
          5. Calculate per-location scores, aggregate to a batch score.
          6. Generate and return the full integrity report string.

        Args:
            excel_results    : List of raw Excel parsed dicts (from parsers).
            merged_locations : List of merged location dicts (from DataMerger).
            pdf_by_location  : Dict of PDF results keyed by lowercase location name.

        Returns:
            (report_str, batch_score_ConfidenceScore)

        Raises:
            ValueError          : Hard fail from CrossFileVerifier or HistoricalBaselineValidator.
            BatchProcessingError: Propagated from trust layer atomic processor.
        """
        scorer   = ConfidenceScorer()
        reporter = IntegrityReporter()

        # Build fast Excel lookup (normalised name → parsed dict)
        excel_by_location = {
            r.get("location", "").lower().strip(): r
            for r in excel_results
        }

        all_scores: List = []

        for loc in merged_locations:
            loc_name   = loc.get("location", "").lower().strip()
            excel_data = excel_by_location.get(loc_name, loc)   # fall back to merged dict
            pdf_data   = pdf_by_location.get(loc_name)          # None for Excel-only locations

            # Hard failures (ValueError) propagate immediately — batch aborted.
            checks = run_trust_validation(excel_data, pdf_data)
            loc["trust_checks"] = checks

            score = scorer.calculate_score(checks)
            all_scores.append(score)

            logger.debug(
                "Trust [%s]: score=%d%% tier=%s — %s",
                loc.get("location", "?"),
                score.score,
                score.tier,
                score.summary,
            )

        batch_score  = scorer.aggregate_batch_score(all_scores)
        report_str   = reporter.generate_report(merged_locations)

        return report_str, batch_score

    # ------------------------------------------------------------------
    # Human-readable summary
    # ------------------------------------------------------------------

    @staticmethod
    def format_summary(summary: Dict) -> str:
        """Return a clean multi-line summary string for logging or printing."""
        # Trust tier → display icon
        _tier_icon = {"high": "🟢", "moderate": "🟡", "low": "🔴"}
        trust_score = summary.get("trust_score")
        trust_tier  = summary.get("trust_tier")
        trust_icon  = _tier_icon.get(trust_tier, "⚪") if trust_tier else "—"
        trust_line  = (
            f"{trust_icon} {trust_score}% ({trust_tier})"
            if trust_score is not None
            else "skipped (trust_layer not available)"
        )

        lines = [
            "─" * 55,
            "TIER 2 BATCH PROCESSOR — RUN SUMMARY",
            "─" * 55,
            f"  Input dir      : {summary['input_dir']}",
            f"  Files found    : {summary['files_found']}",
            f"  Excel parsed   : {summary['excel_parsed']}",
            f"  PDF parsed     : {summary['pdf_parsed']}",
            f"  Merged (Excel+PDF): {summary['merged']}",
            f"  Excel-only     : {summary['excel_only']}",
            f"  Stylists total : {summary['stylists_total']}",
            f"  Write mode     : {summary['write_mode']}",
            f"  Dry run        : {summary['dry_run']}",
            f"  Trust score    : {trust_line}",
        ]

        if summary.get("trust_error"):
            lines.append(f"\n  ❌ Trust error: {summary['trust_error']}")

        if summary.get("trust_report"):
            lines.append("")
            lines.append(summary["trust_report"])

        if summary["parse_errors"]:
            lines.append(f"\n  ⚠️  Parse errors ({len(summary['parse_errors'])}):")
            for err in summary["parse_errors"]:
                lines.append(f"    • {err}")

        if summary["validation_errors"]:
            lines.append(f"\n  ⚠️  Validation errors ({len(summary['validation_errors'])}):")
            for err in summary["validation_errors"]:
                lines.append(f"    • {err}")

        if (
            not summary["parse_errors"]
            and not summary["validation_errors"]
            and not summary.get("trust_error")
        ):
            lines.append("\n  ✅ All checks passed")

        lines.append("─" * 55)
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="tier2_batch_processor",
        description="Process a weekly batch of Zenoti + Salon Ultimate files and write to Google Sheets.",
    )
    p.add_argument(
        "--input-dir",
        required=True,
        help="Directory containing all Excel + PDF files for the week.",
    )
    p.add_argument(
        "--mode",
        choices=["append", "overwrite"],
        default="append",
        help="Write mode: 'append' adds rows below existing data; 'overwrite' replaces the sheet. Default: append.",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and validate but do not write to Google Sheets.",
    )
    p.add_argument(
        "--verbose",
        action="store_true",
        help="Enable DEBUG logging.",
    )
    return p


def main(argv=None):
    args = _build_arg_parser().parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    processor = Tier2BatchProcessor(
        input_dir=args.input_dir,
        dry_run=args.dry_run,
    )

    try:
        summary = processor.process(write_mode=args.mode)
    except NotADirectoryError as exc:
        logger.error(str(exc))
        sys.exit(1)
    except Exception as exc:
        # Catches BatchProcessingError (trust layer block) and ValueError (hard fails).
        # summary may be partial — print whatever we have, then exit non-zero.
        logger.error("Batch processing aborted: %s", exc)
        print(f"\n❌  BATCH ABORTED\n{exc}")
        sys.exit(1)

    print(Tier2BatchProcessor.format_summary(summary))

    # Exit non-zero if there were hard errors or a zero-output batch
    has_errors = bool(
        summary["parse_errors"]
        or summary.get("trust_error")
        or (summary["stylists_total"] == 0 and summary["excel_parsed"] > 0)
    )
    if has_errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
