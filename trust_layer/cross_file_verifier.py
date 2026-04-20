"""
Trust Layer — Cross-File Verifier
Verifies that the Excel and PDF being merged are from the same location and period.

HARD VALIDATION — location or period mismatch raises ValueError immediately.
The batch processor catches this and rejects the entire batch.
"""

from datetime import datetime, timedelta
from difflib import SequenceMatcher
from typing import Dict, List

from trust_layer.severity import CompletenessCheck


class CrossFileVerifier:
    """
    Verifies Excel + PDF file pairs represent the same location and period.

    Both dicts must be in the normalized format produced by the parsers:
      {
        'location':      str,   # canonical location name
        'period':        {'start_date': 'YYYY-MM-DD', 'end_date': 'YYYY-MM-DD'},
        'pos_system':    'zenoti' | 'salon_ultimate',
        ...
      }

    The verifier flattens 'period' internally; callers do not need to pre-flatten.
    """

    LOCATION_SIMILARITY_THRESHOLD = 0.70   # <70% match → hard fail
    LOCATION_EXACT_THRESHOLD      = 0.95   # <95% match → warn (but continue)
    PERIOD_TOLERANCE_DAYS         = 3       # ±3 days tolerance on period boundaries

    def verify(self, excel_data: Dict, pdf_data: Dict) -> List[CompletenessCheck]:
        """
        Run all cross-file verification checks.

        Args:
            excel_data: Parsed Excel result dict.
            pdf_data:   Parsed PDF result dict.

        Returns:
            List of CompletenessCheck results.

        Raises:
            ValueError: If a critical mismatch is detected (location, period, or system).
        """
        checks: List[CompletenessCheck] = []
        checks.extend(self._verify_location_match(excel_data, pdf_data))
        checks.extend(self._verify_period_match(excel_data, pdf_data))
        checks.extend(self._verify_system_match(excel_data, pdf_data))
        return checks

    # ------------------------------------------------------------------
    # Helpers — flatten period dict from parser output
    # ------------------------------------------------------------------

    @staticmethod
    def _get_period(data: Dict) -> Dict[str, str]:
        """Extract period dates from nested 'period' key."""
        period = data.get("period") or {}
        return {
            "start": period.get("start_date") or "",
            "end":   period.get("end_date")   or "",
        }

    # ------------------------------------------------------------------
    # Individual checks
    # ------------------------------------------------------------------

    def _verify_location_match(
        self, excel_data: Dict, pdf_data: Dict
    ) -> List[CompletenessCheck]:
        """
        Verify location names match (fuzzy — allows minor label variations).

        Hard fails on <70% similarity (clearly different locations).
        Warns on <95% similarity (same location, minor suffix difference).
        """
        excel_loc = str(excel_data.get("location", "")).lower().strip()
        pdf_loc   = str(pdf_data.get("location",  "")).lower().strip()

        similarity = SequenceMatcher(None, excel_loc, pdf_loc).ratio()

        if similarity < self.LOCATION_SIMILARITY_THRESHOLD:
            raise ValueError(
                f"❌ LOCATION MISMATCH DETECTED\n\n"
                f"  Excel file location : '{excel_data.get('location', '')}'\n"
                f"  PDF file location   : '{pdf_data.get('location', '')}'\n"
                f"  Similarity          : {similarity:.0%}\n\n"
                f"These files appear to be from DIFFERENT locations.\n"
                f"Verify files are correctly paired and re-upload.\n\n"
                f"Processing stopped to prevent data corruption."
            )

        if similarity < self.LOCATION_EXACT_THRESHOLD:
            return [CompletenessCheck(
                name="Location Name Match",
                status="warn",
                message=(
                    f"Location names similar but not identical: "
                    f"'{excel_data.get('location', '')}' vs "
                    f"'{pdf_data.get('location', '')}' "
                    f"({similarity:.0%} match)"
                ),
                severity=5,
            )]

        return [CompletenessCheck(
            name="Location Name Match",
            status="pass",
            message=f"Location names match: '{excel_data.get('location', '')}'",
            severity=0,
        )]

    def _verify_period_match(
        self, excel_data: Dict, pdf_data: Dict
    ) -> List[CompletenessCheck]:
        """
        Verify reporting periods overlap within tolerance.

        Hard fails if start or end dates differ by > PERIOD_TOLERANCE_DAYS.
        Warns if within tolerance but not exactly equal.
        """
        ep = self._get_period(excel_data)
        pp = self._get_period(pdf_data)

        # Graceful skip if dates couldn't be extracted
        if not ep["start"] or not pp["start"]:
            return [CompletenessCheck(
                name="Period Match",
                status="warn",
                message="Could not extract period from one or both files — verify manually",
                severity=10,
            )]

        try:
            excel_start = datetime.fromisoformat(ep["start"])
            excel_end   = datetime.fromisoformat(ep["end"])
            pdf_start   = datetime.fromisoformat(pp["start"])
            pdf_end     = datetime.fromisoformat(pp["end"])
        except ValueError as exc:
            return [CompletenessCheck(
                name="Period Match",
                status="warn",
                message=f"Could not parse period dates: {exc}",
                severity=10,
            )]

        start_diff = abs((excel_start - pdf_start).days)
        end_diff   = abs((excel_end   - pdf_end  ).days)

        if start_diff > self.PERIOD_TOLERANCE_DAYS or end_diff > self.PERIOD_TOLERANCE_DAYS:
            raise ValueError(
                f"❌ PERIOD MISMATCH DETECTED\n\n"
                f"  Excel period : {ep['start']} → {ep['end']}\n"
                f"  PDF period   : {pp['start']} → {pp['end']}\n"
                f"  Start delta  : {start_diff} days\n"
                f"  End delta    : {end_diff} days\n\n"
                f"These files appear to be from DIFFERENT reporting periods.\n"
                f"Verify files are correctly paired and re-upload.\n\n"
                f"Processing stopped to prevent data corruption."
            )

        if start_diff > 0 or end_diff > 0:
            return [CompletenessCheck(
                name="Period Match",
                status="warn",
                message=(
                    f"Periods close but not identical "
                    f"(±{max(start_diff, end_diff)} day(s)) — "
                    f"verify correct files were uploaded"
                ),
                severity=5,
            )]

        return [CompletenessCheck(
            name="Period Match",
            status="pass",
            message=f"Periods match: {ep['start']} → {ep['end']}",
            severity=0,
        )]

    def _verify_system_match(
        self, excel_data: Dict, pdf_data: Dict
    ) -> List[CompletenessCheck]:
        """
        Verify both files are from the same POS system.

        A Zenoti Excel paired with a Salon Ultimate PDF is always wrong.
        """
        excel_sys = excel_data.get("pos_system", "")
        pdf_sys   = pdf_data.get("pos_system",   "")

        if excel_sys != pdf_sys:
            raise ValueError(
                f"❌ SYSTEM MISMATCH DETECTED\n\n"
                f"  Excel file : {excel_sys}\n"
                f"  PDF file   : {pdf_sys}\n\n"
                f"These files are from DIFFERENT POS systems.\n"
                f"Files are incorrectly paired — this should never happen.\n\n"
                f"Processing stopped to prevent data corruption."
            )

        return [CompletenessCheck(
            name="System Match",
            status="pass",
            message=f"Both files from {excel_sys}",
            severity=0,
        )]
