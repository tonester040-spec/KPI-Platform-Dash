"""
Trust Layer — Historical Baseline Validator  (Phase 3B)
Validates current data against rolling historical baselines.

While AnomalyDetector flags week-over-week revenue changes,
HistoricalBaselineValidator focuses on a specific dangerous pattern:
  A category that is usually significant (e.g., color averages 40 services/week)
  suddenly shows ZERO — which likely means a missing PDF page, not
  a real business result.

Hard fail condition:
  category_count == 0 AND rolling_avg > MIN_SIGNIFICANT_AVERAGE

PDF page count validation:
  Zenoti Salon Summary must have ≥ 3 pages
  SU Dashboard must have ≥ 1 page

Storage used: reads from DATA/STYLISTS_DATA tabs for baseline calculation.
"""

import logging
from typing import Dict, List, Optional

from trust_layer.severity import CompletenessCheck

logger = logging.getLogger(__name__)


class HistoricalBaselineValidator:
    """
    Validates that categories with historical significance aren't silently zero.

    Phase 3B — Sheets I/O is stubbed; all logic fully functional once wired.
    """

    # Minimum historical average below which a zero count is acceptable
    MIN_SIGNIFICANT_AVERAGE = 5.0   # <5 services/week avg → zero is plausible

    # Minimum page counts per POS system
    MIN_PAGES = {
        "zenoti":          3,
        "salon_ultimate":  1,
    }

    def validate(
        self,
        location_data:       Dict,
        historical_baseline: Optional[Dict] = None,
    ) -> List[CompletenessCheck]:
        """
        Validate current data against historical baseline.

        Args:
            location_data:       Merged location dict.
            historical_baseline: Rolling averages from previous 4 weeks.
                                 Keys: '{category}_count_avg', '{category}_net_avg'.
                                 Pass None to skip baseline checks.

        Returns:
            List of CompletenessCheck results.
        """
        checks: List[CompletenessCheck] = []
        checks.extend(self._validate_pdf_page_count(location_data))
        checks.extend(self._validate_zero_categories(location_data, historical_baseline))
        return checks

    # ------------------------------------------------------------------
    # PDF page count validation
    # ------------------------------------------------------------------

    def _validate_pdf_page_count(self, data: Dict) -> List[CompletenessCheck]:
        """
        Verify the PDF had enough pages to contain all service categories.

        PDF metadata is injected by the PDF parsers as 'pdf_page_count'.
        If not present, skip this check (Excel-only batch).
        """
        page_count = data.get("pdf_page_count")
        pos_system = data.get("pos_system", "")
        min_pages  = self.MIN_PAGES.get(pos_system, 1)

        if page_count is None:
            # No PDF in this batch — skip
            return []

        if page_count < min_pages:
            return [CompletenessCheck(
                name="PDF Page Count",
                status="fail",
                message=(
                    f"PDF has {page_count} page(s) but {pos_system} reports "
                    f"require ≥{min_pages}. "
                    f"Possible incomplete PDF or download failure."
                ),
                severity=40,
            )]

        return [CompletenessCheck(
            name="PDF Page Count",
            status="pass",
            message=f"PDF has {page_count} page(s) (≥{min_pages} required)",
            severity=0,
        )]

    # ------------------------------------------------------------------
    # Zero-category baseline validation
    # ------------------------------------------------------------------

    def _validate_zero_categories(
        self,
        data:     Dict,
        baseline: Optional[Dict],
    ) -> List[CompletenessCheck]:
        """
        Hard fail if a usually-significant category is zero this week.

        A category is "usually significant" if its rolling 4-week average
        exceeds MIN_SIGNIFICANT_AVERAGE services/week.
        """
        cats = data.get("service_categories")
        if not cats:
            return []   # Excel-only, nothing to check

        if not baseline:
            return [CompletenessCheck(
                name="Historical Baseline",
                status="pass",
                message="No baseline available — baseline check skipped (first upload)",
                severity=0,
            )]

        blocking_failures: List[str] = []

        for category in ("wax", "color", "treatment"):
            count_field   = f"{category}_count"
            avg_key       = f"{category}_count_avg"

            current_count = cats.get(count_field, 0) or 0
            hist_avg      = float(baseline.get(avg_key, 0) or 0)

            if current_count == 0 and hist_avg >= self.MIN_SIGNIFICANT_AVERAGE:
                blocking_failures.append(
                    f"  {category.upper()}: current=0, "
                    f"4-week avg={hist_avg:.1f} services/week"
                )

        if blocking_failures:
            detail = "\n".join(blocking_failures)
            raise ValueError(
                f"❌ HISTORICAL BASELINE VIOLATION\n\n"
                f"  The following categories are zero this week but have\n"
                f"  historically significant averages:\n\n"
                f"{detail}\n\n"
                f"  This strongly indicates a missing PDF page or parser failure,\n"
                f"  NOT a real business result.\n\n"
                f"  ACTION REQUIRED:\n"
                f"    1. Re-download the source PDF from the POS system\n"
                f"    2. Verify the PDF has all service category pages\n"
                f"    3. Re-upload the corrected file\n\n"
                f"  Processing stopped to prevent coaching decisions based on "
                f"missing data."
            )

        return [CompletenessCheck(
            name="Historical Baseline",
            status="pass",
            message="All categories consistent with historical baseline",
            severity=0,
        )]

    # ------------------------------------------------------------------
    # Sheets query stub
    # ------------------------------------------------------------------

    def get_baseline(self, location_id: str, weeks: int = 4) -> Optional[Dict]:
        """
        Fetch rolling N-week averages for a location.

        Stub — returns None until Sheets read interface is implemented.

        Expected return:
            {
              'wax_count_avg':       float,   # avg wax services/week
              'color_count_avg':     float,
              'treatment_count_avg': float,
              'wax_net_avg':         float,
              'color_net_avg':       float,
              'treatment_net_avg':   float,
            }
        """
        logger.debug(
            "HistoricalBaselineValidator: baseline stub for %s → None",
            location_id,
        )
        return None
