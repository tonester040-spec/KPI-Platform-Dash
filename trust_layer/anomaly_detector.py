"""
Trust Layer — Historical Anomaly Detector
Compares current week's data against historical baseline to flag unusual changes.

Phase 3B note: _get_historical_data is a stub that returns None until the
Sheets read interface is implemented. When no history exists, all checks pass
(first-upload scenario). All detection logic is fully functional once history
is provided.

Severity weighting (Phase 3B enhanced — critical changes block auto-approval):
  ≥ 40% revenue change → 40 points deducted (CRITICAL)
  ≥ 25% revenue change → 25 points deducted (MAJOR)
  ≥ 15% revenue change → 10 points deducted (SIGNIFICANT)
"""

import logging
from typing import Dict, List, Optional

from trust_layer.severity import CompletenessCheck

logger = logging.getLogger(__name__)


class AnomalyDetector:
    """
    Flags unusual week-over-week changes by comparing current data
    against the most recent historical record for the same location.
    """

    # Revenue anomaly thresholds (absolute % change)
    REVENUE_CRITICAL    = 0.40   # ±40% → critical (was 50% in 3A spec, tightened for 3B)
    REVENUE_MAJOR       = 0.25   # ±25% → major
    REVENUE_SIGNIFICANT = 0.15   # ±15% → significant

    # Guest count threshold
    GUEST_COUNT_WARNING = 0.25   # ±25%

    # Service mix threshold (percentage points, not percent change)
    SERVICE_MIX_PP_WARNING = 0.20   # ±20pp change (e.g. 30% → 50%)

    def detect_anomalies(
        self,
        current_data:   Dict,
        historical_data: Optional[Dict] = None,
    ) -> List[CompletenessCheck]:
        """
        Detect anomalies by comparing current vs historical location data.

        Args:
            current_data:   Merged location dict (from DataMerger).
            historical_data: Previous week's aggregated totals for this location.
                            Expected keys: total_sales, guest_count,
                            service_categories (with wax_pct, color_pct, treatment_pct).
                            Pass None if no history available.

        Returns:
            List of CompletenessCheck results.
        """
        if not historical_data:
            return [CompletenessCheck(
                name="Historical Anomaly Detection",
                status="pass",
                message="No historical data available — anomaly detection skipped (first upload)",
                severity=0,
            )]

        checks: List[CompletenessCheck] = []
        checks.extend(self._check_revenue_anomaly(current_data, historical_data))
        checks.extend(self._check_guest_count_anomaly(current_data, historical_data))
        checks.extend(self._check_service_mix_anomaly(current_data, historical_data))
        return checks

    # ------------------------------------------------------------------
    # Individual anomaly checks
    # ------------------------------------------------------------------

    def _check_revenue_anomaly(
        self, current: Dict, historical: Dict
    ) -> List[CompletenessCheck]:
        """Detect unusual revenue changes (Phase 3B enhanced weighting)."""
        loc_totals       = current.get("location_totals", {})
        current_revenue  = float(loc_totals.get("total_sales", 0) or 0)
        hist_revenue     = float(historical.get("total_sales", 0) or 0)

        if hist_revenue == 0:
            return []   # Can't calculate change

        pct_change = (current_revenue - hist_revenue) / hist_revenue
        abs_change = abs(pct_change)

        if abs_change >= self.REVENUE_CRITICAL:
            return [CompletenessCheck(
                name="Revenue Anomaly",
                status="warn",
                message=(
                    f"🚨 CRITICAL REVENUE CHANGE: {pct_change:+.0%} vs last week "
                    f"(${hist_revenue:,.2f} → ${current_revenue:,.2f}) — "
                    f"verify source data; auto-approval blocked"
                ),
                severity=40,   # Phase 3B: heavy weight blocks auto-approval
            )]

        if abs_change >= self.REVENUE_MAJOR:
            return [CompletenessCheck(
                name="Revenue Anomaly",
                status="warn",
                message=(
                    f"MAJOR REVENUE CHANGE: {pct_change:+.0%} vs last week "
                    f"(${hist_revenue:,.2f} → ${current_revenue:,.2f})"
                ),
                severity=25,
            )]

        if abs_change >= self.REVENUE_SIGNIFICANT:
            return [CompletenessCheck(
                name="Revenue Anomaly",
                status="warn",
                message=(
                    f"Revenue change: {pct_change:+.0%} vs last week "
                    f"(${hist_revenue:,.2f} → ${current_revenue:,.2f})"
                ),
                severity=10,
            )]

        return [CompletenessCheck(
            name="Revenue Continuity",
            status="pass",
            message=f"Revenue within {abs_change:.0%} of last week",
            severity=0,
        )]

    def _check_guest_count_anomaly(
        self, current: Dict, historical: Dict
    ) -> List[CompletenessCheck]:
        """Detect unusual guest count changes."""
        loc_totals     = current.get("location_totals", {})
        current_guests = float(
            loc_totals.get("guest_count", 0)
            or sum(float(s.get("guest_count", 0)) for s in current.get("stylists", []))
        )
        hist_guests = float(historical.get("guest_count", 0) or 0)

        if hist_guests == 0:
            return []

        pct_change = (current_guests - hist_guests) / hist_guests
        abs_change = abs(pct_change)

        if abs_change >= self.GUEST_COUNT_WARNING:
            return [CompletenessCheck(
                name="Guest Count Anomaly",
                status="warn",
                message=(
                    f"Guest count change: {pct_change:+.0%} vs last week "
                    f"({hist_guests:.0f} → {current_guests:.0f})"
                ),
                severity=10,
            )]

        return [CompletenessCheck(
            name="Guest Count Continuity",
            status="pass",
            message=f"Guest count within {abs_change:.0%} of last week",
            severity=0,
        )]

    def _check_service_mix_anomaly(
        self, current: Dict, historical: Dict
    ) -> List[CompletenessCheck]:
        """
        Detect unusual service mix changes.

        Uses percentage-point change (not percent-of-percent), so a move
        from 30% → 50% wax is a 20pp change.
        """
        cats      = current.get("service_categories", {})
        hist_cats = historical.get("service_categories", {})

        if not cats or not hist_cats:
            return []

        anomaly_checks: List[CompletenessCheck] = []

        for category in ("wax", "color", "treatment"):
            pct_field  = f"{category}_pct"
            curr_pct   = float(cats.get(pct_field, 0) or 0) / 100   # normalise to 0–1
            hist_pct   = float(hist_cats.get(pct_field, 0) or 0) / 100

            pp_change = abs(curr_pct - hist_pct)   # percentage-point change

            if pp_change >= self.SERVICE_MIX_PP_WARNING:
                anomaly_checks.append(CompletenessCheck(
                    name=f"{category.title()} Mix Anomaly",
                    status="warn",
                    message=(
                        f"{category.title()} % changed by {pp_change:+.0%}pp "
                        f"({hist_pct:.0%} → {curr_pct:.0%})"
                    ),
                    severity=5,
                ))

        if not anomaly_checks:
            return [CompletenessCheck(
                name="Service Mix Continuity",
                status="pass",
                message="Service mix within normal range vs last week",
                severity=0,
            )]

        return anomaly_checks

    # ------------------------------------------------------------------
    # Historical data query stub
    # ------------------------------------------------------------------

    def get_historical_data(self, location_id: str) -> Optional[Dict]:
        """
        Fetch last week's aggregated data for a location.

        Currently a stub — returns None (no history, checks gracefully skip).
        Replace body with GoogleSheetsReader.get_last_week_totals(location_id)
        when the read interface is implemented.

        Expected return dict:
            {
              'total_sales':         float,
              'guest_count':         float,
              'service_categories':  {'wax_pct': float, 'color_pct': float,
                                      'treatment_pct': float},
            }
        """
        logger.debug(
            "AnomalyDetector: Sheets query stub — returning None for "
            "location '%s' (no historical anomaly check performed)",
            location_id,
        )
        return None
