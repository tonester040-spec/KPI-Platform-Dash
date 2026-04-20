"""
Trust Layer — Completeness Validator
Validates that extracted data is COMPLETE, not just mathematically valid.

The core problem: a missing PDF page still parses "successfully" —
color_count returns 0.0, all formulas calculate correctly, and
Karissa sees "zero color services" when the real answer is "data missing."
"""

from typing import Dict, List

from trust_layer.severity import CompletenessCheck


class CompletenessValidator:
    """
    Validates that a merged location dict has complete, non-trivially-zero data.

    Accepts the merged location dict produced by DataMerger.merge_location_data()
    plus a 'location_totals' key (from the Excel parser or the merge result).
    """

    # Expected service categories (keys in service_categories dict)
    EXPECTED_CATEGORIES = ["wax", "color", "treatment"]

    # Sanity bounds for stylist counts
    MIN_STYLISTS = 3
    MAX_STYLISTS = 30

    # Sanity bounds for service mix (as fractions, not percentages)
    MAX_WAX_PCT        = 0.50   # >50% of guests got wax — very unusual
    MAX_COLOR_PCT      = 0.80   # Color >80% of service revenue — unusual
    MAX_TREATMENT_PCT  = 0.60   # >60% of guests got treatment — unusual

    def validate(self, location_data: Dict) -> List[CompletenessCheck]:
        """
        Run all completeness checks on a merged location dict.

        Args:
            location_data: Merged dict from DataMerger (or Excel-only parse result).
                           Must contain 'stylists' list and optionally 'service_categories'.

        Returns:
            List of CompletenessCheck results.
        """
        checks: List[CompletenessCheck] = []

        checks.extend(self._check_category_presence(location_data))
        checks.extend(self._check_stylist_count(location_data))
        checks.extend(self._check_cross_file_totals(location_data))
        checks.extend(self._check_service_mix(location_data))

        return checks

    # ------------------------------------------------------------------
    # Individual checks
    # ------------------------------------------------------------------

    def _check_category_presence(self, data: Dict) -> List[CompletenessCheck]:
        """
        Verify all expected service categories have non-zero values.

        Zero count = RED FLAG (may indicate missing PDF page, parser failure,
        or data corruption).  We warn rather than hard-fail because a location
        genuinely might have run zero wax services a given week — but it's
        rare enough to warrant review.
        """
        cats = data.get("service_categories")

        if not cats:
            # No PDF data at all — Phase 1 Excel-only row, skip category checks
            return [CompletenessCheck(
                name="Category Presence",
                status="pass",
                message="Excel-only batch — no PDF service categories expected",
                severity=0,
            )]

        missing_fields: List[str] = []
        zero_categories: List[str] = []

        for category in self.EXPECTED_CATEGORIES:
            count_field = f"{category}_count"
            net_field   = f"{category}_net"

            if count_field not in cats or net_field not in cats:
                missing_fields.append(category)
                continue

            if cats[count_field] == 0 and cats[net_field] == 0.0:
                zero_categories.append(category)

        checks: List[CompletenessCheck] = []

        if missing_fields:
            checks.append(CompletenessCheck(
                name="Category Fields Present",
                status="fail",
                message=f"Missing category fields: {', '.join(missing_fields)}",
                severity=50,
            ))

        if zero_categories:
            checks.append(CompletenessCheck(
                name="Category Completeness",
                status="warn",
                message=(
                    f"Categories with zero count: {', '.join(zero_categories)} — "
                    f"verify source data (possible missing PDF page)"
                ),
                severity=10 * len(zero_categories),
            ))
        else:
            checks.append(CompletenessCheck(
                name="Category Completeness",
                status="pass",
                message="All service categories present with non-zero values",
                severity=0,
            ))

        return checks

    def _check_stylist_count(self, data: Dict) -> List[CompletenessCheck]:
        """Verify parsed stylist count is within expected range."""
        count = len(data.get("stylists", []))

        if count < self.MIN_STYLISTS:
            return [CompletenessCheck(
                name="Stylist Count",
                status="warn",
                message=(
                    f"Only {count} stylists parsed — "
                    f"verify all staff are included in source file"
                ),
                severity=15,
            )]

        if count > self.MAX_STYLISTS:
            return [CompletenessCheck(
                name="Stylist Count",
                status="warn",
                message=(
                    f"{count} stylists parsed — "
                    f"unusually high, verify no duplicate rows"
                ),
                severity=10,
            )]

        return [CompletenessCheck(
            name="Stylist Count",
            status="pass",
            message=f"{count} stylists parsed",
            severity=0,
        )]

    def _check_cross_file_totals(self, data: Dict) -> List[CompletenessCheck]:
        """
        Verify PDF service totals reconcile with Excel service total.

        PDF wax+color+treatment is a SUBSET of Excel's total service revenue.
        If PDF subset > Excel total (beyond rounding) → data integrity issue.
        If PDF subset < 30% of Excel total → likely missing categories.
        """
        cats = data.get("service_categories")
        location_totals = data.get("location_totals", {})

        if not cats or not location_totals:
            return []   # Not enough data to reconcile

        excel_service_total = float(location_totals.get("service_net", 0))

        if excel_service_total == 0:
            return []   # Can't reconcile if Excel total is zero

        pdf_subset_total = sum(
            float(cats.get(f"{cat}_net", 0))
            for cat in self.EXPECTED_CATEGORIES
        )

        # PDF > Excel + 10% → something is very wrong
        if pdf_subset_total > excel_service_total * 1.10:
            return [CompletenessCheck(
                name="Total Reconciliation",
                status="fail",
                message=(
                    f"PDF category total (${pdf_subset_total:,.2f}) exceeds "
                    f"Excel service total (${excel_service_total:,.2f}) — "
                    f"data integrity issue"
                ),
                severity=40,
            )]

        # PDF < 30% of Excel → probably missing categories
        coverage = pdf_subset_total / excel_service_total
        if coverage < 0.30:
            return [CompletenessCheck(
                name="Total Reconciliation",
                status="warn",
                message=(
                    f"PDF categories cover only {coverage:.0%} of Excel service total "
                    f"(${pdf_subset_total:,.2f} of ${excel_service_total:,.2f}) — "
                    f"verify all categories were captured"
                ),
                severity=15,
            )]

        return [CompletenessCheck(
            name="Total Reconciliation",
            status="pass",
            message=f"PDF covers {coverage:.0%} of Excel service total",
            severity=0,
        )]

    def _check_service_mix(self, data: Dict) -> List[CompletenessCheck]:
        """
        Verify service mix percentages are within realistic bounds.

        Extreme values may indicate data issues (not impossible, but flag-worthy).
        The spec uses fraction values here (0–1 scale matching percentage/100).
        """
        cats = data.get("service_categories")
        if not cats:
            return []

        flags: List[str] = []

        wax_pct       = float(cats.get("wax_pct",       0)) / 100
        color_pct     = float(cats.get("color_pct",     0)) / 100
        treatment_pct = float(cats.get("treatment_pct", 0)) / 100

        if wax_pct > self.MAX_WAX_PCT:
            flags.append(f"wax {wax_pct:.0%} >50%")
        if color_pct > self.MAX_COLOR_PCT:
            flags.append(f"color {color_pct:.0%} >80%")
        if treatment_pct > self.MAX_TREATMENT_PCT:
            flags.append(f"treatment {treatment_pct:.0%} >60%")

        if flags:
            return [CompletenessCheck(
                name="Service Mix Sanity",
                status="warn",
                message=f"Unusual service mix: {', '.join(flags)} — verify accuracy",
                severity=10,
            )]

        return [CompletenessCheck(
            name="Service Mix Sanity",
            status="pass",
            message="Service mix percentages within normal ranges",
            severity=0,
        )]
