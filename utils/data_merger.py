"""
Data Merger — Combine Excel + PDF data, distribute service categories to stylists.

Architecture:
  Phase 1 (Excel) → stylist-level totals: guest_count, service_net, product_net, ppg_net
  Phase 2 (PDF)   → location-level totals: wax_count, color_net, treatment_count, etc.
  Phase 3 (this)  → distribute PDF location totals proportionally to each stylist

Distribution formula (Karissa-approved):
  stylist_share = stylist.service_net / sum(all_stylists.service_net at location)
  stylist.wax_count       = location_wax_count       × stylist_share
  stylist.color_net       = location_color_net       × stylist_share
  stylist.treatment_count = location_treatment_count × stylist_share

Derived metrics after distribution:
  wax_pct   = (wax_count / guest_count) × 100   — wax guest penetration %
  color_pct = (color_net / service_net) × 100   — color revenue mix %

Matching strategy (Excel result ↔ PDF result):
  Match on (location_name, period_start, period_end).
  If no PDF match exists, stylist row is returned as-is from Excel (phase 1 only).
  Partial period overlaps are NOT attempted — exact match or nothing.

Edge cases:
  - location_service_net == 0 → stylist_share = 0 for all stylists (avoid ZeroDivisionError)
  - guest_count == 0          → wax_pct = 0
  - service_net == 0          → color_pct = 0
  - wax_count values are rounded to 1 decimal (preserve sub-unit precision for aggregation)
  - treatment_count values are rounded to 1 decimal (same reason)
  - color_net is rounded to 2 decimal places (currency)
"""

from typing import Dict, List, Optional, Tuple


class DataMerger:
    """Merge Excel + PDF data and distribute service categories to stylists."""

    # ------------------------------------------------------------------
    # Single-location merge
    # ------------------------------------------------------------------

    @staticmethod
    def merge_location_data(excel_data: Dict, pdf_data: Dict) -> Dict:
        """
        Merge one location's Excel data with its PDF service category data.

        Args:
            excel_data : Output dict from ZenotiExcelParser.parse() or
                         SalonUltimateExcelParser.parse().
                         Must contain 'stylists' list.
            pdf_data   : Output dict from ZenotiPDFParser.parse() or
                         SalonUltimatePDFParser.parse().
                         Must contain 'service_categories' dict.

        Returns:
            A copy of excel_data with each stylist dict augmented by:
              wax_count, wax_pct, color_net, color_pct, treatment_count
            Plus a top-level 'service_categories' key holding the PDF location totals.
        """
        if not excel_data or not pdf_data:
            return excel_data or {}

        stylists    = excel_data.get("stylists", [])
        service_cats = pdf_data.get("service_categories", {})

        if not stylists or not service_cats:
            return excel_data

        # Location-level totals from PDF
        wax_count_total       = float(service_cats.get("wax_count",       0))
        color_net_total       = float(service_cats.get("color_net",       0))
        treatment_count_total = float(service_cats.get("treatment_count", 0))

        # Sum of service_net across all stylists at this location
        location_service_net = sum(float(s.get("service_net", 0)) for s in stylists)

        enhanced = []
        for stylist in stylists:
            service_net = float(stylist.get("service_net", 0))
            guest_count = float(stylist.get("guest_count", 0))

            # Proportional share of this stylist vs. the whole location
            share = (service_net / location_service_net) if location_service_net > 0 else 0.0

            # Distribute PDF location totals
            wax_count       = round(wax_count_total       * share, 1)
            color_net       = round(color_net_total       * share, 2)
            treatment_count = round(treatment_count_total * share, 1)

            # Derived penetration/mix metrics
            wax_pct   = round((wax_count / guest_count * 100),  2) if guest_count  > 0 else 0.0
            color_pct = round((color_net / service_net * 100),  2) if service_net  > 0 else 0.0

            augmented = stylist.copy()
            augmented.update({
                "wax_count":       wax_count,
                "wax_pct":         wax_pct,
                "color_net":       color_net,
                "color_pct":       color_pct,
                "treatment_count": treatment_count,
            })
            enhanced.append(augmented)

        result = excel_data.copy()
        result["stylists"]           = enhanced
        result["service_categories"] = service_cats   # keep PDF location totals for reference

        return result

    # ------------------------------------------------------------------
    # Multi-location batch merge
    # ------------------------------------------------------------------

    @staticmethod
    def merge_multiple_locations(
        excel_results: List[Dict],
        pdf_results:   List[Dict],
    ) -> List[Dict]:
        """
        Merge a batch of Excel + PDF results from multiple locations.

        Matching key: (location_name, period_start, period_end)
        Excel results without a matching PDF are returned unchanged (phase 1 only).

        Args:
            excel_results : List of Excel parser outputs.
            pdf_results   : List of PDF parser outputs.

        Returns:
            List of merged location dicts (same order as excel_results).
        """
        # Build PDF lookup indexed by (location, start, end)
        pdf_lookup: Dict[Tuple, Dict] = {}
        for pdf in pdf_results:
            key = (
                pdf.get("location"),
                (pdf.get("period") or {}).get("start_date"),
                (pdf.get("period") or {}).get("end_date"),
            )
            pdf_lookup[key] = pdf

        merged: List[Dict] = []
        for excel in excel_results:
            key = (
                excel.get("location"),
                (excel.get("period") or {}).get("start_date"),
                (excel.get("period") or {}).get("end_date"),
            )
            matching_pdf = pdf_lookup.get(key)

            if matching_pdf:
                merged.append(DataMerger.merge_location_data(excel, matching_pdf))
            else:
                # No PDF for this location/period — pass through Excel-only data
                merged.append(excel)

        return merged

    # ------------------------------------------------------------------
    # Flatten helper
    # ------------------------------------------------------------------

    @staticmethod
    def flatten_stylists(merged_results: List[Dict]) -> List[Dict]:
        """
        Flatten a list of merged location dicts into a single stylist list.

        Useful for writing all stylists to a single Google Sheets tab.

        Args:
            merged_results : Output from merge_multiple_locations() or a
                             list of single merge_location_data() calls.

        Returns:
            Flat list of all stylist dicts with complete data.
        """
        all_stylists: List[Dict] = []
        for loc in merged_results:
            all_stylists.extend(loc.get("stylists", []))
        return all_stylists

    # ------------------------------------------------------------------
    # Diagnostic helper
    # ------------------------------------------------------------------

    @staticmethod
    def summarize_merge(merged_result: Dict) -> str:
        """
        Return a human-readable summary of a merged location result.
        Useful for logging / test output.
        """
        location  = merged_result.get("location", "?")
        period    = merged_result.get("period", {})
        stylists  = merged_result.get("stylists", [])
        has_pdf   = "service_categories" in merged_result

        lines = [
            f"Location : {location}",
            f"Period   : {period.get('start_date')} → {period.get('end_date')}",
            f"Stylists : {len(stylists)}",
            f"PDF data : {'yes' if has_pdf else 'no — Excel only'}",
        ]

        if has_pdf and stylists:
            sample = stylists[0]
            lines += [
                "",
                f"Sample stylist: {sample.get('stylist_name', '?')}",
                f"  service_net     : ${sample.get('service_net', 0):.2f}",
                f"  wax_count       : {sample.get('wax_count', 0)}",
                f"  wax_pct         : {sample.get('wax_pct', 0):.2f}%",
                f"  color_net       : ${sample.get('color_net', 0):.2f}",
                f"  color_pct       : {sample.get('color_pct', 0):.2f}%",
                f"  treatment_count : {sample.get('treatment_count', 0)}",
            ]

        return "\n".join(lines)
