"""
Trust Layer — Schema Validator
Validates Excel file column headers match expected schema before parsing.

Detects schema drift — when a POS system changes its export format,
causing columns to shift and parsers to silently read wrong cells.

Hard fail on any mismatch: better to reject the batch than corrupt data.
"""

from dataclasses import dataclass
from typing import Dict, List, Optional

from trust_layer.severity import CompletenessCheck


@dataclass
class SchemaVersion:
    """Schema definition for a specific Excel file format version."""
    version:     str
    columns:     List[str]      # Expected column headers in order
    header_row:  int            # 1-based row number of the header
    description: str


class SchemaValidator:
    """
    Validates that an openpyxl worksheet matches the expected schema.

    Run this BEFORE the parser reads any data — if headers have shifted,
    every downstream calculation will be silently wrong.
    """

    # ------------------------------------------------------------------
    # Known schemas — update version string when POS format changes
    # ------------------------------------------------------------------

    ZENOTI_EXCEL_V1 = SchemaVersion(
        version="1.0",
        columns=[
            "Employee Name",
            "Invoice Count",
            "Avg Invoice Value",
            "Guest Count",
            "Service Sales",
            "Avg Service Value",
            "Product Sales",
            "Avg Product Value",
        ],
        header_row=3,   # CRITICAL: actual files use Row 3 (not Row 4 as Zenoti docs claim)
        description="Zenoti Employee KPI Excel export (2026 format)",
    )

    SU_EXCEL_V1 = SchemaVersion(
        version="1.0",
        columns=[
            "Provider Name",
            "Service Sales",
            "Retail Sales",
            "Hours Worked",
            "Product $/Ticket",
            "Serv $ Hour",
            "Retail Client",
            "Service Clients",
            "Prebk. Guest",
            "Prebk. %",
            "First Time",
            "Avg Ticket",
            "Color Sales",
            "Total Color",
        ],
        header_row=5,
        description="Salon Ultimate Stylist Tracking Report (2026 format)",
    )

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def validate_zenoti_excel(self, worksheet) -> List[CompletenessCheck]:
        """Validate a Zenoti Employee KPI Excel worksheet."""
        return self._validate_schema(worksheet, self.ZENOTI_EXCEL_V1)

    def validate_su_excel(self, worksheet) -> List[CompletenessCheck]:
        """Validate a Salon Ultimate Stylist Tracking worksheet."""
        return self._validate_schema(worksheet, self.SU_EXCEL_V1)

    def validate_by_system(
        self, worksheet, pos_system: str
    ) -> List[CompletenessCheck]:
        """Dispatch to correct schema validator based on POS system string."""
        if pos_system == "zenoti":
            return self.validate_zenoti_excel(worksheet)
        elif pos_system == "salon_ultimate":
            return self.validate_su_excel(worksheet)
        else:
            return [CompletenessCheck(
                name="Schema Validation",
                status="warn",
                message=f"Unknown POS system '{pos_system}' — schema validation skipped",
                severity=5,
            )]

    # ------------------------------------------------------------------
    # Core validation logic
    # ------------------------------------------------------------------

    def _validate_schema(
        self, worksheet, expected: SchemaVersion
    ) -> List[CompletenessCheck]:
        """
        Compare actual worksheet headers against expected schema.

        Raises:
            ValueError: If any column header doesn't match (schema drift).
        Returns:
            List of CompletenessCheck results (single pass entry on success).
        """
        mismatches: List[Dict] = []

        for col_idx, expected_name in enumerate(expected.columns, start=1):
            actual_cell = worksheet.cell(
                row=expected.header_row, column=col_idx
            )
            actual_name = str(actual_cell.value).strip() if actual_cell.value else ""

            # Flexible match: strip whitespace, case-insensitive
            if actual_name.lower() != expected_name.lower():
                mismatches.append({
                    "column":   col_idx,
                    "expected": expected_name,
                    "actual":   actual_name or "(empty)",
                })

        if mismatches:
            error_lines = "\n".join(
                f"  Col {m['column']}: expected '{m['expected']}', "
                f"found '{m['actual']}'"
                for m in mismatches
            )
            raise ValueError(
                f"❌ SCHEMA DRIFT DETECTED\n\n"
                f"Parser version : {expected.version} ({expected.description})\n\n"
                f"Header mismatches (row {expected.header_row}):\n"
                f"{error_lines}\n\n"
                f"Possible causes:\n"
                f"  • POS system updated its export format\n"
                f"  • Wrong file type uploaded\n"
                f"  • File opened and re-saved (column order changed)\n\n"
                f"ACTION REQUIRED:\n"
                f"  1. Verify the correct file was uploaded\n"
                f"  2. If the POS format changed, the parser column constants "
                f"need updating\n"
                f"  3. Contact support if the issue persists\n\n"
                f"Processing stopped to prevent silent data corruption."
            )

        return [CompletenessCheck(
            name="Schema Validation",
            status="pass",
            message=f"Column headers match schema v{expected.version}",
            severity=0,
        )]
