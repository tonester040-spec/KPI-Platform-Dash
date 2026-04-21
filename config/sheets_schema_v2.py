"""
config/sheets_schema_v2.py
KPI Platform — Google Sheets schema (v2, parallel build).

This schema defines the NEW parallel Sheets being built alongside the existing
production Sheet (1JY6L7H1Pb2JFmNoz2XNkvG0ogrYgagLVDwH01vuWT28). It is NOT
loaded by main.py or the production pipeline. It is used only by:

    - scripts/initialize_sheets_v2.py   (creates new Sheets)
    - scripts/verify_sheets_v2.py       (health check)
    - core/google_sheets_store.py       (DataStore implementation)
    - core/sheets_writer.py             (dual-write hook, off by default)

DESIGN PRINCIPLES
    1. LOCATIONS and STYLISTS stay as separate tabs (not merged).
    2. Clean column names (period_end, service_net) — schema_mapper.py translates
       from the existing data_processor output.
    3. No Sheet formulas — all calculations happen in Python.
    4. Date columns are forced to Plain Text format to prevent locale drift.
    5. Schema version enforced at runtime via A1 note on every tab.
    6. Composite keys include a row_type discriminator to prevent location/stylist
       collisions (e.g. a stylist named "Prior Lake" can't collide with the
       Prior Lake location total).
    7. ALERTS is a flat anomaly log only. Coach briefs (JESS_BRIEF/JENN_BRIEF)
       live in the dedicated COACH_BRIEFS tab, not in ALERTS!A100-A101.
    8. AUDIT_LOG tracks every write operation with a SHA256 batch hash for
       integrity verification.

DO NOT modify the new Sheets manually in the UI. All changes flow through
this file + GoogleSheetsStore.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


# ─── Schema version ──────────────────────────────────────────────────────────
#
# Bump the MINOR version when adding columns (backwards-compatible).
# Bump the MAJOR version when renaming/removing columns (breaking change).
# GoogleSheetsStore hard-fails at init if the A1 note doesn't match this string.

SCHEMA_VERSION = "2.0.0"

# Marker text written to cell A1 of every tab (as a cell note).
SCHEMA_VERSION_NOTE = f"Schema Version: {SCHEMA_VERSION}"


# ─── Column type metadata ────────────────────────────────────────────────────

class DataType(Enum):
    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    PERCENTAGE = "percentage"   # stored as fraction (0.18), formatted as 18.00%
    CURRENCY = "currency"       # stored as float, formatted as $#,##0.00
    DATE = "date"               # stored as plain-text YYYY-MM-DD to prevent drift
    JSON = "json"               # serialized JSON string


@dataclass(frozen=True)
class Column:
    """One column in a tab."""
    name: str                                # Python-friendly snake_case (e.g. stylist_name)
    data_type: DataType
    width: int = 120                         # pixel width in Sheets UI
    format_string: str | None = None         # Sheets number format pattern, or None
    description: str = ""

    @property
    def is_date(self) -> bool:
        return self.data_type is DataType.DATE


# ─── Tab: LOCATIONS (current + data) ─────────────────────────────────────────
#
# 21 columns. Matches existing data_processor output via schema_mapper.py:
#     loc_name      -> location
#     week_ending   -> period_end   (plain text, no locale drift)
#     platform      -> pos_system
#     guests        -> guest_count
#     total_sales   -> total_sales
#     service       -> service_net
#     product       -> product_net
#     product_pct   -> product_pct
#     ppg           -> ppg_net
#     pph           -> pph_net
#     avg_ticket    -> avg_ticket
#     prod_hours    -> productive_hours
#     wax_count     -> wax_count
#     wax           -> wax_net
#     wax_pct       -> wax_pct
#     color         -> color_net
#     color_pct     -> color_pct
#     treat_count   -> treatment_count
#     treat         -> treatment_net
#     treat_pct     -> treatment_pct
#     flag/coach    -> coach + coach_emoji  (new columns, data_processor already computes them)

LOCATION_COLUMNS: list[Column] = [
    Column("location",         DataType.STRING,    width=150, description="Location display name"),
    Column("period_start",     DataType.DATE,      width=110, format_string="@", description="ISO date YYYY-MM-DD"),
    Column("period_end",       DataType.DATE,      width=110, format_string="@", description="ISO date YYYY-MM-DD (week ending)"),
    Column("pos_system",       DataType.STRING,    width=110, description="zenoti or salon_ultimate"),
    Column("guest_count",      DataType.INTEGER,   width=100, format_string="#,##0"),
    Column("total_sales",      DataType.CURRENCY,  width=120, format_string='"$"#,##0.00'),
    Column("service_net",      DataType.CURRENCY,  width=120, format_string='"$"#,##0.00'),
    Column("product_net",      DataType.CURRENCY,  width=120, format_string='"$"#,##0.00'),
    Column("product_pct",      DataType.PERCENTAGE, width=100, format_string="0.00%"),
    Column("ppg_net",          DataType.CURRENCY,  width=100, format_string='"$"#,##0.00'),
    Column("pph_net",          DataType.CURRENCY,  width=100, format_string='"$"#,##0.00'),
    Column("avg_ticket",       DataType.CURRENCY,  width=100, format_string='"$"#,##0.00'),
    Column("productive_hours", DataType.FLOAT,     width=120, format_string="0.0"),
    Column("wax_count",        DataType.FLOAT,     width=100, format_string="0.0"),
    Column("wax_net",          DataType.CURRENCY,  width=110, format_string='"$"#,##0.00'),
    Column("wax_pct",          DataType.PERCENTAGE, width=100, format_string="0.00%"),
    Column("color_net",        DataType.CURRENCY,  width=110, format_string='"$"#,##0.00'),
    Column("color_pct",        DataType.PERCENTAGE, width=100, format_string="0.00%"),
    Column("treatment_count",  DataType.FLOAT,     width=120, format_string="0.0"),
    Column("treatment_net",    DataType.CURRENCY,  width=120, format_string='"$"#,##0.00'),
    Column("treatment_pct",    DataType.PERCENTAGE, width=100, format_string="0.00%"),
    Column("flag",             DataType.STRING,    width=90,  description="star / watch / solid"),
    Column("coach",            DataType.STRING,    width=100),
    Column("coach_emoji",      DataType.STRING,    width=80),
]


# ─── Tab: STYLISTS (current + data) ──────────────────────────────────────────
#
# Matches existing data_processor stylist output via schema_mapper.py.

STYLIST_COLUMNS: list[Column] = [
    Column("period_end",       DataType.DATE,      width=110, format_string="@"),
    Column("stylist_name",     DataType.STRING,    width=160),
    Column("location",         DataType.STRING,    width=150),
    Column("location_id",      DataType.STRING,    width=100, description="z001 / su001 / etc."),
    Column("status",           DataType.STRING,    width=90,  description="active / inactive / departed"),
    Column("tenure_years",     DataType.FLOAT,     width=100, format_string="0.0"),
    Column("pph",              DataType.CURRENCY,  width=100, format_string='"$"#,##0.00'),
    Column("rebook_pct",       DataType.PERCENTAGE, width=100, format_string="0.00%"),
    Column("product_pct",      DataType.PERCENTAGE, width=100, format_string="0.00%"),
    Column("avg_ticket",       DataType.CURRENCY,  width=100, format_string='"$"#,##0.00'),
    Column("services",         DataType.FLOAT,     width=100, format_string="0.0"),
    Column("color",            DataType.FLOAT,     width=100, format_string="0.0"),
    Column("coach",            DataType.STRING,    width=100),
    Column("coach_emoji",      DataType.STRING,    width=80),
]


# ─── Tab: GOALS ──────────────────────────────────────────────────────────────

GOAL_COLUMNS: list[Column] = [
    Column("location",       DataType.STRING,    width=150),
    Column("metric",         DataType.STRING,    width=150, description="e.g. weekly_revenue, product_pct"),
    Column("target",         DataType.FLOAT,     width=120, format_string="#,##0.00"),
    Column("unit",           DataType.STRING,    width=90,  description="currency / pct / count"),
    Column("period_end",     DataType.DATE,      width=110, format_string="@"),
    Column("current_value",  DataType.FLOAT,     width=120, format_string="#,##0.00"),
    Column("variance_pct",   DataType.PERCENTAGE, width=110, format_string="0.00%"),
    Column("notes",          DataType.STRING,    width=300),
]


# ─── Tab: ALERTS (flat anomaly log only) ─────────────────────────────────────
#
# In v2, ALERTS is PURELY a flat anomaly log written to by:
#     - drift_checker violations (auto-written via GoogleSheetsStore.write_anomaly)
#     - AI card flag detections (star/watch)
#     - Manual ops alerts
#
# Coach briefs (JESS_BRIEF/JENN_BRIEF) are NOT written here anymore — they live
# in the dedicated COACH_BRIEFS tab. This kills the A100/A101 magic-row hack.

ALERT_COLUMNS: list[Column] = [
    Column("timestamp",       DataType.STRING,    width=170, description="UTC ISO-8601"),
    Column("period_end",      DataType.DATE,      width=110, format_string="@"),
    Column("location",        DataType.STRING,    width=150),
    Column("stylist_name",    DataType.STRING,    width=160, description="Empty if location-level alert"),
    Column("alert_type",      DataType.STRING,    width=140, description="DRIFT_WARNING / DRIFT_IMPOSSIBLE / PRODUCT_DROP / PPH_DROP / STAR / WATCH / COACH_BRIEF_FALLBACK"),
    Column("severity",        DataType.STRING,    width=100, description="LOW / MEDIUM / HIGH / CRITICAL"),
    Column("metric",          DataType.STRING,    width=130, description="Metric name if applicable"),
    Column("metric_value",    DataType.STRING,    width=120, description="Stored as string to allow mixed types"),
    Column("threshold",       DataType.STRING,    width=120, description="Breached threshold, if applicable"),
    Column("message",         DataType.STRING,    width=500),
    Column("source",          DataType.STRING,    width=130, description="drift_checker / ai_cards / manual / etc."),
]


# ─── Tab: COACH_BRIEFS ───────────────────────────────────────────────────────
#
# One row per (manager, week_ending). Replaces the A100/A101 magic rows that
# used to live in ALERTS. Natural primary key: composite_key column.

COACH_BRIEF_COLUMNS: list[Column] = [
    Column("composite_key",  DataType.STRING,    width=220, description="BRIEF|{manager}|{period_end}"),
    Column("period_end",     DataType.DATE,      width=110, format_string="@"),
    Column("manager",        DataType.STRING,    width=120, description="jess / jenn"),
    Column("model",          DataType.STRING,    width=180, description="claude-sonnet-4-6 / claude-haiku-4-5-... / dry_run"),
    Column("generated_at",   DataType.STRING,    width=170, description="UTC ISO-8601"),
    Column("brief_json",     DataType.JSON,      width=700, description="Full coach card JSON"),
    Column("brief_hash",     DataType.STRING,    width=140, description="SHA256 of brief_json for integrity"),
]


# ─── Tab: AUDIT_LOG ──────────────────────────────────────────────────────────
#
# Every write operation by GoogleSheetsStore appends a row here.
# batch_hash is SHA256 of the serialized row batch — lets future readers
# verify that what the store THINKS got written matches what is actually sitting
# in the target tab.

AUDIT_LOG_COLUMNS: list[Column] = [
    Column("timestamp",         DataType.STRING,    width=170, description="UTC ISO-8601"),
    Column("operation",         DataType.STRING,    width=170, description="write_locations_current / append_stylists_historical / etc."),
    Column("target_tab",        DataType.STRING,    width=150),
    Column("row_count",         DataType.INTEGER,   width=100, format_string="#,##0"),
    Column("duplicates_skipped", DataType.INTEGER,  width=120, format_string="#,##0"),
    Column("status",            DataType.STRING,    width=110, description="SUCCESS / PARTIAL / FAILURE / DRY_RUN"),
    Column("batch_hash",        DataType.STRING,    width=140, description="SHA256 of row batch"),
    Column("schema_version",    DataType.STRING,    width=100),
    Column("error_message",     DataType.STRING,    width=400),
    Column("caller",            DataType.STRING,    width=180, description="Python module.function that initiated the call"),
]


# ─── Tab definitions ─────────────────────────────────────────────────────────

@dataclass(frozen=True)
class TabDefinition:
    name: str
    columns: tuple[Column, ...]
    freeze_rows: int = 1
    freeze_cols: int = 0
    header_bg_hex: str = "#0F1117"        # Navy (brand)
    header_fg_hex: str = "#FFFFFF"
    description: str = ""

    def __post_init__(self) -> None:
        # Validate uniqueness of column names at import time.
        names = [c.name for c in self.columns]
        if len(set(names)) != len(names):
            dupes = [n for n in names if names.count(n) > 1]
            raise ValueError(f"Duplicate column names in tab {self.name}: {dupes}")


# Master Sheet = 8 tabs
MASTER_SHEET_TABS: tuple[TabDefinition, ...] = (
    TabDefinition(
        name="LOCATIONS_CURRENT",
        columns=tuple(LOCATION_COLUMNS),
        freeze_rows=1,
        freeze_cols=1,
        description="Current week location rows. Cleared + rewritten each pipeline run.",
    ),
    TabDefinition(
        name="LOCATIONS_DATA",
        columns=tuple(LOCATION_COLUMNS),
        freeze_rows=1,
        freeze_cols=1,
        description="Append-only historical ledger for locations. Idempotent by composite key.",
    ),
    TabDefinition(
        name="STYLISTS_CURRENT",
        columns=tuple(STYLIST_COLUMNS),
        freeze_rows=1,
        freeze_cols=2,
        description="Current week stylist rows. Cleared + rewritten each pipeline run.",
    ),
    TabDefinition(
        name="STYLISTS_DATA",
        columns=tuple(STYLIST_COLUMNS),
        freeze_rows=1,
        freeze_cols=2,
        description="Append-only historical ledger for stylists. Idempotent by composite key.",
    ),
    TabDefinition(
        name="GOALS",
        columns=tuple(GOAL_COLUMNS),
        freeze_rows=1,
        description="Per-location goals and current-vs-target variance.",
    ),
    TabDefinition(
        name="ALERTS",
        columns=tuple(ALERT_COLUMNS),
        freeze_rows=1,
        header_bg_hex="#B8860B",  # Amber — this is the place to look when things go sideways
        description="Flat anomaly log. Drift violations + AI flags auto-land here.",
    ),
    TabDefinition(
        name="COACH_BRIEFS",
        columns=tuple(COACH_BRIEF_COLUMNS),
        freeze_rows=1,
        header_bg_hex="#2E7D32",  # Green — coaching growth
        description="JESS_BRIEF / JENN_BRIEF JSON per week. Replaces ALERTS!A100-A101 hack.",
    ),
    TabDefinition(
        name="AUDIT_LOG",
        columns=tuple(AUDIT_LOG_COLUMNS),
        freeze_rows=1,
        header_bg_hex="#7B1E1E",  # Dark red — audit trail
        description="Every write operation logged with SHA256 batch_hash. Append-only.",
    ),
)


# Coach Cards Sheet = 1 tab for long-form weekly cards (legacy-compat with the
# build prompt). The structured JSON lives in COACH_BRIEFS in the master sheet;
# this tab is for human-readable archival if the team wants it.
COACH_CARDS_SHEET_TABS: tuple[TabDefinition, ...] = (
    TabDefinition(
        name="Weekly Cards",
        columns=(
            Column("period_end",    DataType.DATE,     width=110, format_string="@"),
            Column("manager",       DataType.STRING,   width=120),
            Column("location",      DataType.STRING,   width=150),
            Column("card_content",  DataType.STRING,   width=700),
            Column("generated_at",  DataType.STRING,   width=170),
        ),
        freeze_rows=1,
        header_bg_hex="#2E7D32",
        description="Human-readable weekly coach cards (archival). Structured JSON is in COACH_BRIEFS.",
    ),
)


# Convenience registry by tab name
TABS_BY_NAME: dict[str, TabDefinition] = {
    t.name: t for t in MASTER_SHEET_TABS
} | {
    t.name: t for t in COACH_CARDS_SHEET_TABS
}


# ─── Composite key helpers ───────────────────────────────────────────────────
#
# Every row has a stable composite key used for idempotency. The row_type
# discriminator at the front prevents collisions if a stylist is ever named
# after a location (tiny probability, huge blast radius if it ever happens).
#
#   LOCATION rows  -> LOCATION|{location}|{period_end}
#   STYLIST rows   -> STYLIST|{location}|{stylist_name}|{period_end}
#   GOAL rows      -> GOAL|{location}|{metric}|{period_end}
#   BRIEF rows     -> BRIEF|{manager}|{period_end}
#   ALERT rows     -> ALERT|{period_end}|{timestamp}|{location}|{alert_type}
#                     (timestamp makes this unique per event; not used for dedup)

def location_key(row: dict[str, Any]) -> str:
    """Composite key for a LOCATIONS_* row."""
    return f"LOCATION|{row.get('location', '')}|{row.get('period_end', '')}"


def stylist_key(row: dict[str, Any]) -> str:
    """Composite key for a STYLISTS_* row."""
    return "|".join([
        "STYLIST",
        str(row.get("location", "")),
        str(row.get("stylist_name", "")),
        str(row.get("period_end", "")),
    ])


def goal_key(row: dict[str, Any]) -> str:
    return "|".join([
        "GOAL",
        str(row.get("location", "")),
        str(row.get("metric", "")),
        str(row.get("period_end", "")),
    ])


def brief_key(manager: str, period_end: str) -> str:
    return f"BRIEF|{manager}|{period_end}"


# ─── A1 notation helpers ─────────────────────────────────────────────────────

def col_letter(index: int) -> str:
    """0-based column index -> A1 letter (0 -> A, 26 -> AA)."""
    if index < 0:
        raise ValueError(f"col_letter: index must be >= 0, got {index}")
    result = ""
    n = index
    while True:
        result = chr(ord("A") + (n % 26)) + result
        n = n // 26 - 1
        if n < 0:
            break
    return result


def header_range(tab: TabDefinition) -> str:
    last = col_letter(len(tab.columns) - 1)
    return f"{tab.name}!A1:{last}1"


def data_range(tab: TabDefinition, start_row: int = 2, num_rows: int | None = None) -> str:
    """
    A1 notation for a data range.

    If num_rows is None, returns open-ended range starting at start_row
    (e.g. 'LOCATIONS_CURRENT!A2:W') — callers use this for clear operations.

    If num_rows is provided, returns exact bounded range.
    """
    last_col = col_letter(len(tab.columns) - 1)
    if num_rows is None:
        return f"{tab.name}!A{start_row}:{last_col}"
    end_row = start_row + num_rows - 1
    return f"{tab.name}!A{start_row}:{last_col}{end_row}"


def full_bounded_range(tab: TabDefinition, max_rows: int = 1000) -> str:
    """Bounded range including header row. Used for formatting operations."""
    last_col = col_letter(len(tab.columns) - 1)
    return f"{tab.name}!A1:{last_col}{max_rows}"


# ─── Tab lookup ──────────────────────────────────────────────────────────────

def get_tab(name: str) -> TabDefinition:
    """Fetch a tab definition by name. Raises KeyError if not found."""
    if name not in TABS_BY_NAME:
        raise KeyError(
            f"Unknown tab: {name}. Known tabs: {sorted(TABS_BY_NAME.keys())}"
        )
    return TABS_BY_NAME[name]


def column_names(tab_name: str) -> list[str]:
    """List of column names in declaration order for a tab."""
    return [c.name for c in get_tab(tab_name).columns]
