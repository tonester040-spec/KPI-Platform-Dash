"""
core/data_store.py
KPI Platform — Abstract data persistence interface.

Purpose
-------
Decouple the weekly pipeline from any specific storage backend. Today the
production implementation is GoogleSheetsStore (config/sheets_schema_v2.py +
core/google_sheets_store.py). Tomorrow it might be PostgresStore or
BigQueryStore.

When that migration happens, the only thing that changes in the pipeline is
which concrete DataStore subclass is instantiated. Every call site above
that line stays identical.

Design notes
------------
- All write methods accept dicts keyed by the v2 schema column names
  (see config/sheets_schema_v2.py). Callers must map from the existing
  data_processor output using core/schema_mapper.py before calling in.

- `dry_run=True` is surfaced at the store level. When set, the store
  validates + logs every operation but does not perform any writes. This
  matches the DRY_RUN=true convention main.py already uses for the legacy
  sheets_writer.

- Every method has an idempotency contract documented in its docstring.
  Callers should expect duplicate calls on the same (composite key) to be
  no-ops rather than errors.

- Methods should never raise for recoverable failures (e.g. a missing
  audit log tab). Unrecoverable failures (schema mismatch, auth failure,
  invalid input) should raise.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class DataStoreError(Exception):
    """Base class for all DataStore exceptions."""


class SchemaMismatchError(DataStoreError):
    """Raised when the store's schema version doesn't match the code's expectation."""


class ValidationError(DataStoreError):
    """Raised when input rows fail validation before a write."""


class DataStore(ABC):
    """
    Abstract interface for KPI data persistence.

    Subclasses implement this against a concrete backend. The pipeline
    never imports a concrete subclass directly — it holds a DataStore
    reference and lets the orchestrator decide which backend to wire in.
    """

    # ─── Metadata / health ──────────────────────────────────────────────
    @abstractmethod
    def schema_version(self) -> str:
        """Return the schema version the backend is currently running."""

    @abstractmethod
    def health_check(self) -> dict[str, Any]:
        """
        Return a dict describing the backend's current health. At minimum:
            {
                "ok": bool,
                "schema_version": str,
                "reachable": bool,
                "details": str,
            }
        Must never raise — used for status dashboards.
        """

    # ─── Locations ───────────────────────────────────────────────────────
    @abstractmethod
    def write_locations_current(
        self,
        rows: list[dict[str, Any]],
        *,
        caller: str = "",
    ) -> None:
        """
        Overwrite the current-week locations view.

        Clears the current tab and rewrites it with `rows`. MUST be atomic:
        if the write fails, the tab must either end up with the new rows
        or be restored to its prior state. Half-written state is not
        acceptable.

        Args:
            rows: list of dicts keyed by v2 LOCATION_COLUMNS names.
            caller: Python module.function that initiated the call (for audit).

        Idempotency: calling twice with identical rows produces identical
            tab state. No duplicate tracking needed.
        """

    @abstractmethod
    def append_locations_historical(
        self,
        rows: list[dict[str, Any]],
        *,
        caller: str = "",
    ) -> None:
        """
        Append location rows to the historical ledger.

        Idempotency: composite key (LOCATION|{location}|{period_end}) —
        rows with keys already present in the ledger are silently skipped.
        Returning without error on a full-duplicate call is required.
        """

    # ─── Stylists ────────────────────────────────────────────────────────
    @abstractmethod
    def write_stylists_current(
        self,
        rows: list[dict[str, Any]],
        *,
        caller: str = "",
    ) -> None:
        """Overwrite the current-week stylists view. Atomic. See write_locations_current."""

    @abstractmethod
    def append_stylists_historical(
        self,
        rows: list[dict[str, Any]],
        *,
        caller: str = "",
    ) -> None:
        """
        Append stylist rows to the historical ledger.

        Idempotency: composite key
        (STYLIST|{location}|{stylist_name}|{period_end}) — duplicates skipped.
        """

    # ─── Coach briefs ────────────────────────────────────────────────────
    @abstractmethod
    def write_coach_briefs(
        self,
        briefs: dict[str, dict[str, Any]],
        *,
        period_end: str,
        model: str = "",
        caller: str = "",
    ) -> None:
        """
        Write per-manager coach briefs for a week.

        Args:
            briefs: {"jess": {...}, "jenn": {...}} — JSON-serializable dicts.
            period_end: ISO date (YYYY-MM-DD) that all briefs share.
            model: Claude model name that generated the briefs (for audit).
            caller: Python module.function.

        Idempotency: composite key BRIEF|{manager}|{period_end} — if a brief
        for (manager, period_end) exists, it is REPLACED (briefs are
        regenerated each run). Missing managers are NOT cleared; only
        supplied briefs are written.
        """

    # ─── Anomalies (drift checker + AI flags) ────────────────────────────
    @abstractmethod
    def write_anomaly(
        self,
        *,
        period_end: str,
        location: str,
        alert_type: str,
        severity: str,
        message: str,
        stylist_name: str = "",
        metric: str = "",
        metric_value: str = "",
        threshold: str = "",
        source: str = "drift_checker",
        caller: str = "",
    ) -> None:
        """
        Append one anomaly to the flat anomaly log.

        No idempotency — every call is a new log entry (timestamp makes
        each row unique). The severity field drives downstream routing:
        HIGH/CRITICAL anomalies should additionally be forwarded to
        core.alerter by the caller.
        """

    # ─── Reads ───────────────────────────────────────────────────────────
    @abstractmethod
    def read_current_locations(self) -> list[dict[str, Any]]:
        """
        Read the current-week locations view. Returns list of dicts keyed
        by LOCATION_COLUMNS names. Empty list if no data.
        """

    @abstractmethod
    def read_current_stylists(self) -> list[dict[str, Any]]:
        """Read the current-week stylists view."""

    @abstractmethod
    def get_historical_baseline(
        self,
        *,
        weeks: int = 4,
        entity: str = "locations",
    ) -> list[dict[str, Any]]:
        """
        Return rows for the last N weeks from the historical ledger.

        Args:
            weeks: Number of distinct period_end values to pull back.
            entity: "locations" or "stylists".

        Used by drift_checker and AI card builders for trend context.
        """
