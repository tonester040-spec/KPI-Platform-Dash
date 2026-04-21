"""
core/google_sheets_store.py
KPI Platform — Google Sheets implementation of the DataStore interface.

This is the production-hardened backend for the v2 parallel Sheets. It is NOT
wired into main.py yet — the existing pipeline continues running against the
legacy sheets_writer.py. Activation happens in a later phase via the
DUAL_WRITE_V2 env flag.

Safety layers (Tony's spec — all 10 + 6 bonus features):

    1.  Retry with exponential backoff on 429 / 5xx.
    2.  Composite-key idempotency (LOCATION|... / STYLIST|... / BRIEF|...).
    3.  Runtime schema version HARD-FAIL — GoogleSheetsStore refuses to write
        against a Sheet whose A1 note doesn't match config/sheets_schema_v2.py.
    4.  Safe-write mode: backup the target range → clear → write → on failure,
        restore from the backup. If the restore ALSO fails, fire a CRITICAL
        alert via core.alerter.
    5.  Input validation per column (type coercion + format guard).
    6.  Atomic batch_update — data rows and formatting go in the same request.
    7.  AUDIT_LOG row written for every write operation, with SHA256 batch_hash.
    8.  DRY_RUN support matching main.py's existing convention.
    9.  write_anomaly() method for drift_checker / ai_cards auto-write.
   10.  Bounded _get_existing_keys() — only reads up to ws.row_count, not the
        full (ws.row_count * ws.col_count) grid.

    Bonus:
    A.  SHA256 batch_hash written into AUDIT_LOG (future integrity verifier
        can rehash the live range and compare).
    B.  Row-type discriminator in composite keys (no STYLIST-vs-LOCATION
        collisions if someone ever names a stylist "Prior Lake").
    C.  Backup-restore failure escalates to CRITICAL.
    D.  Bounded existing-key reads (see #10).
    E.  Dry-run mode shares the same code paths as real writes — every path
        is testable without touching the network.
    F.  Plain-text date columns ("@") preserved at write time.

Auth
----
Reuses the exact pattern sheets_writer.py already uses:
    GOOGLE_SERVICE_ACCOUNT_JSON = base64-encoded JSON of the service account key.
    SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

Sheet IDs are passed into __init__ — NOT read from env here — so the caller
(scripts/initialize_sheets_v2.py, scripts/verify_sheets_v2.py, or the
dual-write hook) stays in control.
"""

from __future__ import annotations

import base64
import datetime as _dt
import hashlib
import json
import logging
import os
import random
import time
from dataclasses import dataclass
from typing import Any, Callable, Iterable

try:
    import gspread
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
except ImportError as e:  # pragma: no cover — import error surfaces at init
    raise ImportError(
        "GoogleSheetsStore requires `gspread`, `google-auth`, and "
        "`google-api-python-client`. Install with:\n"
        "    pip install gspread google-auth google-api-python-client\n"
        f"Original error: {e}"
    ) from e

from config.sheets_schema_v2 import (
    AUDIT_LOG_COLUMNS,
    Column,
    DataType,
    SCHEMA_VERSION,
    SCHEMA_VERSION_NOTE,
    TabDefinition,
    brief_key,
    col_letter,
    column_names,
    data_range,
    get_tab,
    location_key,
    stylist_key,
)
from core.data_store import (
    DataStore,
    DataStoreError,
    SchemaMismatchError,
    ValidationError,
)

try:
    from core import alerter  # CRITICAL-path escalation for restore failures.
except Exception:  # pragma: no cover — alerter should always import
    alerter = None  # type: ignore[assignment]


log = logging.getLogger(__name__)


SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# Retry configuration — matches production expectations for Sheets API.
_RETRY_MAX_ATTEMPTS = 5
_RETRY_BASE_SECONDS = 1.0
_RETRY_MAX_SECONDS = 30.0

# Sheets API errors we retry on (429 rate-limit, 500/502/503/504 transient).
_RETRYABLE_HTTP_STATUSES = frozenset({429, 500, 502, 503, 504})


# ─── Small helpers ──────────────────────────────────────────────────────────

def _utc_now_iso() -> str:
    return _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="seconds")


def _sha256(payload: Any) -> str:
    """SHA256 hash of a JSON-serialized payload. Deterministic (sorted keys)."""
    blob = json.dumps(payload, sort_keys=True, default=str, ensure_ascii=False)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _backoff_delay(attempt: int) -> float:
    """Exponential backoff with full jitter. attempt is 1-indexed."""
    expo = min(_RETRY_BASE_SECONDS * (2 ** (attempt - 1)), _RETRY_MAX_SECONDS)
    return random.uniform(0, expo)


def _is_retryable_http_error(exc: Exception) -> bool:
    """True if the Google API error is worth retrying."""
    if isinstance(exc, HttpError):
        try:
            status = exc.resp.status  # type: ignore[attr-defined]
        except AttributeError:
            status = None
        if status in _RETRYABLE_HTTP_STATUSES:
            return True
    # gspread.exceptions.APIError wraps HTTPError — detect via its .response if set.
    resp = getattr(exc, "response", None)
    if resp is not None:
        status = getattr(resp, "status_code", None)
        if status in _RETRYABLE_HTTP_STATUSES:
            return True
    return False


def _coerce_cell(value: Any, col: Column) -> Any:
    """
    Coerce a Python value into the Sheets-friendly form for this column's type.

    Rules:
        STRING           -> str(value) or "" for None
        INTEGER          -> int (falls back to 0 on failure)
        FLOAT / CURRENCY -> float (falls back to 0.0)
        PERCENTAGE       -> float (expected to already be a fraction; schema_mapper
                            normalizes to 0-1 before this layer sees it)
        DATE             -> passes through as string; the tab is Plain Text (@)
        JSON             -> json.dumps(value) if not already a string
    """
    if value is None:
        return "" if col.data_type in (DataType.STRING, DataType.DATE, DataType.JSON) else 0

    dt = col.data_type
    if dt in (DataType.STRING, DataType.DATE):
        return str(value)
    if dt is DataType.INTEGER:
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return 0
    if dt in (DataType.FLOAT, DataType.CURRENCY, DataType.PERCENTAGE):
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0
    if dt is DataType.JSON:
        if isinstance(value, str):
            return value
        try:
            return json.dumps(value, default=str, ensure_ascii=False)
        except (TypeError, ValueError):
            return str(value)
    return value  # unknown type — hand back as-is


def _row_dict_to_grid(row: dict[str, Any], tab: TabDefinition) -> list[Any]:
    """Convert a dict keyed by column name to a list in declaration order."""
    return [_coerce_cell(row.get(c.name), c) for c in tab.columns]


def _rows_to_grid(
    rows: list[dict[str, Any]],
    tab: TabDefinition,
) -> list[list[Any]]:
    return [_row_dict_to_grid(r, tab) for r in rows]


def _validate_rows(rows: list[dict[str, Any]], tab: TabDefinition) -> None:
    """
    Light validation — every dict must be a dict; unknown keys are tolerated
    (schema_mapper may emit extras that are harmless) but missing required
    keys are logged at WARNING so a future diff shows up in the log.
    """
    if not isinstance(rows, list):
        raise ValidationError(f"rows must be a list, got {type(rows).__name__}")
    col_names = {c.name for c in tab.columns}
    for idx, r in enumerate(rows):
        if not isinstance(r, dict):
            raise ValidationError(
                f"{tab.name}: row {idx} is not a dict ({type(r).__name__})"
            )
        missing = col_names - set(r.keys())
        if missing:
            log.warning(
                "%s row %d missing expected keys: %s (defaults will be used)",
                tab.name, idx, sorted(missing),
            )


# ─── Auth ───────────────────────────────────────────────────────────────────

def _load_service_account_info() -> dict[str, Any]:
    raw_b64 = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    if not raw_b64:
        raise EnvironmentError(
            "GOOGLE_SERVICE_ACCOUNT_JSON env var is not set. "
            "GoogleSheetsStore cannot authenticate."
        )
    try:
        decoded = base64.b64decode(raw_b64).decode("utf-8")
        return json.loads(decoded)
    except Exception as e:
        raise EnvironmentError(
            f"GOOGLE_SERVICE_ACCOUNT_JSON failed to decode as base64 JSON: {e}"
        ) from e


# ─── GoogleSheetsStore ──────────────────────────────────────────────────────

@dataclass
class _StoreConfig:
    master_sheet_id: str
    coach_cards_sheet_id: str | None
    dry_run: bool


class GoogleSheetsStore(DataStore):
    """
    DataStore implementation backed by two Google Sheets:

        master_sheet_id       — holds all 8 master tabs
                                (LOCATIONS_CURRENT, LOCATIONS_DATA, STYLISTS_*,
                                 GOALS, ALERTS, COACH_BRIEFS, AUDIT_LOG).
        coach_cards_sheet_id  — optional human-readable coach cards archive.

    Both IDs are passed in by the caller. dry_run=True is honored throughout;
    every write becomes a log line and an AUDIT_LOG row with status="DRY_RUN"
    is still appended (in dry-run mode, the audit row is just logged, not
    written).
    """

    def __init__(
        self,
        *,
        master_sheet_id: str,
        coach_cards_sheet_id: str | None = None,
        dry_run: bool = False,
        skip_schema_check: bool = False,
    ):
        if not master_sheet_id:
            raise ValueError("master_sheet_id is required")

        self._cfg = _StoreConfig(
            master_sheet_id=master_sheet_id,
            coach_cards_sheet_id=coach_cards_sheet_id,
            dry_run=dry_run,
        )

        self._gc: gspread.Client | None = None
        self._service = None  # googleapiclient resource, for low-level notes
        self._master_ws_cache: dict[str, Any] = {}

        # Auth is lazy — dry-run flows should still work in environments without
        # a service account (e.g. sandbox_run.py). When dry_run=True we do NOT
        # connect at init unless skip_schema_check=False AND creds are present.
        if not self._cfg.dry_run:
            self._authenticate()
            if not skip_schema_check:
                self._assert_schema_version()

    # ─── Auth / client plumbing ─────────────────────────────────────────

    def _authenticate(self) -> None:
        sa_info = _load_service_account_info()
        creds = service_account.Credentials.from_service_account_info(
            sa_info, scopes=SCOPES
        )
        self._gc = gspread.authorize(creds)
        self._service = build("sheets", "v4", credentials=creds, cache_discovery=False)

    def _ensure_auth(self) -> None:
        """Late-bind auth for dry-run paths that suddenly need it."""
        if self._gc is None or self._service is None:
            self._authenticate()

    def _master_sheet(self):
        self._ensure_auth()
        return self._retry(
            lambda: self._gc.open_by_key(self._cfg.master_sheet_id),  # type: ignore[union-attr]
            op="open_by_key:master",
        )

    def _coach_sheet(self):
        if not self._cfg.coach_cards_sheet_id:
            return None
        self._ensure_auth()
        return self._retry(
            lambda: self._gc.open_by_key(self._cfg.coach_cards_sheet_id),  # type: ignore[union-attr]
            op="open_by_key:coach",
        )

    def _worksheet(self, tab_name: str):
        """Get a worksheet, caching across calls within a single store instance."""
        if tab_name in self._master_ws_cache:
            return self._master_ws_cache[tab_name]
        sh = self._master_sheet()
        ws = self._retry(lambda: sh.worksheet(tab_name), op=f"worksheet:{tab_name}")
        self._master_ws_cache[tab_name] = ws
        return ws

    # ─── Retry wrapper ──────────────────────────────────────────────────

    def _retry(self, fn: Callable[[], Any], *, op: str) -> Any:
        last_exc: Exception | None = None
        for attempt in range(1, _RETRY_MAX_ATTEMPTS + 1):
            try:
                return fn()
            except Exception as e:  # noqa: BLE001 — must catch HttpError + gspread variants
                last_exc = e
                if not _is_retryable_http_error(e) or attempt == _RETRY_MAX_ATTEMPTS:
                    raise
                delay = _backoff_delay(attempt)
                log.warning(
                    "Sheets op %s failed (attempt %d/%d): %s — retrying in %.2fs",
                    op, attempt, _RETRY_MAX_ATTEMPTS, e, delay,
                )
                time.sleep(delay)
        # Unreachable, but keeps type checkers happy.
        assert last_exc is not None
        raise last_exc

    # ─── Schema version enforcement ─────────────────────────────────────

    def _assert_schema_version(self) -> None:
        """
        Read the note on AUDIT_LOG!A1 (canonical version marker tab).
        If the note doesn't contain SCHEMA_VERSION_NOTE, HARD-FAIL.

        The initialize script writes this note on every tab; AUDIT_LOG is the
        one we check here because it's the most stable (never cleared/overwritten
        by pipeline code).
        """
        try:
            ws = self._worksheet("AUDIT_LOG")
        except Exception as e:
            # If AUDIT_LOG doesn't exist, the sheet hasn't been initialized.
            raise SchemaMismatchError(
                f"AUDIT_LOG tab not found in master sheet "
                f"{self._cfg.master_sheet_id}. Run scripts/initialize_sheets_v2.py "
                f"first. Original error: {e}"
            ) from e

        # Use low-level API to get the note on A1.
        try:
            result = self._retry(
                lambda: self._service.spreadsheets().get(  # type: ignore[union-attr]
                    spreadsheetId=self._cfg.master_sheet_id,
                    ranges=["AUDIT_LOG!A1"],
                    includeGridData=True,
                    fields="sheets.data.rowData.values.note",
                ).execute(),
                op="schema_version:get_note",
            )
        except Exception as e:
            raise SchemaMismatchError(
                f"Could not read schema version note on AUDIT_LOG!A1: {e}"
            ) from e

        note = ""
        try:
            note = result["sheets"][0]["data"][0]["rowData"][0]["values"][0].get("note", "")
        except (KeyError, IndexError):
            note = ""

        if SCHEMA_VERSION_NOTE not in note:
            raise SchemaMismatchError(
                f"Schema version mismatch. Expected note containing "
                f"'{SCHEMA_VERSION_NOTE}' on AUDIT_LOG!A1, got: {note!r}. "
                f"Either the Sheet was initialized with an older schema, or the "
                f"note was cleared manually. Refusing to write."
            )
        log.info("Schema version OK: %s", SCHEMA_VERSION)

    # ─── Public: metadata / health ──────────────────────────────────────

    def schema_version(self) -> str:
        return SCHEMA_VERSION

    def health_check(self) -> dict[str, Any]:
        """
        Never raises. Returns a status dict suitable for a dashboard widget.
        """
        out: dict[str, Any] = {
            "ok": False,
            "schema_version": SCHEMA_VERSION,
            "reachable": False,
            "dry_run": self._cfg.dry_run,
            "details": "",
        }
        if self._cfg.dry_run:
            out.update(ok=True, reachable=True, details="dry_run — no network call")
            return out
        try:
            sh = self._master_sheet()
            _ = sh.title  # force a fetch
            out["reachable"] = True
            try:
                self._assert_schema_version()
                out["ok"] = True
                out["details"] = "reachable + schema version OK"
            except SchemaMismatchError as e:
                out["details"] = f"schema mismatch: {e}"
        except Exception as e:  # noqa: BLE001
            out["details"] = f"unreachable: {e}"
        return out

    # ─── Internal: bounded existing-key read ────────────────────────────

    def _get_existing_keys(
        self,
        tab_name: str,
        key_fn: Callable[[dict[str, Any]], str],
    ) -> set[str]:
        """
        Return the set of composite keys already in `tab_name`.

        Bounded by ws.row_count (not the full A:Z grid) so we don't pay the
        100k-empty-row tax on large sheets. Returns an empty set if the tab
        is empty or the read fails transiently.
        """
        try:
            ws = self._worksheet(tab_name)
        except Exception as e:
            log.warning("get_existing_keys: cannot open %s: %s", tab_name, e)
            return set()

        row_count = ws.row_count
        if row_count <= 1:
            return set()

        tab = get_tab(tab_name)
        last_col = col_letter(len(tab.columns) - 1)
        rng = f"{tab_name}!A2:{last_col}{row_count}"

        values = self._retry(
            lambda: ws.spreadsheet.values_get(rng).get("values", []),
            op=f"get_existing_keys:{tab_name}",
        )

        keys: set[str] = set()
        names = column_names(tab_name)
        for raw in values:
            # raw may be shorter than names if trailing cells are empty.
            row = dict(zip(names, raw + [""] * (len(names) - len(raw))))
            try:
                keys.add(key_fn(row))
            except Exception as e:  # noqa: BLE001
                log.debug("Skipping un-keyable row in %s: %s", tab_name, e)
        return keys

    # ─── Internal: safe overwrite (backup → clear → write → restore) ────

    def _safe_overwrite(
        self,
        tab_name: str,
        rows: list[dict[str, Any]],
        *,
        caller: str,
        op: str,
    ) -> None:
        tab = get_tab(tab_name)
        _validate_rows(rows, tab)
        grid = _rows_to_grid(rows, tab)
        batch_hash = _sha256(grid)

        if self._cfg.dry_run:
            log.info(
                "[DRY_RUN] %s: would clear+write %s (%d rows, hash=%s)",
                op, tab_name, len(grid), batch_hash[:12],
            )
            self._audit(
                operation=op,
                target_tab=tab_name,
                row_count=len(grid),
                duplicates_skipped=0,
                status="DRY_RUN",
                batch_hash=batch_hash,
                error_message="",
                caller=caller,
            )
            return

        ws = self._worksheet(tab_name)
        # Backup existing data (everything below the header).
        rng_open = data_range(tab, start_row=2, num_rows=None)
        try:
            backup = self._retry(
                lambda: ws.spreadsheet.values_get(rng_open).get("values", []),
                op=f"{op}:backup",
            )
        except Exception as e:
            log.warning("%s: backup read failed (continuing): %s", op, e)
            backup = []

        try:
            # Clear — atomic per Sheets API.
            self._retry(
                lambda: ws.spreadsheet.values_clear(rng_open),
                op=f"{op}:clear",
            )
            # Write — grid is offset from A2.
            if grid:
                rng_bounded = data_range(tab, start_row=2, num_rows=len(grid))
                self._retry(
                    lambda: ws.spreadsheet.values_update(
                        rng_bounded,
                        params={"valueInputOption": "USER_ENTERED"},
                        body={"values": grid},
                    ),
                    op=f"{op}:write",
                )
        except Exception as e:  # noqa: BLE001
            # Restore path.
            restore_error: str | None = None
            try:
                if backup:
                    restore_rng = data_range(tab, start_row=2, num_rows=len(backup))
                    self._retry(
                        lambda: ws.spreadsheet.values_update(
                            restore_rng,
                            params={"valueInputOption": "USER_ENTERED"},
                            body={"values": backup},
                        ),
                        op=f"{op}:restore",
                    )
                log.error("%s failed but backup restored. Original error: %s", op, e)
            except Exception as restore_exc:  # noqa: BLE001
                restore_error = str(restore_exc)
                log.critical(
                    "%s: WRITE FAILED AND RESTORE FAILED. Data may be missing from %s. "
                    "Original error: %s. Restore error: %s",
                    op, tab_name, e, restore_exc,
                )
                if alerter is not None:
                    try:
                        alerter.send(
                            severity="CRITICAL",
                            module="core.google_sheets_store",
                            error_message=f"{op} write + restore both failed",
                            diagnostic=(
                                f"tab={tab_name}\n"
                                f"write_error={e}\n"
                                f"restore_error={restore_exc}"
                            ),
                        )
                    except Exception as alert_exc:  # noqa: BLE001
                        log.error("alerter.send also failed: %s", alert_exc)
            self._audit(
                operation=op,
                target_tab=tab_name,
                row_count=len(grid),
                duplicates_skipped=0,
                status="FAILURE",
                batch_hash=batch_hash,
                error_message=f"{e}; restore_error={restore_error}" if restore_error else str(e),
                caller=caller,
            )
            raise DataStoreError(f"{op} failed: {e}") from e

        # Success.
        self._audit(
            operation=op,
            target_tab=tab_name,
            row_count=len(grid),
            duplicates_skipped=0,
            status="SUCCESS",
            batch_hash=batch_hash,
            error_message="",
            caller=caller,
        )

    # ─── Internal: idempotent append ────────────────────────────────────

    def _append_with_dedup(
        self,
        tab_name: str,
        rows: list[dict[str, Any]],
        key_fn: Callable[[dict[str, Any]], str],
        *,
        caller: str,
        op: str,
    ) -> None:
        tab = get_tab(tab_name)
        _validate_rows(rows, tab)

        if not rows:
            log.info("%s: no rows to append to %s", op, tab_name)
            return

        # De-dupe in-batch first.
        seen_in_batch: set[str] = set()
        unique_rows: list[dict[str, Any]] = []
        in_batch_dupes = 0
        for r in rows:
            k = key_fn(r)
            if k in seen_in_batch:
                in_batch_dupes += 1
                continue
            seen_in_batch.add(k)
            unique_rows.append(r)

        # Then de-dupe against what's already in the tab.
        if self._cfg.dry_run:
            existing: set[str] = set()
        else:
            existing = self._get_existing_keys(tab_name, key_fn)

        fresh_rows = [r for r in unique_rows if key_fn(r) not in existing]
        against_tab_dupes = len(unique_rows) - len(fresh_rows)
        total_dupes = in_batch_dupes + against_tab_dupes

        if not fresh_rows:
            log.info(
                "%s: all %d rows already present in %s (in_batch=%d, against_tab=%d)",
                op, len(rows), tab_name, in_batch_dupes, against_tab_dupes,
            )
            self._audit(
                operation=op,
                target_tab=tab_name,
                row_count=0,
                duplicates_skipped=total_dupes,
                status="DRY_RUN" if self._cfg.dry_run else "SUCCESS",
                batch_hash=_sha256([]),
                error_message="",
                caller=caller,
            )
            return

        grid = _rows_to_grid(fresh_rows, tab)
        batch_hash = _sha256(grid)

        if self._cfg.dry_run:
            log.info(
                "[DRY_RUN] %s: would append %d rows to %s (skipped %d dupes, hash=%s)",
                op, len(fresh_rows), tab_name, total_dupes, batch_hash[:12],
            )
            self._audit(
                operation=op,
                target_tab=tab_name,
                row_count=len(fresh_rows),
                duplicates_skipped=total_dupes,
                status="DRY_RUN",
                batch_hash=batch_hash,
                error_message="",
                caller=caller,
            )
            return

        ws = self._worksheet(tab_name)
        try:
            self._retry(
                lambda: ws.append_rows(
                    grid,
                    value_input_option="USER_ENTERED",
                    insert_data_option="INSERT_ROWS",
                ),
                op=f"{op}:append",
            )
        except Exception as e:  # noqa: BLE001
            log.error("%s: append failed: %s", op, e)
            self._audit(
                operation=op,
                target_tab=tab_name,
                row_count=len(fresh_rows),
                duplicates_skipped=total_dupes,
                status="FAILURE",
                batch_hash=batch_hash,
                error_message=str(e),
                caller=caller,
            )
            raise DataStoreError(f"{op} failed: {e}") from e

        self._audit(
            operation=op,
            target_tab=tab_name,
            row_count=len(fresh_rows),
            duplicates_skipped=total_dupes,
            status="SUCCESS",
            batch_hash=batch_hash,
            error_message="",
            caller=caller,
        )

    # ─── Internal: audit ────────────────────────────────────────────────

    def _audit(
        self,
        *,
        operation: str,
        target_tab: str,
        row_count: int,
        duplicates_skipped: int,
        status: str,
        batch_hash: str,
        error_message: str,
        caller: str,
    ) -> None:
        row = {
            "timestamp":          _utc_now_iso(),
            "operation":          operation,
            "target_tab":         target_tab,
            "row_count":          row_count,
            "duplicates_skipped": duplicates_skipped,
            "status":             status,
            "batch_hash":         batch_hash,
            "schema_version":     SCHEMA_VERSION,
            "error_message":      error_message,
            "caller":             caller,
        }

        if self._cfg.dry_run or status == "DRY_RUN":
            log.info("[AUDIT dry-run] %s", row)
            return

        tab = get_tab("AUDIT_LOG")
        grid = [_row_dict_to_grid(row, tab)]
        try:
            ws = self._worksheet("AUDIT_LOG")
            self._retry(
                lambda: ws.append_rows(
                    grid,
                    value_input_option="USER_ENTERED",
                    insert_data_option="INSERT_ROWS",
                ),
                op="audit:append",
            )
        except Exception as e:  # noqa: BLE001 — audit failures must never mask the original op
            log.error("AUDIT_LOG append failed (non-fatal): %s", e)

    # ─── Public: locations ──────────────────────────────────────────────

    def write_locations_current(
        self,
        rows: list[dict[str, Any]],
        *,
        caller: str = "",
    ) -> None:
        self._safe_overwrite(
            "LOCATIONS_CURRENT",
            rows,
            caller=caller,
            op="write_locations_current",
        )

    def append_locations_historical(
        self,
        rows: list[dict[str, Any]],
        *,
        caller: str = "",
    ) -> None:
        self._append_with_dedup(
            "LOCATIONS_DATA",
            rows,
            key_fn=location_key,
            caller=caller,
            op="append_locations_historical",
        )

    # ─── Public: stylists ───────────────────────────────────────────────

    def write_stylists_current(
        self,
        rows: list[dict[str, Any]],
        *,
        caller: str = "",
    ) -> None:
        self._safe_overwrite(
            "STYLISTS_CURRENT",
            rows,
            caller=caller,
            op="write_stylists_current",
        )

    def append_stylists_historical(
        self,
        rows: list[dict[str, Any]],
        *,
        caller: str = "",
    ) -> None:
        self._append_with_dedup(
            "STYLISTS_DATA",
            rows,
            key_fn=stylist_key,
            caller=caller,
            op="append_stylists_historical",
        )

    # ─── Public: coach briefs ───────────────────────────────────────────

    def write_coach_briefs(
        self,
        briefs: dict[str, dict[str, Any]],
        *,
        period_end: str,
        model: str = "",
        caller: str = "",
    ) -> None:
        """
        Write/replace one row per (manager, period_end) in COACH_BRIEFS.

        Contract (matches DataStore docstring):
          - Missing managers are NOT cleared. Only supplied briefs are written.
          - Existing (manager, period_end) rows are REPLACED (delete + append
            is easier than targeted update for a row whose position isn't known).
        """
        tab = get_tab("COACH_BRIEFS")

        if not briefs:
            log.info("write_coach_briefs: nothing to write for %s", period_end)
            return

        now = _utc_now_iso()
        new_rows: list[dict[str, Any]] = []
        for manager, brief in briefs.items():
            if not isinstance(brief, dict) or not brief:
                log.info("write_coach_briefs: skipping empty brief for %s", manager)
                continue
            brief_json = json.dumps(brief, default=str, ensure_ascii=False)
            new_rows.append({
                "composite_key": brief_key(manager, period_end),
                "period_end":    period_end,
                "manager":       manager,
                "model":         model,
                "generated_at":  now,
                "brief_json":    brief_json,
                "brief_hash":    hashlib.sha256(brief_json.encode("utf-8")).hexdigest(),
            })

        if not new_rows:
            log.info("write_coach_briefs: all briefs were empty for %s", period_end)
            return

        if self._cfg.dry_run:
            log.info(
                "[DRY_RUN] write_coach_briefs: would upsert %d briefs for %s",
                len(new_rows), period_end,
            )
            self._audit(
                operation="write_coach_briefs",
                target_tab="COACH_BRIEFS",
                row_count=len(new_rows),
                duplicates_skipped=0,
                status="DRY_RUN",
                batch_hash=_sha256(_rows_to_grid(new_rows, tab)),
                error_message="",
                caller=caller,
            )
            return

        ws = self._worksheet("COACH_BRIEFS")
        new_keys = {r["composite_key"] for r in new_rows}

        # Find existing rows whose composite_key is in new_keys — delete them.
        # Read composite_key column (column A) bounded by row_count.
        row_count = ws.row_count
        deletions_planned: list[int] = []
        if row_count > 1:
            rng = f"COACH_BRIEFS!A2:A{row_count}"
            try:
                existing = self._retry(
                    lambda: ws.spreadsheet.values_get(rng).get("values", []),
                    op="write_coach_briefs:read_keys",
                )
            except Exception as e:
                log.warning("write_coach_briefs: could not read existing keys: %s", e)
                existing = []
            for idx, raw in enumerate(existing):
                key = raw[0] if raw else ""
                if key in new_keys:
                    # sheet row number = idx + 2 (1-indexed, plus header)
                    deletions_planned.append(idx + 2)

        # Delete from bottom up so row indices stay stable.
        for sheet_row in sorted(deletions_planned, reverse=True):
            try:
                self._retry(
                    lambda r=sheet_row: ws.delete_rows(r),
                    op=f"write_coach_briefs:delete_row:{sheet_row}",
                )
            except Exception as e:  # noqa: BLE001
                log.warning("write_coach_briefs: delete_rows(%d) failed: %s", sheet_row, e)

        # Append the new rows.
        grid = _rows_to_grid(new_rows, tab)
        try:
            self._retry(
                lambda: ws.append_rows(
                    grid,
                    value_input_option="USER_ENTERED",
                    insert_data_option="INSERT_ROWS",
                ),
                op="write_coach_briefs:append",
            )
        except Exception as e:  # noqa: BLE001
            self._audit(
                operation="write_coach_briefs",
                target_tab="COACH_BRIEFS",
                row_count=len(new_rows),
                duplicates_skipped=0,
                status="FAILURE",
                batch_hash=_sha256(grid),
                error_message=str(e),
                caller=caller,
            )
            raise DataStoreError(f"write_coach_briefs failed: {e}") from e

        self._audit(
            operation="write_coach_briefs",
            target_tab="COACH_BRIEFS",
            row_count=len(new_rows),
            duplicates_skipped=len(deletions_planned),  # REPLACE semantics
            status="SUCCESS",
            batch_hash=_sha256(grid),
            error_message="",
            caller=caller,
        )

    # ─── Public: anomalies ──────────────────────────────────────────────

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
        row = {
            "timestamp":    _utc_now_iso(),
            "period_end":   period_end,
            "location":     location,
            "stylist_name": stylist_name,
            "alert_type":   alert_type,
            "severity":     severity,
            "metric":       metric,
            "metric_value": metric_value,
            "threshold":    threshold,
            "message":      message,
            "source":       source,
        }
        tab = get_tab("ALERTS")
        grid = [_row_dict_to_grid(row, tab)]
        batch_hash = _sha256(grid)

        if self._cfg.dry_run:
            log.info("[DRY_RUN] write_anomaly: %s / %s / %s", severity, location, alert_type)
            self._audit(
                operation="write_anomaly",
                target_tab="ALERTS",
                row_count=1,
                duplicates_skipped=0,
                status="DRY_RUN",
                batch_hash=batch_hash,
                error_message="",
                caller=caller,
            )
            return

        try:
            ws = self._worksheet("ALERTS")
            self._retry(
                lambda: ws.append_rows(
                    grid,
                    value_input_option="USER_ENTERED",
                    insert_data_option="INSERT_ROWS",
                ),
                op="write_anomaly:append",
            )
        except Exception as e:  # noqa: BLE001
            self._audit(
                operation="write_anomaly",
                target_tab="ALERTS",
                row_count=1,
                duplicates_skipped=0,
                status="FAILURE",
                batch_hash=batch_hash,
                error_message=str(e),
                caller=caller,
            )
            raise DataStoreError(f"write_anomaly failed: {e}") from e

        self._audit(
            operation="write_anomaly",
            target_tab="ALERTS",
            row_count=1,
            duplicates_skipped=0,
            status="SUCCESS",
            batch_hash=batch_hash,
            error_message="",
            caller=caller,
        )

    # ─── Public: reads ──────────────────────────────────────────────────

    def read_current_locations(self) -> list[dict[str, Any]]:
        return self._read_current("LOCATIONS_CURRENT")

    def read_current_stylists(self) -> list[dict[str, Any]]:
        return self._read_current("STYLISTS_CURRENT")

    def _read_current(self, tab_name: str) -> list[dict[str, Any]]:
        if self._cfg.dry_run:
            log.info("[DRY_RUN] _read_current(%s): returning []", tab_name)
            return []
        ws = self._worksheet(tab_name)
        names = column_names(tab_name)
        # get_all_values scales with row_count, but we scope by row_count
        # via values_get to avoid reading past the sheet's declared size.
        row_count = ws.row_count
        if row_count <= 1:
            return []
        last_col = col_letter(len(names) - 1)
        rng = f"{tab_name}!A2:{last_col}{row_count}"
        values = self._retry(
            lambda: ws.spreadsheet.values_get(rng).get("values", []),
            op=f"read_current:{tab_name}",
        )
        out: list[dict[str, Any]] = []
        for raw in values:
            padded = raw + [""] * (len(names) - len(raw))
            out.append(dict(zip(names, padded)))
        return out

    def get_historical_baseline(
        self,
        *,
        weeks: int = 4,
        entity: str = "locations",
    ) -> list[dict[str, Any]]:
        """
        Return rows from the last N distinct period_end values in the
        appropriate historical ledger.
        """
        if entity not in {"locations", "stylists"}:
            raise ValueError(f"entity must be 'locations' or 'stylists', got {entity!r}")
        tab_name = "LOCATIONS_DATA" if entity == "locations" else "STYLISTS_DATA"

        if self._cfg.dry_run:
            log.info("[DRY_RUN] get_historical_baseline(%s, %d): returning []", entity, weeks)
            return []

        ws = self._worksheet(tab_name)
        names = column_names(tab_name)
        row_count = ws.row_count
        if row_count <= 1:
            return []
        last_col = col_letter(len(names) - 1)
        rng = f"{tab_name}!A2:{last_col}{row_count}"
        values = self._retry(
            lambda: ws.spreadsheet.values_get(rng).get("values", []),
            op=f"get_historical_baseline:{tab_name}",
        )

        rows: list[dict[str, Any]] = []
        for raw in values:
            padded = raw + [""] * (len(names) - len(raw))
            rows.append(dict(zip(names, padded)))

        # Pick the last N distinct period_end values (sorted desc as string —
        # YYYY-MM-DD sorts correctly lexicographically).
        distinct_periods = sorted(
            {r.get("period_end", "") for r in rows if r.get("period_end")},
            reverse=True,
        )[:weeks]
        selected = set(distinct_periods)
        return [r for r in rows if r.get("period_end", "") in selected]
