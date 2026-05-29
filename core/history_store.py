"""
core/history_store.py
KPI Platform — append-only, idempotent, correction-aware HISTORICAL persistence
for the penny-verified extraction layer (Tracks A/C salon-grain LOCATIONS_DATA +
Tracks B/D stylist-grain STYLISTS_DATA).

WHY THIS EXISTS (the keystone)
------------------------------
The 4-quadrant extraction layer can parse any week to the penny, but nothing yet
*files* those rows into durable history. Every trend / MTD / YoY / retention
feature — and the Monday report generator that shows weekly columns + an
accumulating month-to-date total (Karissa Q7) + YoY columns — reads period
snapshots out of a history store. The Tier-3 YoY schema columns
(prior_year_total_sales, ...) are explicitly "BLOCKED: needs append_to_historical".
This module is that function: the hinge between "we can parse any week" and
"Karissa has a year of history baked in."

WHAT IT DOES — and DELIBERATELY DOES NOT
----------------------------------------
PERSISTENCE + IDEMPOTENCY + CORRECTION-TRACKING. For each incoming period row,
keyed by its composite key (see TABLE_SPECS):
  * new key                       -> APPEND
  * key present, same fingerprint  -> NO-OP        (skipped_duplicate)
  * key present, diff fingerprint  -> SUPERSEDE     (replace the live row, AND
                                      append the prior version to an append-only
                                      audit log — never a silent overwrite, because
                                      per Karissa Q9 a cumulative number CAN be
                                      restated and she needs to know it was).

It does NOT accumulate. Each weekly extract Elaina sends is ALREADY
cumulative-month-to-date (her "Week 3" file already contains Wk1+2+3 totals —
March Forest Lake Total Sales Net Wk1 8,147 -> Wk3 22,358 -> Wk5 33,191). So this
store holds each period row EXACTLY as extracted; MTD is a READ-TIME rollup the
report generator performs by reading the right snapshot — NOT a write-time sum
here. Summing stored weekly rows would double-count. (See the cumulative-MTD model
in CLAUDE.md.)

NON-NEGOTIABLE RULES baked in
-----------------------------
  * NEVER backfill a prior period from a later report. A Week-3 extract has a
    DIFFERENT composite key (period_start/period_end) than Week-1, so it can only
    ever touch Week-3's row — earlier periods are structurally untouchable.
  * Closed/holiday weeks and provisional rows are STORED, never dropped
    (data_complete_flag rides into history verbatim; a later complete extract
    supersedes the provisional one because its fingerprint differs).
  * Salon and stylist histories are INDEPENDENT stores (separate `table`). A
    correction can hit LOCATIONS_DATA without hitting STYLISTS_DATA (Karissa Q9 —
    salon-level corrections don't always reach per-stylist totals). They are never
    reconciled against each other at write time.
  * Append-only ethos: a supersede preserves the prior version in the audit log;
    the live history is never destructively edited.

STORAGE — thin interface, swappable backend
--------------------------------------------
The upsert/supersede LOGIC lives in `append_to_historical` and is
backend-agnostic. It talks to a `HistoryStore` (read_existing / write_rows /
append_audit). The MVP backend is `JsonlHistoryStore` (stdlib-only JSON Lines:
one `<table>.jsonl` current-state file + a sibling append-only
`<table>.audit.jsonl`). A future Postgres/DigitalOcean backend implements the same
three methods and the logic is unchanged. There was no prior extraction-layer
store in the repo (the legacy core/sheets_writer.append_to_historical writes the
OLD Google-Sheets DATA tab keyed by week_ending — a different schema, a different
era); this is the new one.

NOT WIRED. Like Tracks A-D, this lands standalone (built + tested), not yet hooked
into main.py. Wiring — and deciding whether the live store is committed/un-ignored
the way data/processed_attachments.json is — is a later track.
"""
from __future__ import annotations

import hashlib
import json
import os
import tempfile
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Iterable


# ─────────────────────────────────────────────────────────────────────────────
# Table contracts — the composite key + the idempotency-fingerprint source.
#
# Key field names match what the parsers ACTUALLY emit (not an idealized schema):
#   LOCATIONS_DATA  (parsers/locations_grouper._build_row, 39-col):
#       location_id, period_start, period_end, period_type   — all real key cols.
#   STYLISTS_DATA   (the shared 18-key contract — Track B zenoti_stylist_parser
#                    & Track D su_provider_tracker_parser):
#       loc_id, name, period_start, period_end               — its real key cols.
#       `period_type` is carried for parity with LOCATIONS and forward-proofing:
#        the 18-key contract has no such field today, so it resolves to None for
#        every stylist row — harmless, since period_start+period_end already pin
#        the exact window. If a weekly-vs-MTD split is ever added at stylist grain,
#        this key disambiguates it for free.
#
# hash_field — the explicit per-row extract hash used as the fingerprint when
# present. LOCATIONS rows carry `source_extract_hash` (sha256 of the source file)
# by schema. Stylist rows do NOT, so the fingerprint falls back to a deterministic
# content hash of the row (see `_fingerprint`). Both are stable across re-parses of
# the same data.
# ─────────────────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class TableSpec:
    key_fields: tuple[str, ...]
    hash_field: str = "source_extract_hash"


TABLE_SPECS: dict[str, TableSpec] = {
    "LOCATIONS_DATA": TableSpec(
        key_fields=("location_id", "period_start", "period_end", "period_type"),
    ),
    "STYLISTS_DATA": TableSpec(
        key_fields=("loc_id", "name", "period_start", "period_end", "period_type"),
    ),
}

# Excluded from the content-hash fallback: the hash field itself (absent on the
# content path anyway) and any volatile audit stamp that legitimately differs
# between two extracts of identical data — so two parses of the same file are a
# NO-OP, not a spurious supersede.
_VOLATILE_FIELDS = frozenset({"generated_at"})


Key = tuple


def composite_key(row: dict, key_fields: Iterable[str]) -> Key:
    """The idempotency anchor for a row: a tuple of its key-field values (a missing
    field resolves to None). Deterministic and hashable."""
    return tuple(row.get(f) for f in key_fields)


def _fingerprint(row: dict, hash_field: str) -> str:
    """Content fingerprint that decides NO-OP vs SUPERSEDE at an already-present key.

    Prefer the row's explicit extract hash (`hash_field` — e.g. source_extract_hash
    on LOCATIONS rows, the sha256 of the source file). If absent (stylist rows),
    hash the row's content with volatile/audit fields stripped, so identical data
    re-parsed is a NO-OP while any changed primitive supersedes."""
    explicit = row.get(hash_field)
    if explicit:
        return f"hash:{explicit}"
    payload = {k: v for k, v in row.items()
               if k != hash_field and k not in _VOLATILE_FIELDS}
    blob = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)
    return "content:" + hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _audit_entry(table, key_fields, key, old_row, old_fp, new_fp) -> dict:
    """One append-only audit record for a superseded period row. Preserves the
    FULL prior version so a restated number is always recoverable."""
    return {
        "table": table,
        "key": dict(zip(key_fields, key)),
        "superseded_at": _utc_now_iso(),
        "old_fingerprint": old_fp,
        "new_fingerprint": new_fp,
        "superseded_row": old_row,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Result object.
# ─────────────────────────────────────────────────────────────────────────────
@dataclass
class AppendResult:
    """What `append_to_historical` did, in full — so callers and tests can assert
    EXACTLY which periods were appended / left untouched / restated."""
    table: str
    appended: int = 0
    skipped_duplicate: int = 0
    superseded: int = 0
    appended_keys: list[Key] = field(default_factory=list)
    skipped_keys: list[Key] = field(default_factory=list)
    superseded_keys: list[Key] = field(default_factory=list)

    @property
    def total(self) -> int:
        return self.appended + self.skipped_duplicate + self.superseded

    @property
    def store_changed(self) -> bool:
        """True iff the live store was modified (something appended or superseded)."""
        return bool(self.appended or self.superseded)

    def __str__(self) -> str:
        return (f"AppendResult({self.table}: appended={self.appended} "
                f"skipped_duplicate={self.skipped_duplicate} "
                f"superseded={self.superseded})")


# ─────────────────────────────────────────────────────────────────────────────
# Storage interface (thin) + JSON Lines backend (MVP).
# ─────────────────────────────────────────────────────────────────────────────
class HistoryStore(ABC):
    """Backend contract for the history logic. Three operations; a Postgres or
    BigQuery backend implements the same three and the upsert/supersede logic in
    `append_to_historical` is unchanged."""

    @abstractmethod
    def read_existing(self) -> dict[Key, dict]:
        """All current rows as {composite_key: row} (the live current-state view)."""

    @abstractmethod
    def write_rows(self, rows: list[dict]) -> None:
        """Insert-or-replace each row by its composite key (current-state upsert).
        MUST be a no-op with zero side effects when `rows` is empty."""

    @abstractmethod
    def append_audit(self, entries: list[dict]) -> None:
        """Append superseded-version records to the append-only audit log."""


class JsonlHistoryStore(HistoryStore):
    """JSON Lines MVP: one `<table>.jsonl` holding the current snapshot (exactly
    one line per composite key) + a sibling append-only `<table>.audit.jsonl`.

    Rewrites of the live file are atomic (temp file + os.replace) and happen ONLY
    when there is something to write — so an all-duplicate reprocess leaves the file
    byte-for-byte identical."""

    def __init__(self, store_dir: str, table: str):
        spec = TABLE_SPECS.get(table)
        if spec is None:
            raise ValueError(f"unknown table {table!r}; expected one of {sorted(TABLE_SPECS)}")
        self.store_dir = store_dir
        self.table = table
        self.key_fields = spec.key_fields
        self.path = os.path.join(store_dir, f"{table}.jsonl")
        self.audit_path = os.path.join(store_dir, f"{table}.audit.jsonl")

    # -- reads --
    def _read_rows(self) -> list[dict]:
        if not os.path.exists(self.path):
            return []
        rows: list[dict] = []
        with open(self.path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    rows.append(json.loads(line))
        return rows

    def read_existing(self) -> dict[Key, dict]:
        out: dict[Key, dict] = {}
        for r in self._read_rows():
            out[composite_key(r, self.key_fields)] = r  # last line wins on dup key
        return out

    # -- writes --
    def write_rows(self, rows: list[dict]) -> None:
        if not rows:
            return
        current = self._read_rows()
        index = {composite_key(r, self.key_fields): i for i, r in enumerate(current)}
        appends: list[dict] = []
        for row in rows:
            k = composite_key(row, self.key_fields)
            if k in index:
                current[index[k]] = row          # replace in place (supersede)
            else:
                index[k] = len(current) + len(appends)
                appends.append(row)              # new period -> append (order preserved)
        current.extend(appends)
        self._atomic_write(current)

    def append_audit(self, entries: list[dict]) -> None:
        if not entries:
            return
        os.makedirs(self.store_dir, exist_ok=True)
        with open(self.audit_path, "a", encoding="utf-8") as f:
            for e in entries:
                f.write(json.dumps(e, ensure_ascii=False, default=str) + "\n")

    def _atomic_write(self, rows: list[dict]) -> None:
        os.makedirs(self.store_dir, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=self.store_dir, prefix=f".{self.table}.", suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                for row in rows:
                    f.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")
            os.replace(tmp, self.path)           # atomic on Windows + POSIX (same dir)
        except BaseException:
            if os.path.exists(tmp):
                os.remove(tmp)
            raise


# ─────────────────────────────────────────────────────────────────────────────
# The keystone function.
# ─────────────────────────────────────────────────────────────────────────────
DEFAULT_STORE_DIR = os.path.join("data", "history")


def append_to_historical(rows, table, *, store_dir: str = DEFAULT_STORE_DIR,
                         store: "HistoryStore | None" = None) -> AppendResult:
    """File period rows into durable, append-only, correction-aware history.

    rows  : list[dict] — penny-verified period rows straight from the extraction
            layer (LOCATIONS_DATA: parsers/locations_grouper; STYLISTS_DATA: the
            shared 18-key Track B/D contract). Stored EXACTLY as extracted — the
            rows are already cumulative-MTD; MTD is a read-time rollup, NOT summed
            here (summing would double-count).
    table : 'LOCATIONS_DATA' | 'STYLISTS_DATA' — independent histories.
    store_dir : directory for the JSONL backend (ignored when `store` is given).
    store : inject a HistoryStore (tests / a future Postgres backend); defaults to
            a JsonlHistoryStore under store_dir.

    Idempotency / supersede (per row, by composite key — see TABLE_SPECS):
      new key             -> APPEND
      same fingerprint     -> NO-OP (skipped_duplicate)
      different fingerprint -> SUPERSEDE (replace live row; prior version -> audit)

    Returns an AppendResult with appended / skipped_duplicate / superseded counts
    and the exact composite keys in each bucket.
    """
    spec = TABLE_SPECS.get(table)
    if spec is None:
        raise ValueError(f"unknown table {table!r}; expected one of {sorted(TABLE_SPECS)}")
    if store is None:
        store = JsonlHistoryStore(store_dir, table)
    rows = list(rows or [])

    existing = store.read_existing()
    # Working fingerprint + prior-row views, seeded from the store and updated AS WE
    # GO — so a correction that appears later in the SAME batch supersedes the row
    # added earlier in this batch (deterministic; never double-stores a key).
    fp_view: dict[Key, str] = {k: _fingerprint(r, spec.hash_field) for k, r in existing.items()}
    prior_row: dict[Key, dict] = dict(existing)

    changed: dict[Key, dict] = {}   # insertion order == incoming order (ordered appends)
    audit_entries: list[dict] = []
    appended_keys: list[Key] = []
    skipped_keys: list[Key] = []
    superseded_keys: list[Key] = []

    for row in rows:
        key = composite_key(row, spec.key_fields)
        fp = _fingerprint(row, spec.hash_field)
        if key not in fp_view:
            fp_view[key] = fp
            prior_row[key] = row
            changed[key] = row
            appended_keys.append(key)
        elif fp_view[key] == fp:
            skipped_keys.append(key)
        else:
            audit_entries.append(
                _audit_entry(table, spec.key_fields, key, prior_row[key], fp_view[key], fp))
            fp_view[key] = fp
            prior_row[key] = row
            changed[key] = row
            superseded_keys.append(key)

    # Audit BEFORE the live write so a prior version is never lost to a failed write.
    if audit_entries:
        store.append_audit(audit_entries)
    if changed:
        store.write_rows(list(changed.values()))

    return AppendResult(
        table=table,
        appended=len(appended_keys),
        skipped_duplicate=len(skipped_keys),
        superseded=len(superseded_keys),
        appended_keys=appended_keys,
        skipped_keys=skipped_keys,
        superseded_keys=superseded_keys,
    )


def read_history(table, *, store_dir: str = DEFAULT_STORE_DIR,
                 store: "HistoryStore | None" = None) -> list[dict]:
    """Read back the current-state period rows for `table` (what the report
    generator will consume). Insertion order; empty list if nothing is stored."""
    if table not in TABLE_SPECS:
        raise ValueError(f"unknown table {table!r}; expected one of {sorted(TABLE_SPECS)}")
    if store is None:
        store = JsonlHistoryStore(store_dir, table)
    return list(store.read_existing().values())


if __name__ == "__main__":  # pragma: no cover — ad-hoc inspection
    import argparse
    ap = argparse.ArgumentParser(description="Inspect a history store table.")
    ap.add_argument("table", choices=sorted(TABLE_SPECS))
    ap.add_argument("--store-dir", default=DEFAULT_STORE_DIR)
    args = ap.parse_args()
    rows = read_history(args.table, store_dir=args.store_dir)
    print(f"{args.table}: {len(rows)} live row(s) in {args.store_dir}")
    for r in rows:
        print("  ", composite_key(r, TABLE_SPECS[args.table].key_fields))
