#!/usr/bin/env python3
"""
scripts/backfill_master_table.py
KPI Platform — One-time Stylist_Master table backfill from historical data.

PURPOSE
-------
The Phase 3B Trust Layer tracks stylists with stable IDs across name variations
and locations, stored in a 'Stylist_Master' Google Sheets tab. When first
deployed, that tab is empty — historical data in STYLISTS_DATA has no IDs.

This script backfills the master table by:
  1. Reading all historical rows from the STYLISTS_DATA tab
  2. For each unique (stylist_name, location_id) pair, resolving a stable ID
     via StylistIdentityResolver (fuzzy-match deduplication included)
  3. Writing the resulting master records to the Stylist_Master tab
  4. Logging a summary of new records, matched variants, and collisions

SAFETY
------
- Always run with --dry-run first. Dry run reads STYLISTS_DATA and resolves
  IDs but writes NOTHING to Sheets.
- Idempotent: records already in Stylist_Master are skipped (not duplicated).
- If a stylist appears at multiple locations in history, each location gets its
  own master record with a distinct stylist_id.
- Handles name variations: "Sarah Ross Jr" and "S. Ross" resolve to the same
  canonical record if fuzzy similarity ≥ 85%.

USAGE
-----
    # Dry run first — always
    python scripts/backfill_master_table.py --dry-run

    # Live run (writes to Stylist_Master tab)
    python scripts/backfill_master_table.py

    # Limit to a specific location
    python scripts/backfill_master_table.py --location z006

    # Verbose output (shows every row processed)
    python scripts/backfill_master_table.py --dry-run --verbose

ENVIRONMENT
-----------
Requires GOOGLE_SERVICE_ACCOUNT_JSON (base64-encoded) in environment, OR a
local .env file with the same variable. Copies behavior from main.py.

SHEETS TABS USED
----------------
  Read:  STYLISTS_DATA   — historical stylist rows (source of truth)
  Write: Stylist_Master  — created if missing; appended to if existing
                           Columns: stylist_id | canonical_name |
                                    normalized_name | location_id |
                                    first_seen | name_variants | status
"""

import argparse
import json
import logging
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone

# ── path setup ────────────────────────────────────────────────────────────────
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from trust_layer import StylistIdentityResolver

# ── logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("backfill")

# ── constants ─────────────────────────────────────────────────────────────────
STYLISTS_DATA_TAB  = "STYLISTS_DATA"
MASTER_TAB         = "Stylist_Master"
MASTER_COLUMNS     = [
    "stylist_id", "canonical_name", "normalized_name", "location_id",
    "first_seen", "name_variants", "status",
]


# ─────────────────────────────────────────────────────────────────────────────
# Google Sheets helpers
# ─────────────────────────────────────────────────────────────────────────────

def _get_sheet(tab_name: str):
    """
    Open a specific tab from the configured spreadsheet.

    Returns a gspread Worksheet object, or raises RuntimeError with a clear
    message if credentials / sheet access is missing.
    """
    try:
        import base64
        import gspread
        from google.oauth2.service_account import Credentials as SACredentials

        raw = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")
        if not raw:
            raise RuntimeError(
                "GOOGLE_SERVICE_ACCOUNT_JSON not set in environment. "
                "Export the base64-encoded service account JSON before running."
            )

        decoded  = base64.b64decode(raw).decode("utf-8")
        sa_info  = json.loads(decoded)
        scopes   = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]
        creds    = SACredentials.from_service_account_info(sa_info, scopes=scopes)
        gc       = gspread.authorize(creds)

        sheet_id = os.environ.get(
            "GOOGLE_SHEET_ID", "1JY6L7H1Pb2JFmNoz2XNkvG0ogrYgagLVDwH01vuWT28"
        )
        spreadsheet = gc.open_by_key(sheet_id)
        return spreadsheet.worksheet(tab_name)

    except Exception as exc:
        raise RuntimeError(f"Could not open '{tab_name}' tab: {exc}") from exc


def _load_stylists_data(location_filter: str = None) -> list:
    """
    Load all rows from STYLISTS_DATA as a list of dicts.

    Applies optional location_filter if provided (matches against 'location_id'
    or 'location' column).

    Returns list of row dicts, sorted by 'week_ending' ascending so the earliest
    appearance of a name becomes its canonical 'first_seen' date.
    """
    try:
        ws   = _get_sheet(STYLISTS_DATA_TAB)
        rows = ws.get_all_records()
        logger.info("Loaded %d rows from %s", len(rows), STYLISTS_DATA_TAB)
    except RuntimeError:
        raise
    except Exception as exc:
        raise RuntimeError(f"Failed to read {STYLISTS_DATA_TAB}: {exc}") from exc

    if location_filter:
        rows = [
            r for r in rows
            if (str(r.get("location_id", "")).lower() == location_filter.lower()
                or str(r.get("location", "")).lower() == location_filter.lower())
        ]
        logger.info("After location filter '%s': %d rows", location_filter, len(rows))

    # Sort by week_ending so first appearance = oldest record
    def _sort_key(row):
        return str(row.get("week_ending", ""))

    return sorted(rows, key=_sort_key)


def _load_existing_master(dry_run: bool) -> list:
    """
    Load existing Stylist_Master records so we don't duplicate.

    Returns list of dicts (empty list on dry run or if tab doesn't exist yet).
    """
    if dry_run:
        logger.debug("Dry run — skipping master load")
        return []

    try:
        ws   = _get_sheet(MASTER_TAB)
        rows = ws.get_all_records()
        logger.info("Loaded %d existing master records", len(rows))
        return rows
    except Exception:
        logger.info("Stylist_Master tab not found or empty — treating as new")
        return []


def _ensure_master_tab(dry_run: bool):
    """
    Create the Stylist_Master tab with header row if it doesn't exist.
    No-op in dry run.
    """
    if dry_run:
        return

    try:
        import gspread, base64, json as _json
        from google.oauth2.service_account import Credentials as SACredentials

        raw      = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")
        decoded  = base64.b64decode(raw).decode("utf-8")
        sa_info  = _json.loads(decoded)
        scopes   = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]
        creds       = SACredentials.from_service_account_info(sa_info, scopes=scopes)
        gc          = gspread.authorize(creds)
        sheet_id    = os.environ.get(
            "GOOGLE_SHEET_ID", "1JY6L7H1Pb2JFmNoz2XNkvG0ogrYgagLVDwH01vuWT28"
        )
        spreadsheet = gc.open_by_key(sheet_id)

        try:
            spreadsheet.worksheet(MASTER_TAB)
            logger.debug("Stylist_Master tab already exists")
        except Exception:
            ws = spreadsheet.add_worksheet(title=MASTER_TAB, rows=1000, cols=len(MASTER_COLUMNS))
            ws.append_row(MASTER_COLUMNS)
            logger.info("Created Stylist_Master tab with headers")

    except Exception as exc:
        logger.warning("Could not ensure Stylist_Master tab: %s", exc)


def _write_master_records(records: list, dry_run: bool):
    """
    Append new master records to the Stylist_Master tab.
    In dry run, just logs what would be written.
    """
    if not records:
        logger.info("No new records to write")
        return

    if dry_run:
        logger.info("[DRY RUN] Would write %d new master records:", len(records))
        for r in records[:10]:
            logger.info("  %s  %-30s  %s", r["stylist_id"], r["canonical_name"], r["location_id"])
        if len(records) > 10:
            logger.info("  ... and %d more", len(records) - 10)
        return

    try:
        ws = _get_sheet(MASTER_TAB)
        rows_to_append = [
            [
                r.get("stylist_id", ""),
                r.get("canonical_name", ""),
                r.get("normalized_name", ""),
                r.get("location_id", ""),
                r.get("first_seen", ""),
                json.dumps(r.get("name_variants", [])),
                r.get("status", "active"),
            ]
            for r in records
        ]
        ws.append_rows(rows_to_append, value_input_option="RAW")
        logger.info("Wrote %d master records to Stylist_Master", len(records))
    except Exception as exc:
        raise RuntimeError(f"Failed to write master records: {exc}") from exc


# ─────────────────────────────────────────────────────────────────────────────
# Core backfill logic
# ─────────────────────────────────────────────────────────────────────────────

def backfill_master_table_from_historical_data(
    location_filter: str  = None,
    dry_run:         bool = False,
    verbose:         bool = False,
) -> dict:
    """
    Backfill the Stylist_Master table from STYLISTS_DATA history.

    This is the main entry point. It is safe to run multiple times — existing
    master records are loaded first, and the resolver only creates new records
    for names not already in the master.

    Algorithm
    ---------
    For each (stylist_name, location_id) pair in STYLISTS_DATA (sorted oldest
    first):
      1. Normalize the name
      2. Fuzzy-match against already-resolved names for this location
      3. If match ≥ 85%: record as name variant of the canonical record
      4. If no match: create a new master record with a stable ID
         - first_seen = earliest week_ending where this name appears

    Returns a summary dict:
        {
          'rows_processed':   int,
          'new_records':      int,
          'variants_merged':  int,
          'locations':        List[str],  # distinct locations processed
          'records':          List[dict], # new master records (for inspection)
        }
    """
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    logger.info("=" * 60)
    logger.info("Stylist_Master Backfill%s", "  [DRY RUN]" if dry_run else "")
    logger.info("=" * 60)

    # ── Load data ────────────────────────────────────────────────────────────
    rows = _load_stylists_data(location_filter)
    if not rows:
        logger.warning("No rows found in %s — nothing to backfill", STYLISTS_DATA_TAB)
        return {"rows_processed": 0, "new_records": 0, "variants_merged": 0,
                "locations": [], "records": []}

    existing_master = _load_existing_master(dry_run)
    existing_ids    = {r.get("stylist_id") for r in existing_master}

    # ── Build per-location in-memory master (mirrors resolver's Sheets stub) ─
    # Key: location_id → list of {stylist_id, canonical_name, normalized_name, first_seen, name_variants}
    in_memory_master: dict = defaultdict(list)

    # Seed from existing Sheets master so we don't re-create anything
    for rec in existing_master:
        loc = rec.get("location_id", "")
        in_memory_master[loc].append({
            "stylist_id":      rec.get("stylist_id", ""),
            "canonical_name":  rec.get("canonical_name", ""),
            "normalized_name": rec.get("normalized_name", ""),
            "first_seen":      rec.get("first_seen", ""),
            "name_variants":   json.loads(rec.get("name_variants", "[]")),
        })

    # ── Resolve each row ─────────────────────────────────────────────────────
    resolver        = StylistIdentityResolver()
    new_records:    list = []
    variants_count: int  = 0
    locations_seen: set  = set()

    for row in rows:
        raw_name    = str(row.get("stylist_name", row.get("name", ""))).strip()
        location_id = str(
            row.get("location_id", row.get("location", ""))
        ).lower().strip()
        week_ending = str(row.get("week_ending", ""))

        if not raw_name or not location_id:
            logger.debug("Skipping row — missing name or location: %s", row)
            continue

        locations_seen.add(location_id)
        normalized = resolver.normalize_name(raw_name)

        # ── Check exact match in our in-memory master for this location ──────
        exact_hit = next(
            (r for r in in_memory_master[location_id]
             if r["normalized_name"] == normalized),
            None,
        )
        if exact_hit:
            if raw_name not in exact_hit["name_variants"] and raw_name != exact_hit["canonical_name"]:
                exact_hit["name_variants"].append(raw_name)
                variants_count += 1
                if verbose:
                    logger.debug(
                        "VARIANT  %-30s  →  %s  (%s)",
                        raw_name, exact_hit["canonical_name"], location_id,
                    )
            continue

        # ── Fuzzy match ───────────────────────────────────────────────────────
        best_record, best_score = resolver._fuzzy_match(
            normalized, in_memory_master[location_id]
        )
        if best_record and best_score >= resolver.FUZZY_MATCH_THRESHOLD:
            if raw_name not in best_record["name_variants"] and raw_name != best_record["canonical_name"]:
                best_record["name_variants"].append(raw_name)
                variants_count += 1
                if verbose:
                    logger.debug(
                        "FUZZY    %-30s  →  %s  (%.0f%%, %s)",
                        raw_name, best_record["canonical_name"],
                        best_score * 100, location_id,
                    )
            continue

        # ── New stylist ───────────────────────────────────────────────────────
        # Use the week_ending from the earliest occurrence as first_seen
        first_seen = week_ending or datetime.now(tz=timezone.utc).isoformat()
        stylist_id = resolver._generate_id(location_id, normalized, first_seen=first_seen)

        # Guard: don't re-create an ID that's already in the master sheet
        if stylist_id in existing_ids:
            logger.debug("Skipping %s — id already exists in master", stylist_id)
            continue

        record = {
            "stylist_id":      stylist_id,
            "canonical_name":  raw_name,
            "normalized_name": normalized,
            "location_id":     location_id,
            "first_seen":      first_seen,
            "name_variants":   [],
            "status":          "active",
        }
        in_memory_master[location_id].append(record)
        new_records.append(record)
        existing_ids.add(stylist_id)  # prevent duplicates within this run

        if verbose:
            logger.debug(
                "NEW      %-30s  →  %s  (%s)",
                raw_name, stylist_id, location_id,
            )

    # ── Summary before writing ────────────────────────────────────────────────
    logger.info("")
    logger.info("Backfill results:")
    logger.info("  Rows processed   : %d", len(rows))
    logger.info("  New records      : %d", len(new_records))
    logger.info("  Variants merged  : %d", variants_count)
    logger.info("  Locations        : %s", ", ".join(sorted(locations_seen)))
    logger.info("")

    if new_records:
        _ensure_master_tab(dry_run)
        _write_master_records(new_records, dry_run)
    else:
        logger.info("Nothing new — Stylist_Master is already up to date")

    if dry_run:
        logger.info("[DRY RUN] No changes written.")

    return {
        "rows_processed":  len(rows),
        "new_records":     len(new_records),
        "variants_merged": variants_count,
        "locations":       sorted(locations_seen),
        "records":         new_records,
    }


# ─────────────────────────────────────────────────────────────────────────────
# CLI entry point
# ─────────────────────────────────────────────────────────────────────────────

def _parse_args():
    p = argparse.ArgumentParser(
        description="Backfill Stylist_Master table from STYLISTS_DATA history.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/backfill_master_table.py --dry-run
  python scripts/backfill_master_table.py --dry-run --verbose
  python scripts/backfill_master_table.py --location z006
  python scripts/backfill_master_table.py
        """,
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Read STYLISTS_DATA and resolve IDs but write NOTHING to Sheets.",
    )
    p.add_argument(
        "--location",
        metavar="LOCATION_ID",
        default=None,
        help="Only process rows for this location (e.g. z006, su001).",
    )
    p.add_argument(
        "--verbose", "-v",
        action="store_true",
        default=False,
        help="Show every row processed (not just summary).",
    )
    return p.parse_args()


def main():
    # Load .env if present (local development)
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    args = _parse_args()

    if not args.dry_run:
        print()
        print("⚠️  LIVE RUN — this will write to the Stylist_Master Google Sheet.")
        print("   Always run with --dry-run first to verify the output.")
        print("   Press Ctrl-C within 5 seconds to abort...")
        print()
        import time
        time.sleep(5)

    try:
        result = backfill_master_table_from_historical_data(
            location_filter=args.location,
            dry_run=args.dry_run,
            verbose=args.verbose,
        )

        print()
        print("=" * 60)
        print(f"  Backfill {'(DRY RUN) ' if args.dry_run else ''}complete")
        print(f"  Rows processed : {result['rows_processed']}")
        print(f"  New records    : {result['new_records']}")
        print(f"  Variants merged: {result['variants_merged']}")
        print("=" * 60)
        sys.exit(0)

    except RuntimeError as exc:
        logger.error("Backfill failed: %s", exc)
        sys.exit(1)

    except KeyboardInterrupt:
        print("\nAborted.")
        sys.exit(0)


if __name__ == "__main__":
    main()
