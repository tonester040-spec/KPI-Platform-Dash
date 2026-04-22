"""
parsers/tier2_pdf_batch.py
──────────────────────────
Tier 2 PDF-only batch processor. The second stage in the weekly ingestion
pipeline — consumes the manifest written by `gmail_attachment_watcher.py`
and writes a V1-compatible CURRENT-tab snapshot to Google Sheets.

Where this sits in the flow
───────────────────────────
    Elaina's POS exports → karissaperformanceintelligence@gmail.com
        ↓
    parsers/gmail_attachment_watcher.py   (Step 0)
        ↓ (archives + dedups + writes manifest)
        ↓
    data/inbox/manifest.json
        ↓
┌── parsers/tier2_pdf_batch.py          (Step 0.5 — THIS MODULE)
│       ↓ detect platform per PDF
│       ↓ route to pdf_zenoti_v2 / pdf_salon_ultimate_v2
│       ↓ normalize location name to customer-config canonical
│       ↓ compute Karissa-canonical KPIs (already done by the parser)
│       ↓ transform to V1 CURRENT-tab schema (20 columns)
│       ↓ merge with existing CURRENT tab (preserve locations without fresh PDFs)
│       ↓ write CURRENT tab + update manifest.trust_layer_flags
│       ↓ write run log + email notification
└── ↓
    main.py runs (Step 1+) against the freshly-populated CURRENT tab

Why PDF-only (for now)
──────────────────────
Karissa's team currently enters CURRENT manually. Phase 1 goal: replace that
manual entry with parsed POS exports — highest-leverage automation step. Excel
support in this orchestrator is deliberately out of scope (see the legacy
`tier2_batch_processor.py` for the Excel+PDF merge path, which is used for
stylist-level data in STYLISTS_CURRENT). We'll layer Excel in later once the
PDF path is proven against a few live Mondays.

Trust layer
───────────
Every parsed PDF gets a `trust_layer_flags` array written back into the
manifest. Flags come from two sources:
    - The parser itself (FLAG_NO_GUEST_COUNT, FLAG_TOTAL_SALES_MISMATCH, …)
    - This orchestrator (UNKNOWN_PLATFORM, LOCATION_NOT_IN_CONFIG,
      DUPLICATE_LOCATION_OVERWRITTEN, DETECT_FAILED)

Downstream consumers (main.py, drift_checker, coach cards) read CURRENT and
can choose to ignore/warn on rows whose trust_layer_flags are non-empty.

Never raise — always log
────────────────────────
Every step is wrapped in try/except. One bad PDF can never crash the batch,
and a Sheets API failure can never prevent the run log from being written.
The batch exits with status in {success, partial_success, no_files, error}.

Public API
──────────
    process_manifest(manifest_path, customer_config, dry_run=False) -> dict
        Process a manifest file end-to-end. Returns a result dict with
        status, counts, per-file outcomes, and the run log payload.

    transform_to_current_row(parsed, platform, loc_name) -> dict
        Pure transform: parser output → CURRENT tab row dict. Testable in
        isolation. No I/O.

    main() -> int
        CLI entrypoint. Exit 0 on success/partial, 1 on fatal error.

CLI
───
    # Normal run (reads data/inbox/manifest.json, writes CURRENT tab)
    python -m parsers.tier2_pdf_batch

    # Dry run (no Sheets write, no email, run log still written)
    DRY_RUN=true python -m parsers.tier2_pdf_batch

    # Explicit manifest path
    python -m parsers.tier2_pdf_batch --manifest data/inbox/manifest.json

    # Explicit customer
    ACTIVE_CUSTOMER_ID=karissa_001 python -m parsers.tier2_pdf_batch
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import os
import sys
import traceback
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Make the project root importable when the module is run with `-m parsers.X`
# or directly as `python parsers/tier2_pdf_batch.py`.
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from parsers.pdf_detect import detect_pos_from_file, ZENOTI, SALON_ULTIMATE
from parsers.pdf_zenoti_v2 import parse_file as parse_zenoti_pdf
from parsers.pdf_salon_ultimate_v2 import parse_file as parse_su_pdf

logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-7s  %(name)s  %(message)s",
    )


# ---------------------------------------------------------------------------
# Config / paths
# ---------------------------------------------------------------------------

_DEFAULT_MANIFEST_PATH = _REPO_ROOT / "data" / "inbox" / "manifest.json"
_LOG_DIR = _REPO_ROOT / "data" / "logs"
_CUSTOMER_CONFIG_DIR = _REPO_ROOT / "config" / "customers"

# Orchestrator-added trust-layer flag vocabulary. Parser flags pass through
# unchanged and are documented in the parser modules themselves.
FLAG_UNKNOWN_PLATFORM = "UNKNOWN_PLATFORM"
FLAG_DETECT_FAILED = "DETECT_FAILED"
FLAG_PARSE_FAILED = "PARSE_FAILED"
FLAG_LOCATION_NOT_IN_CONFIG = "LOCATION_NOT_IN_CONFIG"
FLAG_DUPLICATE_LOCATION_OVERWRITTEN = "DUPLICATE_LOCATION_OVERWRITTEN"
FLAG_MISSING_WEEK_ENDING = "MISSING_WEEK_ENDING"
FLAG_NO_LOCATION_RESOLVED = "NO_LOCATION_RESOLVED"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _utc_now_stamp() -> str:
    """Compact UTC timestamp for filenames — matches watcher conventions."""
    return dt.datetime.utcnow().strftime("%Y%m%d_%H%M%S")


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _safe_load_json(path: Path) -> Any:
    """Load JSON and return the parsed payload; return None on any error."""
    try:
        with path.open("r", encoding="utf-8") as fh:
            return json.load(fh)
    except FileNotFoundError:
        logger.warning("JSON file not found: %s", path)
        return None
    except (json.JSONDecodeError, OSError) as exc:
        logger.error("Failed to load JSON %s: %s", path, exc)
        return None


def _safe_write_json_atomic(path: Path, payload: Any) -> bool:
    """
    Atomically write JSON to `path` (temp file + rename). Returns True on
    success. Never raises.
    """
    try:
        _ensure_dir(path.parent)
        tmp = path.with_suffix(path.suffix + ".tmp")
        with tmp.open("w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2, ensure_ascii=False)
        tmp.replace(path)
        return True
    except Exception as exc:
        logger.error("Failed to write JSON %s: %s", path, exc)
        return False


def _load_customer_config(customer_id: str) -> Optional[Dict[str, Any]]:
    """Load `config/customers/{customer_id}.json`."""
    path = _CUSTOMER_CONFIG_DIR / f"{customer_id}.json"
    cfg = _safe_load_json(path)
    if cfg is None:
        logger.error("Could not load customer config at %s", path)
    return cfg


def _build_location_lookup(
    customer_config: Dict[str, Any],
) -> Dict[str, Dict[str, str]]:
    """
    Build a case-insensitive lookup from canonical short name
    (e.g. "Andover") to the customer-config display name and platform.

    Config `locations[]` entries look like:
        {"id": "z001", "name": "Andover FS", "platform": "zenoti"}

    The parser returns the canonical short name ("Andover") — this lookup
    maps it to the customer's display name ("Andover FS") used everywhere
    else in the pipeline (CURRENT tab, dashboards, coach cards).

    Handles: "Andover" → "Andover FS", "Crystal" → "Crystal FS",
             "Elk River" → "Elk River FS"; exact matches pass through
             ("Blaine" → "Blaine", "Apple Valley" → "Apple Valley").
    """
    lookup: Dict[str, Dict[str, str]] = {}
    for loc in customer_config.get("locations", []) or []:
        display = (loc.get("name") or "").strip()
        platform = (loc.get("platform") or "").strip()
        if not display:
            continue
        # Canonical short name: strip trailing " FS" suffix (case-insensitive)
        short = display
        if short.lower().endswith(" fs"):
            short = short[:-3].strip()
        lookup[short.lower()] = {
            "display": display,
            "platform": platform,
            "id": loc.get("id", ""),
        }
        # Also allow exact (already-short) match
        lookup[display.lower()] = {
            "display": display,
            "platform": platform,
            "id": loc.get("id", ""),
        }
    return lookup


# ---------------------------------------------------------------------------
# Transform: parser output → CURRENT tab row
# ---------------------------------------------------------------------------

def transform_to_current_row(
    parsed: Dict[str, Any],
    platform: str,
    loc_display_name: str,
) -> Dict[str, Any]:
    """
    Build a V1 CURRENT-tab row dict from a parser's output.

    CURRENT tab schema (sheets_writer.py write_current, columns A..T):
        A  loc_name            J  pph
        B  week_ending         K  avg_ticket
        C  platform            L  prod_hours
        D  guests              M  wax_count
        E  total_sales         N  wax (sales)
        F  service             O  wax_pct (penetration)
        G  product             P  color (sales)
        H  product_pct         Q  color_pct (revenue share!)
        I  ppg                 R  treat_count
                               S  treat (sales)
                               T  treat_pct (penetration)

    All Karissa-canonical fields come straight from parsed['karissa'] —
    no re-derivation here. Service category sales come from
    parsed['service_categories'].

    Returns a dict with all 20 keys populated (missing values → 0 for
    numerics, "" for strings). Pure function: no I/O, no logging side
    effects beyond warnings.
    """
    k = parsed.get("karissa", {}) or {}
    sc = parsed.get("service_categories", {}) or {}

    week_ending = parsed.get("week_end") or ""  # parsers return ISO date strings

    def _num(x: Any, default: float = 0.0) -> float:
        if x is None:
            return default
        try:
            return float(x)
        except (TypeError, ValueError):
            return default

    def _int(x: Any, default: int = 0) -> int:
        if x is None:
            return default
        try:
            return int(x)
        except (TypeError, ValueError):
            try:
                return int(float(x))
            except (TypeError, ValueError):
                return default

    # Service category sales with guaranteed keys
    wax_sales = 0.0
    # Prefer the pre-summed wax_combined from the Zenoti parser; SU doesn't
    # split the category so `wax` alone is the right field there.
    if "wax_combined" in sc:
        wax_sales = _num(sc.get("wax_combined", {}).get("sales"), 0.0)
    elif "wax" in sc:
        wax_sales = _num(sc.get("wax", {}).get("sales"), 0.0)

    color_sales = _num(sc.get("color", {}).get("sales"), 0.0) if "color" in sc else \
                  _num(k.get("color_sales"), 0.0)
    treatment_sales = _num(sc.get("treatment", {}).get("sales"), 0.0) if "treatment" in sc else \
                      _num(k.get("treatment_sales"), 0.0)

    total_sales = _num(k.get("total_sales"), 0.0)
    product_net = _num(k.get("product_net"), 0.0)
    service_net = _num(k.get("service_net"), 0.0)

    # product_pct = product revenue share of total. This is a NEW field vs
    # Karissa's spec — it's not in the contract but the existing CURRENT
    # schema has a product_pct column, so we compute it here defensively.
    product_pct = round(product_net / total_sales, 4) if total_sales > 0 else 0.0

    return {
        "loc_name":    loc_display_name,
        "week_ending": week_ending,
        "platform":    platform,
        "guests":      _int(k.get("guest_count"), 0),
        "total_sales": round(total_sales, 2),
        "service":     round(service_net, 2),
        "product":     round(product_net, 2),
        "product_pct": product_pct,
        "ppg":         round(_num(k.get("ppg"), 0.0), 2),
        "pph":         round(_num(k.get("pph"), 0.0), 2),
        "avg_ticket":  round(_num(k.get("avg_ticket"), 0.0), 2),
        "prod_hours":  round(_num(k.get("production_hours"), 0.0), 2),
        "wax_count":   _int(k.get("wax_count"), 0),
        "wax":         round(wax_sales, 2),
        "wax_pct":     round(_num(k.get("wax_pct"), 0.0), 4),
        "color":       round(color_sales, 2),
        "color_pct":   round(_num(k.get("color_pct"), 0.0), 4),
        "treat_count": _int(k.get("treatment_count"), 0),
        "treat":       round(treatment_sales, 2),
        "treat_pct":   round(_num(k.get("treatment_pct"), 0.0), 4),
    }


# ---------------------------------------------------------------------------
# Transform: parser employees[] → STYLISTS_CURRENT row dicts
# ---------------------------------------------------------------------------

def transform_to_stylist_rows(
    parsed: Dict[str, Any],
    platform: str,
    loc_display_name: str,
    loc_id: str,
) -> List[Dict[str, Any]]:
    """
    Build STYLISTS_CURRENT row dicts from a parser's `employees[]` output.

    STYLISTS_CURRENT schema (sheets_writer.py write_stylists_current +
    append_to_stylists_historical, columns A..L):

        A  week_ending           G  cur_pph
        B  name                  H  cur_rebook
        C  loc_name              I  cur_product
        D  loc_id                J  cur_ticket
        E  status                K  services (count)
        F  tenure / tenure_yrs   L  color (sales)

    The two writers use slightly different key names for the same fields —
    `tenure` vs `tenure_yrs`, `weeks[-1]` vs the explicit `week_ending`
    argument. We populate BOTH so either function can consume our rows.

    Key mapping:
    ────────────
                               Zenoti                     Salon Ultimate
      cur_pph                 net_service_per_hr          pph
      cur_product (USD)       net_product                 net_retail
      cur_ticket              avg_invoice_value           avg_ticket
      services (count)        service_qty                 guests
                              (Zenoti has no separate
                              guests field per stylist — use service_qty
                              which is the productive service count.)

    Fields the PDFs don't expose per-stylist (yet):
      • cur_rebook (%) → 0.0
      • tenure / tenure_yrs → 0 (will be populated from employee master file
                                 in a future phase)
      • color (per-stylist color sales) → 0.0 (SU Employee Summary and
        Zenoti Employee Sale Details both aggregate color into service total;
        per-stylist color split requires a different report)

    Filtered out:
      • SU phantom "House" row (is_phantom_house=True) — retail-only bucket
      • Rows with empty / whitespace-only name
      • Rows whose entire numeric payload is zero (dead roster entries)

    Pure function: no I/O, no logging side effects beyond warnings.
    """
    employees = parsed.get("employees") or []
    if not employees:
        return []

    week_ending = parsed.get("week_end") or ""

    def _num(x: Any, default: float = 0.0) -> float:
        if x is None:
            return default
        try:
            return float(x)
        except (TypeError, ValueError):
            return default

    def _int(x: Any, default: int = 0) -> int:
        if x is None:
            return default
        try:
            return int(x)
        except (TypeError, ValueError):
            try:
                return int(float(x))
            except (TypeError, ValueError):
                return default

    rows: List[Dict[str, Any]] = []
    for emp in employees:
        name = (emp.get("name") or "").strip()
        if not name:
            continue

        # SU phantom House row — retail sits in the "House _" bucket, not a
        # stylist. Skip cleanly.
        if emp.get("is_phantom_house"):
            continue
        # Defensive: some SU exports slip past the parser's House normalization
        if name.rstrip(" _").lower() == "house":
            continue

        if platform == ZENOTI:
            cur_pph     = _num(emp.get("net_service_per_hr"))
            cur_product = _num(emp.get("net_product"))
            cur_ticket  = _num(emp.get("avg_invoice_value"))
            services    = _int(emp.get("service_qty"))
        else:  # SALON_ULTIMATE
            cur_pph     = _num(emp.get("pph"))
            cur_product = _num(emp.get("net_retail"))
            cur_ticket  = _num(emp.get("avg_ticket"))
            services    = _int(emp.get("guests"))

        # Drop dead roster rows — all four core metrics zero almost always
        # means the stylist didn't work this week (off the schedule).
        if cur_pph == 0 and cur_product == 0 and cur_ticket == 0 and services == 0:
            continue

        rows.append({
            "name":        name,
            "loc_name":    loc_display_name,
            "loc_id":      loc_id,
            "status":      "active",
            # Both writer paths: one reads "tenure", the other "tenure_yrs"
            "tenure":      0,
            "tenure_yrs":  0,
            "cur_pph":     round(cur_pph, 2),
            "cur_rebook":  0.0,   # not extractable from POS weekly exports
            "cur_product": round(cur_product, 2),
            "cur_ticket":  round(cur_ticket, 2),
            # write_stylists_current reads weeks[-1] as the week label;
            # append_to_stylists_historical uses its week_ending parameter
            "weeks":       [week_ending] if week_ending else [],
            # Both writers pick [-1] off these arrays
            "services":    [services],
            "color":       [0.0],  # per-stylist color split not in weekly PDFs
        })

    return rows


# ---------------------------------------------------------------------------
# Per-file processing
# ---------------------------------------------------------------------------

def _process_one_pdf(
    record: Dict[str, Any],
    location_lookup: Dict[str, Dict[str, str]],
) -> Tuple[Optional[Dict[str, Any]], List[Dict[str, Any]], List[str], Optional[str]]:
    """
    Process a single manifest record (one attachment). Returns:
        (row_dict_or_None, stylist_rows, trust_flags, resolved_location_name_or_None)

    `stylist_rows` is always a list — empty if parsing failed or the PDF had
    no employee table. The location row may be None while stylist_rows is
    non-empty in edge cases (shouldn't happen normally), so callers should
    treat them independently.

    Never raises — all failure modes are captured as trust_layer_flags.
    """
    flags: List[str] = []
    stylist_rows: List[Dict[str, Any]] = []
    inbox_path = record.get("inbox_path") or ""
    if not inbox_path:
        flags.append(FLAG_PARSE_FAILED)
        logger.warning("Record has no inbox_path: %r", record.get("filename"))
        return None, stylist_rows, flags, None

    # Normalize Windows-style backslashes to forward slashes. The Gmail
    # attachment watcher is run on Windows by Tony and serializes Windows
    # paths into the manifest (e.g. "data\\inbox\\foo.pdf"). On a Linux
    # runner those backslashes are literal filename chars — we have to
    # translate them before handing to Path().
    inbox_path_norm = inbox_path.replace("\\", "/")

    # Resolve to absolute path under repo root. Also fall back to
    # `data/inbox/<safe_filename>` if the literal path doesn't exist,
    # since the watcher's path field is informational — the safe_filename
    # is the authoritative handle inside the inbox dir.
    abs_path = Path(inbox_path_norm)
    if not abs_path.is_absolute():
        abs_path = _REPO_ROOT / inbox_path_norm
    if not abs_path.exists():
        safe = record.get("safe_filename")
        if safe:
            fallback = _REPO_ROOT / "data" / "inbox" / safe
            if fallback.exists():
                abs_path = fallback
    if not abs_path.exists():
        flags.append(FLAG_PARSE_FAILED)
        logger.error("PDF not found at %s", abs_path)
        return None, stylist_rows, flags, None

    # ----- 1. Detect platform
    platform = detect_pos_from_file(str(abs_path))
    if platform is None:
        flags.append(FLAG_DETECT_FAILED)
        logger.error("pdf_detect returned None for %s", abs_path.name)
        return None, stylist_rows, flags, None
    if platform not in (ZENOTI, SALON_ULTIMATE):
        flags.append(FLAG_UNKNOWN_PLATFORM)
        logger.error("Unknown platform %r for %s", platform, abs_path.name)
        return None, stylist_rows, flags, None

    # ----- 2. Route to parser
    try:
        if platform == ZENOTI:
            parsed = parse_zenoti_pdf(str(abs_path))
        else:
            parsed = parse_su_pdf(str(abs_path))
    except Exception as exc:
        flags.append(FLAG_PARSE_FAILED)
        logger.error(
            "Parser crashed on %s: %s\n%s",
            abs_path.name, exc, traceback.format_exc(),
        )
        return None, stylist_rows, flags, None

    if parsed is None:
        flags.append(FLAG_PARSE_FAILED)
        return None, stylist_rows, flags, None

    # Pass through any flags the parser itself raised
    parser_flags = parsed.get("flags") or []
    flags.extend(parser_flags)

    # ----- 3. Resolve location to customer-config display name
    raw_location = parsed.get("location")
    if not raw_location:
        flags.append(FLAG_NO_LOCATION_RESOLVED)
        logger.error("Parser returned no canonical location for %s", abs_path.name)
        return None, stylist_rows, flags, None

    lookup_hit = location_lookup.get(raw_location.lower())
    if not lookup_hit:
        flags.append(FLAG_LOCATION_NOT_IN_CONFIG)
        logger.error(
            "Parsed location %r not found in customer config locations[]",
            raw_location,
        )
        return None, stylist_rows, flags, None

    display_name = lookup_hit["display"]
    loc_id = lookup_hit.get("id", "")
    # Defense in depth: if the parser's platform disagrees with the config,
    # trust the parser (it inspected PDF content) but record the mismatch.
    cfg_platform = lookup_hit.get("platform")
    if cfg_platform and cfg_platform != platform:
        logger.warning(
            "Platform mismatch for %s: parser=%s config=%s — trusting parser",
            display_name, platform, cfg_platform,
        )
        # No trust flag added — the parser's content-signature detection is
        # ground truth. Config might just be mis-labelled.

    # ----- 4. Transform to CURRENT row
    if not parsed.get("week_end"):
        flags.append(FLAG_MISSING_WEEK_ENDING)

    row = transform_to_current_row(parsed, platform, display_name)

    # ----- 5. Transform employees[] → STYLISTS_CURRENT rows. Failures here
    # never drop the location row — stylists are a layered enrichment.
    try:
        stylist_rows = transform_to_stylist_rows(
            parsed, platform, display_name, loc_id,
        )
    except Exception as exc:
        logger.error(
            "Stylist transform crashed on %s: %s\n%s",
            abs_path.name, exc, traceback.format_exc(),
        )
        stylist_rows = []

    return row, stylist_rows, flags, display_name


# ---------------------------------------------------------------------------
# Sheets integration — read existing, merge, write
# ---------------------------------------------------------------------------

def _read_existing_current(
    service: Any,
    sheet_id: str,
) -> List[Dict[str, Any]]:
    """
    Read the current CURRENT tab so we can MERGE (preserve locations that
    didn't get a fresh PDF this run). On any error, return [] — the merge
    will then be a pure overwrite of whatever rows we do have.
    """
    try:
        from core.data_source import COL, _build_service  # noqa: F401
    except Exception:
        COL = None  # type: ignore

    try:
        resp = service.spreadsheets().values().get(
            spreadsheetId=sheet_id,
            range="CURRENT!A2:T",
        ).execute()
        values = resp.get("values", []) or []
    except Exception as exc:
        logger.warning("Could not read existing CURRENT tab: %s", exc)
        return []

    rows: List[Dict[str, Any]] = []
    for r in values:
        if not r or not r[0].strip():
            continue
        # Pad to 20 cols
        padded = list(r) + [""] * (20 - len(r))

        def _num(i: int, default: float = 0.0) -> float:
            try:
                return float(str(padded[i]).replace(",", "").replace("$", "").strip() or 0)
            except (ValueError, TypeError):
                return default

        def _int(i: int, default: int = 0) -> int:
            try:
                return int(float(str(padded[i]).replace(",", "").strip() or 0))
            except (ValueError, TypeError):
                return default

        rows.append({
            "loc_name":    padded[0].strip(),
            "week_ending": padded[1].strip(),
            "platform":    padded[2].strip(),
            "guests":      _int(3),
            "total_sales": _num(4),
            "service":     _num(5),
            "product":     _num(6),
            "product_pct": _num(7),
            "ppg":         _num(8),
            "pph":         _num(9),
            "avg_ticket":  _num(10),
            "prod_hours":  _num(11),
            "wax_count":   _int(12),
            "wax":         _num(13),
            "wax_pct":     _num(14),
            "color":       _num(15),
            "color_pct":   _num(16),
            "treat_count": _int(17),
            "treat":       _num(18),
            "treat_pct":   _num(19),
        })
    return rows


def _merge_rows(
    existing: List[Dict[str, Any]],
    fresh: List[Dict[str, Any]],
    customer_config: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """
    Merge strategy:
      - Build a dict keyed by loc_name from existing rows
      - Overwrite with fresh rows (keyed by loc_name)
      - Re-order by customer_config.locations[] order so the tab is stable

    Locations configured but absent from both existing and fresh are written
    as empty rows (loc_name only) so the tab always has 12 predictable rows.
    """
    by_name: Dict[str, Dict[str, Any]] = {}
    for r in existing:
        if r.get("loc_name"):
            by_name[r["loc_name"]] = r
    for r in fresh:
        if r.get("loc_name"):
            by_name[r["loc_name"]] = r

    ordered: List[Dict[str, Any]] = []
    for loc in customer_config.get("locations", []) or []:
        display = (loc.get("name") or "").strip()
        if not display:
            continue
        if display in by_name:
            ordered.append(by_name[display])
        else:
            # Stable empty row so the tab layout matches config
            ordered.append({
                "loc_name": display,
                "platform": loc.get("platform", ""),
            })
    return ordered


def _write_current_tab(
    customer_config: Dict[str, Any],
    rows: List[Dict[str, Any]],
    dry_run: bool,
) -> bool:
    """
    Write the merged rows to CURRENT!A2:T. Returns True on success.
    Never raises.
    """
    if dry_run:
        logger.info("DRY RUN: skipping CURRENT write (%d rows prepared)", len(rows))
        return True

    try:
        from core.sheets_writer import _build_service, write_current
    except Exception as exc:
        logger.error("Cannot import core.sheets_writer: %s", exc)
        return False

    try:
        service = _build_service(customer_config)
        write_current(service, customer_config, rows, dry_run=False)
        return True
    except Exception as exc:
        logger.error(
            "Failed to write CURRENT tab: %s\n%s",
            exc, traceback.format_exc(),
        )
        return False


def _write_stylists_tabs(
    customer_config: Dict[str, Any],
    stylists: List[Dict[str, Any]],
    week_ending: str,
    dry_run: bool,
) -> bool:
    """
    Write the parsed stylist rows to BOTH:
      • STYLISTS_CURRENT (overwrite — current-week snapshot)
      • STYLISTS_DATA    (append, idempotent on week_ending)

    Why both:
      • main.py later reads STYLISTS_DATA as its source of truth for stylist
        history, so the APPEND must land before main.py runs.
      • STYLISTS_CURRENT gets overwritten again by main.py with enriched
        stylist dicts, but writing it here ensures the dashboard has fresh
        data even if main.py fails mid-run.

    Returns True if BOTH writes succeed (or are skipped cleanly on dry-run
    / empty stylist list). Returns False if either write raises.
    Never raises.
    """
    if not stylists:
        logger.info("No stylist rows parsed — skipping STYLISTS_* writes")
        return True

    if dry_run:
        logger.info(
            "DRY RUN: skipping STYLISTS_CURRENT + STYLISTS_DATA writes "
            "(%d stylist rows prepared for week_ending=%s)",
            len(stylists), week_ending,
        )
        return True

    try:
        from core.sheets_writer import (
            _build_service,
            write_stylists_current,
            append_to_stylists_historical,
        )
    except Exception as exc:
        logger.error(
            "Cannot import core.sheets_writer (stylists path): %s", exc,
        )
        return False

    ok = True
    try:
        service = _build_service(customer_config)
    except Exception as exc:
        logger.error(
            "Cannot build Sheets service for stylists write: %s\n%s",
            exc, traceback.format_exc(),
        )
        return False

    # 1. Overwrite STYLISTS_CURRENT
    try:
        write_stylists_current(service, customer_config, stylists, dry_run=False)
    except Exception as exc:
        logger.error(
            "Failed to write STYLISTS_CURRENT tab: %s\n%s",
            exc, traceback.format_exc(),
        )
        ok = False

    # 2. Append to STYLISTS_DATA — idempotent on week_ending, so a second
    # tier2 run on the same Monday is a safe no-op here.
    if week_ending:
        try:
            append_to_stylists_historical(
                service, customer_config, stylists, week_ending, dry_run=False,
            )
        except Exception as exc:
            logger.error(
                "Failed to append STYLISTS_DATA: %s\n%s",
                exc, traceback.format_exc(),
            )
            ok = False
    else:
        logger.warning(
            "No week_ending available — skipping STYLISTS_DATA append "
            "(STYLISTS_CURRENT was still written)",
        )

    return ok


# ---------------------------------------------------------------------------
# Manifest update
# ---------------------------------------------------------------------------

def _update_manifest(
    manifest_path: Path,
    per_file_flags: Dict[str, List[str]],
    processed_filenames: List[str],
) -> bool:
    """
    Write trust_layer_flags back into the manifest for each processed file,
    keyed by safe_filename. `per_file_flags` is {safe_filename: [flag, ...]}.

    Also bumps processing_status to 'processed' on success (empty flags)
    or 'processed_with_flags' on non-empty flags. Files not processed
    retain their original status.

    Never raises.
    """
    try:
        records = _safe_load_json(manifest_path)
        if not isinstance(records, list):
            logger.warning("Manifest is not a list — skipping update")
            return False

        for rec in records:
            safe = rec.get("safe_filename")
            if safe in per_file_flags:
                new_flags = per_file_flags[safe]
                rec["trust_layer_flags"] = new_flags
                # Only promote processing_status if Tier 2 actually touched it
                if safe in processed_filenames:
                    rec["processing_status"] = (
                        "processed_with_flags" if new_flags else "processed"
                    )

        return _safe_write_json_atomic(manifest_path, records)
    except Exception as exc:
        logger.error("Manifest update failed: %s", exc)
        return False


# ---------------------------------------------------------------------------
# Run log
# ---------------------------------------------------------------------------

def _write_run_log(payload: Dict[str, Any]) -> Optional[Path]:
    """Write per-run JSON log to data/logs/. Returns the path or None."""
    try:
        _ensure_dir(_LOG_DIR)
        path = _LOG_DIR / f"tier2_pdf_batch_{_utc_now_stamp()}.json"
        with path.open("w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2, ensure_ascii=False)
        logger.info("Wrote run log: %s", path)
        return path
    except Exception as exc:
        logger.error("Failed to write run log: %s", exc)
        return None


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------

def process_manifest(
    manifest_path: Path,
    customer_config: Dict[str, Any],
    dry_run: bool = False,
) -> Dict[str, Any]:
    """
    End-to-end: manifest → parsed rows → merged CURRENT write.

    Returns a result dict:
        {
            "status": "success" | "partial_success" | "no_files" | "error",
            "counts": {...},
            "written_locations": [...],
            "per_file_flags": {safe_filename: [flag, ...]},
            "notes": "human-readable one-liner",
        }
    """
    result: Dict[str, Any] = {
        "status": "error",
        "started_at": dt.datetime.utcnow().isoformat() + "Z",
        "finished_at": None,
        "dry_run": dry_run,
        "manifest_path": str(manifest_path),
        "counts": {
            "records_total": 0,
            "records_ready": 0,
            "records_pdf": 0,
            "parsed_ok": 0,
            "parsed_with_flags": 0,
            "parse_errors": 0,
            "rows_written": 0,
            "stylist_rows_written": 0,
        },
        "written_locations": [],
        "per_file_flags": {},
        "processed_filenames": [],
        "notes": "",
    }

    # ----- Load manifest
    records = _safe_load_json(manifest_path)
    if records is None:
        result["status"] = "error"
        result["notes"] = f"Could not load manifest at {manifest_path}"
        result["finished_at"] = dt.datetime.utcnow().isoformat() + "Z"
        return result

    if not isinstance(records, list):
        result["status"] = "error"
        result["notes"] = "Manifest payload is not a list"
        result["finished_at"] = dt.datetime.utcnow().isoformat() + "Z"
        return result

    result["counts"]["records_total"] = len(records)
    if not records:
        result["status"] = "no_files"
        result["notes"] = "Manifest is empty"
        result["finished_at"] = dt.datetime.utcnow().isoformat() + "Z"
        return result

    # ----- Filter to ready PDFs
    ready_pdfs: List[Dict[str, Any]] = []
    for rec in records:
        status = (rec.get("processing_status") or "").lower()
        fname = (rec.get("filename") or "").lower()
        if status == "ready" and fname.endswith(".pdf"):
            ready_pdfs.append(rec)
    result["counts"]["records_ready"] = sum(
        1 for r in records if (r.get("processing_status") or "").lower() == "ready"
    )
    result["counts"]["records_pdf"] = len(ready_pdfs)

    if not ready_pdfs:
        result["status"] = "no_files"
        result["notes"] = (
            f"No ready PDFs in manifest "
            f"(total={result['counts']['records_total']}, "
            f"ready={result['counts']['records_ready']})"
        )
        result["finished_at"] = dt.datetime.utcnow().isoformat() + "Z"
        return result

    # ----- Build location lookup
    location_lookup = _build_location_lookup(customer_config)
    if not location_lookup:
        result["status"] = "error"
        result["notes"] = "Customer config has no locations[]"
        result["finished_at"] = dt.datetime.utcnow().isoformat() + "Z"
        return result

    # ----- Process each PDF
    fresh_rows: List[Dict[str, Any]] = []
    fresh_stylists: List[Dict[str, Any]] = []
    per_file_flags: Dict[str, List[str]] = {}
    processed_filenames: List[str] = []
    seen_locations: Dict[str, str] = {}  # display_name -> safe_filename

    for rec in ready_pdfs:
        safe = rec.get("safe_filename") or rec.get("filename") or "UNKNOWN"
        processed_filenames.append(safe)

        row, stylist_rows, flags, display_name = _process_one_pdf(
            rec, location_lookup,
        )

        if row is None:
            per_file_flags[safe] = flags
            result["counts"]["parse_errors"] += 1
            continue

        # Duplicate-location guard — if a second PDF lands for the same
        # location, the newer one wins for BOTH the CURRENT row and its
        # stylist rows. Drop the earlier location's stylists too so we
        # don't double-count Rebecca Follansbee across two exports.
        if display_name in seen_locations:
            flags.append(FLAG_DUPLICATE_LOCATION_OVERWRITTEN)
            logger.warning(
                "Duplicate location %r: %s overwrites %s",
                display_name, safe, seen_locations[display_name],
            )
            fresh_rows = [r for r in fresh_rows if r.get("loc_name") != display_name]
            fresh_stylists = [
                s for s in fresh_stylists if s.get("loc_name") != display_name
            ]

        seen_locations[display_name] = safe
        fresh_rows.append(row)
        fresh_stylists.extend(stylist_rows)
        per_file_flags[safe] = flags
        if flags:
            result["counts"]["parsed_with_flags"] += 1
        else:
            result["counts"]["parsed_ok"] += 1

    # ----- Merge with existing CURRENT + write
    written_ok = False
    if fresh_rows:
        existing_rows: List[Dict[str, Any]] = []
        if not dry_run:
            try:
                from core.sheets_writer import _build_service as _ss_service
                service = _ss_service(customer_config)
                existing_rows = _read_existing_current(
                    service, customer_config["sheet_id"]
                )
            except Exception as exc:
                logger.warning(
                    "Skipping merge read (treating as overwrite): %s", exc,
                )
                existing_rows = []

        merged = _merge_rows(existing_rows, fresh_rows, customer_config)
        written_ok = _write_current_tab(customer_config, merged, dry_run=dry_run)
        result["counts"]["rows_written"] = len(merged) if written_ok else 0

    # ----- Write stylists — independent of CURRENT write success. Even if
    # CURRENT failed for some reason, stylist data landing in STYLISTS_DATA
    # is still useful for the next main.py run.
    stylists_written_ok = True
    if fresh_stylists:
        # Every fresh row this run shares the same week_ending (weekly POS
        # exports all target the Sunday prior). Pick the first non-empty
        # one from our freshly-parsed location rows.
        week_ending = ""
        for r in fresh_rows:
            if r.get("week_ending"):
                week_ending = r["week_ending"]
                break
        stylists_written_ok = _write_stylists_tabs(
            customer_config, fresh_stylists, week_ending, dry_run=dry_run,
        )
        result["counts"]["stylist_rows_written"] = (
            len(fresh_stylists) if stylists_written_ok else 0
        )

    result["written_locations"] = sorted({r["loc_name"] for r in fresh_rows})
    result["per_file_flags"] = per_file_flags
    result["processed_filenames"] = processed_filenames

    # ----- Update manifest
    _update_manifest(manifest_path, per_file_flags, processed_filenames)

    # ----- Determine status
    # Stylist write is a nice-to-have — a stylist-only failure degrades to
    # partial_success but does NOT flip an otherwise-clean run to error.
    if (
        result["counts"]["parse_errors"] == 0
        and fresh_rows and written_ok and stylists_written_ok
    ):
        result["status"] = "success"
    elif fresh_rows and (written_ok or dry_run):
        result["status"] = "partial_success"
    elif not fresh_rows:
        result["status"] = "no_files"
    else:
        result["status"] = "error"

    # Human-readable notes
    c = result["counts"]
    result["notes"] = (
        f"pdfs={c['records_pdf']} ok={c['parsed_ok']} "
        f"with_flags={c['parsed_with_flags']} errors={c['parse_errors']} "
        f"rows_written={c['rows_written']} "
        f"stylist_rows={c['stylist_rows_written']}"
    )
    result["finished_at"] = dt.datetime.utcnow().isoformat() + "Z"
    return result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Tier 2 PDF batch processor (writes V1 CURRENT tab)",
    )
    parser.add_argument(
        "--manifest",
        default=str(_DEFAULT_MANIFEST_PATH),
        help="Path to manifest.json (default: data/inbox/manifest.json)",
    )
    parser.add_argument(
        "--customer",
        default=os.environ.get("ACTIVE_CUSTOMER_ID", "karissa_001"),
        help="Customer config ID (default: karissa_001 or $ACTIVE_CUSTOMER_ID)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=os.environ.get("DRY_RUN", "").lower() in ("1", "true", "yes"),
        help="Dry run: parse + log but skip Sheets writes",
    )
    args = parser.parse_args(argv)

    # Load customer config
    customer_config = _load_customer_config(args.customer)
    if customer_config is None:
        _write_run_log({
            "status": "error",
            "notes": f"Could not load customer config {args.customer}",
            "started_at": dt.datetime.utcnow().isoformat() + "Z",
            "finished_at": dt.datetime.utcnow().isoformat() + "Z",
        })
        return 1

    # Process
    try:
        result = process_manifest(
            manifest_path=Path(args.manifest),
            customer_config=customer_config,
            dry_run=args.dry_run,
        )
    except Exception as exc:
        logger.error(
            "tier2_pdf_batch fatal: %s\n%s",
            exc, traceback.format_exc(),
        )
        _write_run_log({
            "status": "error",
            "notes": f"Fatal: {exc}",
            "traceback": traceback.format_exc(),
            "started_at": dt.datetime.utcnow().isoformat() + "Z",
            "finished_at": dt.datetime.utcnow().isoformat() + "Z",
        })
        return 1

    _write_run_log(result)

    # Log summary
    logger.info("=" * 60)
    logger.info("Tier 2 PDF batch: %s", result["status"])
    logger.info("%s", result["notes"])
    for loc in result["written_locations"]:
        logger.info("  ✓ %s", loc)
    for safe, flags in result["per_file_flags"].items():
        if flags:
            logger.info("  ⚠ %s  flags=%s", safe, flags)
    logger.info("=" * 60)

    # Exit code: success/partial/no_files → 0; error → 1
    return 0 if result["status"] in ("success", "partial_success", "no_files") else 1


if __name__ == "__main__":
    sys.exit(main())
