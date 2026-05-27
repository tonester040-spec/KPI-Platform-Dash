#!/usr/bin/env python3
"""
scripts/backfill/patch_new_fields.py
─────────────────────────────────────
One-shot patcher that re-parses April + May 2026 PDFs and updates ONLY the
new columns added 2026-05-27 (rebook_pct on DATA_MONTHLY, req_pct +
avg_service_time_min on STYLISTS_DATA_MONTHLY) for rows that already exist.

Why this exists: the standard append_to_*_monthly writers are idempotent —
they skip rows whose (loc, year_month) key is already present. Adding new
columns to the schema doesn't backfill them for existing rows. Rather than
delete-and-reinsert (risky), this script targets ONLY the new column cells
via Sheets batch update.

Run order:
  1. core/sheets_writer.py header migration must already be applied (done).
  2. python -m scripts.backfill.patch_new_fields  (this script)

Usage:
    GOOGLE_SERVICE_ACCOUNT_JSON=... python -m scripts.backfill.patch_new_fields
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import fitz  # PyMuPDF

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_REPO_ROOT))

from core.sheets_writer import _build_service  # noqa: E402
from parsers import pdf_salon_ultimate_v2, pdf_zenoti_v2  # noqa: E402
from config.locations import LOCATION_POS_MAP  # noqa: E402

HISTORICAL_DIR = Path(r"C:\Users\TonyGrant\Downloads\KPI Historical Data")

# Map filename stems → (canonical_name_in_Sheet, bare_name_for_platform_lookup).
# Sheet rows use the canonical config name (e.g. "Andover FS"); LOCATION_POS_MAP
# uses the bare name ("Andover"). FS-suffix split is intentional and predates
# this script.
FILENAME_TO_LOCATION = {
    "Andover":      ("Andover FS",    "Andover"),
    "Apple Valley": ("Apple Valley",  "Apple Valley"),
    "Blaine":       ("Blaine",        "Blaine"),
    "Crystal":      ("Crystal FS",    "Crystal"),
    "Elk River":    ("Elk River FS",  "Elk River"),
    "Farmington":   ("Farmington",    "Farmington"),
    "Forest Lake":  ("Forest Lake",   "Forest Lake"),
    "Hudson":       ("Hudson",        "Hudson"),
    "Lakeville":    ("Lakeville",     "Lakeville"),
    "New Richmond": ("New Richmond",  "New Richmond"),
    "Prior Lake":   ("Prior Lake",    "Prior Lake"),
    "Roseville":    ("Roseville",     "Roseville"),
}


def parse_pdf(path: Path) -> dict | None:
    """Parse a single PDF and return its salon-level + employee data."""
    doc = fitz.open(str(path))
    text = "\n".join(p.get_text() for p in doc)
    doc.close()

    mapping = FILENAME_TO_LOCATION.get(path.stem)
    if not mapping:
        print(f"  [WARN] Unknown location for {path.name}, skipping")
        return None
    canonical, bare = mapping

    platform = LOCATION_POS_MAP.get(bare)
    if platform == "zenoti":
        result = pdf_zenoti_v2.ZenotiV2Parser(text).parse()
    elif platform == "salon_ultimate":
        result = pdf_salon_ultimate_v2.SalonUltimateV2Parser(text).parse()
    else:
        print(f"  [WARN] Unknown platform for {canonical}, skipping")
        return None

    return {
        "loc_name": canonical,
        "platform": platform,
        "result": result,
    }


def extract_patch_data(parsed: dict, year_month: str) -> tuple[dict, list[dict]]:
    """Build patch data: salon-level rebook + per-stylist req%/svc_time."""
    loc_name = parsed["loc_name"]
    platform = parsed["platform"]
    result = parsed["result"]
    k = result.get("karissa") or {}

    # Salon-level rebook_pct — both parsers return fraction (0-1). Verified
    # 2026-05-27 against Apple Valley PDF (13.14% → 0.1314).
    rebook_pct = float(k.get("rebook_pct") or 0)

    loc_patch = {
        "loc_name": loc_name,
        "year_month": year_month,
        "rebook_pct": rebook_pct,
    }

    # Per-stylist req% + avg_service_time
    stylist_patches = []
    for emp in result.get("employees") or []:
        name = (emp.get("name") or "").strip()
        if not name:
            continue
        if platform == "zenoti":
            req_count = int(emp.get("req_services_count") or 0)
            svc_qty = int(emp.get("service_qty") or 0)
            req_pct = (req_count / svc_qty) if svc_qty else 0.0
            avg_svc_time = 0.0  # not cleanly available on Zenoti
        else:  # salon_ultimate
            req_pct = float(emp.get("request_pct") or 0)
            avg_svc_time = float(emp.get("avg_service_time_min") or 0)
        stylist_patches.append({
            "year_month": year_month,
            "name": name,
            "loc_name": loc_name,
            "req_pct": req_pct,
            "avg_service_time_min": avg_svc_time,
        })

    return loc_patch, stylist_patches


def patch_data_monthly(svc, sheet_id: str, loc_patches: list[dict]) -> int:
    """Update DATA_MONTHLY rows in place — only the rebook_pct (col X) cell."""
    # Read existing rows to find the row index for each (loc_name, year_month)
    existing = svc.spreadsheets().values().get(
        spreadsheetId=sheet_id, range="DATA_MONTHLY!A2:B",
    ).execute().get("values", [])
    row_idx_by_key = {(r[0], r[1]): i + 2 for i, r in enumerate(existing) if len(r) >= 2}

    # Build batch updates — one cell per matching row
    data_updates = []
    matched = 0
    for p in loc_patches:
        key = (p["loc_name"], p["year_month"])
        row_idx = row_idx_by_key.get(key)
        if row_idx is None:
            print(f"  [WARN] No DATA_MONTHLY row for {key}, skipping")
            continue
        data_updates.append({
            "range": f"DATA_MONTHLY!X{row_idx}",
            "values": [[p["rebook_pct"]]],
        })
        matched += 1

    if data_updates:
        svc.spreadsheets().values().batchUpdate(
            spreadsheetId=sheet_id,
            body={"valueInputOption": "USER_ENTERED", "data": data_updates},
        ).execute()
    return matched


def patch_stylists_data_monthly(svc, sheet_id: str, stylist_patches: list[dict]) -> int:
    """Update STYLISTS_DATA_MONTHLY rows in place — req_pct (col Q) + avg_service_time_min (col R)."""
    existing = svc.spreadsheets().values().get(
        spreadsheetId=sheet_id, range="STYLISTS_DATA_MONTHLY!A2:C",
    ).execute().get("values", [])
    row_idx_by_key = {
        (r[0], r[1], r[2]): i + 2
        for i, r in enumerate(existing)
        if len(r) >= 3
    }

    data_updates = []
    matched = 0
    for p in stylist_patches:
        key = (p["year_month"], p["name"], p["loc_name"])
        row_idx = row_idx_by_key.get(key)
        if row_idx is None:
            # Stylist not in existing data — silently skip (new stylist would
            # need full insert, not a patch).
            continue
        data_updates.append({
            "range": f"STYLISTS_DATA_MONTHLY!Q{row_idx}:R{row_idx}",
            "values": [[p["req_pct"], p["avg_service_time_min"]]],
        })
        matched += 1

    if data_updates:
        svc.spreadsheets().values().batchUpdate(
            spreadsheetId=sheet_id,
            body={"valueInputOption": "USER_ENTERED", "data": data_updates},
        ).execute()
    return matched


def main():
    cfg = json.loads((_REPO_ROOT / "config/customers/karissa_001.json").read_text())
    svc = _build_service({})
    sheet_id = cfg["sheet_id"]

    months_to_patch = {
        "2026-04": HISTORICAL_DIR / "4-1-26 - 4-30-26",
        "2026-05": HISTORICAL_DIR / "5-1-24 - 5-24-26",
    }

    for year_month, pdf_dir in months_to_patch.items():
        if not pdf_dir.exists():
            print(f"[WARN] {pdf_dir} not found, skipping {year_month}")
            continue
        print(f"\n=== Patching {year_month} from {pdf_dir.name} ===")
        pdfs = sorted(pdf_dir.glob("*.pdf"))
        print(f"  Found {len(pdfs)} PDFs")

        all_loc_patches = []
        all_stylist_patches = []
        for pdf_path in pdfs:
            parsed = parse_pdf(pdf_path)
            if not parsed:
                continue
            loc_patch, stylist_patches = extract_patch_data(parsed, year_month)
            print(f"  {parsed['loc_name']:<18} rebook={loc_patch['rebook_pct']:.2%} "
                  f"stylist_patches={len(stylist_patches)}")
            all_loc_patches.append(loc_patch)
            all_stylist_patches.extend(stylist_patches)

        # Apply patches
        loc_matched = patch_data_monthly(svc, sheet_id, all_loc_patches)
        styl_matched = patch_stylists_data_monthly(svc, sheet_id, all_stylist_patches)
        print(f"  Patched {loc_matched}/{len(all_loc_patches)} DATA_MONTHLY rows")
        print(f"  Patched {styl_matched}/{len(all_stylist_patches)} STYLISTS_DATA_MONTHLY rows")


if __name__ == "__main__":
    main()
