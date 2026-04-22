"""
scripts/test_stylist_transform.py
─────────────────────────────────
Unit smoke test for parsers.tier2_pdf_batch.transform_to_stylist_rows.

Feeds synthetic Zenoti and Salon Ultimate parser outputs through the new
transform and validates:
  1. Row shape matches keys expected by core.sheets_writer.write_stylists_current
  2. Row shape matches keys expected by core.sheets_writer.append_to_stylists_historical
  3. Platform-specific field mapping (Zenoti avg_invoice_value → cur_ticket;
     SU pph → cur_pph)
  4. House phantom filter works
  5. Dead-roster zero filter works

No network. No Sheets API. No PDF parsing. Runs in <1s.
"""
from __future__ import annotations
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT))

from parsers.tier2_pdf_batch import transform_to_stylist_rows
from parsers.pdf_detect import ZENOTI, SALON_ULTIMATE


# Keys the two downstream writers actually read off each stylist dict.
# Sourced from core/sheets_writer.py lines 94–110 (write_stylists_current)
# and lines 377–392 (append_to_stylists_historical).
REQUIRED_KEYS_CURRENT = {
    "weeks",         # reads s["weeks"][-1]
    "name",
    "loc_name",
    "loc_id",
    "status",
    "tenure",        # note: current writer reads "tenure"
    "cur_pph",
    "cur_rebook",
    "cur_product",
    "cur_ticket",
    "services",      # reads s["services"][-1]
    "color",         # reads s["color"][-1]
}
REQUIRED_KEYS_APPEND = {
    "name",
    "loc_name",
    "loc_id",
    "status",
    "tenure_yrs",    # note: append writer reads "tenure_yrs"
    "cur_pph",
    "cur_rebook",
    "cur_product",
    "cur_ticket",
    "services",
    "color",
}


def _fake_zenoti_parsed():
    return {
        "location": "Andover",
        "week_end": "2026-04-21",
        "employees": [
            {
                "name": "Rebecca Follansbee",
                "role_group": "STYLIST",
                "net_service": 2640.50,
                "service_qty": 24,
                "invoice_count": 31,
                "net_product": 312.75,
                "avg_invoice_value": 98.42,
                "net_service_per_hr": 68.25,
                "production_hours": 38.7,
            },
            {
                "name": "Dead Roster Entry",  # all zeros → should be dropped
                "role_group": "STYLIST",
                "net_service": 0,
                "service_qty": 0,
                "net_product": 0,
                "avg_invoice_value": 0,
                "net_service_per_hr": 0,
            },
            {
                "name": "  ",  # blank → should be dropped
                "role_group": "STYLIST",
                "net_service_per_hr": 42.0,
            },
        ],
    }


def _fake_su_parsed():
    return {
        "location": "Apple Valley",
        "week_end": "2026-04-21",
        "employees": [
            {
                "name": "Magdalene York",
                "net_service": 1895.00,
                "net_retail": 245.50,
                "pph": 52.75,
                "guests": 18,
                "avg_ticket": 91.00,
                "is_phantom_house": False,
            },
            {
                "name": "House",   # phantom → should be dropped
                "net_retail": 1200.00,
                "pph": 0,
                "guests": 0,
                "avg_ticket": 0,
                "is_phantom_house": True,
            },
        ],
    }


def _assert_row_shape(row, label):
    missing_cur = REQUIRED_KEYS_CURRENT - set(row.keys())
    missing_app = REQUIRED_KEYS_APPEND - set(row.keys())
    assert not missing_cur, (
        f"{label}: row missing write_stylists_current keys: {missing_cur}"
    )
    assert not missing_app, (
        f"{label}: row missing append_to_stylists_historical keys: {missing_app}"
    )
    # Array-typed fields that writers index with [-1]
    assert isinstance(row["services"], list) and row["services"], \
        f"{label}: services must be a non-empty list"
    assert isinstance(row["color"], list) and row["color"], \
        f"{label}: color must be a non-empty list"
    assert isinstance(row["weeks"], list), \
        f"{label}: weeks must be a list"


def test_zenoti():
    parsed = _fake_zenoti_parsed()
    rows = transform_to_stylist_rows(parsed, ZENOTI, "Andover FS", "z001")
    assert len(rows) == 1, f"Zenoti: expected 1 row (after filters), got {len(rows)}"
    r = rows[0]
    _assert_row_shape(r, "Zenoti")
    assert r["name"] == "Rebecca Follansbee"
    assert r["loc_name"] == "Andover FS"
    assert r["loc_id"] == "z001"
    assert r["cur_pph"] == 68.25
    assert r["cur_product"] == 312.75          # net_product → cur_product
    assert r["cur_ticket"] == 98.42            # avg_invoice_value → cur_ticket
    assert r["services"][-1] == 24             # service_qty → services
    assert r["weeks"] == ["2026-04-21"]
    assert r["color"] == [0.0]
    assert r["cur_rebook"] == 0.0
    assert r["tenure"] == 0 and r["tenure_yrs"] == 0
    assert r["status"] == "active"
    print("  ✓ Zenoti mapping correct")


def test_salon_ultimate():
    parsed = _fake_su_parsed()
    rows = transform_to_stylist_rows(parsed, SALON_ULTIMATE, "Apple Valley", "z010")
    assert len(rows) == 1, f"SU: expected 1 row (House filtered), got {len(rows)}"
    r = rows[0]
    _assert_row_shape(r, "SU")
    assert r["name"] == "Magdalene York"
    assert r["loc_name"] == "Apple Valley"
    assert r["loc_id"] == "z010"
    assert r["cur_pph"] == 52.75               # pph → cur_pph
    assert r["cur_product"] == 245.50          # net_retail → cur_product
    assert r["cur_ticket"] == 91.00            # avg_ticket → cur_ticket
    assert r["services"][-1] == 18             # guests → services
    assert r["weeks"] == ["2026-04-21"]
    print("  ✓ SU mapping correct (House phantom filtered)")


def test_empty_employees():
    rows = transform_to_stylist_rows(
        {"location": "X", "week_end": "2026-04-21", "employees": []},
        ZENOTI, "X", "z999",
    )
    assert rows == [], f"Empty employees must produce no rows, got {rows}"
    rows = transform_to_stylist_rows(
        {"location": "X", "week_end": "2026-04-21"},  # key absent
        ZENOTI, "X", "z999",
    )
    assert rows == [], "Missing employees key must produce no rows"
    print("  ✓ Empty employees handled")


def test_writer_compatibility():
    """
    Actually execute the writer row-building logic in isolation (no Sheets
    call) to confirm our rows don't KeyError or index off the end of
    services/color/weeks lists.
    """
    rows = transform_to_stylist_rows(
        _fake_zenoti_parsed(), ZENOTI, "Andover FS", "z001",
    )
    # Mimic write_stylists_current's row construction
    for s in rows:
        _ = [
            s["weeks"][-1] if s.get("weeks") else "",
            s.get("name", ""),
            s.get("loc_name", ""),
            s.get("loc_id", ""),
            s.get("status", "active"),
            s.get("tenure", 0),
            s.get("cur_pph", 0),
            s.get("cur_rebook", 0),
            s.get("cur_product", 0),
            s.get("cur_ticket", 0),
            s["services"][-1] if s.get("services") else 0,
            s["color"][-1] if s.get("color") else 0,
        ]
    # Mimic append_to_stylists_historical's row construction
    for s in rows:
        _ = [
            "2026-04-21",
            s.get("name", ""),
            s.get("loc_name", ""),
            s.get("loc_id", ""),
            s.get("status", "active"),
            s.get("tenure_yrs", 0),
            s.get("cur_pph", 0),
            s.get("cur_rebook", 0),
            s.get("cur_product", 0),
            s.get("cur_ticket", 0),
            s["services"][-1] if s.get("services") else 0,
            s["color"][-1] if s.get("color") else 0,
        ]
    print("  ✓ Both writer row-builders consume cleanly")


if __name__ == "__main__":
    print("transform_to_stylist_rows smoke test")
    print("─" * 40)
    test_zenoti()
    test_salon_ultimate()
    test_empty_employees()
    test_writer_compatibility()
    print("─" * 40)
    print("ALL PASS")
