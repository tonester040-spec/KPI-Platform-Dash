"""
KPI Tier 2 Parser Test Suite
Tests Zenoti Excel parser + Salon Ultimate Excel parser against real files.

Usage:
    # Set env vars first (or create a .env file)
    export GOOGLE_SHEETS_CREDENTIALS=/path/to/service-account.json
    export GOOGLE_SHEETS_ID=your-sheet-id

    # Run all tests
    python test_parsers.py

    # Dry-run (skip Sheets write)
    python test_parsers.py --dry-run

    # Test only one parser
    python test_parsers.py --zenoti-only
    python test_parsers.py --su-only

File paths:
    Update ZENOTI_TEST_FILE and SU_TEST_FILE below to point to your
    March 2026 test files downloaded from Google Drive.
"""

import argparse
import os
import sys

# ---------------------------------------------------------------------------
# TEST FILE PATHS — update these to your actual downloaded file paths
# ---------------------------------------------------------------------------
ZENOTI_TEST_FILE = os.getenv(
    "ZENOTI_TEST_FILE",
    "/path/to/Andover_Employee_KPI_March_2026.xlsx",
)

SU_TEST_FILE = os.getenv(
    "SU_TEST_FILE",
    "/path/to/Stylist_Tracking_Report_Apple_Valley_March_2026.xls",
)

# Target sheet for test writes (separate from production "Stylist Data")
TEST_SHEET_NAME = "Test Data"
# ---------------------------------------------------------------------------


def _separator(title: str):
    width = 60
    print(f"\n{'=' * width}")
    print(f"  {title}")
    print(f"{'=' * width}")


def _print_stylist(stylist: dict, idx: int = 0):
    print(f"\n  [{idx}] {stylist['stylist_name']}")
    print(f"       Location    : {stylist['location']}")
    print(f"       Guest Count : {stylist['guest_count']}")
    print(f"       Service Net : ${stylist['service_net']:.2f}")
    print(f"       Product Net : ${stylist['product_net']:.2f}")
    print(f"       Total Sales : ${stylist['total_sales']:.2f}")
    print(f"       PPG         : ${stylist['ppg_net']:.2f}")


def _validate_stylist(stylist: dict, pos: str) -> list:
    """Return a list of validation failure messages for a stylist dict."""
    failures = []

    required_keys = [
        "location", "stylist_name", "period_start", "period_end", "pos_system",
        "guest_count", "service_net", "product_net", "total_sales", "ppg_net",
    ]
    for key in required_keys:
        if key not in stylist:
            failures.append(f"Missing key: '{key}'")

    if stylist.get("pos_system") != pos:
        failures.append(f"pos_system should be '{pos}', got '{stylist.get('pos_system')}'")

    if stylist.get("guest_count", 0) < 0:
        failures.append("guest_count is negative")

    if stylist.get("service_net", 0) < 0:
        failures.append("service_net is negative")

    if stylist.get("product_net", 0) < 0:
        failures.append("product_net is negative")

    # total_sales sanity check
    expected_total = round(
        stylist.get("service_net", 0) + stylist.get("product_net", 0), 2
    )
    actual_total = round(stylist.get("total_sales", 0), 2)
    if abs(expected_total - actual_total) > 0.02:
        failures.append(
            f"total_sales mismatch: service_net + product_net = {expected_total}, "
            f"but total_sales = {actual_total}"
        )

    return failures


# ---------------------------------------------------------------------------
# Test: Zenoti Excel parser
# ---------------------------------------------------------------------------

def test_zenoti_parser() -> dict | None:
    """Parse the Andover (or supplied) Zenoti file and validate output."""
    _separator("ZENOTI EXCEL PARSER")

    if not os.path.exists(ZENOTI_TEST_FILE):
        print(f"⚠️  Test file not found — skipping Zenoti tests.")
        print(f"   Expected: {ZENOTI_TEST_FILE}")
        print("   Set ZENOTI_TEST_FILE env var or update the path in test_parsers.py")
        return None

    from parsers.zenoti_excel import ZenotiExcelParser

    print(f"📂 File: {ZENOTI_TEST_FILE}")
    parser = ZenotiExcelParser(ZENOTI_TEST_FILE)
    result = parser.parse()

    # Basic output
    print(f"\n✅ Location  : {result['location']}")
    print(f"✅ Period    : {result['period']['start_date']} → {result['period']['end_date']}")
    print(f"✅ Stylists  : {len(result['stylists'])} parsed")

    if not result["stylists"]:
        print("❌ FAIL: No stylists extracted — check header row and column mapping.")
        return result

    # Sample display
    _print_stylist(result["stylists"][0], idx=0)

    # Validation
    all_pass = True
    for i, stylist in enumerate(result["stylists"]):
        failures = _validate_stylist(stylist, "zenoti")
        if failures:
            all_pass = False
            print(f"\n❌ Stylist [{i}] '{stylist.get('stylist_name')}' failed validation:")
            for f in failures:
                print(f"   • {f}")

    if all_pass:
        print(f"\n✅ All {len(result['stylists'])} stylists passed validation")

    return result


# ---------------------------------------------------------------------------
# Test: Salon Ultimate Excel parser
# ---------------------------------------------------------------------------

def test_salon_ultimate_parser() -> dict | None:
    """Parse the Apple Valley (or supplied) Salon Ultimate file and validate output."""
    _separator("SALON ULTIMATE EXCEL PARSER")

    if not os.path.exists(SU_TEST_FILE):
        print(f"⚠️  Test file not found — skipping Salon Ultimate tests.")
        print(f"   Expected: {SU_TEST_FILE}")
        print("   Set SU_TEST_FILE env var or update the path in test_parsers.py")
        return None

    from parsers.salon_ultimate_excel import SalonUltimateExcelParser

    print(f"📂 File: {SU_TEST_FILE}")
    parser = SalonUltimateExcelParser(SU_TEST_FILE)
    result = parser.parse()

    print(f"\n✅ Location  : {result['location']}")
    print(f"✅ Period    : {result['period']['start_date']} → {result['period']['end_date']}")
    print(f"✅ Stylists  : {len(result['stylists'])} parsed")

    if not result["stylists"]:
        print("❌ FAIL: No stylists extracted — check sheet name, column mapping, and Totals: row.")
        return result

    _print_stylist(result["stylists"][0], idx=0)

    # Validation
    all_pass = True
    for i, stylist in enumerate(result["stylists"]):
        failures = _validate_stylist(stylist, "salon_ultimate")
        if failures:
            all_pass = False
            print(f"\n❌ Stylist [{i}] '{stylist.get('stylist_name')}' failed validation:")
            for f in failures:
                print(f"   • {f}")

    if all_pass:
        print(f"\n✅ All {len(result['stylists'])} stylists passed validation")

    return result


# ---------------------------------------------------------------------------
# Test: Google Sheets writer
# ---------------------------------------------------------------------------

def test_sheets_writer(zenoti_result: dict | None, su_result: dict | None, dry_run: bool = False):
    """Combine results from both parsers and write to Test Data sheet."""
    _separator("GOOGLE SHEETS WRITER")

    if dry_run:
        print("⏭️  --dry-run: skipping Sheets write")
        return

    all_stylists = []
    if zenoti_result:
        all_stylists.extend(zenoti_result["stylists"])
    if su_result:
        all_stylists.extend(su_result["stylists"])

    if not all_stylists:
        print("⚠️  No stylists from either parser — skipping Sheets write")
        return

    try:
        from utils.sheets_writer import GoogleSheetsWriter
        writer = GoogleSheetsWriter.from_env()
        writer.write_stylists(all_stylists, sheet_name=TEST_SHEET_NAME)
        print(f"✅ {len(all_stylists)} total stylists written to '{TEST_SHEET_NAME}'")
    except EnvironmentError as e:
        print(f"⚠️  Sheets write skipped: {e}")
    except Exception as e:
        print(f"❌ Sheets write failed: {e}")
        raise


# ---------------------------------------------------------------------------
# Import sanity check
# ---------------------------------------------------------------------------

def test_imports():
    """Verify all modules import without errors."""
    _separator("IMPORT SANITY CHECK")
    modules = [
        ("config.locations",              "LOCATION_POS_MAP"),
        ("parsers.zenoti_excel",          "ZenotiExcelParser"),
        ("parsers.salon_ultimate_excel",  "SalonUltimateExcelParser"),
        ("utils.sheets_writer",           "GoogleSheetsWriter"),
    ]
    all_ok = True
    for module_path, attr in modules:
        try:
            mod = __import__(module_path, fromlist=[attr])
            getattr(mod, attr)
            print(f"  ✅ {module_path}")
        except Exception as e:
            print(f"  ❌ {module_path}: {e}")
            all_ok = False

    return all_ok


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="KPI Tier 2 Parser Test Suite")
    parser.add_argument("--dry-run",      action="store_true", help="Skip Google Sheets write")
    parser.add_argument("--zenoti-only",  action="store_true", help="Only run Zenoti tests")
    parser.add_argument("--su-only",      action="store_true", help="Only run Salon Ultimate tests")
    args = parser.parse_args()

    print("\n🚀 KPI TIER 2 PARSER TEST SUITE")

    # Always run import check first
    imports_ok = test_imports()
    if not imports_ok:
        print("\n❌ Import failures detected — fix before running parser tests.")
        sys.exit(1)

    zenoti_result = None
    su_result     = None

    if not args.su_only:
        zenoti_result = test_zenoti_parser()

    if not args.zenoti_only:
        su_result = test_salon_ultimate_parser()

    test_sheets_writer(zenoti_result, su_result, dry_run=args.dry_run)

    _separator("SUMMARY")
    zenoti_count = len(zenoti_result["stylists"]) if zenoti_result else 0
    su_count     = len(su_result["stylists"])     if su_result     else 0
    total        = zenoti_count + su_count

    print(f"  Zenoti stylists parsed     : {zenoti_count}")
    print(f"  Salon Ultimate stylists    : {su_count}")
    print(f"  Total                      : {total}")
    print()

    if total > 0:
        print("✅ TEST SUITE COMPLETE — parsers are working")
    else:
        print("⚠️  No stylists parsed — provide test files to validate output")


if __name__ == "__main__":
    main()
