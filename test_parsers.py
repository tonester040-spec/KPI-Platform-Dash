"""
KPI Tier 2 Parser Test Suite — Phase 1 (Excel) + Phase 2 (PDF) + Merger
Tests all parsers, validates output, optionally writes to Google Sheets.

Usage:
    python test_parsers.py                  # Full run (skips missing files gracefully)
    python test_parsers.py --dry-run        # Skip Google Sheets write
    python test_parsers.py --zenoti-only    # Zenoti Excel + PDF only
    python test_parsers.py --su-only        # Salon Ultimate Excel + PDF only
    python test_parsers.py --excel-only     # Skip PDF parsers + merger
    python test_parsers.py --pdf-only       # Skip Excel parsers

Environment variables (or update file paths below):
    ZENOTI_EXCEL_FILE   — Zenoti .xlsx file (e.g. Andover Employee KPI March 2026)
    SU_EXCEL_FILE       — Salon Ultimate .xls file (e.g. Apple Valley Stylist Tracking March 2026)
    ZENOTI_PDF_FILE     — Zenoti Salon Summary .pdf (e.g. Andover Salon Summary April 2026)
    SU_PDF_FILE         — Salon Ultimate Dashboard .pdf (e.g. Apple Valley Dashboard April 2026)
    GOOGLE_SHEETS_CREDENTIALS — path to service-account JSON
    GOOGLE_SHEETS_ID    — target spreadsheet ID
"""

import argparse
import os
import sys

# ---------------------------------------------------------------------------
# TEST FILE PATHS — update these or set via environment variables
# ---------------------------------------------------------------------------
ZENOTI_EXCEL_FILE = os.getenv("ZENOTI_EXCEL_FILE",  "/path/to/Andover_Employee_KPI_March_2026.xlsx")
SU_EXCEL_FILE     = os.getenv("SU_EXCEL_FILE",      "/path/to/Stylist_Tracking_Report_Apple_Valley_March_2026.xls")
ZENOTI_PDF_FILE   = os.getenv("ZENOTI_PDF_FILE",    "/path/to/Andover_Salon_Summary_April_2026.pdf")
SU_PDF_FILE       = os.getenv("SU_PDF_FILE",        "/path/to/Apple_Valley_Dashboard_April_2026.pdf")

# For backward compat with Phase 1 env vars
if not os.path.exists(ZENOTI_EXCEL_FILE):
    ZENOTI_EXCEL_FILE = os.getenv("ZENOTI_TEST_FILE", ZENOTI_EXCEL_FILE)
if not os.path.exists(SU_EXCEL_FILE):
    SU_EXCEL_FILE = os.getenv("SU_TEST_FILE", SU_EXCEL_FILE)

TEST_SHEET_EXCEL  = "Test Data"
TEST_SHEET_MERGED = "Phase 2 Test Data"
# ---------------------------------------------------------------------------


def _separator(title: str):
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")


def _print_stylist(stylist: dict, idx: int = 0):
    has_pdf = stylist.get("wax_count", "") != ""
    print(f"\n  [{idx}] {stylist.get('stylist_name', '?')}")
    print(f"       Location       : {stylist.get('location', '?')}")
    print(f"       Guest Count    : {stylist.get('guest_count', 0)}")
    print(f"       Service Net    : ${stylist.get('service_net', 0):.2f}")
    print(f"       Product Net    : ${stylist.get('product_net', 0):.2f}")
    print(f"       PPG            : ${stylist.get('ppg_net', 0):.2f}")
    if has_pdf:
        print(f"       Wax Count      : {stylist.get('wax_count', 0)}")
        print(f"       Wax %          : {stylist.get('wax_pct', 0):.2f}%")
        print(f"       Color Net      : ${stylist.get('color_net', 0):.2f}")
        print(f"       Color %        : {stylist.get('color_pct', 0):.2f}%")
        print(f"       Treatment Count: {stylist.get('treatment_count', 0)}")


def _validate_excel_stylist(stylist: dict, pos: str) -> list:
    """Return list of validation failures for a Phase 1 stylist dict."""
    failures = []
    required = [
        "location", "stylist_name", "period_start", "period_end", "pos_system",
        "guest_count", "service_net", "product_net", "total_sales", "ppg_net",
    ]
    for k in required:
        if k not in stylist:
            failures.append(f"Missing key: '{k}'")

    if stylist.get("pos_system") != pos:
        failures.append(f"pos_system should be '{pos}', got '{stylist.get('pos_system')}'")
    if stylist.get("guest_count", 0) < 0:
        failures.append("guest_count is negative")
    if stylist.get("service_net", 0) < 0:
        failures.append("service_net is negative")

    expected_total = round(stylist.get("service_net", 0) + stylist.get("product_net", 0), 2)
    actual_total   = round(stylist.get("total_sales",  0), 2)
    if abs(expected_total - actual_total) > 0.02:
        failures.append(
            f"total_sales mismatch: service_net + product_net = {expected_total}, "
            f"but total_sales = {actual_total}"
        )
    return failures


def _validate_merged_stylist(stylist: dict) -> list:
    """Return list of validation failures for a Phase 2 merged stylist dict."""
    failures = _validate_excel_stylist(stylist, stylist.get("pos_system", "?"))

    # Phase 2 keys must be present and numeric
    for key in ["wax_count", "wax_pct", "color_net", "color_pct", "treatment_count"]:
        val = stylist.get(key, "MISSING")
        if val == "MISSING":
            failures.append(f"Missing Phase 2 key: '{key}'")
        elif not isinstance(val, (int, float)):
            failures.append(f"'{key}' is not numeric: {val!r}")

    # wax_pct sanity
    guest = stylist.get("guest_count", 1)
    wax   = stylist.get("wax_count",   0)
    if guest > 0 and wax > guest * 1.5:
        failures.append(f"wax_count ({wax}) suspiciously high vs guest_count ({guest})")

    return failures


# ---------------------------------------------------------------------------
# Import sanity check
# ---------------------------------------------------------------------------

def test_imports(include_pdf: bool = True) -> bool:
    _separator("IMPORT SANITY CHECK")
    modules = [
        ("config.locations",             "LOCATION_POS_MAP"),
        ("parsers.zenoti_excel",         "ZenotiExcelParser"),
        ("parsers.salon_ultimate_excel", "SalonUltimateExcelParser"),
        ("utils.sheets_writer",          "GoogleSheetsWriter"),
        ("utils.data_merger",            "DataMerger"),
    ]
    if include_pdf:
        modules += [
            ("parsers.zenoti_pdf",          "ZenotiPDFParser"),
            ("parsers.salon_ultimate_pdf",  "SalonUltimatePDFParser"),
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
# Phase 1 — Excel parsers
# ---------------------------------------------------------------------------

def test_zenoti_excel() -> dict | None:
    _separator("ZENOTI EXCEL PARSER")

    if not os.path.exists(ZENOTI_EXCEL_FILE):
        print(f"⚠️  File not found — skipping.  Set ZENOTI_EXCEL_FILE or update path.")
        print(f"   Expected: {ZENOTI_EXCEL_FILE}")
        return None

    from parsers.zenoti_excel import ZenotiExcelParser
    print(f"📂 {ZENOTI_EXCEL_FILE}")

    parser = ZenotiExcelParser(ZENOTI_EXCEL_FILE)
    result = parser.parse()

    print(f"\n✅ Location  : {result['location']}")
    print(f"✅ Period    : {result['period']['start_date']} → {result['period']['end_date']}")
    print(f"✅ Stylists  : {len(result['stylists'])}")

    if result["stylists"]:
        _print_stylist(result["stylists"][0], idx=0)

    all_pass = True
    for i, s in enumerate(result["stylists"]):
        fails = _validate_excel_stylist(s, "zenoti")
        if fails:
            all_pass = False
            print(f"\n❌ Stylist [{i}] failed:")
            for f in fails:
                print(f"   • {f}")

    if all_pass and result["stylists"]:
        print(f"\n✅ All {len(result['stylists'])} stylists passed validation")

    return result


def test_salon_ultimate_excel() -> dict | None:
    _separator("SALON ULTIMATE EXCEL PARSER")

    if not os.path.exists(SU_EXCEL_FILE):
        print(f"⚠️  File not found — skipping.  Set SU_EXCEL_FILE or update path.")
        print(f"   Expected: {SU_EXCEL_FILE}")
        return None

    from parsers.salon_ultimate_excel import SalonUltimateExcelParser
    print(f"📂 {SU_EXCEL_FILE}")

    parser = SalonUltimateExcelParser(SU_EXCEL_FILE)
    result = parser.parse()

    print(f"\n✅ Location  : {result['location']}")
    print(f"✅ Period    : {result['period']['start_date']} → {result['period']['end_date']}")
    print(f"✅ Stylists  : {len(result['stylists'])}")

    if result["stylists"]:
        _print_stylist(result["stylists"][0], idx=0)

    all_pass = True
    for i, s in enumerate(result["stylists"]):
        fails = _validate_excel_stylist(s, "salon_ultimate")
        if fails:
            all_pass = False
            print(f"\n❌ Stylist [{i}] failed:")
            for f in fails:
                print(f"   • {f}")

    if all_pass and result["stylists"]:
        print(f"\n✅ All {len(result['stylists'])} stylists passed validation")

    return result


# ---------------------------------------------------------------------------
# Phase 2 — PDF parsers
# ---------------------------------------------------------------------------

def test_zenoti_pdf() -> dict | None:
    _separator("ZENOTI PDF PARSER")

    if not os.path.exists(ZENOTI_PDF_FILE):
        print(f"⚠️  File not found — skipping.  Set ZENOTI_PDF_FILE or update path.")
        print(f"   Expected: {ZENOTI_PDF_FILE}")
        return None

    from parsers.zenoti_pdf import ZenotiPDFParser
    print(f"📂 {ZENOTI_PDF_FILE}")

    parser = ZenotiPDFParser(ZENOTI_PDF_FILE)
    result = parser.parse()

    print(f"\n✅ Location  : {result['location']}")
    print(f"✅ Period    : {result['period']['start_date']} → {result['period']['end_date']}")

    cats = result["service_categories"]
    print(f"\n📊 Service Categories:")
    print(f"   Haircut   : {cats['haircut_count']} services  ${cats['haircut_net']:.2f}")
    print(f"   Color     : {cats['color_count']} services  ${cats['color_net']:.2f}")
    print(f"   Wax       : {cats['wax_count']} services  ${cats['wax_net']:.2f}")
    print(f"   Treatment : {cats['treatment_count']} services  ${cats['treatment_net']:.2f}")

    # Sanity: at least one category must have non-zero qty
    if all(cats[k] == 0 for k in ["haircut_count", "color_count", "wax_count", "treatment_count"]):
        print("\n❌ WARNING: All service category counts are zero — regex may not match PDF format")
    else:
        print("\n✅ At least one service category extracted successfully")

    return result


def test_salon_ultimate_pdf() -> dict | None:
    _separator("SALON ULTIMATE PDF PARSER")

    if not os.path.exists(SU_PDF_FILE):
        print(f"⚠️  File not found — skipping.  Set SU_PDF_FILE or update path.")
        print(f"   Expected: {SU_PDF_FILE}")
        return None

    from parsers.salon_ultimate_pdf import SalonUltimatePDFParser
    print(f"📂 {SU_PDF_FILE}")

    parser = SalonUltimatePDFParser(SU_PDF_FILE)
    result = parser.parse()

    print(f"\n✅ Location  : {result['location']}")
    print(f"✅ Period    : {result['period']['start_date']} → {result['period']['end_date']}")

    cats = result["service_categories"]
    print(f"\n📊 Service Categories:")
    print(f"   Haircut   : {cats['haircut_count']} services  ${cats['haircut_net']:.2f}")
    print(f"   Color     : {cats['color_count']} services  ${cats['color_net']:.2f}")
    print(f"   Wax       : {cats['wax_count']} services  ${cats['wax_net']:.2f}")
    print(f"   Treatment : {cats['treatment_count']} services  ${cats['treatment_net']:.2f}")

    if all(cats[k] == 0 for k in ["haircut_count", "color_count", "wax_count", "treatment_count"]):
        print("\n❌ WARNING: All service category counts are zero — regex may not match PDF format")
    else:
        print("\n✅ At least one service category extracted successfully")

    return result


# ---------------------------------------------------------------------------
# Phase 3 — Data merger
# ---------------------------------------------------------------------------

def test_data_merger(excel_result: dict, pdf_result: dict, label: str = "") -> dict | None:
    _separator(f"DATA MERGER{' — ' + label if label else ''}")

    if not excel_result or not pdf_result:
        print("⚠️  Missing Excel or PDF result — skipping merger test")
        return None

    from utils.data_merger import DataMerger

    merged = DataMerger.merge_location_data(excel_result, pdf_result)

    print(f"\n✅ Merged location : {merged['location']}")
    print(f"✅ Stylists        : {len(merged['stylists'])}")
    print(f"✅ PDF data present: {'service_categories' in merged}")

    if merged["stylists"]:
        _print_stylist(merged["stylists"][0], idx=0)

    all_pass = True
    for i, s in enumerate(merged["stylists"]):
        fails = _validate_merged_stylist(s)
        if fails:
            all_pass = False
            print(f"\n❌ Merged stylist [{i}] '{s.get('stylist_name')}' failed:")
            for f in fails:
                print(f"   • {f}")

    if all_pass and merged["stylists"]:
        print(f"\n✅ All {len(merged['stylists'])} merged stylists passed validation")

    # Print merger summary
    from utils.data_merger import DataMerger as DM
    print(f"\n{DM.summarize_merge(merged)}")

    return merged


# ---------------------------------------------------------------------------
# Sheets writer tests
# ---------------------------------------------------------------------------

def test_sheets_excel(excel_results: list, dry_run: bool = False):
    """Write Phase 1 (Excel-only) data to Sheets."""
    if dry_run:
        print("\n⏭️  --dry-run: skipping Phase 1 Sheets write")
        return

    all_stylists = []
    for r in excel_results:
        if r:
            all_stylists.extend(r.get("stylists", []))

    if not all_stylists:
        print("\n⚠️  No Excel stylists to write")
        return

    try:
        from utils.sheets_writer import GoogleSheetsWriter
        writer = GoogleSheetsWriter.from_env()
        writer.write_stylists(all_stylists, sheet_name=TEST_SHEET_EXCEL)
    except EnvironmentError as e:
        print(f"⚠️  Sheets write skipped: {e}")
    except Exception as e:
        print(f"❌ Sheets write failed: {e}")


def test_sheets_merged(merged_results: list, dry_run: bool = False):
    """Write Phase 2 (merged Excel + PDF) data to Sheets."""
    _separator("WRITE MERGED DATA → GOOGLE SHEETS")

    if dry_run:
        print("⏭️  --dry-run: skipping Phase 2 Sheets write")
        return

    from utils.data_merger import DataMerger
    all_stylists = DataMerger.flatten_stylists([r for r in merged_results if r])

    if not all_stylists:
        print("⚠️  No merged stylists to write")
        return

    try:
        from utils.sheets_writer import GoogleSheetsWriter
        writer = GoogleSheetsWriter.from_env()
        writer.write_stylists(all_stylists, sheet_name=TEST_SHEET_MERGED)
        print(f"✅ {len(all_stylists)} merged stylists written to '{TEST_SHEET_MERGED}'")
    except EnvironmentError as e:
        print(f"⚠️  Sheets write skipped: {e}")
    except Exception as e:
        print(f"❌ Sheets write failed: {e}")
        raise


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="KPI Tier 2 Parser Test Suite")
    parser.add_argument("--dry-run",     action="store_true", help="Skip Google Sheets writes")
    parser.add_argument("--zenoti-only", action="store_true", help="Only run Zenoti tests")
    parser.add_argument("--su-only",     action="store_true", help="Only run Salon Ultimate tests")
    parser.add_argument("--excel-only",  action="store_true", help="Skip PDF parsers and merger")
    parser.add_argument("--pdf-only",    action="store_true", help="Skip Excel parsers")
    args = parser.parse_args()

    include_pdf = not args.excel_only
    print("\n🚀 KPI TIER 2 PARSER TEST SUITE")

    imports_ok = test_imports(include_pdf=include_pdf)
    if not imports_ok:
        print("\n❌ Import failures — fix before running parser tests.")
        sys.exit(1)

    # ---- Phase 1: Excel ----
    zenoti_excel = su_excel = None

    if not args.pdf_only:
        if not args.su_only:
            zenoti_excel = test_zenoti_excel()
        if not args.zenoti_only:
            su_excel = test_salon_ultimate_excel()

        if not args.dry_run:
            _separator("WRITE EXCEL DATA → GOOGLE SHEETS")
            test_sheets_excel([zenoti_excel, su_excel], dry_run=args.dry_run)

    # ---- Phase 2: PDF ----
    zenoti_pdf_result = su_pdf_result = None

    if include_pdf:
        if not args.su_only:
            zenoti_pdf_result = test_zenoti_pdf()
        if not args.zenoti_only:
            su_pdf_result = test_salon_ultimate_pdf()

    # ---- Phase 3: Merge ----
    merged_zenoti = merged_su = None

    if include_pdf and not args.pdf_only:
        if zenoti_excel and zenoti_pdf_result:
            merged_zenoti = test_data_merger(zenoti_excel, zenoti_pdf_result, label="Zenoti")
        if su_excel and su_pdf_result:
            merged_su = test_data_merger(su_excel, su_pdf_result, label="Salon Ultimate")

        test_sheets_merged([merged_zenoti, merged_su], dry_run=args.dry_run)

    # ---- Summary ----
    _separator("SUMMARY")
    zenoti_excel_count = len(zenoti_excel["stylists"]) if zenoti_excel else 0
    su_excel_count     = len(su_excel["stylists"])     if su_excel     else 0
    zenoti_merged      = len(merged_zenoti["stylists"]) if merged_zenoti else 0
    su_merged          = len(merged_su["stylists"])     if merged_su     else 0

    print(f"  Zenoti Excel stylists       : {zenoti_excel_count}")
    print(f"  Salon Ultimate Excel        : {su_excel_count}")
    print(f"  Zenoti merged (Excel + PDF) : {zenoti_merged}")
    print(f"  SU merged     (Excel + PDF) : {su_merged}")
    total_merged = zenoti_merged + su_merged
    total_excel  = zenoti_excel_count + su_excel_count
    print()

    if total_merged > 0:
        print("✅ TEST SUITE COMPLETE — Phase 1 + Phase 2 parsers working")
    elif total_excel > 0:
        print("✅ Phase 1 COMPLETE — Excel parsers working (provide PDFs to test Phase 2)")
    else:
        print("⚠️  No data parsed — provide test files to validate output")


if __name__ == "__main__":
    main()
