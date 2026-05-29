"""
Tests for core/history_store.append_to_historical — the MTD/history foundation.

Layers (mirrors the Track A-D test style: plain script, `check()`, SKIP-gated
real-file smoke):
  1. UNIT        — synthetic LOCATIONS_DATA / STYLISTS_DATA rows against a tmp
                   JSONL store. No parser, no network. Covers the full contract:
                   append / idempotent no-op (byte-identical) / supersede + audit /
                   closed-week + provisional rows stored / anti-backfill /
                   table independence / stylist content-hash path / within-batch
                   correction / bad-table guard / empty no-op.
  2. INTEGRATION — files REAL extracted rows from data/inbox/Forest Lake.pdf
                   (LOCATIONS_DATA via locations_grouper + STYLISTS_DATA via
                   zenoti_stylist_parser) into a tmp store; asserts keys land and a
                   reprocess is idempotent. SKIPS cleanly if the file or parse deps
                   are absent (Track D pattern).

Run:  python tests/test_history_store.py
"""
from __future__ import annotations
import json
import os
import shutil
import sys
import tempfile
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
from core import history_store as H  # noqa: E402

FAILS: list[str] = []


def check(name, cond):
    print(("PASS" if cond else "FAIL"), "-", name)
    if not cond:
        FAILS.append(name)


def _fresh_dir():
    return tempfile.mkdtemp(prefix="kpi_hist_")


def _bytes(d, table):
    with open(os.path.join(d, f"{table}.jsonl"), "rb") as f:
        return f.read()


def _audit(d, table):
    p = os.path.join(d, f"{table}.audit.jsonl")
    if not os.path.exists(p):
        return []
    return [json.loads(line) for line in open(p, encoding="utf-8") if line.strip()]


# ─────────────────────────────────────────────────────────────────────────────
# Synthetic row factories — minimal but schema-faithful.
#   LOCATIONS_DATA carries an explicit source_extract_hash (the idempotency anchor)
#   + a volatile generated_at. STYLISTS_DATA (18-key contract) carries NEITHER, so
#   its fingerprint comes from row content.
# ─────────────────────────────────────────────────────────────────────────────
def loc_row(*, location_id="888-11812", period_start="2026-05-01",
            period_end="2026-05-03", period_type="weekly", extract_hash="hashA",
            total=1000.0, data_complete=True, **over):
    r = {
        "location_id": location_id,
        "location_name_canonical": "Forest Lake",
        "period_start": period_start,
        "period_end": period_end,
        "period_type": period_type,
        "guest_count": 50,
        "total_sales_net": total,
        "service_net": total,
        "product_net": 0.0,
        "productive_hours": 40.0,
        "source_extract_hash": extract_hash,
        "generated_at": "2026-05-26T12:00:00+00:00",
        "data_complete_flag": data_complete,
    }
    r.update(over)
    return r


def styl_row(*, loc_id="888-11812", name="Alice Adams", period_start="2026-05-01",
             period_end="2026-05-24", net_service=1000.0, **over):
    r = {
        "year_month": period_start[:7],
        "name": name,
        "loc_name": "Forest Lake",
        "loc_id": loc_id,
        "platform": "zenoti",
        "invoices": 15,
        "guests": 15,
        "net_service": net_service,
        "net_product": 200.0,
        "avg_ticket": 80.0,
        "pph": 33.33,
        "ppg": 13.33,
        "production_hours": 30.0,
        "source": "zenoti_salon_summary",
        "period_start": period_start,
        "period_end": period_end,
        "req_pct": 0.0,
        "avg_service_time_min": 0.0,
    }
    r.update(over)
    return r


# ─────────────────────────────────────────────────────────────────────────────
# UNIT
# ─────────────────────────────────────────────────────────────────────────────
def t_append_new():
    d = _fresh_dir()
    try:
        rows = [loc_row(period_start="2026-05-01", period_end="2026-05-03"),
                loc_row(period_start="2026-05-04", period_end="2026-05-10"),
                loc_row(period_start="2026-05-11", period_end="2026-05-17")]
        res = H.append_to_historical(rows, "LOCATIONS_DATA", store_dir=d)
        check("3 new rows -> appended=3", res.appended == 3)
        check("3 new rows -> skipped_duplicate=0", res.skipped_duplicate == 0)
        check("3 new rows -> superseded=0", res.superseded == 0)
        check("3 keys reported", len(res.appended_keys) == 3)
        check("3 rows persisted + retrievable", len(H.read_history("LOCATIONS_DATA", store_dir=d)) == 3)
    finally:
        shutil.rmtree(d, ignore_errors=True)


def t_idempotent_byte_identical():
    d = _fresh_dir()
    try:
        rows = [loc_row(period_start="2026-05-01", period_end="2026-05-03"),
                loc_row(period_start="2026-05-04", period_end="2026-05-10"),
                loc_row(period_start="2026-05-11", period_end="2026-05-17")]
        H.append_to_historical(rows, "LOCATIONS_DATA", store_dir=d)
        b1 = _bytes(d, "LOCATIONS_DATA")
        # Reprocess the SAME extracts — generated_at even bumped — must stay a no-op
        # (the source_extract_hash, not the volatile stamp, drives idempotency).
        rows2 = [loc_row(period_start="2026-05-01", period_end="2026-05-03", generated_at="2026-05-30T09:00:00+00:00"),
                 loc_row(period_start="2026-05-04", period_end="2026-05-10", generated_at="2026-05-30T09:00:00+00:00"),
                 loc_row(period_start="2026-05-11", period_end="2026-05-17", generated_at="2026-05-30T09:00:00+00:00")]
        res = H.append_to_historical(rows2, "LOCATIONS_DATA", store_dir=d)
        check("reprocess -> appended=0", res.appended == 0)
        check("reprocess -> skipped_duplicate=3", res.skipped_duplicate == 3)
        check("reprocess -> superseded=0 (generated_at change ignored)", res.superseded == 0)
        check("store BYTE-IDENTICAL after idempotent reprocess", _bytes(d, "LOCATIONS_DATA") == b1)
        check("no audit file written on a pure no-op", not os.path.exists(os.path.join(d, "LOCATIONS_DATA.audit.jsonl")))
    finally:
        shutil.rmtree(d, ignore_errors=True)


def t_supersede_with_audit():
    d = _fresh_dir()
    try:
        H.append_to_historical([loc_row(extract_hash="hashA", total=100.0)], "LOCATIONS_DATA", store_dir=d)
        res = H.append_to_historical([loc_row(extract_hash="hashB", total=200.0)], "LOCATIONS_DATA", store_dir=d)
        check("corrected (new hash) -> superseded=1", res.superseded == 1)
        check("corrected -> appended=0", res.appended == 0)
        back = H.read_history("LOCATIONS_DATA", store_dir=d)
        check("exactly 1 live row after supersede", len(back) == 1)
        check("live row reflects NEW total 200", back[0]["total_sales_net"] == 200.0)
        audit = _audit(d, "LOCATIONS_DATA")
        check("audit log has 1 entry", len(audit) == 1)
        check("audit preserves OLD total 100", audit[0]["superseded_row"]["total_sales_net"] == 100.0)
        check("audit old/new fingerprints differ", audit[0]["old_fingerprint"] != audit[0]["new_fingerprint"])
        check("audit records the composite key", audit[0]["key"]["location_id"] == "888-11812")
    finally:
        shutil.rmtree(d, ignore_errors=True)


def t_closed_week_stored():
    d = _fresh_dir()
    try:
        closed = loc_row(period_start="2026-05-25", period_end="2026-05-31",
                         total=0.0, data_complete=False, guest_count=0,
                         service_net=0.0, product_net=0.0, productive_hours=0.0,
                         extract_hash="zeroweek")
        res = H.append_to_historical([closed], "LOCATIONS_DATA", store_dir=d)
        check("closed/zero week -> appended=1 (NOT dropped)", res.appended == 1)
        back = H.read_history("LOCATIONS_DATA", store_dir=d)
        check("closed week retrievable", len(back) == 1)
        check("data_complete_flag=False preserved verbatim", back[0]["data_complete_flag"] is False)
        check("zero values preserved", back[0]["total_sales_net"] == 0.0 and back[0]["guest_count"] == 0)
    finally:
        shutil.rmtree(d, ignore_errors=True)


def t_provisional_then_complete():
    d = _fresh_dir()
    try:
        prov = loc_row(period_type="mtd", data_complete=False, productive_hours=None,
                       extract_hash="prov", total=5000.0)
        H.append_to_historical([prov], "LOCATIONS_DATA", store_dir=d)
        b = H.read_history("LOCATIONS_DATA", store_dir=d)
        check("provisional row stored (flag False)", b[0]["data_complete_flag"] is False)
        comp = loc_row(period_type="mtd", data_complete=True, productive_hours=412.0,
                       extract_hash="complete", total=5000.0)
        res = H.append_to_historical([comp], "LOCATIONS_DATA", store_dir=d)
        check("later complete extract supersedes provisional", res.superseded == 1)
        b2 = H.read_history("LOCATIONS_DATA", store_dir=d)
        check("live row now complete (flag True)", b2[0]["data_complete_flag"] is True)
        check("live row hours now filled (412)", b2[0]["productive_hours"] == 412.0)
        check("provisional version preserved in audit", _audit(d, "LOCATIONS_DATA")[0]["superseded_row"]["data_complete_flag"] is False)
    finally:
        shutil.rmtree(d, ignore_errors=True)


def t_anti_backfill():
    d = _fresh_dir()
    try:
        wk1 = loc_row(period_start="2026-05-01", period_end="2026-05-03", total=8147.0, extract_hash="wk1")
        H.append_to_historical([wk1], "LOCATIONS_DATA", store_dir=d)
        # A LATER period arrives — it must NOT touch (backfill) the earlier wk1 row.
        wk3 = loc_row(period_start="2026-05-11", period_end="2026-05-17", total=22358.0, extract_hash="wk3")
        res = H.append_to_historical([wk3], "LOCATIONS_DATA", store_dir=d)
        check("later period -> appended=1", res.appended == 1)
        check("later period -> superseded=0 (wk1 NOT backfilled)", res.superseded == 0)
        by_period = {(r["period_start"], r["period_end"]): r for r in H.read_history("LOCATIONS_DATA", store_dir=d)}
        check("both periods present", len(by_period) == 2)
        check("earlier wk1 row UNCHANGED (still 8147, byte-identical round-trip)",
              by_period[("2026-05-01", "2026-05-03")] == wk1)
    finally:
        shutil.rmtree(d, ignore_errors=True)


def t_tables_independent():
    d = _fresh_dir()
    try:
        rloc = H.append_to_historical([loc_row()], "LOCATIONS_DATA", store_dir=d)
        rsty = H.append_to_historical([styl_row()], "STYLISTS_DATA", store_dir=d)
        check("LOCATIONS append independent (1)", rloc.appended == 1)
        check("STYLISTS append independent (1)", rsty.appended == 1)
        loc_back = H.read_history("LOCATIONS_DATA", store_dir=d)
        sty_back = H.read_history("STYLISTS_DATA", store_dir=d)
        check("LOCATIONS file holds only the salon row", len(loc_back) == 1 and "total_sales_net" in loc_back[0])
        check("STYLISTS file holds only the stylist row",
              len(sty_back) == 1 and "net_service" in sty_back[0] and "total_sales_net" not in sty_back[0])
        check("two separate files on disk",
              os.path.exists(os.path.join(d, "LOCATIONS_DATA.jsonl"))
              and os.path.exists(os.path.join(d, "STYLISTS_DATA.jsonl")))
    finally:
        shutil.rmtree(d, ignore_errors=True)


def t_stylist_content_hash_path():
    d = _fresh_dir()
    try:
        # Stylist rows carry NO source_extract_hash -> idempotency via content hash.
        H.append_to_historical([styl_row(net_service=1000.0)], "STYLISTS_DATA", store_dir=d)
        b1 = _bytes(d, "STYLISTS_DATA")
        same = H.append_to_historical([styl_row(net_service=1000.0)], "STYLISTS_DATA", store_dir=d)
        check("identical stylist reprocess -> skipped=1 (content hash)", same.skipped_duplicate == 1)
        check("stylist no-op byte-identical", _bytes(d, "STYLISTS_DATA") == b1)
        # A restated per-stylist figure (Karissa Q9 staff-level correction) supersedes.
        corr = H.append_to_historical([styl_row(net_service=1050.0)], "STYLISTS_DATA", store_dir=d)
        check("changed stylist value -> superseded=1", corr.superseded == 1)
        back = H.read_history("STYLISTS_DATA", store_dir=d)
        check("stylist live row reflects corrected 1050", len(back) == 1 and back[0]["net_service"] == 1050.0)
        check("stylist audit preserves old 1000", _audit(d, "STYLISTS_DATA")[0]["superseded_row"]["net_service"] == 1000.0)
    finally:
        shutil.rmtree(d, ignore_errors=True)


def t_within_batch_correction():
    d = _fresh_dir()
    try:
        # One batch carries two versions of the SAME key (a correction lands in the
        # same call): first appends, second supersedes it in-memory before write.
        batch = [loc_row(extract_hash="a", total=100.0), loc_row(extract_hash="b", total=150.0)]
        res = H.append_to_historical(batch, "LOCATIONS_DATA", store_dir=d)
        check("within-batch -> appended=1 and superseded=1", res.appended == 1 and res.superseded == 1)
        back = H.read_history("LOCATIONS_DATA", store_dir=d)
        check("within-batch final live row = last version (150)", len(back) == 1 and back[0]["total_sales_net"] == 150.0)
        check("within-batch audit preserves first version (100)",
              _audit(d, "LOCATIONS_DATA")[0]["superseded_row"]["total_sales_net"] == 100.0)
    finally:
        shutil.rmtree(d, ignore_errors=True)


def t_unknown_table_raises():
    try:
        H.append_to_historical([], "NOPE_DATA")
        check("unknown table raises", False)
    except ValueError:
        check("unknown table raises ValueError", True)


def t_empty_rows_noop():
    d = _fresh_dir()
    try:
        res = H.append_to_historical([], "LOCATIONS_DATA", store_dir=d)
        check("empty rows -> all-zero result", res.appended == 0 and res.skipped_duplicate == 0 and res.superseded == 0)
        check("empty rows -> no file created", not os.path.exists(os.path.join(d, "LOCATIONS_DATA.jsonl")))
    finally:
        shutil.rmtree(d, ignore_errors=True)


# ─────────────────────────────────────────────────────────────────────────────
# INTEGRATION — real Forest Lake extract -> history (gated; Track D skip pattern)
# ─────────────────────────────────────────────────────────────────────────────
_FL_PDF = str(_REPO_ROOT / "data" / "inbox" / "Forest Lake.pdf")
_FL_KEYFIELDS = ("location_id", "period_start", "period_end", "period_type")


def integration_forest_lake():
    if not os.path.exists(_FL_PDF):
        print("SKIP - integration: data/inbox/Forest Lake.pdf not found")
        return
    try:
        from parsers.locations_grouper import build_location_row
        from parsers.zenoti_stylist_parser import build_zenoti_stylist_rows
        loc = build_location_row(_FL_PDF, location_id="888-11812",
                                 period_start="2026-05-01", period_end="2026-05-24",
                                 period_type="mtd", period_label="MTD")
        styl = build_zenoti_stylist_rows(_FL_PDF)
    except Exception as e:  # missing pdfplumber/bs4, parse/reconcile error, etc.
        print(f"SKIP - integration: parse unavailable ({type(e).__name__}: {e})")
        return

    d = _fresh_dir()
    try:
        rloc = H.append_to_historical([loc], "LOCATIONS_DATA", store_dir=d)
        rsty = H.append_to_historical(styl, "STYLISTS_DATA", store_dir=d)
        check("[integration] LOCATIONS appended=1", rloc.appended == 1)
        check("[integration] STYLISTS appended == #stylists (>=1)", rsty.appended == len(styl) and len(styl) >= 1)

        loc_back = H.read_history("LOCATIONS_DATA", store_dir=d)
        want_key = H.composite_key(loc, _FL_KEYFIELDS)
        have_keys = {H.composite_key(r, _FL_KEYFIELDS) for r in loc_back}
        check("[integration] LOCATIONS composite key landed", want_key in have_keys)
        check("[integration] LOCATIONS row round-trips (total_sales_net intact)",
              loc_back[0]["total_sales_net"] == loc["total_sales_net"])
        check("[integration] STYLISTS all keys landed", len(H.read_history("STYLISTS_DATA", store_dir=d)) == len(styl))

        # Reprocess the SAME file (rebuilt rows: LOCATIONS generated_at differs, but
        # source_extract_hash is identical) -> fully idempotent, no growth.
        loc2 = build_location_row(_FL_PDF, location_id="888-11812",
                                  period_start="2026-05-01", period_end="2026-05-24",
                                  period_type="mtd", period_label="MTD")
        styl2 = build_zenoti_stylist_rows(_FL_PDF)
        rloc2 = H.append_to_historical([loc2], "LOCATIONS_DATA", store_dir=d)
        rsty2 = H.append_to_historical(styl2, "STYLISTS_DATA", store_dir=d)
        check("[integration] LOCATIONS reprocess idempotent (skipped=1, appended=0)",
              rloc2.skipped_duplicate == 1 and rloc2.appended == 0)
        check("[integration] STYLISTS reprocess idempotent (skipped=all, appended=0)",
              rsty2.skipped_duplicate == len(styl) and rsty2.appended == 0)
        print(f"-- integration filed {rloc.appended} salon + {rsty.appended} stylist rows from Forest Lake.pdf")
    finally:
        shutil.rmtree(d, ignore_errors=True)


if __name__ == "__main__":
    print("=== UNIT (synthetic rows; tmp JSONL store) ===")
    t_append_new()
    t_idempotent_byte_identical()
    t_supersede_with_audit()
    t_closed_week_stored()
    t_provisional_then_complete()
    t_anti_backfill()
    t_tables_independent()
    t_stylist_content_hash_path()
    t_within_batch_correction()
    t_unknown_table_raises()
    t_empty_rows_noop()
    print("\n=== INTEGRATION (real Forest Lake extract -> history; gated) ===")
    integration_forest_lake()
    print("\n" + ("ALL PASS" if not FAILS else f"{len(FAILS)} FAILED: {FAILS}"))
    sys.exit(1 if FAILS else 0)
