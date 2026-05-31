"""
scripts/backfill/weekly_run.py
KPI Platform — WEEKLY backfill loader (salon + stylist grain).

Reads the downloaded POS reports from backfill/weekly/{Zenoti,SU}/<YYYY-MM-DD>/,
parses them penny-exact with the existing Track A/B/C/D parsers, and (on --write)
appends to the live Sheet's CUMULATIVE_MTD + STYLISTS_CUMULATIVE_MTD tabs. Idempotent
on (loc_name, year_month, week_ending) — re-running never duplicates.

THE CANONICAL GUIDE IS `BACKFILL_RUNBOOK.md` — read it first.

Folder convention (folder name = week-ending Sunday, the row key + source of month):
    backfill/weekly/Zenoti/<YYYY-MM-DD>/   9 files: each salon's Salon Summary
                                           (ONE file carries salon AND stylist data)
    backfill/weekly/SU/<YYYY-MM-DD>/       6 files: per SU salon, TWO reports —
                                           "FS Salon Dashboard" (salon) +
                                           "Provider Tracker Report" (stylist)

Usage:
    python -m scripts.backfill.weekly_run --status              # what's already loaded
    python -m scripts.backfill.weekly_run --week 2025-06-08 --dry-run
    python -m scripts.backfill.weekly_run --all --dry-run        # review everything on disk
    python -m scripts.backfill.weekly_run --week 2025-06-08 --write
    python -m scripts.backfill.weekly_run --all --write          # load every new week

Flags: --root (default backfill/weekly), --salon-only (skip stylist tab),
       --customer (default karissa_001), --accept "Loc:CODE" (accept a known anomaly).

DRY-RUN NEEDS NO CREDENTIALS — it parses + reconciles + prints a review table and
writes nothing. --status / --write need GOOGLE_SERVICE_ACCOUNT_JSON (base64, same as
the monthly backfill; .env is honored via python-dotenv).
"""
from __future__ import annotations

import argparse
import calendar
import sys
import traceback
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from parsers.locations_grouper import CROSSWALK  # noqa: E402

DEFAULT_ROOT = _REPO_ROOT / "backfill" / "weekly"
REPORT_EXTS = {".xls", ".xlsx", ".xlsm", ".pdf"}
SOURCE_TAG = "weekly_backfill"


# ─────────────────────────────────────────────────────────────────────────────
# Roster + filename → (salon, report-type) dispatch.
# ─────────────────────────────────────────────────────────────────────────────
# canonical name -> (location_id, pos_system)
ROSTER: dict[str, tuple[str, str]] = {
    v["location_name_canonical"]: (lid, v["pos_system"]) for lid, v in CROSSWALK.items()
}

# Match keys per salon: the canonical name + a de-suffixed core ("Andover FS"->"andover")
# so a file named "Andover.pdf" or "FS Salon Dashboard - Apple Valley.xls" resolves.
def _match_keys(name: str) -> list[str]:
    low = name.lower()
    keys = {low}
    if low.endswith(" fs"):
        keys.add(low[:-3].strip())
    return sorted(keys, key=len, reverse=True)  # longest first = most specific


_SALON_KEYS: list[tuple[str, str]] = sorted(
    ((k, name) for name in ROSTER for k in _match_keys(name)),
    key=lambda t: len(t[0]), reverse=True,
)


def resolve_salon(filename: str) -> str | None:
    """Which roster salon does this filename belong to? Longest key match wins."""
    low = filename.lower()
    for key, name in _SALON_KEYS:
        if key in low:
            return name
    return None


def resolve_report_type(filename: str, bucket: str) -> str:
    """'salon' or 'stylist'. Zenoti = one Salon Summary feeds both. SU splits:
    'Provider Tracker' -> stylist, else (Salon Dashboard) -> salon."""
    if bucket == "Zenoti":
        return "both"
    return "stylist" if "provider tracker" in filename.lower() else "salon"


# ─────────────────────────────────────────────────────────────────────────────
# Period derivation from the folder name (the week-ending date).
# ─────────────────────────────────────────────────────────────────────────────
def derive_period(week_ending: str) -> dict:
    """folder 'YYYY-MM-DD' -> period fields. Cumulative-MTD: start = 1st of month,
    end = the week-ending date, type = mtd."""
    year_month = week_ending[:7]
    period_start = f"{year_month}-01"
    return {
        "week_ending": week_ending,
        "year_month": year_month,
        "period_start": period_start,
        "period_end": week_ending,
        "period_type": "mtd",
        "period_label": f"MTD thru {week_ending}",
    }


# ─────────────────────────────────────────────────────────────────────────────
# Parser dispatch -> tab-row mapping.
# ─────────────────────────────────────────────────────────────────────────────
def _salon_to_cumulative(r: dict, p: dict) -> dict:
    """39-col LOCATIONS_DATA row -> CUMULATIVE_MTD 22-col shape (read-as-stored)."""
    return {
        "loc_name": r["location_name_canonical"],
        "year_month": p["year_month"],
        "week_ending": p["week_ending"],
        "platform": r["pos_system"],
        "guests": r["guest_count"],
        "total_sales": r["total_sales_net"],
        "service": r["service_net"],
        "product": r["product_net"],
        "product_pct": r["product_pct"],
        "ppg": r["ppg_net"],
        "pph": r["pph_net"],
        "avg_ticket": r["avg_ticket"],
        "prod_hours": r["productive_hours"],
        "wax_count": r["wax_count"],
        "wax": r["wax_net"],
        "wax_pct": r["wax_pct"],
        "color": r["color_net"],
        "color_pct": r["color_pct"],
        "treat_count": r["treatment_count"],
        "treat": r["treatment_net"],
        "treat_pct": r["treatment_pct"],
        "source": SOURCE_TAG,
    }


def _stylist_to_cumulative(s: dict, p: dict) -> dict:
    """18-key STYLISTS_DATA row -> STYLISTS_CUMULATIVE_MTD 15-col shape."""
    return {
        "year_month": p["year_month"],
        "week_ending": p["week_ending"],
        "name": s["name"],
        "loc_name": s["loc_name"],
        "loc_id": s["loc_id"],
        "platform": s["platform"],
        "invoices": s.get("invoices"),
        "guests": s.get("guests"),
        "net_service": s.get("net_service"),
        "net_product": s.get("net_product"),
        "avg_ticket": s.get("avg_ticket"),
        "pph": s.get("pph"),
        "ppg": s.get("ppg"),
        "production_hours": s.get("production_hours"),
        "source": SOURCE_TAG,
    }


def parse_file(path: Path, bucket: str, salon: str, p: dict, *, salon_only: bool):
    """Parse one report file -> ([cumulative salon rows], [cumulative stylist rows]).
    Raises on parse/reconcile failure (the parsers fail loud); the caller flags it."""
    loc_id, _platform = ROSTER[salon]
    rtype = resolve_report_type(path.name, bucket)
    salon_rows: list[dict] = []
    stylist_rows: list[dict] = []

    if bucket == "Zenoti":
        # One Salon Summary feeds both grains.
        from parsers.locations_grouper import build_location_row
        sr = build_location_row(str(path), location_id=loc_id,
                                period_start=p["period_start"], period_end=p["period_end"],
                                period_type=p["period_type"], period_label=p["period_label"])
        salon_rows.append(_salon_to_cumulative(sr, p))
        if not salon_only:
            from parsers.zenoti_stylist_parser import build_zenoti_stylist_rows
            for s in build_zenoti_stylist_rows(str(path), location_id=loc_id,
                                               year_month=p["year_month"]):
                stylist_rows.append(_stylist_to_cumulative(s, p))
    else:  # SU
        if rtype == "salon":
            from parsers.su_dashboard_parser import build_su_location_row
            sr = build_su_location_row(str(path), location_id=loc_id,
                                       period_start=p["period_start"], period_end=p["period_end"],
                                       period_type=p["period_type"], period_label=p["period_label"])
            salon_rows.append(_salon_to_cumulative(sr, p))
        elif rtype == "stylist" and not salon_only:
            from parsers.su_provider_tracker_parser import build_su_stylist_rows
            for s in build_su_stylist_rows(str(path), location_id=loc_id,
                                           year_month=p["year_month"]):
                stylist_rows.append(_stylist_to_cumulative(s, p))
    return salon_rows, stylist_rows


# ─────────────────────────────────────────────────────────────────────────────
# Per-week processing (parse + collect + flag).
# ─────────────────────────────────────────────────────────────────────────────
def process_week(root: Path, week_ending: str, *, salon_only: bool):
    """Parse every file in both buckets for one week. Returns
    (salon_rows, stylist_rows, problems) — problems is a list of (file, error)."""
    p = derive_period(week_ending)
    salon_rows: list[dict] = []
    stylist_rows: list[dict] = []
    problems: list[tuple[str, str]] = []

    for bucket in ("Zenoti", "SU"):
        wdir = root / bucket / week_ending
        if not wdir.is_dir():
            continue
        for f in sorted(wdir.iterdir()):
            if not f.is_file() or f.suffix.lower() not in REPORT_EXTS:
                continue
            salon = resolve_salon(f.name)
            if salon is None:
                problems.append((f.name, "could not match a salon by filename"))
                continue
            try:
                sr, st = parse_file(f, bucket, salon, p, salon_only=salon_only)
                salon_rows.extend(sr)
                stylist_rows.extend(st)
            except Exception as e:  # parsers fail loud on reconcile/parse errors
                problems.append((f.name, f"{type(e).__name__}: {e}"))
    return salon_rows, stylist_rows, problems


def weeks_on_disk(root: Path) -> list[str]:
    """Distinct week-ending folder names that actually contain a report file."""
    found: set[str] = set()
    for bucket in ("Zenoti", "SU"):
        bdir = root / bucket
        if not bdir.is_dir():
            continue
        for wdir in bdir.iterdir():
            if wdir.is_dir() and any(
                f.suffix.lower() in REPORT_EXTS for f in wdir.iterdir() if f.is_file()
            ):
                found.add(wdir.name)
    return sorted(found)


# ─────────────────────────────────────────────────────────────────────────────
# Review rendering.
# ─────────────────────────────────────────────────────────────────────────────
def _f(v, money=False):
    if v is None:
        return "—"
    if money and isinstance(v, (int, float)):
        return f"{v:,.2f}"
    return str(v)


def render_week(week_ending: str, salon_rows, stylist_rows, problems):
    print(f"\n── Week ending {week_ending} " + "─" * 40)
    if salon_rows:
        print(f"  {'Salon':<16}{'Guests':>8}{'Total':>13}{'Service':>13}{'Product':>12}")
        tg = tt = tsv = tpr = 0.0
        for r in sorted(salon_rows, key=lambda x: x["loc_name"]):
            print(f"  {r['loc_name']:<16}{_f(r['guests']):>8}{_f(r['total_sales'], 1):>13}"
                  f"{_f(r['service'], 1):>13}{_f(r['product'], 1):>12}")
            tg += r["guests"] or 0; tt += r["total_sales"] or 0
            tsv += r["service"] or 0; tpr += r["product"] or 0
        print(f"  {'TOTAL':<16}{tg:>8.0f}{tt:>13,.2f}{tsv:>13,.2f}{tpr:>12,.2f}")
    print(f"  salon rows: {len(salon_rows)}   stylist rows: {len(stylist_rows)}", end="")
    print(f"   ⚠ problems: {len(problems)}" if problems else "   ✓ clean")
    for fn, err in problems:
        print(f"     ⚠ {fn}: {err}")


# ─────────────────────────────────────────────────────────────────────────────
# Sheet I/O (status / write) — credentialed.
# ─────────────────────────────────────────────────────────────────────────────
def _load_config(customer: str) -> dict:
    import json
    path = _REPO_ROOT / "config" / "customers" / f"{customer}.json"
    return json.loads(path.read_text(encoding="utf-8"))


def _build_service(config: dict):
    from core import sheets_writer as sw
    return sw._build_service(config)


def cmd_status(config: dict):
    """Report which (week_ending) snapshots already exist in CUMULATIVE_MTD."""
    from core import data_source
    service = _build_service(config)
    # Read every distinct year_month present, then coverage by week.
    by_week: dict[str, set] = {}
    # data_source reads per year_month; sweep the months we care about (disk + a wide net).
    months = sorted({w[:7] for w in weeks_on_disk(DEFAULT_ROOT)}) or \
        [f"{y}-{m:02d}" for y in (2025, 2026) for m in range(1, 13)]
    for ym in months:
        for s in data_source.read_cumulative_mtd_snapshots(service, config, ym):
            by_week.setdefault(s["week_ending"], set()).add(s["loc_name"])
    print(f"\n=== CUMULATIVE_MTD coverage ({len(by_week)} weeks loaded) ===")
    for wk in sorted(by_week):
        print(f"  {wk}: {len(by_week[wk])}/12 salons")
    disk = set(weeks_on_disk(DEFAULT_ROOT))
    remaining = sorted(disk - set(by_week))
    print(f"\non disk, NOT yet loaded: {len(remaining)} week(s)"
          + (": " + ", ".join(remaining) if remaining else ""))


def write_week(config, service, salon_rows, stylist_rows, *, salon_only):
    from core import sheets_writer as sw
    if salon_rows:
        sw.append_to_cumulative_mtd(service, config, salon_rows, dry_run=False)
    if stylist_rows and not salon_only:
        sw.append_to_stylists_cumulative_mtd(service, config, stylist_rows, dry_run=False)


# ─────────────────────────────────────────────────────────────────────────────
# CLI.
# ─────────────────────────────────────────────────────────────────────────────
def main(argv=None):
    ap = argparse.ArgumentParser(description="Weekly backfill loader — see BACKFILL_RUNBOOK.md")
    ap.add_argument("--root", default=str(DEFAULT_ROOT))
    ap.add_argument("--customer", default="karissa_001")
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--week", help="one week-ending folder, YYYY-MM-DD")
    g.add_argument("--all", action="store_true", help="every week present on disk")
    g.add_argument("--status", action="store_true", help="what's already in the Sheet")
    ap.add_argument("--write", action="store_true", help="append to the Sheet (default: dry-run)")
    ap.add_argument("--dry-run", action="store_true",
                    help="parse + reconcile + review, write nothing (the default; needs no creds)")
    ap.add_argument("--salon-only", action="store_true", help="skip the stylist tab")
    args = ap.parse_args(argv)
    root = Path(args.root)

    if args.status:
        cmd_status(_load_config(args.customer))
        return 0

    if args.week:
        weeks = [args.week]
    elif args.all:
        weeks = weeks_on_disk(root)
        if not weeks:
            print(f"No populated week folders under {root}. Drop reports in first.")
            return 0
    else:
        ap.error("choose one of --week / --all / --status")

    config = service = None
    if args.write:
        config = _load_config(args.customer)
        service = _build_service(config)

    grand_salon = grand_stylist = total_problems = 0
    for wk in weeks:
        salon_rows, stylist_rows, problems = process_week(root, wk, salon_only=args.salon_only)
        render_week(wk, salon_rows, stylist_rows, problems)
        grand_salon += len(salon_rows); grand_stylist += len(stylist_rows)
        total_problems += len(problems)
        if args.write:
            if problems:
                print(f"  ✗ SKIPPING WRITE for {wk} — fix the {len(problems)} problem(s) first.")
                continue
            write_week(config, service, salon_rows, stylist_rows, salon_only=args.salon_only)
            print(f"  ✓ wrote {len(salon_rows)} salon + {len(stylist_rows)} stylist rows to the Sheet")

    mode = "WROTE" if args.write else "DRY-RUN (nothing written)"
    print(f"\n=== {mode}: {len(weeks)} week(s), {grand_salon} salon + {grand_stylist} stylist "
          f"rows, {total_problems} problem(s) ===")
    return 1 if (total_problems and not args.write) else 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception:
        traceback.print_exc()
        sys.exit(1)
