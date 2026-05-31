# Weekly Backfill Runbook — the canonical guide

**If you are a fresh session: read this file + `CLAUDE.md`, then run the loader in
`--status` mode (below). That tells you exactly which weeks are already loaded and
what's left. Nothing else needs to be remembered — the Sheet is the source of truth
for progress, and the load is idempotent so re-running is always safe.**

This is the single source of truth for the historical backfill. It supersedes any
ad-hoc notes. Last updated 2026-05-29.

---

## 1. The goal

Load ~1 year of **weekly** POS history for **all 12 salons**, at **salon grain**
*and* **stylist grain**, into the live Google Sheet — so the report generator and
the GitHub dashboard light up with a year of real, penny-exact history (trends, YoY,
and the Jess/Jenn coaching + retention view).

- **Salon grain →** `CUMULATIVE_MTD` tab (feeds `core/report_generator.py` + the
  cumulative→weekly differencing).
- **Stylist grain →** `STYLISTS_CUMULATIVE_MTD` tab (feeds the coaching dashboard).

## 2. Why files, not Tableau (decided 2026-05-29)

The weekly **reports** are the penny-exact source and our parsers already read them
to the cent (Tracks A/B/C/D, all 12 salons verified). The Zenoti **Tableau** API was
evaluated and rejected for backfill: its `KPI_Extracts` views are lifetime totals
with **no date dimension** (one stylist showed 12,449 invoices), and Tableau is
**Zenoti-only** (no Salon Ultimate). The reports cover all 12 salons and carry their
own date range. Files win. (See the Tableau notes in the 2026-05-29 session.)

## 3. Non-negotiable facts (so a fresh session doesn't re-derive them)

- **The live Google Sheet is the store** (id `1JY6L7H1Pb2JFmNoz2XNkvG0ogrYgagLVDwH01vuWT28`).
  We do NOT keep a local JSONL history. See [[report-generator-canonical]] /
  `core/report_generator.py`.
- **The Sheet is also the progress ledger.** `CUMULATIVE_MTD` is keyed by
  `(loc_name, year_month, week_ending)`. To know what's done, read it (the loader's
  `--status` does this). Don't rely on a hand-kept checklist.
- **Idempotent.** `core/sheets_writer.append_to_cumulative_mtd` /
  `append_to_stylists_cumulative_mtd` skip rows whose key already exists. Re-running
  a week, a month, or the whole tree never duplicates.
- **Cumulative-MTD model.** Each weekly report is month-to-date (Week 3 already
  contains Wk1+2+3). We store each snapshot AS-IS; we never sum weeks. The week's
  `period_end` (the Sunday) is the key; `year_month` derives from it; `period_start`
  is the 1st of that month.
- **Read-as-stored.** The parsers compute the canonical ratios; we write them
  verbatim. No recompute in the loader.

## 4. Folder convention

```
backfill/weekly/
  Zenoti/<YYYY-MM-DD>/   9 files — ONE Salon Summary per salon (carries salon AND stylist)
  SU/<YYYY-MM-DD>/       6 files — per SU salon TWO reports: FS Salon Dashboard (salon)
                                  + Provider Tracker Report (stylist)   [SAME week-ending date]
```

- Folder name = **week-ending Sunday, ISO `YYYY-MM-DD`** (e.g. `2025-06-01`). This
  is the row key and the source of `year_month` — **never** "Week 1".
- Salon is identified by **filename** (matched to the roster, e.g. `Andover.*`,
  `Apple Valley.*`, or `FS Salon Dashboard - Apple Valley …`). **Zenoti = 1 file/salon**
  (the Salon Summary carries both grains); **SU = 2 files/salon** (Dashboard + Provider
  Tracker — SU splits salon and stylist into separate reports).
- Report files are **git-ignored** (bulk, carry stylist names). Provided locally.

### The 12 salons (filename ↔ canonical name ↔ id)

| Bucket | Filename stem | Canonical name | location_id |
|--------|---------------|----------------|-------------|
| Zenoti | Andover       | Andover FS     | 888-10278 |
| Zenoti | Blaine        | Blaine         | 888-9816  |
| Zenoti | Crystal       | Crystal FS     | 888-7663  |
| Zenoti | Elk River     | Elk River FS   | 887-10199 |
| Zenoti | Forest Lake   | Forest Lake    | 888-11812 |
| Zenoti | Prior Lake    | Prior Lake     | 888-11091 |
| Zenoti | Hudson        | Hudson         | 910-7232  |
| Zenoti | New Richmond  | New Richmond   | 910-6916  |
| Zenoti | Roseville     | Roseville      | 888-40098 |
| SU     | Apple Valley  | Apple Valley   | z010      |
| SU     | Lakeville     | Lakeville      | su001     |
| SU     | Farmington    | Farmington     | su002     |

(Source of truth: `parsers/locations_grouper.py::CROSSWALK` +
`config/customers/karissa_001.json`.)

## 5. The loader — `scripts/backfill/weekly_run.py`

> **BUILD STATUS: BUILT + dry-run tested 2026-05-31.** Validated penny-exact on real
> samples — Forest Lake (Zenoti: salon + 6 stylists) and Apple Valley (SU: salon + 17
> stylists), both reconciled clean. Ready to run. `--dry-run` needs NO credentials.

### Usage

```bash
# What's already loaded? (reads CUMULATIVE_MTD — your resume map)
python -m scripts.backfill.weekly_run --status

# Dry-run a single week (parse + reconcile + print review table; NO writes)
python -m scripts.backfill.weekly_run --week 2025-06-01 --dry-run

# Dry-run EVERY week present on disk (review grouped by month)
python -m scripts.backfill.weekly_run --all --dry-run

# Write a single week (after eyeballing its dry-run)
python -m scripts.backfill.weekly_run --week 2025-06-01 --write

# Write every not-yet-loaded week, week by week until done (idempotent)
python -m scripts.backfill.weekly_run --all --write
```

`--root backfill/weekly` is the default. `--salon-only` skips the stylist tab.
`--accept "LOC:CODE"` accepts a known reconcile anomaly (e.g. Prior Lake's $34
stylist-sum drift), mirroring the monthly `run_batch`.

### What it does, per week folder

1. Resolve `week_ending` from the folder name; `year_month = week_ending[:7]`;
   `period_start = first-of-month`; `period_type = "mtd"`.
2. **Zenoti/** — for each of 9 files:
   - salon: `parsers/locations_grouper.build_location_row(...)`
   - stylist: `parsers/zenoti_stylist_parser.build_zenoti_stylist_rows(...)`
3. **SU/** — for each of 3 files:
   - salon: `parsers/su_dashboard_parser.build_su_location_row(...)`
   - stylist: `parsers/su_provider_tracker_parser.build_su_stylist_rows(...)`
4. Map parser output → tab shapes (§6), reconcile (§8), and either render the
   review table (`--dry-run`) or append via the idempotent writers (`--write`).

## 6. Field mapping (parser → Sheet tab)

**Salon → `CUMULATIVE_MTD`** (22 cols). The parser emits the 39-col LOCATIONS_DATA
schema; map the snake_case subset:

| CUMULATIVE_MTD | from parser row |
|---|---|
| loc_name | location_name_canonical |
| year_month / week_ending | derived from folder date |
| platform | pos_system |
| guests | guest_count |
| total_sales / service / product | total_sales_net / service_net / product_net |
| product_pct / ppg / pph / avg_ticket | product_pct / ppg_net / pph_net / avg_ticket |
| prod_hours | productive_hours |
| wax_count / wax | wax_count / wax_net |
| color | color_net |
| treat_count / treat | treatment_count / treatment_net |
| wax_pct / color_pct / treat_pct | wax_pct / color_pct / treatment_pct |
| source | `"weekly_backfill"` |

**Stylist → `STYLISTS_CUMULATIVE_MTD`** (15 cols): the shared 18-key STYLISTS_DATA
dict maps near-directly — `name, loc_name, loc_id, platform, invoices, guests,
net_service, net_product, avg_ticket, pph, ppg, production_hours, source` + the
derived `year_month, week_ending`.

## 7. The run workflow

1. **Organize** — drop the week folders into `backfill/weekly/{Zenoti,SU}/`,
   date-named.
2. **`--status`** — see what's already loaded; pick the next gap.
3. **`--all --dry-run`** — review the parsed numbers (per month, with reconcile
   ✓/✗). Spot-check a salon or two against the source report.
4. **`--all --write`** — load. Idempotent, so this only adds the new weeks.
5. **Verify** — re-run `--status`; optionally regenerate a report month
   (`python -m core.report_generator <YYYY-MM>`) and eyeball.

## 8. Reconciliation / accuracy

The parsers penny-check internally (salon totals must tie; stylist sums reconcile to
salon for money). A failing salon/week is **flagged in the review and skipped on
write** unless `--accept`ed. Known accepted anomaly: Prior Lake stylist-sum ~$34
(salon-level corrections don't always reach per-stylist totals — Karissa Q9).
Guest-count is NOT reconciled stylist-vs-salon (expected to differ).

## 9. Scope + known gaps (set expectations honestly)

- **All 12 salons come from reports** — the 3 SU salons aren't in Tableau but their
  reports parse fine (Track C/D).
- **SU weekly = TWO `.xls` reports per salon (CONFIRMED 2026-05-31):** *FS Salon
  Dashboard* (salon → Track C `build_su_location_row`) + *Provider Tracker Report*
  (stylist → Track D `build_su_stylist_rows`). Both parse penny-clean (the `.xls` path
  needs **LibreOffice** installed). The loader dispatches SU files by filename
  ("provider tracker" → stylist, else salon); Zenoti's single Salon Summary feeds both.
- **Dashboard fields NOT in these reports:** salon/stylist **rebook %**, stylist
  **tenure / status**, and possibly **request %**. These are retention/HR metrics
  the weekly performance reports may not carry. The backfill lights up the money +
  guests + service-mix + hours history (the bulk of the dashboard); rebook/tenure
  need a separate source (a retention report or manual entry) — a follow-up, not a
  blocker.
- **2025 prior-year** rows fill the report's YoY columns once a full prior year is
  loaded.

## 10. Progress log

Authoritative progress = `CUMULATIVE_MTD` in the Sheet (use `--status`). This table
is a human convenience; update it as ranges complete.

| Date range loaded | Salons | Stylist tab? | By | Notes |
|---|---|---|---|---|
| _(none yet — first run pending)_ | | | | |

## 11. Auth / environment

Writes need `GOOGLE_SERVICE_ACCOUNT_JSON` (base64 service-account key) exported in
the shell — same as the monthly backfill (`scripts/backfill/run_batch.py`). The CLI
tries `python-dotenv`; a `.env` may supply it. `--dry-run` needs no credentials.
