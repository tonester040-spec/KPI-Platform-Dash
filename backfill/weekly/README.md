# Weekly backfill drop zone

Tony downloads the weekly POS reports into here. The loader reads them and writes
penny-exact rows to the Google Sheet (`CUMULATIVE_MTD` + `STYLISTS_CUMULATIVE_MTD`).

**Full instructions: [`../../BACKFILL_RUNBOOK.md`](../../BACKFILL_RUNBOOK.md)** — read that first.

## Structure (the convention)

```
backfill/weekly/
  Zenoti/
    2025-06-01/        ← folder name = week-ending SUNDAY (ISO YYYY-MM-DD)
      Andover.xls          (9 Zenoti salon reports — one file per salon)
      Blaine.xls
      ... (9 total)
    2025-06-08/
      ...
  SU/
    2025-06-01/        ← same week-ending date as the Zenoti folder
      FS Salon Dashboard - Apple Valley ….xls     (salon)
      Provider Tracker Report - Apple Valley ….xls (stylist)
      ... Lakeville (x2), Farmington (x2)  →  6 files total
    ...
```

## Granularity (hybrid — 2026-05-31)

The folders that EXIST are exactly the ones to fill — no guessing.

- **Zenoti (24 folders):** recent **3 months weekly** (Mar/Apr/May 2026, all weeks) +
  **9 month-end-only** folders for the older months (Jun 2025–Feb 2026). The dashboard
  trend is month-over-month, so old months only need their full-month total. Use the
  **Salon Summary only** — NOT the Sales Accrual (the Salon Summary already carries
  salon + stylist; Sales Accrual is unused).
- **SU (59 folders):** already fully downloaded weekly — left as-is.

## Rules

- **Folder name = the week-ending Sunday, ISO `YYYY-MM-DD`.** Not "Week 1". The
  cumulative-MTD month is derived from this date, and it is the Sheet row key. (A
  month-end folder dated the month's last day = that month's full cumulative total.)
- **Zenoti = 1 file per salon** (Salon Summary, carries salon AND stylist) → 9 per week folder.
  **SU = 2 files per salon** (FS Salon Dashboard + Provider Tracker Report) → 6 per week folder.
- **The report files themselves are git-ignored** (bulk inputs, carry stylist
  names). Only this README + the empty bucket dirs are committed. You provide the
  files locally; you don't pull them.
- **Re-running is safe.** The load is idempotent on `(salon, week_ending)` — a week
  already in the Sheet is skipped, so you can fix one folder and re-run.
