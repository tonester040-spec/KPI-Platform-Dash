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

## Rules

- **Folder name = the week-ending Sunday, ISO `YYYY-MM-DD`.** Not "Week 1". The
  cumulative-MTD month is derived from this date, and it is the Sheet row key.
- **Zenoti = 1 file per salon** (the Salon Summary carries salon AND stylist) → **9 per Zenoti week**.
  **SU = 2 files per salon** (FS Salon Dashboard + Provider Tracker Report) → **6 per SU week**.
- **The report files themselves are git-ignored** (bulk inputs, carry stylist
  names). Only this README + the empty bucket dirs are committed. You provide the
  files locally; you don't pull them.
- **Re-running is safe.** The load is idempotent on `(salon, week_ending)` — a week
  already in the Sheet is skipped, so you can fix one folder and re-run.
