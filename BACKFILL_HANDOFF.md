# Weekly Backfill — Session Handoff (2026-05-31)

**You are a fresh session taking over a weekly historical backfill. Read this top to
bottom, then `CLAUDE.md`, `BACKFILL_RUNBOOK.md`, and `KARISSA_GOLDEN_RULES.md`. The
goal of this session: get `scripts/backfill/weekly_run.py` running locally and load
~1 year of penny-exact history into the live Google Sheet.**

---

## 0. The one thing that's different about this session

**You are running LOCALLY on Tony's Windows machine** (`C:\Users\tones\OneDrive\
Documents\GitHub\KPI-Platform-Dash`, Python **3.14**), where the downloaded report
files actually live. Earlier sessions ran on a separate cloud VM that could NOT see
the files (the report files are git-ignored, so they never sync over git — only the
empty folder structure does). That's why we're moving local: so you can install deps,
run the loader, and see the real `.xls` files.

Don't try to `git push` from here unless asked — Tony manages that. Your job is to get
the loader green locally.

## 1. Where the backfill stands

- **Loader is BUILT + validated:** `scripts/backfill/weekly_run.py`. Dry-run proven
  penny-exact on samples (Forest Lake Zenoti + Apple Valley SU). Walks
  `backfill/weekly/{Zenoti,SU}/<YYYY-MM-DD>/`, dispatches by bucket + filename, parses,
  reconciles, and on `--write` appends to `CUMULATIVE_MTD` + `STYLISTS_CUMULATIVE_MTD`.
- **Files are downloaded and correctly placed** (Tony verified naming via a dry-run —
  every file matched the right salon + parser, zero structure/naming errors):
  - `backfill/weekly/SU/` — all **59 weeks**, 6 files each (FS Salon Dashboard +
    Provider Tracker per salon).
  - `backfill/weekly/Zenoti/` — **24 folders** (recent 3 months weekly Mar/Apr/May 2026
    + 9 month-end folders Jun 2025–Feb 2026), 9 Salon Summaries each.
- **Reports are `.xls`:** Zenoti = HTML tables (read with BeautifulSoup); SU = binary
  OLE2 (read via LibreOffice → openpyxl). No PDFs anywhere in the backfill.

## 2. THE BLOCKER and THE FIX (do this first)

The last dry-run found all 59 weeks and matched every file, but produced **0 rows** —
purely a Python-environment problem:

1. `python -m pip install -r requirements.txt` **fails on PyMuPDF 1.23.26** (no wheel
   for Python 3.14 → tries to compile → needs Visual Studio). That failure aborts the
   whole install, so `openpyxl` and `bs4` never get installed.
2. **PyMuPDF is NOT needed for the backfill** (verified: none of the four backfill
   parsers import `fitz`). So:

```powershell
cd C:\Users\tones\OneDrive\Documents\GitHub\KPI-Platform-Dash
git pull
python -m pip install -r requirements-backfill.txt     # minimal, PyMuPDF-free, 3.14-safe
```

3. **Install LibreOffice** (https://www.libreoffice.org/download) — required for the SU
   `.xls`. The parser auto-finds `C:\Program Files\LibreOffice\program\soffice.exe`.
   (Zenoti `.xls` are HTML + bs4 — they don't need it.)

Use `python -m pip` (not bare `pip`) so packages land in the same interpreter that runs
`python -m scripts...` — a `pip`/`python` mismatch already bit us once (openpyxl "missing"
despite being in requirements).

## 3. Verify (no credentials needed)

```powershell
python -m scripts.backfill.weekly_run --all --dry-run
```

Expect each week to print a per-salon table (Guests / Total / Service / Product) + a
stylist count + `✓ clean`. **The dry-run IS the verification** — it reconciles every
file penny-exact and writes nothing. If a salon/week fails reconcile, it's flagged and
(on write) skipped. Paste any new errors to Tony / debug them here.

Likely next bumps after the install, and how to read them:
- `ModuleNotFoundError: bs4` → the install didn't take (wrong interpreter) — re-run with
  `python -m pip`.
- `openpyxl is required to read SU dashboards` → same (install didn't take).
- `soffice` / LibreOffice not found → install LibreOffice (step 2.3).

## 4. Write to the Sheet (needs credentials)

Once the dry-run is clean:

```powershell
python -m scripts.backfill.weekly_run --all --write     # idempotent; only adds new weeks
python -m scripts.backfill.weekly_run --status          # coverage in the Sheet vs disk
```

`--write` needs **`GOOGLE_SERVICE_ACCOUNT_JSON`** (base64 of the Google service-account
key JSON) in the environment or a local `.env` (the CLI honors `python-dotenv`). Tony has
access to the Google Cloud project that owns the service account; this is the only step
that needs the key. The target Sheet is `1JY6L7H1Pb2JFmNoz2XNkvG0ogrYgagLVDwH01vuWT28`.

## 5. Facts already settled (don't re-derive these)

- **Zenoti = Salon Summary ONLY** (1 file/salon — it carries salon AND stylist). NOT the
  Sales Accrual (its only extra is unique-guest-per-stylist, which Karissa doesn't use).
- **SU = 2 files/salon**: FS Salon Dashboard (salon) + Provider Tracker Report (stylist).
- **Cumulative-MTD:** every report = 1st-of-month → its folder date; numbers accumulate;
  we never sum weeks (see `KARISSA_GOLDEN_RULES.md` "Report Time Model").
- **Hybrid granularity:** Zenoti recent-3-months weekly + older months month-end-only; SU
  all 59 weeks. The folders that EXIST are the ones to load.
- **Read-as-stored, penny-exact, idempotent** on `(loc_name, year_month, week_ending)`.
- **The whole data→deliverable chain is built:** parsers (Tracks A–D) → this loader →
  `CUMULATIVE_MTD`/`STYLISTS_CUMULATIVE_MTD` → `core/report_generator.py` (Karissa's weekly
  Excel) + the GitHub dashboard. This backfill is the data that lights it all up.

## 6. After the backfill loads

- Regenerate a report month to eyeball: `python -m core.report_generator 2026-05`.
- The 2025 YoY columns can come from Karissa's hand-kept monthly YoY breakdown (Q13) —
  ask Tony for that file rather than downloading a year of 2025 weeklies.

**Canonical references:** `CLAUDE.md` (project), `BACKFILL_RUNBOOK.md` (backfill canon),
`KARISSA_GOLDEN_RULES.md` (KPI formulas + report time model).
