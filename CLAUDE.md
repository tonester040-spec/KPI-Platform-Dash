# KPI — Karissa Performance Intelligence
### Claude project context — read this before every session

---

## 🚨 READ FIRST: Karissa's Golden Rules

**Before computing or displaying ANY KPI, read [`KARISSA_GOLDEN_RULES.md`](./KARISSA_GOLDEN_RULES.md) at the repo root.**

It's the canonical formula spec — every rule Karissa has confirmed (via two rounds of Q&A on 2026-05-27), every edge case, every reconciliation we owe her, and the source-of-truth on which fields come from which POS section. If anything in CLAUDE.md's KPI section conflicts with that doc, **the doc wins**. CLAUDE.md will fall out of sync occasionally; the Golden Rules doc gets updated every time a new Karissa answer changes a contract.

Specifically, do not skip `KARISSA_GOLDEN_RULES.md` §3 (PPG reconciliation), §6 (Color %), or §7 (edge cases) — these are the spots where naive math diverges from what Karissa wants on the printed dashboard.

---

## What this project is

A weekly salon analytics platform for **Karissa**, a multi-location salon owner in Minnesota. Two automated pipelines run via GitHub Actions:

1. **Weekly KPI pipeline** — Every Monday at 7:00 AM Central. Reads salon performance data from a Google Sheet, generates AI commentary, builds HTML dashboards for 3 different managers, sends an Excel report by email, and pushes everything to GitHub Pages.
2. **Daily Email Assistant** — Every weekday (Mon–Fri) at 7:30 AM Central. Reads Karissa's Gmail inbox, filters noise, categorizes real emails via Claude, generates draft replies in Karissa's voice, and publishes a morning debrief page to GitHub Pages.

**Live dashboard:** https://tonester040-spec.github.io/KPI-Platform-Dash/
**Morning debrief:** https://tonester040-spec.github.io/KPI-Platform-Dash/karissa-debrief.html
**GitHub repo:** https://github.com/tonester040-spec/KPI-Platform-Dash
**Google Sheet ID:** `1JY6L7H1Pb2JFmNoz2XNkvG0ogrYgagLVDwH01vuWT28`
**Owner contact:** Tony (tonester60@hotmail.com) — not Karissa's dev, he's building this FOR her

---

## Architecture at a glance

### Pipeline 1 — Weekly KPI (Mondays)

```
parsers/gmail_attachment_watcher.py (Step 0 — runs BEFORE main.py in the workflow)
    ↓ pulls weekly POS export attachments from karissaperformanceintelligence@gmail.com
    ↓ (validates sender via headers, SHA256 dedup, archives, writes manifest)
    ↓ writes data/inbox/*.xlsx + data/inbox/manifest_YYYY-MM-DD.json
    ↓ (Tier 2 batch processor — future — consumes manifest, populates Google Sheets)
    ↓
Google Sheets (source of truth — Karissa's team enters current week into CURRENT tab;
              Tier 2 will auto-populate from POS exports once wired)
    ↓
main.py (pipeline orchestrator)
    ↓ reads
core/data_source.py       → reads CURRENT (locations), STYLISTS_DATA, DATA (history) tabs
core/cumulative_pipeline.py → snapshot CURRENT (cumulative-MTD) to CUMULATIVE_MTD,
                             look up prior week's snapshot for same year_month,
                             difference current vs prior to produce TRUE WEEKLY
                             records. Per Karissa 2026-05-26: source POS reports
                             are cumulative-MTD with hard month boundaries.
                             Week 1 of month = current as-is (no prior to subtract).
                             Derived KPIs recomputed from differenced primitives.
core/data_processor.py    → enriches, ranks, flags (now operates on true weekly)
core/ai_cards.py          → Claude API summaries per location + stylist
                             (claude-haiku-4-5-20251001 for bulk stylist cards,
                              claude-sonnet-4-6 for coach briefing)
core/ai_coach_cards.py    → Claude API coach cards for Jess & Jenn (claude-sonnet-4-6)
                             Hardened prompt: Observation → Context → Question format
                             Falls back to dry-run placeholder on JSON parse failure
core/sheets_writer.py     → writes CURRENT, STYLISTS_CURRENT, ALERTS tabs back
                             + JESS_BRIEF (ALERTS!A100) and JENN_BRIEF (ALERTS!A101)
core/report_generator.py → generates monthly Excel workbook in Karissa's exact
                             layout (5 weekly tabs + Year Over Year) from the LIVE
                             SHEET (via core/data_source.py): CUMULATIVE_MTD +
                             DATA_MONTHLY + MONTHLY_GOALS. Read-as-stored + canonical
                             consistent headers + drift-proof header mapping.
                             Supersedes karissa_workbook.py (consolidated 2026-05-29;
                             legacy report_builder.py deleted 2026-05-27).
core/dashboard_builder.py → builds docs/index.html, docs/jess.html, docs/jenn.html
                             + injects COACH_CARD_DATA JS constant into manager HTML files
core/email_sender.py      → sends Excel to Tony (tonester60@hotmail.com) via Gmail App Password
                             + sends HTML coach card emails to Jess & Jenn (when email configured)
core/git_pusher.py        → commits docs/ locally (workflow step pushes to main)
    ↓
data/logs/pipeline_YYYYMMDD_HHMMSS.log  → uploaded as GitHub Actions artifact (30 days)
```

### Pipeline 2 — Email Assistant (Mon–Fri mornings)

```
Gmail inbox (Karissa's email — OAuth access)
    ↓
email_assistant/run_assistant.py (orchestrator)
    ↓
email_assistant/gmail_connector.py   → Gmail OAuth (GMAIL_CLIENT_ID/SECRET/REFRESH_TOKEN)
email_assistant/noise_filter.py      → drops marketing/automated noise
email_assistant/categorizer.py       → categorizes real emails via Claude (urgency, tasks)
email_assistant/draft_generator.py   → generates draft replies in Karissa's voice via Claude
email_assistant/debrief_builder.py   → builds docs/karissa-debrief.html
email_assistant/friday_recap.py      → fetches week's emails for Friday summary (Fridays only)
    ↓
docs/karissa-debrief.html → published to GitHub Pages (committed + pushed by workflow)
```

### Voice profile (one-time setup)

```
voice/samples/           → 30-40 of Karissa's sent emails as .txt (gitignored — never committed)
    ↓
email_assistant/build_profile.py → analyzes samples, generates voice profile
email_assistant/voice_profile.py → used by draft_generator at runtime
    ↓
voice/karissa_voice_profile.json → committed (style metadata only, no real email content)
```

### GitHub Pages (public PWA)

```
docs/index.html          → Karissa's full dashboard (all 12 locations) — 3 tabs: Locations, Stylists, (no coach card)
docs/jess.html           → Jess's PIN-gated dashboard (her 4 locations) — 4 tabs: Locations, Stylists, Coach Card, Visit Prep
docs/jenn.html           → Jenn's PIN-gated dashboard (her 5 locations) — 4 tabs: Locations, Stylists, Coach Card, Visit Prep
docs/owners.html         → Private owner dashboard (John/Patti) — PIN 7291, never linked publicly
docs/karissa-debrief.html → Daily morning email debrief (rebuilt Mon–Fri by email_assistant)
docs/manifest.json + docs/sw.js → PWA (installable on iPhone)
docs/offline.html        → shown when app opened with no connection
docs/icons/              → icon-192.png, icon-512.png
docs/kpi-demo.html       → prototype/demo file (not regenerated by pipeline)
docs/kpi-dashboard-v2.html → prototype/demo file (not regenerated by pipeline)
docs/kpi-music.mp3       → audio file (not part of pipeline)
```

---

## KPI formulas — Karissa's canonical definitions (MISSION CRITICAL)

**These formulas come directly from Karissa and govern every KPI the pipeline computes or displays. Do NOT trust the pre-computed statistics printed on the POS PDFs when they conflict with these rules — compute from first principles.** Ignoring this contract silently ships wrong numbers to coaches and owners.

### Guest count — the denominator for everything else

Definition differs by POS platform:

| Platform       | `guest_count` formula                                     | PDF source                                          |
|----------------|-----------------------------------------------------------|-----------------------------------------------------|
| Salon Ultimate | `Serviced Guests + Retail Only Guests`                    | Equivalent to the "TOTAL Guests" line               |
| Zenoti         | `Invoice count` (total invoices with services or product) | "Total invoices with services or product" line      |

**Do not use** Zenoti's "Total guest count" statistic — that's unique guests, which differs from invoice count (e.g., Andover 4/1–4/5: 94 unique guests vs 95 invoices). Karissa uses invoice count.

### Sales

| Field         | Formula                            | Notes                                                        |
|---------------|------------------------------------|--------------------------------------------------------------|
| `service_net` | PDF direct (Service sales NET)     | Pre-tax                                                      |
| `product_net` | PDF direct (Product / Retail NET)  | Pre-tax                                                      |
| `total_sales` | `service_net + product_net`        | NOT "Sales(Inc. Tax)" in Zenoti. NOT tax-inclusive anywhere. |

### Per-guest KPIs (all use Karissa's guest_count as the denominator)

| Field         | Formula                         | PDF stat to reject?                                                                                                               |
|---------------|---------------------------------|-----------------------------------------------------------------------------------------------------------------------------------|
| `avg_ticket`  | `total_sales / guest_count`     | Matches SU's "TOTAL Avg Ticket". Matches Zenoti's "Avg. invoice value" (Zenoti uses invoice count internally). Compute, don't trust. |
| `ppg`         | `product_net / guest_count`     | Reject Zenoti's "Net product sales per guest" — it uses unique-guest denominator, not invoice count. SU's PPG stat matches since SU's denominator already matches Karissa's. |
| `pph`         | PDF direct                      | Net service sales / productive hour. Same on both platforms.                                                                      |
| `product_pct` | `product_net / total_sales`     | Karissa's "Product %" column. Validated against her master spreadsheet 2026-04-21. Always divide by TOTAL sales here, not service. |

### Service penetration percentages — counts over guest_count, NOT sales share

`wax_pct` and `treatment_pct` are **penetration rates** (share of guests who got that service), not revenue shares. Coaches use these to answer "how many of our guests got a wax this week" — not "what percent of our revenue came from wax".

| Field                | Formula                                                |
|----------------------|--------------------------------------------------------|
| `wax_count` (SU)     | `Wax.qty` from Service Categories table                |
| `wax_count` (Zenoti) | `Wax.qty + Waxing.qty` — sum BOTH buckets when present. Four locations (Crystal, Elk River, Hudson, Roseville) ship a separate "Waxing" category; **Roseville uses ONLY "Waxing" with no "Wax" row at all.** Always sum both; either can be zero or absent. |
| `wax_pct`            | `wax_count / guest_count`                              |
| `treatment_count`    | `Treatment.qty` from Service Categories (SU) / Service Details (Zenoti) |
| `treatment_pct`      | `treatment_count / guest_count`                        |

**Do NOT use** the "% Sales" or "% Qty" columns from the PDF Service Categories table — those are share-of-sales or share-of-services-line-items, not share-of-guests.

### Color — share of SERVICE revenue (NOT total sales, NOT penetration)

**Per Karissa's master spreadsheet (verified 2026-04-21 by formula inspection): `color_pct` is a share of SERVICE revenue — the denominator is `service_net`, NOT `total_sales`.** This intentionally uses a different formula than `wax_pct` and `treatment_pct` above.

| Field          | Formula                       | Notes                                                                                                              |
|----------------|-------------------------------|--------------------------------------------------------------------------------------------------------------------|
| `color_sales`  | PDF direct (Color row, Sales) | Service Categories table (SU) / Service Details (Zenoti). Pre-tax service sales for the Color category.            |
| `color_pct`    | `color_sales / service_net`   | Share of service revenue. Do **NOT** use `total_sales` as denominator. Do **NOT** compute `color_count / guest_count`. Do **NOT** trust the PDF "% Sales" column — it uses a different denominator. |

Worked example (Blaine, Week 1): `color_sales = $3,964.50`, `service_net = $11,478.25`, so `color_pct = 3964.50 / 11478.25 = 34.54%`. Using `total_sales = $12,485.55` would give 31.75%, which is wrong per Karissa's spreadsheet.

Color parser note: the Service Categories / Service Details table always lists Color. If a PDF genuinely has no color sales (zero-revenue week), `color_sales = 0` and `color_pct = 0`. Never drop the column.

### Productive hours

Not yet specified by Karissa under this contract. Parser extracts the PDF value directly (SU: "Production Hours"; Zenoti: "Productive Hours" from the dashboard summary) and writes it verbatim. Revisit before any derived KPI uses this field.

### End-of-month projection (weekly field)

Karissa's "Projection (End of month)" column on every weekly sheet.

| Field            | Formula                        | Notes                                                                           |
|------------------|--------------------------------|---------------------------------------------------------------------------------|
| `projection_eom` | `(total_sales / 7) * 24`       | This week's sales extrapolated over a 24-day working month. NOT calendar-aware — February's 28 days and March's 31 days both use 24 as the divisor. Validated against her spreadsheet 2026-04-21. |

Worked example (Blaine, Week 1): `$12,485.55 / 7 * 24 = $42,807.60`. Pipeline must compute and write this each week. One column, one formula, no seasonality adjustment.

### Daily Goal / Day Goal (monthly pacing fields)

On Karissa's Year Over Year sheet, every location has two pacing divisors that vary per location and per month:

| Field          | Formula                              | Source of divisor                                                                 |
|----------------|--------------------------------------|-----------------------------------------------------------------------------------|
| `daily_goal`   | `monthly_goal / daily_goal_divisor`  | `daily_goal_divisor` varies 22–25 by location-month — Karissa's operating-days figure. |
| `day_goal`     | `monthly_goal / day_goal_divisor`    | `day_goal_divisor` varies 22–30 by location-month — second pacing divisor she uses. |
| `we_are_at`    | `= mtd_2026`                         | Month-to-date total sales (aggregated from DATA tab).                             |
| `goal_pct`     | `we_are_at / day_goal`               | "Where we are ÷ where we should be" — her pace check.                             |

**Divisors are NOT universal.** March 2026 examples: Andover `/25` and `/26`, New Richmond `/24` and `/22`, Apple Valley `/24` and `/30`, Hudson `/24` and `/26`. Karissa enters these manually each month.

**Storage: GOALS tab extended with two columns** — `daily_goal_divisor` and `day_goal_divisor`. Both int, keyed by `(location_id, year_month)`. Karissa updates monthly; pipeline reads them when building MONTHLY_PACING.

### Monthly aggregation — MONTHLY_PACING tab

New tab introduced 2026-04-21 to hold Karissa's Year Over Year / monthly pacing data. Rebuilt every Monday from the DATA tab + GOALS tab.

Schema:
```
location_id, location_name, year_month,
mtd_2026,           -- SUM(DATA.total_sales WHERE year_month matches)
mtd_2025,           -- prior-year lookup (blank until 12-week backfill lands)
mtd_2019,           -- blank unless 2019 DATA rows exist
sales_pct_2025,     -- mtd_2026 / mtd_2025
sales_pct_2019,     -- mtd_2026 / mtd_2019
monthly_goal,       -- from GOALS tab
daily_goal,         -- monthly_goal / daily_goal_divisor
day_goal,           -- monthly_goal / day_goal_divisor
we_are_at,          -- = mtd_2026
goal_pct            -- we_are_at / day_goal
```

Writer rebuilds the whole tab each Monday (idempotent — full overwrite). Historical preservation lives in DATA tab, not here.

---

## Location master list

| ID    | Name          | Platform        | Manager |
|-------|---------------|-----------------|---------|
| z001  | Andover FS    | Zenoti          | Jenn    |
| z002  | Blaine        | Zenoti          | Jenn    |
| z003  | Crystal FS    | Zenoti          | Jenn    |
| z004  | Elk River FS  | Zenoti          | Jenn    |
| z005  | Forest Lake   | Zenoti          | Karissa |
| z006  | Prior Lake    | Zenoti          | Jess    |
| z007  | Hudson        | Zenoti          | Karissa |
| z008  | New Richmond  | Zenoti          | Karissa |
| z009  | Roseville     | Zenoti          | Jenn    |
| z010  | Apple Valley  | Salon Ultimate  | Jess    |
| su001 | Lakeville     | Salon Ultimate  | Jess    |
| su002 | Farmington    | Salon Ultimate  | Jess    |

**Woodbury (su003) was removed — do not re-add it.**
Source of truth: `config/customers/karissa_001.json`

**⚠️ Prior Lake is Zenoti (z006), NOT Salon Ultimate.** `config/locations.py` LOCATION_POS_MAP was wrong in an earlier version — fixed 2026-04-20 per BUG-1 audit. Do not re-introduce `"Prior Lake": "salon_ultimate"`.

---

## Manager access

| Manager | HTML file    | PIN    | Sees                                      |
|---------|-------------|--------|-------------------------------------------|
| Karissa | index.html  | none   | All 12 locations                          |
| Jess    | jess.html   | `1234` | Prior Lake, Apple Valley, Lakeville, Farmington |
| Jenn    | jenn.html   | `5678` | Andover FS, Blaine, Crystal FS, Elk River FS, Roseville |

**⚠️ DO NOT change PINs without Tony's explicit instruction.**
**⚠️ DO NOT change WebAuthn credential keys:**
- Jess: `CRED_KEY = 'kpi_cred_jess'`
- Jenn: `CRED_KEY = 'kpi_cred_jenn'`
**⚠️ DO NOT change `rpId = 'tonester040-spec.github.io'`** — this is locked to the domain.

## Owner dashboard (private)

| Owner      | HTML file     | PIN    | Sees                                      |
|------------|--------------|--------|-------------------------------------------|
| John/Patti | owners.html  | `7291` | Money + risk only — network-level owner view |

**owners.html — private owner dashboard, PIN 7291, do not link publicly.**
**Phase 3 features (Ghost Hunter, Win-Back, Command Bar) show as PREVIEW.**
**Never link owners.html from index.html, jess.html, jenn.html, or any public page.**
**Weekly pipeline does NOT regenerate owners.html — manual file, safe from Monday overwrites.**

---

## Google Sheets tab structure

| Tab               | What it holds                                              |
|-------------------|------------------------------------------------------------|
| `CURRENT`         | 12 rows — current week snapshot (Karissa's team enters this; pipeline reads it, then overwrites with enriched data each run) |
| `DATA`            | Append ledger — all historical weeks (never overwritten)   |
| `GOALS`           | Per-location annual targets                                |
| `ALERTS`          | Flag summary written by pipeline. **Rows 100–101 reserved for coach briefs:** row 100 = JESS_BRIEF, row 101 = JENN_BRIEF (JSON strings written by `write_coach_briefs()`). Targeted update — missing cards do NOT clear previous week's row. |
| `STYLISTS_CURRENT`| Current week stylist rows (pipeline overwrites)            |
| `STYLISTS_DATA`   | Historical stylist data (append ledger)                    |
| `WEEKLY_DATA`     | Weekly aggregates                                          |
| `WEEK_ENDING`     | Manual reference tab — **not read by pipeline code** (pipeline reads week_ending from row data in CURRENT/DATA tabs) |
| `DATA_MONTHLY`    | **Monthly grain historical backfill.** 23 cols A-W keyed by `(loc_name, year_month)`. Mirrors DATA's KPIs plus `source` / `period_start` / `period_end` provenance. Append-only, idempotent. Populated for backfill (Mar/Apr/May 2026) and intended to back YOY / MTD / pacing features. **Not read by the weekly pipeline** — exists in parallel to DATA, never to replace it. Auto-created by `core/sheets_writer.py::_ensure_data_monthly_tab()` on first write. |
| `STYLISTS_DATA_MONTHLY` | **Monthly stylist roster historical backfill.** 16 cols A-P keyed by `(year_month, name, loc_name)`. Append-only, idempotent. Auto-created. Source column distinguishes `zenoti_xlsx` (March, from Employee KPI .xlsx exports), `zenoti_monthly_pdf`, and `su_monthly_pdf` (April/May, from monthly POS PDFs). |
| `CUMULATIVE_MTD` | **Weekly cumulative-MTD snapshots.** 22 cols A-V keyed by `(loc_name, year_month, week_ending)`. Each Monday's pipeline run writes one snapshot per location capturing the cumulative-month-to-date values entered into CURRENT. Next Monday's run reads these to compute the prior-week subtraction for `core/cumulative_to_weekly.py` differencing. Also feeds the Karissa-format weekly report tabs. Auto-created. Append-only, idempotent. **Writer uses `valueInputOption="RAW"`** to keep `week_ending` as an ISO date string instead of letting Sheets coerce it to an Excel serial number (a date-coercion bug discovered 2026-05-27 that broke idempotency in early CUMULATIVE_MTD writes — fixed before live pipeline runs). |
| `STYLISTS_CUMULATIVE_MTD` | **Weekly cumulative-MTD stylist snapshots.** 15 cols A-O keyed by `(year_month, week_ending, name, loc_name)`. Same role as CUMULATIVE_MTD but at stylist grain. Also uses `valueInputOption="RAW"` for the same date-coercion reason. |
| `MONTHLY_GOALS` | **Per-location monthly $ targets + operating-day divisors.** 6 cols A-F keyed by `(loc_name, year_month)`. Holds `monthly_goal`, `daily_goal_divisor` (Karissa's "Daily Goal" working-days count, varies 22-26), `day_goal_divisor` (her "Day Goal" working-days count, varies 22-30), plus `source` provenance. Read by `report_generator.py` to populate the YoY tab's Goal / Daily Goal / Day Goal columns. Karissa enters these manually each month — without a row for `(loc, year_month)`, the YoY pacing columns will be blank for that location. Auto-created by `_ensure_monthly_goals_tab()`. Seeded for 2026-05 by `scripts/backfill/cumulative_mtd_from_tracker.py`. |

**Data flow:** Karissa's team enters current week data into CURRENT tab manually. The pipeline reads CURRENT (current week) + DATA (location history) + STYLISTS_DATA (stylist history), enriches everything, then writes back to CURRENT + STYLISTS_CURRENT + ALERTS, and appends to DATA + STYLISTS_DATA.

**Historical append (idempotent):** `append_to_historical()` and `append_to_stylists_historical()` in `core/sheets_writer.py` append each Monday's CURRENT and STYLISTS_CURRENT rows to DATA and STYLISTS_DATA respectively. Both check for the week_ending already existing and skip if so — safe to re-run on the same week.

---

## GitHub Actions workflows (4 total)

| File | Trigger | What it does |
|------|---------|--------------|
| `weekly_pipeline.yml` | Monday 7:00 AM Central (12:00 UTC) + manual dispatch | Full KPI pipeline: Step 0 `gmail_attachment_watcher.py` (inbox ingest, `continue-on-error`) → read sheets → AI cards → write sheets → Excel → email → build dashboards → commit + push docs/ |
| `email_assistant.yml` | Mon–Fri 7:30 AM Central (12:30 UTC) + manual dispatch | Email pipeline: Gmail OAuth → noise filter → categorize → draft replies → build debrief HTML → commit + push docs/karissa-debrief.html |
| `deploy.yml` | Push to main + manual dispatch | Deploys `docs/` folder to GitHub Pages |
| `static.yml` | Push to main + manual dispatch | Identical to deploy.yml — older duplicate, both currently active |

**Pipeline push strategy:** `git_pusher.py` commits `docs/` locally during the pipeline run. The workflow's "Push dashboard changes" step then fetches, rebases (`-X theirs`), and pushes. If rebase fails, falls back to `--force-with-lease`.

**Concurrency:** Both `weekly_pipeline.yml` and `email_assistant.yml` use `cancel-in-progress: false`.

---

## Weekly pipeline — key behavior

- **Runs:** Every Monday 7:00 AM Central (12:00 UTC) via `weekly_pipeline.yml`
- **CRITICAL:** The pipeline **regenerates `docs/index.html`, `docs/jess.html`, `docs/jenn.html` from scratch** every run by calling `dashboard_builder.py`. Any manual edits to those HTML files will be overwritten the next Monday.
- **Correct fix:** All permanent additions (PWA meta tags, PIN gate, WebAuthn JS, install banner, Coach Card tab, Visit Prep tab) must be baked INTO `core/dashboard_builder.py` — not hand-edited into docs/*.html.
- **Dry run available:** `DRY_RUN=true python main.py` skips all writes, API calls, email, git push
- **Sandbox validation:** `python scripts/sandbox_run.py` — runs ALL modules against mock data, zero real API calls. Run this before deploying new credentials or after any schema/code change. Must show 9/9 PASS (confirmed 2026-04-20).

## Email assistant — key behavior

- **Runs:** Every weekday at 7:30 AM Central (12:30 UTC) via `email_assistant.yml`
- **What it produces:** `docs/karissa-debrief.html` — a daily morning briefing showing urgent emails, categorized threads, draft reply previews, and location mentions
- **Gmail auth:** OAuth 2.0 (NOT App Password). Secrets: `GMAIL_CLIENT_ID`, `GMAIL_CLIENT_SECRET`, `GMAIL_REFRESH_TOKEN`
- **One-time OAuth setup:** Run `email_assistant/get_token.py` locally to generate the refresh token, then store in GitHub Secrets
- **Not yet live:** Gmail OAuth secrets have not been added to GitHub Secrets yet. Until then, the assistant writes a "Coming Soon" placeholder page and exits cleanly — no crashes
- **Friday behavior:** On Fridays, fetches the full week's emails (last 120 hours) for a recap summary in addition to the daily debrief
- **Voice profile:** Draft replies are generated to sound like Karissa. Set up by dropping 30–40 sent emails into `voice/samples/` and running `email_assistant/build_profile.py` once

---

## Sandbox mode

Full end-to-end validation without touching any real service:

```bash
python scripts/sandbox_run.py
```

What the sandbox does:
- Mocks 12 realistic location rows and 3 stylist rows
- Runs data_processor, drift_checker, ai_cards (DRY_RUN), sheets_writer (DRY_RUN), report_generator (DRY_RUN), email_sender (DRY_RUN), dashboard_builder (DRY_RUN), alerter test, ai_coach_cards (DRY_RUN), cumulative_pipeline
- Confirms all 10 modules pass before any real API credential is connected
- Fires a deliberate test alert so the alerter path is confirmed working

**Sandbox must show 10/10 PASS before any release is considered shippable.** ✓ (Module 10 `cumulative_pipeline` added 2026-05-26 for the cumulative-MTD → weekly differencing step.)

---

## Drift monitoring

`config/drift_config.json` holds per-location KPI thresholds. `core/drift_checker.py` validates computed KPIs against these thresholds after processing but before dashboard update.

**UNCALIBRATED** — placeholder ranges ship with the repo. After 4 weeks of real Zenoti + Salon Ultimate data:
1. Tony reviews actual weekly revenue, appointments, product %, and rebook rates per location
2. Updates `drift_config.json` with real observed ranges
3. Sets `"_calibration_status": "CALIBRATED"` and notes the date

Drift fires WARNINGs for out-of-range values (dashboard still updates).
Drift fires ERRORS and blocks the dashboard update for physically impossible values (negative revenue, zero appointments across all stylists).

---

## Alerting

`core/alerter.py` handles CRITICAL/HIGH failure notifications. Call before pipeline exits:

```python
from core import alerter
alerter.send(severity="CRITICAL", module="my_module", error_message="...", diagnostic="...")
```

Alert routing (in order):
1. GitHub Actions Job Summary (always available in CI)
2. Gmail SMTP email (requires `GMAIL_APP_PASSWORD` + `GMAIL_SENDER` set)

Optional: set `KPI_ALERT_EMAIL` secret to route alerts to a different address than the sender.

---

## PWA status

The app is installable on iPhone (Add to Home Screen). Components:
- `docs/manifest.json` — app manifest (navy #0F1117 theme, white KPI icons)
- `docs/sw.js` — service worker, `cache name: kpi-v1`
- `docs/offline.html` — shown when app opened with no connection
- `docs/icons/icon-192.png` + `docs/icons/icon-512.png` — generated via Pillow

**Service worker strategy:**
- HTML files → Network First (always get fresh pipeline data when online)
- Static assets → Cache First (icons, manifest)

---

## Environment variables (GitHub Actions Secrets)

### Weekly KPI pipeline (`weekly_pipeline.yml`)

| Variable                      | Required | Purpose                              |
|-------------------------------|----------|--------------------------------------|
| `GOOGLE_SERVICE_ACCOUNT_JSON` | ✅       | Base64-encoded service account JSON  |
| `ANTHROPIC_API_KEY`           | ✅       | Claude API for AI card + coach generation |
| `GMAIL_APP_PASSWORD`          | ⚠️ soft | Gmail App Password — skipped if missing |
| `GMAIL_SENDER`                | ⚠️ soft | Gmail sender address (outbound Excel report) |
| `ACTIVE_CUSTOMER_ID`          | default  | Defaults to `karissa_001`            |
| `KPI_INBOX_EMAIL`             | ⚠️ soft | KPI inbox address for the Gmail attachment watcher (Step 0). Missing → watcher skips, pipeline continues. |
| `KPI_INBOX_APP_PASSWORD`      | ⚠️ soft | 16-char Gmail App Password for the KPI inbox account (IMAP login, not OAuth — see watcher section) |

### Email assistant pipeline (`email_assistant.yml`)

| Variable                      | Required | Purpose                              |
|-------------------------------|----------|--------------------------------------|
| `GMAIL_CLIENT_ID`             | ✅       | Gmail OAuth client ID (inbox access) |
| `GMAIL_CLIENT_SECRET`         | ✅       | Gmail OAuth client secret            |
| `GMAIL_REFRESH_TOKEN`         | ✅       | Gmail OAuth refresh token — generate once via `email_assistant/get_token.py` |
| `ANTHROPIC_API_KEY`           | ✅       | Claude API for email categorization + draft generation |

**Three separate Gmail auth flows:**
- KPI pipeline uses **App Password** (SMTP outbound only — sends the Excel report to Tony, and error notifications from the watcher)
- Email assistant uses **OAuth 2.0** (inbox read + draft write for Karissa's personal inbox — `GMAIL_*` secrets)
- Gmail attachment watcher uses **IMAP + App Password** (inbox read + labels + archive on the dedicated `karissaperformanceintelligence@gmail.com` inbox — `KPI_INBOX_EMAIL` / `KPI_INBOX_APP_PASSWORD`). Switched from OAuth on 2026-05-27 — Google's Testing-mode refresh tokens for Gmail restricted scopes expire every 7 days, and production publishing requires a CASA security assessment. App Passwords don't expire.

---

## Excel report structure (report_generator.py)

The Monday pipeline emits **one workbook per month**, regenerated from the LIVE
SHEET each Monday (via `core/data_source.py`: CUMULATIVE_MTD weekly snapshots +
DATA_MONTHLY prior-year reference + MONTHLY_GOALS targets) so old weekly tabs stay
stable as the month progresses. The layout reproduces Karissa's own `May 2026.xlsx`
tracker. Filename: `KPI_<week_ending>.xlsx` (e.g. `KPI_2026-05-31.xlsx`).
`report_generator.py` supersedes the earlier `karissa_workbook.py` (consolidated
2026-05-29) — one report builder, fed by the Sheet. The pure core
(`build_workbook_from_data`) takes plain dicts, so it tests with zero network.

**6 tabs, in order:**

1. **Week 1 / week 2 / week 3 / week 4 / week 5** — five cumulative-MTD weekly
   tabs (Karissa's tab names preserve her leading/trailing whitespace
   intentionally). Each tab has two stacked tables:
   - **Top block** (rows 2-14 + Totals row 15 + network-avg row 16): per-location
     KPIs — Name, (tickets — shape-only/blank), Guest Count, Total Sales Net,
     Service Net, Product Net, Product %, PPG Net, PPH Net, Average Tkt, Prod
     Hours, Projection (cumul ÷ working-days-elapsed × working-days-in-month),
     2025 reference cols (Total / Guests / PPG / Diff / AT / Dif).
   - **Bottom block** (rows 18-31): service-mix penetration — Wax Count, Waxing
     Net, Wax %, Color Net, Color %, Trmt Count, Trmt Net, Trmt %. Right-side
     reference band (prior-month / YoY helpers) renders blank-with-headers until
     backload. Footer rows hold the "% of Guests" treatment-penetration aggregate
     and the verbatim "Goal is 15% plus" annotation. A closed/holiday week is a
     zero row that is still PRESENT (never skipped).
2. **Year Over Year** — one row per location: Name | 2026 (current MTD) | 2025 |
   Sales % | Goal | Daily Goal | Day Goal | We are At! | Goal % | 2019 | Sales %.
   Goal / Daily Goal / Day Goal sourced from `MONTHLY_GOALS`; 2025 / 2019 / Sales%
   render BLANK until the prior-year DATA_MONTHLY backload lands (graceful — never
   zero-fill). (Her tracker's Q2 totals + Q2 monthly-breakdown sub-tables are
   deferred until quarterly history exists.)

**Canonical consistent headers (drift-proofing)**: report_generator emits ONE
fixed header set (the newest = May superset) on EVERY week tab and maps by HEADER
NAME, never column index — per `KPI_LOCATIONS_DATA_Schema` BUILD NOTE #2. It does
NOT replicate Karissa's per-week column drift (her tickets-on-Wk1-only, Wk4 "Goals"
insert, Wk5 Projection→Monthly-goal rename, or her week-4 missing-Name shift). The
`map_top_header_row` reader is SECTION-AWARE (splits at "2025 Total Sales") so the
repeated current-vs-reference Guest Count / PPG / AT labels never collide — which is
what lets it read March- and May-style sheets identically (the key drift test).

**Read-as-stored contract**: every derived ratio (Product %, PPG, PPH, Avg Ticket,
Wax/Color/Trmt %) is written VERBATIM from the stored CUMULATIVE_MTD snapshot — which
the parsers already computed canonically + penny-verified. report_generator does NOT
recompute them (recomputing risks drifting from the stored truth). This preserves the
old Z-mode intent — cells show OUR canonical parser value, not Karissa's hand-typed
POS figure — but by reading the stored canonical value rather than recomputing it.
Only Totals-row aggregates are computed: sums for primitives, Product% = ratio-of-
sums, and PPG/PPH/AT = column sums (matching her own Totals row).

**Projection divisors**: Karissa hand-types `=(cumul / X) * 25` per cell on
each weekly tab, with X = working days elapsed. May 2026 hardcoded as
`{Wk1: 2, Wk2: 8, Wk3: 14, Wk4: 21, Wk5: 25}` in
`core/report_generator.py::PROJECTION_DIVISORS_BY_MONTH`. Future months fall
back to Mon-Sat calc until added to the override table. Per-location quirks
(Roseville/Apple Valley/Farmington use /22*29 on Wk4 in her tracker) are NOT
replicated — single divisor applies to all locations per month.

**Prior-month reference columns** (bottom-right of each weekly tab) source
from `DATA_MONTHLY` 2026-04 backfill, not her tracker's typed values. Values
will differ from her cells by $1-3K per location because she enters from a
slightly different / earlier export. Z-mode aligned.

**Current recipients:** `config/customers/karissa_001.json` → `email_recipients` → `["tonester60@hotmail.com"]` (Tony). Update when Karissa's email is confirmed.

---

## AI model usage

- **Stylist cards** (bulk, ~12 locations × ~10 stylists): `claude-haiku-4-5-20251001` — fast + cheap
- **Coach briefing** (1 per run, network-wide summary): `claude-sonnet-4-6` — higher quality
- **Manager coach cards** (Jess + Jenn, 2 per run): `claude-sonnet-4-6` — in `core/ai_coach_cards.py`
- **Visit Prep** (on-demand, client-side): `claude-sonnet-4-20250514` — browser API call from jess.html / jenn.html
- **Email categorization + draft replies** (email assistant): configured inside `categorizer.py` and `draft_generator.py` — check those files for current model

---

## Pipeline logging

`main.py` writes a run log to `data/logs/pipeline_YYYYMMDD_HHMMSS.log` after each run. Uploaded as a GitHub Actions artifact (`pipeline-log-{run_id}`) with 30-day retention. Use these logs for drift calibration (actual revenue/PPH ranges per location over time).

---

## What's built but not yet live

- **Email assistant** — Fully built (`email_assistant/` module + `email_assistant.yml` workflow + `docs/karissa-debrief.html`). Awaiting Gmail OAuth secrets (`GMAIL_CLIENT_ID`, `GMAIL_CLIENT_SECRET`, `GMAIL_REFRESH_TOKEN`) to be added to GitHub Secrets. Runs the placeholder page loop cleanly until then.
- **Voice profile** — `build_profile.py` and `voice_profile.py` are written and wired in. Awaiting Karissa's sample emails to be placed in `voice/samples/` and `build_profile.py` run once.
- **Manager coach card emails** — `send_manager_coach_cards()` is built and wired into `main.py` Step 7b. Awaiting Tony to fill in real email addresses for Jess and Jenn in `config/customers/karissa_001.json` → `managers[].email`. Currently empty strings — coach cards generate but emails silently skip.
- **Manager coach cards (pipeline)** — `core/ai_coach_cards.py` is built and wired into `main.py` Steps 4b and 5. Coach card JSON is injected into `COACH_CARD_DATA` in jess.html / jenn.html on every Monday pipeline run. Coach briefs also written to ALERTS!A100 (JESS_BRIEF) and ALERTS!A101 (JENN_BRIEF).

## Cumulative-MTD → weekly differencing (`core/cumulative_pipeline.py`)

**Karissa reveal 2026-05-26:** Elaina's weekly POS reports are NOT independent weekly snapshots — they're cumulative month-to-date that reset each month-start. Week 1's report covers the 1st through the first Sunday; Week 2 covers 1st through second Sunday; and so on. The "monthly" April PDFs Tony has are the final-week cumulative for that month.

Karissa's full Q&A (13 questions) is captured in the May 26 conversation thread; key answers:

| Question | Answer | Code implication |
|---|---|---|
| Q1: Time period covered | Always from 1st of month through most recent Sunday; resets each month | Cumulative-MTD model confirmed |
| Q2: New-month mid-week | Week 1 starts on the 1st (could be Wed-Sun, 5-day partial) | Hard month-start boundary |
| Q3: Last week of month | Always ends on month's last day (could be 1-3 day partial) | Hard month-end boundary |
| Q4: All fields MTD? | Yes — sales, guests, percentages all build up | Difference ALL primitives, recompute ALL ratios |
| Q5: Numbers ever decrease? | Yes — if refunds happen (but never that much) | `core/cumulative_to_weekly._safe_div` handles negatives gracefully |
| Q6: Stylist data MTD too? | Yes — each stylist's numbers are her MTD | Same differencing applied to stylist records |
| Q7: Dashboard preference | Weekly columns + monthly total ("monthly adding up as the month goes on") | Phase 2 dashboard restructure |
| Q8: Coach Card comparison | "You guys pick" — we picked true week-over-week | Aligns with Q7 weekly-columns view |
| Q9: Late corrections | Salon-level updates, but per-stylist totals don't always reflect them | `STYLIST_SUM_MISMATCH` downgraded from error → warning (explains Prior Lake April $34 drift) |
| Q10: Re-sent reports? | No — one report per Monday, never re-sent | Pipeline can assume each Monday's snapshot is final |
| Q11: Holiday weeks | Show zero row | `_safe_div` returns 0 for all KPIs when guests=0 |
| Q12: Zenoti vs SU shape? | Same — both cumulative-MTD | Single pipeline handles both |
| Q13: 2025 historical | Same format; Karissa maintains her own monthly YOY by hand | Future: ask for her file to populate 2025 DATA_MONTHLY quickly |

### Architecture

- **`core/sheets_writer.py`** — adds `append_to_cumulative_mtd()` + `append_to_stylists_cumulative_mtd()` + auto-creators for the two new tabs (idempotent, mirror DATA_MONTHLY pattern).
- **`core/cumulative_to_weekly.py`** — pure-function differencing math. `difference_location_record(current_mtd, prior_mtd_or_None)` returns a weekly record with differenced primitives and recomputed derived KPIs. Handles Week 1 (no prior), zero-guest weeks (`_safe_div` → 0), and negative diffs from refunds.
- **`core/data_source.py`** — adds `read_cumulative_mtd_snapshots(service, config, year_month)` and `find_latest_priors_by_location/stylist()` helpers.
- **`core/cumulative_pipeline.py`** — orchestrator that ties it together. `snapshot_and_difference()` runs as Step 2b in `main.py`, between data_source and data_processor.

### Hard rules

- **CURRENT tab now means cumulative-MTD** (Karissa's team enters this Monday's cumulative). Pipeline overwrites CURRENT with the differenced weekly after processing.
- **DATA tab now holds true weekly rows** (differenced). Empty until first live Monday run; populates organically going forward.
- **Derived KPIs (pct, avg, PPH) are NEVER subtracted directly** — they're recomputed from the differenced primitives. Otherwise the math is meaningless.
- **STYLIST_SUM_MISMATCH is a warning, not an error** (per Karissa Q9 — salon-level corrections don't always reach per-stylist totals; explains Prior Lake's $34 April gap).

## Historical backfill — `scripts/backfill/`

One-off loaders that populate **DATA_MONTHLY** and **STYLISTS_DATA_MONTHLY** from 3 months of Karissa's pre-pipeline POS data (Mar/Apr/May 2026). Built 2026-05-26, branch `backfill-data-monthly-2026-05-26`.

### Why monthly tabs (and not weekly DATA)

The original goal was weekly backfill, but the data shape forced a redesign:

- **March:** Karissa's tracker `March 2026.xlsx` has 5 weekly tabs, but they are **cumulative MTD** (Week 5 = full March), not independent weekly snapshots. Only Week 1 is cleanly weekly-derivable. Week 2 is empty, Week 4 has a column shift, Week 5 = full month.
- **April/May:** Only monthly POS Salon Dashboard PDFs are available — no weekly source exists for those months.

Bending the weekly DATA tab to hold monthly rows would silently break every downstream consumer that assumes 7-day grain (projection_eom math, WoW deltas, the Monday pipeline append path). So backfill writes to a **parallel** DATA_MONTHLY tab, leaving DATA untouched for the live pipeline to grow organically.

### Modules

| File | Purpose |
|------|---------|
| `scripts/backfill/tracker_loader.py` | Reads Karissa's tracker Week 5 tab → 12 March DATA_MONTHLY rows |
| `scripts/backfill/zenoti_xlsx_loader.py` | Reads 9 Zenoti Employee KPI .xlsxes from March folder. (a) Cross-validates against tracker (Δ must be ≤ $0.01 on service+product). (b) Emits active March stylist roster |
| `scripts/backfill/monthly_pdf_loader.py` | Wraps existing `pdf_zenoti_v2` / `pdf_salon_ultimate_v2` parsers for April/May monthly PDFs. Verified Δ$0.00 reconciliation on 11/12 April PDFs and 12/12 May PDFs (Prior Lake April has a known $34 stylist-sum anomaly requiring `--accept`) |
| `scripts/backfill/render_review.py` | Pretty-prints per-location KPI tables + validation summaries for dry-run human review |
| `scripts/backfill/run_batch.py` | CLI orchestrator with `--dry-run` / `--write` / `--accept "LOC:CODE"` flags |

### Usage

Local-dev: requires `GOOGLE_SERVICE_ACCOUNT_JSON` (base64) exported in shell. The CLI tries `python-dotenv` automatically; `.env` may also supply it (must be the base64 string, not a file path).

```bash
# Dry-run March (loads tracker + xlsx cross-validation, prints diff)
python -m scripts.backfill.run_batch --month 2026-03 --dry-run

# Write March (12 location rows + 93 stylist rows)
python -m scripts.backfill.run_batch --month 2026-03 --write

# Write April (12 location + 143 stylist rows) — accept known Prior Lake $34 stylist-sum anomaly
python -m scripts.backfill.run_batch --month 2026-04 --write --accept "Prior Lake:STYLIST_SUM_MISMATCH"

# Write May partial month 5/1-5/24 (12 location + 144 stylist rows)
python -m scripts.backfill.run_batch --month 2026-05 --write
```

### Safety contract

- **Dry-run is default.** `--write` is required to actually call Sheets API.
- **Error severity blocks writes** unless explicitly `--accept`ed by `LOC:CODE`.
- **Idempotent.** Re-running is safe — existing `(loc_name, year_month)` or `(year_month, name, loc_name)` rows are skipped.
- **Parsers untouched.** All May-26 parser fixes preserved.
- **No SU March stylist data loaded.** The 3 `Stylist_Tracking_Report (NN).xls` files (real Excel 97-2003) remain on disk un-processed pending future need; would require adding `xlrd<2` dependency.

### What's NOT backfilled

- Weekly DATA rows for March/April/May (no clean weekly source for the latter two; tracker is cumulative for the former)
- Weekly STYLISTS_DATA rows for any backfill month (same reason)
- SU March stylist roster (skipped pending xlrd<2 dependency decision)

These weekly tabs populate organically starting with the first Monday post-go-live.

## What's paused / future state

- **Historical backfill** — Karissa may have 2-4 years of data in Zenoti/Salon Ultimate. 50/50 on whether it's exportable. Architecture is ready: DATA tab is the append ledger, and `append_to_historical()` + `append_to_stylists_historical()` are both implemented and idempotent. Bulk backfill just needs a one-time loader that walks historical weeks through the same append functions.
- **Zenoti/Salon Ultimate API feeds** — placeholder columns exist in Goals & YOY sheet. API access not yet granted by Karissa. Do not wire up API calls until credentials arrive. Schema contracts already written: `config/zenoti_schema.json`, `config/salon_ultimate_schema.json`.
- **AI assistant chat** — floating chat UI, Cloudflare Worker proxy for API key security, scoped per manager. Architecture designed, not built. Waiting on historical data decisions.
- **Visit Prep API key proxy** — Visit Prep fires a direct browser-to-Anthropic API call (acceptable for PIN-gated pilot). Before broader rollout, proxy through a serverless function (Vercel / Cloudflare Worker / GitHub Actions) so the key is not client-side. TODO comment is in the JS code.
- **Visit history log (Phase 2)** — Each Visit Prep generation logs `{ coach, week_ending, locations_visited, generated_at }` to `console.log('[VISIT_LOG]', ...)`. Phase 2: write this to a VISITS tab in Google Sheets. One line change. TODO comment is in the code.

---

---

## Coach cards — key behavior

Built in the 2026-03-25 session. All 8 files modified/created, all syntax-verified.

### Pipeline coach cards (`core/ai_coach_cards.py`)

- Generated every Monday as Step 4b in `main.py`
- One card per manager (Jess and Jenn only — Karissa's direct locations never get a card)
- Managers with no `location_ids` in config are skipped
- Uses hardened prompt: **Observation → Context → Question** format contract. No generic talking points.
- Output JSON schema includes: `territory_headline`, `star_of_week`, `priority_call` (with `coaching_question`), `one_to_watch`, `location_cards[]`, `stylist_spotlight`, `pph_table`, `probable_cause`, `recognition_line`, `network_rank`, `threshold`, `weeks_until_critical`
- Falls back to `_dry_run_brief()` placeholder on JSON parse failure — pipeline never crashes
- Strips markdown fences before `json.loads()`
- Supports `DRY_RUN=true`

### Sheets storage (ALERTS tab rows 100–101)

- `write_coach_briefs()` in `core/sheets_writer.py`
- JESS_BRIEF → ALERTS!A100, JENN_BRIEF → ALERTS!A101
- Targeted `update()` calls — missing cards do NOT clear previous week's data
- Called by `write_all()` after `write_alerts()` when coach_cards dict is present

### Dashboard injection (`core/dashboard_builder.py`)

- `_build_coach_card_data()` builds `COACH_CARD_DATA` JS constant
- Injected into jess.html and jenn.html between KPI_DATA_START / KPI_DATA_END markers
- `COACH_CARD_DATA = null` for index.html (Karissa's dashboard has no coach card)
- Manager HTML files render the Coach Card tab UI from this constant on tab open (lazy render, fires once)

### Coach card emails (`core/email_sender.py`)

- `send_manager_coach_cards()` sends mobile-optimized HTML email to each manager
- Dashboard URL constructed as `https://tonester040-spec.github.io/KPI-Platform-Dash/{filename}`
- Silently skips managers with empty `email` field in config
- Called by `main.py` Step 7b
- **ACTION REQUIRED:** Fill in Jess and Jenn email addresses in `config/customers/karissa_001.json` → `managers[].email`

### Coach card UI (jess.html / jenn.html)

- Third tab "📋 Coach Card" added to both manager dashboards
- Lazy renders on first tab open (not on every switch)
- Shows graceful "Coach Card Not Available" when `COACH_CARD_DATA` is null (before first pipeline run)
- CSS classes: `.cc-wrap`, `.cc-headline`, `.cc-card`, `.cc-card-red`, `.cc-card-gold`, `.cc-flag`, `.cc-metrics`, `.cc-tp`, `.cc-pph-table`

---

## Visit Prep — key behavior

Built in the 2026-03-25 session. Fourth tab added to jess.html and jenn.html.

### What it is

On-demand visit intelligence — not pipeline-generated. Coach selects which locations she's visiting this week, taps Generate, gets a purpose-built visit prep card in ~3 seconds. Available any day, not just Monday.

### How it works

- Entirely client-side. No backend changes. No pipeline changes.
- Reads existing dashboard data (already loaded) — filters to selected locations
- Fires a fetch call to `https://api.anthropic.com/v1/messages` directly from the browser
- Model: `claude-sonnet-4-20250514`
- Renders card from JSON response

### Location selector

- One toggle button per location in the coach's territory
- 2-col grid on mobile (≤768px), 3-col on desktop
- Default state: `#F0F0F0` background, `#0F1117` text, 1px `#CCCCCC` border
- Selected state: `#C8A97E` (gold) background, white text, ✓ prepended to name
- 150ms smooth transition
- "Generate Visit Prep" button (full width, navy bg, gold text) — disabled until ≥1 location selected
- If card already exists this week: button label = "Regenerate Visit Prep"

### Loading states

- Cycling messages: "Pulling this week's data for your visits..." → "Building your visit prep..." → "Almost ready..."
- After 8 seconds: "This is taking a moment — still working."
- On error: "Something went wrong. Try again." + Retry button (restores prior selections)
- Never shows raw API error text

### Output card structure

- **Visit Focus Header**: full-width cream card (`#F5F3EF`), lists selected locations + week ending + cross-location focus (null if single location or no genuine pattern)
- **Per-Location Cards**: one per selected location — location header (name + PPH + WoW delta + STAR/WATCH/SOLID badge), The One Number (metric + current value + prior value + why it matters), Stylist to Address (recognition ⭐ or concern ⚠️ — skipped if null), Talking Points (2, Obs→Context→Question), Visit Goal ("A successful visit looks like...")
- **Regenerate Button**: below cards, restores selector with same selections pre-filled

### Persistence

- localStorage keys: `VISIT_PREP_SELECTIONS_{coachName}` and `VISIT_PREP_CARD_{coachName}`
- Both include `week_ending`. On tab open: if stored week_ending ≠ current data week_ending, both keys are cleared (auto-resets when Monday pipeline delivers new data)
- localStorage unavailable (private browsing): fresh state every session, no error

### Visit history (Phase 2 groundwork)

- Each generation logs: `console.log('[VISIT_LOG]', JSON.stringify({ coach, week_ending, locations_visited, generated_at }))`
- TODO comment in code: Phase 2 write to VISITS tab in Google Sheet

### API key security

- For the PIN-gated pilot: direct browser → Anthropic API call is acceptable
- TODO comment in code: before broader rollout, proxy through serverless function (Vercel / Cloudflare Worker)
- API key must be provided via the `CLAUDE_API_KEY` config object in the dashboard

---

## Gmail Attachment Watcher — Inbox Ingestion Layer

Built 2026-04-21. Step 0 of the weekly KPI pipeline. Turns the dedicated inbox `karissaperformanceintelligence@gmail.com` into a clean, dedup-safe pickup point for Elaina's weekly POS export attachments.

### Purpose

Every Monday at 7:00 AM Central (before `main.py` runs), `parsers/gmail_attachment_watcher.py` polls the dedicated KPI inbox for new attachment emails from whitelisted senders, validates them at the header level, hashes every file for deduplication, archives each file to `data/archive/` for audit, and writes fresh copies to `data/inbox/` along with a manifest the Tier 2 batch processor can consume.

### Configuration

`config/inbox_config.json` — single source of truth for inbox behavior. Fields:

| Field                     | Purpose                                                                 |
|---------------------------|-------------------------------------------------------------------------|
| `whitelisted_senders`     | Email addresses allowed to submit attachments. Validated via headers, not search query. |
| `kpi_inbox`               | The ingestion inbox (`karissaperformanceintelligence@gmail.com`).       |
| `karissa_email`           | Karissa's email (for error notifications).                              |
| `notification_recipients` | Who gets success/error emails (Karissa + Elaina).                       |
| `allowed_extensions`      | `.xlsx`, `.xls`, `.pdf` only.                                           |
| `search_window_days`      | Gmail search window (default 2 days — Monday looks at Sat+Sun+Mon AM).  |
| `archive_retention_days`  | Days to keep archived files (90). Cleanup is a future task.             |
| `kpi_processed_label`     | `KPI-Processed` — applied + INBOX removed on full success.              |
| `kpi_attention_label`     | `KPI-Attention` — applied but INBOX kept on partial/error.              |
| `dry_run`                 | When `true`, disables ALL I/O (no archive, no inbox write, no labels, no archiving emails, no notifications). Safe for validation. |

**Two Karissa placeholders in config must be filled before go-live:** `karissa@[REPLACE_BEFORE_GO_LIVE]` appears in `karissa_email` and `notification_recipients`. Update both when her email is confirmed.

### Authentication

IMAP + Gmail App Password against `karissaperformanceintelligence@gmail.com`. Switched from OAuth on 2026-05-27 because Google's Testing-mode refresh tokens for Gmail restricted scopes (`gmail.modify`, `gmail.compose`, `gmail.readonly`) expire after 7 days, and publishing to Production requires a CASA security assessment that's impractical for a single-user project. App Passwords don't expire until the user changes their account password or revokes the App Password.

Env vars:

| Variable                     | Purpose                                          |
|------------------------------|--------------------------------------------------|
| `KPI_INBOX_EMAIL`            | `karissaperformanceintelligence@gmail.com`       |
| `KPI_INBOX_APP_PASSWORD`     | 16-char App Password (Google Account → Security → 2-Step Verification → App passwords). 2-Step Verification must be on for the option to appear. |
| `GMAIL_APP_PASSWORD`         | Reused from KPI pipeline (SMTP outbound for error emails) |
| `GMAIL_SENDER`               | Reused from KPI pipeline (error email sender)    |

The watcher uses stdlib `imaplib` against `imap.gmail.com:993`. Gmail-specific IMAP extensions used:
- **`X-GM-RAW`** — Gmail-syntax search inside an IMAP UID SEARCH command (preserves the original `has:attachment newer_than:Nd from:...` query verbatim).
- **`X-GM-MSGID`** — stable Gmail message ID returned via UID FETCH; written to the manifest + ledger for traceability.
- **`X-GM-LABELS`** — read/write Gmail labels via UID STORE. Auto-creates user labels on first use; removing the system `\Inbox` label is how we archive a message.

If `KPI_INBOX_EMAIL` or `KPI_INBOX_APP_PASSWORD` is missing, the watcher raises `RuntimeError` at auth time. The top-level handler writes an error run summary and exits 1; the workflow step has `continue-on-error: true` so the rest of the pipeline still runs.

A local smoke test is available at `scripts/test_inbox_imap.py` — connects and counts INBOX messages without writing anything. Use it to validate a freshly generated App Password before pushing.

### Processing order (invariant)

For every valid attachment, the watcher executes steps in this exact order — if any step fails, the next does not run:

1. **Validate sender via headers** — `From:` header must match whitelist. Search query filters the inbox; headers are the gatekeeper.
2. **Validate extension** — must be in `allowed_extensions`.
3. **Compute SHA256** — full-file content hash.
4. **Check ledger** — if hash already in `data/processed_attachments.json`, skip (duplicate).
5. **Archive** — write to `data/archive/YYYY-MM-DD/{hash[:6]}_{filename}`. Archive-before-inbox is a hard invariant.
6. **Write to inbox** — write to `data/inbox/{hash[:6]}_{filename}`. Hash prefix prevents collisions.
7. **Update ledger** — atomic write (temp file + rename) of `processed_attachments.json`.
8. **Write manifest** — `data/inbox/manifest_YYYY-MM-DD.json` with `trust_layer_flags: []` (Tier 2 populates).
9. **Write run summary** — `data/logs/inbox_watcher_YYYYMMDD_HHMMSS.json`.
10. **Label + archive email in Gmail** — only AFTER summary is written, so labeling failures never corrupt the ledger.

### Per-message outcome tracking

The watcher tracks every message's result in a `message_outcomes` dict (`success`, `partial`, or `error`). Overall run status is derived from the mix:

| Outcome           | Gmail behavior                                              |
|-------------------|-------------------------------------------------------------|
| `success`         | Apply `KPI-Processed` label + remove `INBOX` label (archives the thread). |
| `partial_success` | Apply `KPI-Attention` label, keep `INBOX` (stays visible). |
| `error`           | No label change, no archive. Inbox is untouched.            |

### Manifest contract

Tier 2 (`parsers/tier2_pdf_batch.py`) reads `data/inbox/manifest.json` — a single file overwritten each run. It's a JSON array of per-attachment records (one row per attachment, not per email):

```json
[
  {
    "filename": "Karissa_Salon_Weekly_Report.xlsx",
    "safe_filename": "a1b2c3_Karissa_Salon_Weekly_Report.xlsx",
    "archived_path": "data/archive/2026-04-21/a1b2c3_Karissa_Salon_Weekly_Report.xlsx",
    "inbox_path": "data/inbox/a1b2c3_Karissa_Salon_Weekly_Report.xlsx",
    "hash": "a1b2c3...full sha256...",
    "message_id": "18f5d2a0c3e1b4f7",
    "sender": "elaina@karissasalon.com",
    "date_received": "Mon, 21 Apr 2026 06:18:22 -0500",
    "processing_status": "ready",
    "trust_layer_flags": []
  }
]
```

`processing_status` is one of: `ready` (clean, ready for Tier 2), `duplicate_skipped`, `invalid_extension_skipped`, `security_rejected`. Tier 2 should only process records where `processing_status == "ready"`.

Tier 2 populates `trust_layer_flags` after parsing; the watcher never touches that field. When Tier 2 is wired in a future session, it will consume this manifest and call `email_sender.send_inbox_notification(status="success", ...)` on completion. The watcher itself only fires the `"error"` notification path.

### Run logs

Every run writes `data/logs/inbox_run_YYYY-MM-DD-HHMMSS.json` with:

- `status` (`success` / `partial_success` / `no_files` / `error`)
- `emails_scanned`, `attachments_found`, `new_files`, `duplicates_skipped`, `invalid_extension_skipped`, `security_rejections`
- `run_time` (UTC ISO-8601)
- `notes` — human-readable trace (also used for fatal errors)

If the script dies with an unhandled exception, a top-level `try/except` guarantees a `status="error"` run summary is still written before the process exits 1.

### Files created / modified by the watcher

| Path                                    | Purpose                                                |
|-----------------------------------------|--------------------------------------------------------|
| `data/inbox/{hash[:6]}_*.xlsx\|xls\|pdf` | Fresh attachments for Tier 2 to process.              |
| `data/inbox/manifest.json`              | Manifest consumed by Tier 2 (overwritten each run).   |
| `data/archive/YYYY-MM-DD/{hash[:6]}_*`  | Permanent audit copy of every accepted attachment.    |
| `data/processed_attachments.json`       | SHA256 idempotency ledger (append-only).              |
| `data/logs/inbox_run_*.json`            | Per-run execution summary.                            |

### Ledger persistence (committed to git)

GitHub Actions runners are ephemeral — every workflow run starts on a fresh filesystem. To keep the SHA256 dedup ledger durable across Mondays, **`data/processed_attachments.json` is intentionally un-gitignored and committed by the weekly pipeline**.

How it works:

- `.gitignore` has an explicit `!data/processed_attachments.json` exception (everything else under `data/` stays ignored).
- After Step 5 (`Run Gmail Attachment Watcher`), the workflow runs **Step 5.1 `Commit inbox ledger`** — it stages the ledger file, and if `git diff --cached` shows changes, creates a `chore(inbox): update SHA256 dedup ledger` commit. No-op weeks (no new attachments) produce no commit.
- The existing Step 7 `Push dashboard changes` then rebases and pushes the ledger commit alongside main.py's dashboard commit in a single push.
- The ledger commit is skipped under `workflow_dispatch` with `dry_run=true` to avoid polluting git history with test runs.

Contents committed:

```json
{
  "<sha256_hex>": {
    "filename": "Weekly_Report.xlsx",
    "processed_at": "2026-05-26T12:03:17.482912+00:00",
    "message_id": "18f5d2a0c3e1b4f7"
  }
}
```

No PHI — filenames may reveal report cadence, but location names are already public in `config/customers/karissa_001.json` and the dashboards. Annual ledger size is bounded at ~300 KB (52 weeks × ~12 locations × 2 formats). See `INBOX_LEDGER_PERSISTENCE_DECISION.md` at repo root for the option-evaluation that led to this design.

### Dry run

Set `"dry_run": true` in `config/inbox_config.json`:

- Auth still runs (verifies tokens)
- Messages and attachments still fetch
- Hash + dedup check still run
- **No files are written to archive or inbox**
- **No Gmail labels applied or emails archived**
- **No ledger updates**
- **No notification emails sent**
- Run summary IS written (so you can inspect what would have happened)

Flip back to `false` before deploying.

### Manual re-run

`python parsers/gmail_attachment_watcher.py` from repo root is idempotent — the SHA256 ledger guarantees that re-running against the same inbox never duplicates a file. Safe to run ad-hoc for debugging.

### Don't do these things

1. Don't remove the archive-before-inbox invariant — archive is the audit trail; inbox is transient.
2. Don't change the manifest JSON schema without updating Tier 2's reader.
3. Don't add sender validation via search query only — headers are the gatekeeper (defense in depth).
4. Don't delete the SHA256 ledger. If you need to reprocess, restore from `data/archive/` instead.
5. Don't touch `trust_layer_flags` in the manifest from the watcher — Tier 2 owns that field.
6. Don't write `success` notification emails from the watcher — Tier 2 fires that after parse confirms data is good.

---

## Tech stack

- Python 3.x (GitHub Actions runner: 3.11)
- `gspread` / `google-auth` — Google Sheets read/write
- `openpyxl` — Excel report generation
- `anthropic` — Claude API
- `Pillow` — icon generation
- `authlib` — OAuth 2.0 (prepared for Zenoti integration)
- `requests` — HTTP (in requirements.txt; reserved for future API connectors — not currently used in production code)
- `backoff` — retry logic
- `python-dotenv` — local .env loading
- GitHub Actions — weekly + daily automation
- GitHub Pages — hosting (docs/ folder → public)
- WebAuthn API — biometric auth in jess.html / jenn.html (device-side, zero server cost)

---

## Git workflow

- Main branch: `main`
- Pipeline auto-commits to `docs/` every Monday (3 dashboard HTML files)
- Email assistant auto-commits `docs/karissa-debrief.html` every weekday morning
- Tony pushes to GitHub via **GitHub Desktop** — the Cowork VM doesn't store git credentials
- After Cowork makes changes: commit here, Tony opens GitHub Desktop and hits "Push origin"
- If pipeline ran while Cowork was making changes: resolve using the programmatic reapply approach (extract pipeline HTML from conflict markers, re-apply PWA additions on top)

---

## Architecture phases

### Phase 1 — In production (current)
```
Data entry  → Google Sheets (manual weekly entry by Karissa's team into CURRENT tab)
Storage     → Google Sheets tabs (CURRENT, DATA, GOALS, ALERTS, STYLISTS_*)
Processing  → Python pipeline (main.py + core/ modules)
Delivery    → GitHub Pages dashboards (index.html, jess.html, jenn.html, owners.html)
```

### Phase 2 — Built, activating
```
Email layer → Gmail OAuth → Email Assistant → karissa-debrief.html
              BUILT. Awaiting Gmail OAuth secrets in GitHub Secrets.
API feeds   → Zenoti API + Salon Ultimate API
              Schema contracts written. API access not yet granted by Karissa.
```

### Phase 3+ — Future
```
Storage     → PostgreSQL or BigQuery (replace Google Sheets as storage layer)
Processing  → dedicated analytics pipeline (dbt or similar)
Auth        → server-side authentication (Cloudflare Worker or similar)
Chat        → AI assistant chat scoped per manager
```

**Intent documented for future developers:** The Google Sheets layer is a pragmatic bridge, not the final architecture. Schema contracts (`zenoti_schema.json`, `salon_ultimate_schema.json`) and the `DATA` append ledger pattern are designed to survive a Layer 2 migration with minimal pipeline changes.

---

## Don't do these things without asking first

1. Change any PIN values
2. Change WebAuthn credential keys or rpId
3. Delete or rename location IDs (z001-z010, su001-su002)
4. Add Woodbury back
5. Touch `GOOGLE_SERVICE_ACCOUNT_JSON` handling — it's base64 encoded for a reason
6. Edit `docs/*.html` directly for permanent features — put them in `dashboard_builder.py` (exception: `owners.html`, `karissa-debrief.html`, and prototype/demo files are manually maintained and safe from pipeline overwrites)
7. Change `cancel-in-progress` in the pipeline without understanding the concurrency implications
8. Commit anything to `voice/samples/` — Karissa's private emails must never be committed to the repo
9. Add `COACH_CARD_DATA` or `VISIT_PREP_*` coach card data to `index.html` (Karissa's dashboard) — coach cards are for managers only
10. Change the hardened prompt format contract in `ai_coach_cards.py` (Observation → Context → Question) without re-reviewing the full spec in `KPI_Coach_Card_AI_Prompt_Hardened.docx`

---

## Vocabulary Map — spec terminology ↔ existing modules

The PDF Parser Final Spec v1.0.0 (LOCKED) uses some terminology that maps to existing code under different names. This table is the canonical translation. Created 2026-05-26 per PARSER_AUDIT_2026-05-26.md §6.2 and Tony's decision matrix Q2.

| Spec term | Existing module / pattern | Notes |
|---|---|---|
| "Salon-level supremacy" (FINAL_SPEC §5) | `utils/data_merger.py` — Karissa-approved proportional distribution; salon-level PDF totals are sacred, stylist values are derived from them | Verified by KPI_AUDIT_REPORT_2026-04-20.md §4.1 |
| "Reconciliation engine" / cross-file totals | `trust_layer/completeness_validator.py::_check_cross_file_totals` | Returns `CompletenessCheck` objects with severity scores |
| "Same-week file verification" | `trust_layer/cross_file_verifier.py` | Hard-raises ValueError on location/period/system mismatch |
| "Truth Mediation Log" (FINAL_SPEC §10) | **Hybrid (per Tony 2026-05-26):** in-memory = `trust_layer/severity.py::CompletenessCheck` + `IntegrityReporter`; on-disk NDJSON = `trust_layer/truth_mediation_log.py::write_event` / `read_events`, file at `data/logs/truth_mediation_log.json` (gitignored). Tier 2 dispatches via `parsers/tier2_pdf_batch.py::_write_truth_mediation_events` | Implemented 2026-05-26 in `truth-mediation-log-serializer-2026-05-26`. Recognized `rule_applied` values: `salon_level_supremacy`, `product_total_mismatch`, `partial_week_detected`, `cross_file_reconciled`. See PARSER_SPEC_v1.0.1_ADDENDUM.md §J |
| "Trust Layer flags" (general spec language) | (a) `trust_layer_flags[]` per record in `data/inbox/manifest.json`; (b) parser `FLAG_*` constants in `pdf_zenoti_v2.py` / `pdf_salon_ultimate_v2.py`; (c) Tier 2 flags in `tier2_pdf_batch.py` | Three layers — parser, orchestrator, manifest |
| "Atomic batch processing" | `trust_layer/atomic_processor.py::AtomicProcessor` | Staging Phase 4 stubs in place |
| "Stylist identity / canonical IDs" | `trust_layer/stylist_identity_resolver.py` | Phase 3B; Sheets I/O stubbed pending implementation |
| "Chunked architecture" (FINAL_SPEC §8) | `ZenotiV2Parser.parse()` / `SalonUltimateV2Parser.parse()` orchestrating `_extract_raw_fields` → `_extract_service_categories` → `_extract_employees` → `_compute_karissa_kpis` | Conceptually chunked but not structurally gated; functional equivalence to spec's 4-chunk model |
| "Unclosed-day detection + alert" (FINAL_SPEC §6.1) | Detection: `parsers/pdf_common.py::detect_unclosed_days` + `PARTIAL_WEEK` parser flag. Alert: `core/email_sender.py::send_partial_week_alert` fired from `parsers/tier2_pdf_batch.py::process_manifest` after `_update_manifest` succeeds | Phase 1 (detection + alert) implemented 2026-05-26 in `unclosed-day-alert-hook-2026-05-26`. Phase 1.1 (automated rerun + Mon-EOD blank-out) still deferred. See PARSER_SPEC_v1.0.1_ADDENDUM.md §I |
| "Color % = Color Net / Service Net" (FINAL_SPEC §3.2) | Implemented in both `pdf_zenoti_v2.py` and `pdf_salon_ultimate_v2.py` `_compute_karissa_kpis()` as of 2026-05-26 (was incorrectly using `total_sales` denominator prior) | See PARSER_AUDIT_2026-05-26.md §3 |
| "Production hours" (FINAL_SPEC §6.6) Zenoti source | `pdf_zenoti_v2.py::_extract_production_hours_total` reads EMPLOYEE PERFORMANCE Total → PRODUCTION_HOURS column (field 4). HOURLY WORK extraction deleted 2026-05-26 (was buggy + spec-non-compliant) | See PARSER_AUDIT_2026-05-26.md §6.1 amendment |
| "Product header vs detail mismatch detection" (FINAL_SPEC §6.2) | `pdf_salon_ultimate_v2.py::_RE_PRODUCT_LINES_TOTALS` + `FLAG_PRODUCT_TOTAL_MISMATCH`. Compares Sales-block `Total Retail` header vs Top Product Lines TOTALS row. Header stays canonical `product_net` per spec; flag fires on mismatch | Implemented 2026-05-26 in branch `product-mismatch-detection-2026-05-26`. Lakeville is the canonical test case (header $534.50 vs TOTALS $623.25). See PARSER_SPEC_v1.0.1_ADDENDUM.md §H |

**Use this table when:**
- Reading FINAL_SPEC and wondering "is that built? where?"
- Writing new code — prefer existing module names, not spec vocabulary
- Updating PARSER_SPEC_v1.0.1_ADDENDUM.md — cross-reference the modules here

---

## Latest audit

**2026-04-20 senior-level technical audit** — see `KPI_AUDIT_REPORT_2026-04-20.md` at repo root. 14 sections covering architecture, parsers, data merger, Sheets integration, trust layer, tests, production readiness. Verdict: SHIP-READY for Phase 1. Findings resolved same session:

- ✅ BUG-1: Prior Lake POS routing (Zenoti, not Salon Ultimate) — `config/locations.py`
- ✅ BUG-2: Missing "FS" filename aliases (Andover FS, Crystal FS, Elk River FS) — `config/locations.py`
- ✅ Docstring lie in `utils/sheets_writer.py` (20 → 22 columns)
- ✅ Sandbox updated: now validates `ai_coach_cards` as Module 9 (9/9 PASS)
- ✅ `docs/manifest.json` stale "13 locations" → "12 locations"
- ✅ Confirmed `append_to_historical()` + `append_to_stylists_historical()` are already built and wired

**Test status as of 2026-04-20:**
- Sandbox: 9/9 PASS
- Trust layer: 167/167 PASS in 0.34s
- Zero secrets in repo, `.gitignore` covers voice samples and logs
