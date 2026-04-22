# KPI — Karissa Performance Intelligence
### Claude project context — read this before every session

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
core/data_processor.py    → enriches, ranks, flags
core/ai_cards.py          → Claude API summaries per location + stylist
                             (claude-haiku-4-5-20251001 for bulk stylist cards,
                              claude-sonnet-4-6 for coach briefing)
core/ai_coach_cards.py    → Claude API coach cards for Jess & Jenn (claude-sonnet-4-6)
                             Hardened prompt: Observation → Context → Question format
                             Falls back to dry-run placeholder on JSON parse failure
core/sheets_writer.py     → writes CURRENT, STYLISTS_CURRENT, ALERTS tabs back
                             + JESS_BRIEF (ALERTS!A100) and JENN_BRIEF (ALERTS!A101)
core/report_builder.py    → generates 5-sheet Excel report (openpyxl)
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
- Runs data_processor, drift_checker, ai_cards (DRY_RUN), sheets_writer (DRY_RUN), report_builder (DRY_RUN), email_sender (DRY_RUN), dashboard_builder (DRY_RUN), alerter test, ai_coach_cards (DRY_RUN)
- Confirms all 9 modules pass before any real API credential is connected
- Fires a deliberate test alert so the alerter path is confirmed working

**Sandbox must show 9/9 PASS before any release is considered shippable.** ✓ (confirmed 2026-04-20 post-audit)

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
| `KPI_INBOX_CLIENT_ID`         | ⚠️ soft | OAuth client ID for Gmail attachment watcher (Step 0). Missing → watcher skips, pipeline continues. |
| `KPI_INBOX_CLIENT_SECRET`     | ⚠️ soft | OAuth client secret for Gmail attachment watcher |
| `KPI_INBOX_REFRESH_TOKEN`     | ⚠️ soft | OAuth refresh token for Gmail attachment watcher |

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
- Gmail attachment watcher uses **OAuth 2.0** (inbox read + labels + archive on the dedicated `karissaperformanceintelligence@gmail.com` inbox — `KPI_INBOX_*` secrets)

---

## Excel report structure (report_builder.py)

5 sheets generated by `core/report_builder.py`:
1. **Summary** — network-wide KPIs at a glance
2. **Locations** — per-location breakdown including Service Net $, Product Net $, PPG, Prod Hours
3. **Service Mix** — Wax / Color / Treatment breakdown vs network averages (color coded)
4. **Goals & YOY** — 6 live columns + 8 gray placeholder columns (⏳ prefix) for future Zenoti API feeds
5. **Stylists** — per-stylist performance

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

Separate Gmail OAuth flow from the email assistant (different Gmail account). Env vars:

| Variable                     | Purpose                                          |
|------------------------------|--------------------------------------------------|
| `KPI_INBOX_CLIENT_ID`        | OAuth client ID for the KPI inbox                |
| `KPI_INBOX_CLIENT_SECRET`    | OAuth client secret                              |
| `KPI_INBOX_REFRESH_TOKEN`    | Long-lived refresh token — generate once locally |
| `GMAIL_APP_PASSWORD`         | Reused from KPI pipeline (SMTP outbound for error emails) |
| `GMAIL_SENDER`               | Reused from KPI pipeline (error email sender)    |

The watcher calls the Gmail REST API directly via `urllib` (same pattern as `email_assistant/gmail_connector.py`) — no `google-api-python-client` dependency added. If OAuth env vars are missing, the watcher logs and exits cleanly (does not crash the workflow).

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

Tier 2 (`parsers/tier2_batch_processor.py`) reads `data/inbox/manifest.json` — a single file overwritten each run. It's a JSON array of per-attachment records (one row per attachment, not per email):

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
