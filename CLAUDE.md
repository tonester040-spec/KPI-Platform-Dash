# KPI — Karissa Performance Intelligence
### Claude project context — read this before every session

---

## What this project is

A weekly salon analytics platform for **Karissa**, a multi-location salon owner in Minnesota. Every Monday at 7:00 AM Central the pipeline runs automatically via GitHub Actions. It reads salon performance data from a Google Sheet, generates AI commentary, builds HTML dashboards for 3 different managers, sends an Excel report by email, and pushes everything to GitHub Pages.

**Live dashboard:** https://tonester040-spec.github.io/KPI-Platform-Dash/
**GitHub repo:** https://github.com/tonester040-spec/KPI-Platform-Dash
**Google Sheet ID:** `1JY6L7H1Pb2JFmNoz2XNkvG0ogrYgagLVDwH01vuWT28`
**Owner contact:** Tony (tonester60@hotmail.com) — not Karissa's dev, he's building this FOR her

---

## Architecture at a glance

```
Google Sheets (source of truth)
    ↓
main.py (pipeline orchestrator)
    ↓ reads
core/data_source.py       → pulls from Sheets
core/data_processor.py    → enriches, ranks, flags
core/ai_cards.py          → Claude API summaries per location + stylist
core/sheets_writer.py     → writes CURRENT, STYLISTS_CURRENT, ALERTS tabs back
core/report_builder.py    → generates 5-sheet Excel report (openpyxl)
core/dashboard_builder.py → builds docs/index.html, docs/jess.html, docs/jenn.html
core/email_sender.py      → sends Excel to Karissa via Gmail
core/git_pusher.py        → commits + pushes docs/ to main
    ↓
GitHub Pages (public PWA)
    docs/index.html   → Karissa's full dashboard (all 12 locations)
    docs/jess.html    → Jess's PIN-gated dashboard (her 4 locations)
    docs/jenn.html    → Jenn's PIN-gated dashboard (her 5 locations)
    docs/manifest.json + docs/sw.js → PWA (installable on iPhone)
```

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

---

## Google Sheets tab structure

| Tab               | What it holds                                              |
|-------------------|------------------------------------------------------------|
| `CURRENT`         | 12 rows — current week snapshot (pipeline overwrites each run) |
| `DATA`            | Append ledger — all historical weeks (never overwritten)   |
| `GOALS`           | Per-location annual targets                                |
| `ALERTS`          | Flag summary written by pipeline                           |
| `STYLISTS_CURRENT`| Current week stylist rows (pipeline overwrites)            |
| `STYLISTS_DATA`   | Historical stylist data (append ledger)                    |
| `WEEKLY_DATA`     | Weekly aggregates                                          |
| `WEEK_ENDING`     | Lookup tab for current week date                           |

**Known gap:** `sheets_writer.py` rewrites CURRENT and STYLISTS_CURRENT but does NOT automatically append to DATA/STYLISTS_DATA after each run. The `append_to_historical()` function (~20 lines) still needs to be added. Do not assume this is working until confirmed.

---

## Weekly pipeline — key behavior

- **Runs:** Every Monday 7:00 AM Central (12:00 UTC) via `weekly_pipeline.yml`
- **CRITICAL:** The pipeline **regenerates `docs/index.html`, `docs/jess.html`, `docs/jenn.html` from scratch** every run by calling `dashboard_builder.py`. Any manual edits to those HTML files will be overwritten the next Monday.
- **Correct fix:** All permanent additions (PWA meta tags, PIN gate, WebAuthn JS, install banner) must be baked INTO `core/dashboard_builder.py` — not hand-edited into docs/*.html.
- **Concurrency:** `cancel-in-progress: false` (changed from true after stability — flip back if queued runs pile up)
- **Dry run available:** `DRY_RUN=true python main.py` skips all writes, API calls, email, git push

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

| Variable                      | Required | Purpose                              |
|-------------------------------|----------|--------------------------------------|
| `GOOGLE_SERVICE_ACCOUNT_JSON` | ✅       | Base64-encoded service account JSON  |
| `ANTHROPIC_API_KEY`           | ✅       | Claude API for AI card generation    |
| `GMAIL_APP_PASSWORD`          | ⚠️ soft | Gmail App Password — skipped if missing |
| `GMAIL_SENDER`                | ⚠️ soft | Gmail sender address                 |
| `ACTIVE_CUSTOMER_ID`          | default  | Defaults to `karissa_001`            |

---

## Excel report structure (report_builder.py)

5 sheets generated by `core/report_builder.py`:
1. **Summary** — network-wide KPIs at a glance
2. **Locations** — per-location breakdown including Service Net $, Product Net $, PPG, Prod Hours
3. **Service Mix** — Wax / Color / Treatment breakdown vs network averages (color coded)
4. **Goals & YOY** — 6 live columns + 8 gray placeholder columns (⏳ prefix) for future Zenoti API feeds
5. **Stylists** — per-stylist performance

---

## What's paused / future state

- **AI assistant chat** — floating chat UI, Cloudflare Worker proxy for API key security, scoped per manager. Architecture designed, not built. Waiting on historical data decisions.
- **Historical backfill** — Karissa may have 2-4 years of data in Zenoti/Salon Ultimate. 50/50 on whether it's exportable. Architecture ready (DATA tab is the append ledger). `append_to_historical()` function needs to be written in `sheets_writer.py` first.
- **Zenoti/Salon Ultimate API feeds** — placeholder columns exist in Goals & YOY sheet. API access not yet granted by Karissa. Do not wire up API calls until credentials arrive.
- **Current week detection** — `data_source.py` may still have hardcoded date. Should read `max(week_ending)` dynamically. Confirm before each pipeline session.

---

## Tech stack

- Python 3.x
- `gspread` / `google-auth` — Google Sheets read/write
- `openpyxl` — Excel report generation
- `anthropic` — Claude API (claude-sonnet-4-6 for AI cards; consider claude-haiku-4-5 for cheaper categorization tasks)
- `Pillow` — icon generation
- GitHub Actions — weekly automation
- GitHub Pages — hosting (docs/ folder → public)
- WebAuthn API — biometric auth in jess.html / jenn.html (device-side, zero server cost)

---

## Git workflow

- Main branch: `main`
- Pipeline auto-commits to `docs/` every Monday — this will conflict with manual HTML edits
- Tony pushes to GitHub via **GitHub Desktop** — the Cowork VM doesn't store git credentials
- After Cowork makes changes: commit here, Tony opens GitHub Desktop and hits "Push origin"
- If pipeline ran while Cowork was making changes: resolve using the programmatic reapply approach (extract pipeline HTML from conflict markers, re-apply PWA additions on top)

---

## Don't do these things without asking first

1. Change any PIN values
2. Change WebAuthn credential keys or rpId
3. Delete or rename location IDs (z001-z010, su001-su002)
4. Add Woodbury back
5. Touch `GOOGLE_SERVICE_ACCOUNT_JSON` handling — it's base64 encoded for a reason
6. Edit `docs/*.html` directly for permanent features — put them in `dashboard_builder.py`
7. Change `cancel-in-progress` in the pipeline without understanding the concurrency implications
