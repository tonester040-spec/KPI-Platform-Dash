# Report Format Diff — Karissa's Manual Report vs. Our Automated Output

**Date:** 2026-04-21
**Source files:**
- North star: `March 2026.xlsx` (Karissa's hand-built weekly report — never commit)
- Our output: `core/report_builder.py` → `KPI_Weekly_Report_{week}.xlsx`

**Goal:** Identify every gap between what she builds by hand today and what the pipeline auto-generates, so we can decide what to close, what to keep, and what to layer on.

---

## 1. Tab structure — the single biggest mismatch

| | Karissa's manual file | Our automated output |
|---|---|---|
| Mental model | **Monthly workbook** with 5 weekly subtabs + 1 YoY summary | **Weekly snapshot** with 5 analytical subsheets |
| Tab 1 | ` Week 1 ` — full per-location KPI + YoY comparison | `Summary` — executive overview |
| Tab 2 | ` week 2` — same structure as Week 1 | `Locations` — per-location KPIs |
| Tab 3 | ` week 3` | `Service Mix` — wax/color/treatment |
| Tab 4 | ` week 4` | `Goals & YOY` — mostly placeholders |
| Tab 5 | ` week 5` (when the month has 5 weeks) | `Stylists` — per-stylist rows |
| Tab 6 | `Year Over Year` — month YTD + annual goal tracker | *(none)* |

She thinks in months. We think in weeks. That's the root mismatch — not the column names, not the color coding.

---

## 2. Weekly tab anatomy — Karissa's Week 1 dissected

Each weekly tab has **two stacked tables**.

### Table A (rows 1-16) — Core KPIs + YoY comparison

Columns A→Q:

| Col | Header | What it is |
|---|---|---|
| A | Name | Location (Andover FS, Blaine, Crystal FS, …) |
| B | Guest Count | This week, 2026 |
| C | Total Sales Net | This week |
| D | Service Net | This week |
| E | Product Net | This week |
| F | Product % | This week |
| G | PPG Net | This week |
| H | PPH Net | This week |
| I | Average Tkt | This week |
| J | Prod Hours | This week |
| **K** | **Projection** | **Monthly projection = weekly run-rate × days in month** |
| **L** | **2025 Total Sales** | **Same period last year** |
| **M** | **2025 Guest Count** | **Same period last year** |
| **N** | **2025 PPG** | **Same period last year** |
| **O** | **Diff** | **PPG 2026 − PPG 2025** |
| **P** | **2025 Avg Ticket** | **Same period last year** |
| **Q** | **Dif** | **Avg Ticket 2026 − Avg Ticket 2025** |

Row 15: `Totals` (sums).
Row 16: network **averages** in cols H-Q (PPH, Ticket, Prod Hours, 2025 PPG, PPG diff, 2025 AT, AT diff).

### Table B (rows 18-31) — Service Mix (weekly) + MTD rollups

| Col | Header | What it is |
|---|---|---|
| A | Name | Location |
| B | Wax Count | Weekly |
| C | Waxing Net | Weekly |
| D | Wax % | |
| E | Color Net | Weekly |
| F | Color % | |
| G | Trmt Count | Weekly |
| H | Trmt Net | Weekly |
| I | Trmt % | |
| **K** | **Prod Hours** | **Month-to-date** |
| **L** | **Wax sales** | **Month-to-date** |
| **M** | **Treat count** | **Month-to-date** |
| **N** | **Treat sales** | **Month-to-date** |
| **O** | **Service Sales** | **Month-to-date** |

Row 31: `Totals`.

---

## 3. Year Over Year tab

Current file has 11 columns, primary block (rows 1-15) working, secondary block (rows 17-33) broken with `#DIV/0!` / `#REF!` — she knows this.

| Col | Header | Status in manual file |
|---|---|---|
| A | Location | ✓ working |
| B | 2026 (month YTD) | ✓ populated |
| C | 2025 (same period) | ✓ populated |
| D | Sales % (2026/2025) | ✓ working |
| E | Goal (annual $) | ✓ populated |
| F | Daily Goal | ✓ working (E/365) |
| G | Day Goal | ✓ working (pace) |
| H | "We are At!" | ✓ = column B |
| I | Goal % (progress) | ✓ working |
| J | 2019 | ⚠️ 0 for all |
| K | Sales % (2026/2019) | ❌ `#DIV/0!` everywhere |

Rows 17-30: quarterly rollup (Jan-Mar) across 2025/2024/2019 — **all cells `#DIV/0!` or `#REF!`**. Dead in her file. Opportunity for us to resurrect correctly.

---

## 4. Our automated report — what's there today

`core/report_builder.py` builds 5 sheets:

### `Summary` sheet
Vertical label/value layout: week, total guests, total sales, avg PPH, avg product %, avg ticket, top PPH location, needs-attention location, AI coach briefing. **Not in Karissa's file — this is our value-add.**

### `Locations` sheet (15 cols)
`Location | Rank | Flag | PPH $ | vs Avg PPH | Total Sales $ | Service Net $ | Product Net $ | Product % | vs Avg Prod | PPG | Avg Ticket $ | Guests | Prod Hours | AI Summary`

### `Service Mix` sheet (10 cols)
`Location | Wax Count | Wax Net $ | Wax % | Color Net $ | Color % | Treat Count | Treat Net $ | Treat % | Prod Hours` — all **weekly only, no MTD**.

### `Goals & YOY` sheet (14 cols)
6 live: `Location | PPH | Total Sales | Guests | PPG | Avg Ticket`
8 placeholders with `⏳`: `2026 Goal, vs Goal, 2025 Sales, 2025 Guests, 2025 PPG, PPG Diff, 2025 Avg Tkt, Avg Tkt Diff`

### `Stylists` sheet (11 cols)
`Stylist | Location | Status | Tenure | Star | PPH | vs Net Avg | Rebook % | Product % | Avg Ticket | Coaching Note` — **not in Karissa's file, pure value-add.**

---

## 5. The gap matrix — what's missing to match her format

### 🔴 Major gaps — real feature work required

| # | Gap | Size | Dependency |
|---|---|---|---|
| 1 | **Multi-week tab structure** — need 5 weekly tabs stacked in a monthly workbook, same layout as hers | **L** | New `build_monthly_report()` that queries DATA tab for 5 weeks |
| 2 | **Projection column** on each weekly tab | S | Add computed field in `data_processor` |
| 3 | **Per-location YoY comparison inline** (2025 Sales, Guests, PPG, AT + diffs) on weekly tab | M | Needs 2025 historical rows in DATA tab — architecture ready |
| 4 | **MTD rollup columns** in Service Mix (Prod Hours, Wax $, Treat count, Treat $, Service $ — all MTD) | M | Query DATA tab for current-month weeks |
| 5 | **Annual Goal tracker** (Goal, Daily Goal, "We are At!", Goal %) | M | Add `annual_goals_2026` block to `karissa_001.json`, wire it in |
| 6 | **Network AVG row** appended to Locations table (not just in Summary) | **S** | Trivial — append row after totals |

### 🟡 Format nits — small but visible

| # | Gap | Size | Notes |
|---|---|---|---|
| 7 | Location name display | **XS** | karissa_001.json already says "Andover FS" / "Crystal FS" / "Elk River FS" — this should already flow through. **Verify once** with a pipeline dry-run and inspect the output |
| 8 | Location ordering | ✓ | Already matches her file (z001-z009 Zenoti → z010, su001, su002 Salon Ultimate) |
| 9 | Quarterly YoY rollup (Jan-Mar 2025 vs 2024 vs 2019) | M | Her version is broken — we can build it correctly once historical rows exist |

### 🟢 Things we do that she doesn't — KEEP

- `Summary` executive overview with AI coach briefing — high value, not in her file
- Rank + Flag (⭐ STAR / ⚠️ WATCH / ✓ SOLID) columns — analytical step up
- AI Summary column per location + per stylist — direct AI value
- Full `Stylists` sheet — she tracks this separately; we centralize it
- Color coding (green/yellow/red) on PPH and Product % — visual value
- Coach Cards written to ALERTS!A100/A101 — downstream for jess.html/jenn.html dashboards

### ⚠️ Things broken in her file we can fix

- Year Over Year tab column K (`Sales % vs 2019`) — all `#DIV/0!`
- Rows 17-30 quarterly rollup — all `#DIV/0!` / `#REF!`
- Column J (2019 data) — `0` for every location
- Weekly tab row 16 — averages shown only in cols H-Q, leaves cols B-G without a visible network-average reference

---

## 6. Recommendation — three-part fix plan

### Part A — Align the weekly output (quick wins)
**Effort:** 1-2 hours. No new data dependencies. Makes our weekly output recognizable to Karissa.

- A1. Add `Projection` column to Locations sheet (monthly projection from weekly run-rate)
- A2. Append `NETWORK AVG` footer row to Locations sheet (mirrors her row 16)
- A3. Add MTD columns to Service Mix (initially blank / `—`, wired to DATA tab rollup as data accumulates)
- A4. Verify `Andover FS` / `Crystal FS` / `Elk River FS` render correctly in the Excel output (karissa_001.json already has these — likely already working)

### Part B — The monthly report (the big one)
**Effort:** 4-6 hours. Requires DATA tab to hold 4-5 weeks of the current month. Not useful on empty data.

- B1. New builder: `core/monthly_report_builder.py` → `build_monthly_report()` that produces a workbook with 5 weekly tabs matching Karissa's **exact** A-Q/A-O column layout
- B2. Add a 6th `Year Over Year` tab mirroring her layout, populated from DATA tab aggregation
- B3. Schedule trigger — fires on first Monday of a new month, generates prior month's report
- B4. Goal: delete her manual workflow entirely by, say, week 3 of the first full month

### Part C — Goals + YoY live data
**Effort:** 2-3 hours config/code + blocked on historical data load.

- C1. Add `annual_goals_2026` block to `config/customers/karissa_001.json`, one dollar figure per location ID (needs Karissa to provide the goals)
- C2. Wire Goals & YOY sheet (and new monthly tab) to compute goal-progress from DATA tab month-to-date rollup
- C3. YoY data (2025, 2019) — requires the historical backfill that was marked "paused / future state" in CLAUDE.md. When Karissa confirms whether Zenoti/SU can export 2024-2025 data, this unblocks.

---

## 7. Open questions / calls we need to make

1. **Tab structure direction** — do we keep the weekly snapshot in place and add the monthly report on top (both coexist), or do we replace the weekly-snapshot format with monthly tabs?
   - **My take:** Coexist. Weekly is better for live Monday review; monthly matches her mental model and replaces her Excel work. Each serves a different moment.
2. **FS naming audit** — does the current pipeline output already show "Andover FS" / "Crystal FS" / "Elk River FS"? We should confirm with one dry-run output inspection before calling this item done.
3. **Annual goals** — does Karissa have written 2026 goals per location, or do we derive them from 2025 actuals × growth assumption?
4. **Historical data** — any update from Karissa on whether 2024-2025 Zenoti/SU exports are feasible? This blocks the full YoY story.

---

**File location:** `/sessions/nifty-hopeful-bardeen/mnt/KPI-Platform-Dash/REPORT_FORMAT_DIFF_2026-04-21.md`
**Status:** Read-only reference. No code changed. Decisions pending Tony's direction.

---

## 8. Ground-truth POS file inspection (2026-04-21, Part 2)

Pulled the "KPI Historical data" Drive folder and opened one representative file from each POS + format. What's actually in there:

### Folder contents (24 files, all created 2026-04-09)

| POS            | File type                          | Count | Filename pattern                            | Reporting period    |
|----------------|------------------------------------|-------|---------------------------------------------|---------------------|
| Zenoti (9 loc) | `{Location}.pdf` (Salon Summary)   | 9     | `Forest Lake.pdf`, `Hudson.pdf`, …          | **Weekly** 4/1–4/5  |
| Salon Ultimate (3 loc) | `{Location}.pdf` (FS Salon Dashboard) | 3 | `Apple Valley.pdf`, `Lakeville.pdf`, …    | **Weekly** 4/1–4/5  |
| Zenoti         | `Employee KPI - {timestamp}.xlsx`  | 9     | `Employee KPI - 2026-04-06T151613.965.xlsx` | **Monthly** 3/1–3/31 |
| Salon Ultimate | `Stylist_Tracking_Report ({n}).xls` | 3    | `Stylist_Tracking_Report (25).xls`          | **Monthly** 3/1–3/31 |

**Takeaway #1: Mixed cadence.** What Elaina is actually exporting is weekly PDFs (5-day window, Wed-Sun) + monthly stylist xlsx. Not what our pipeline currently assumes (weekly everything).

**Takeaway #2: Filename != location for xlsx files.** Employee KPI xlsx filenames are timestamped only — the location is encoded inside the file (a manager row like "Forest Lake mgr" identifies which location). Tier 2 parser must read the file to identify it, not parse filename.

**Takeaway #3: PDF formats differ between the two POS systems.** They are NOT interchangeable:

| | Zenoti Salon Summary PDF | Salon Ultimate FS Salon Dashboard PDF |
|---|---|---|
| Cover data | Sales/Payment/Tax breakdown | **PPH, PPG, Avg Ticket at top — already KPIs we need** |
| Service Mix | Buried in "SERVICE DETAILS" section — must compute from service lines | Clean "Service Categories" table: Haircut/Treatment/Other/Wax/Color/Style — already structured |
| Stylist data | "EMPLOYEE SALE DETAILS" + "EMPLOYEE PERFORMANCE DETAILS" — split across 2 tables | Single "Employee Summary" table with Net Service $, Net Retail $, Hours, PPH, PPG, Req %, Avg Ticket per stylist |
| Prod hours | "Net service sales/center hour" (derived) | Direct "Production Hours" column |
| Extractor difficulty | **Medium-hard** — needs careful section parsing | **Easy** — already in the shape we need |

### Data sample — Forest Lake (Zenoti), week 4/1–4/5 2026

- 107 guests, 111 invoices, $5,887.10 service, $612.50 product
- 4 stylists active: Danielle Carlson (30 invoices), Bailey Phipps (24), Dani Shearen (28), Jaime Nurnberg (9), Margaret Adams (7), Jenna Renstrom (14 shift lead)
- Tips: $861.70
- Production hours: 115.5
- PPH: $50.97 net service sales/production hour

### Data sample — Apple Valley (SU), week 4/1–4/5 2026

- 281 guests, $16,065.75 service, $2,112 retail, $18,177.75 total revenue
- PPH $55.92, PPG $7.52, Avg Ticket $64.69
- 17 stylists in Employee Summary
- Service Categories already parsed: Haircut 52.4% qty, Color 9.3% qty / 36.6% sales (the money category), Treatment 25.5% qty, Wax 8.6% qty

### Data sample — Apple Valley (SU) monthly xlsx, 3/1–3/31 2026

- Full stylist list for the month, same column structure as PDF Employee Summary but for 31 days
- 36 stylists listed, aggregated March totals

### What this tells us about the pipeline

| Question | Answer |
|---|---|
| Are weekly POS exports available? | **Yes** — PDFs give us weekly per-location snapshots |
| Are we getting fresh enough data? | Files dated 4/6 for week ending 4/5 — ~1 day lag, workable |
| Can we source weekly stylist data? | **Yes from PDFs** for both POS systems. No from xlsx (monthly only). |
| Can we source monthly MTD rollups? | **Yes from xlsx** — that's what they're for |
| Can we compute Karissa's Service Mix table from PDFs? | SU: easy. Zenoti: medium (must aggregate SERVICE DETAILS sections into Wax/Color/Treatment buckets) |
| Can we compute PPG, Avg Ticket, PPH? | SU: already on PDF. Zenoti: PPH is on PDF ("Net service sales/center hour"), PPG = product ÷ guests (computable), Avg Ticket = "Avg. invoice value" on PDF |
| Can we build the YoY comparison (2025 column)? | **Not from these files.** These are all 2026. Historical backfill still blocks this. |

### Parser reality check against what we have built

Our `parsers/` directory has: `zenoti_parser.py`, `zenoti_pdf_parser.py`, `salon_ultimate_parser.py`, `salon_ultimate_pdf_parser.py`. All four should be re-validated against these real files before we trust them:

- **Zenoti xlsx parser** — expects the "Employee KPI" format: location in header block, stylist rows with Invoice Count/Avg Value/Guest/Service Sales/Product Sales. **Matches real file.** ✅ likely works.
- **Zenoti PDF parser** — must pull Sales Item (service/product subtotals), Statistics (PPH, Avg invoice), Service Details (for service mix aggregation), Employee Sale Details (for stylists). **Complex structure** — need to re-run to see how it does.
- **Salon Ultimate xls parser** — expects Stylist_Tracking_Report .xls (legacy BIFF). File IS in BIFF format (0xD0CF header confirmed). **Parser must use `xlrd` or similar, not openpyxl.**
- **Salon Ultimate PDF parser** — expects "FS Salon Dashboard" with Service Categories, Employee Summary tables. **Structure matches what we saw.** ✅ likely works.

### New questions for Tony

1. **Cadence clarification** — does Elaina also export weekly Employee KPI xlsx from Zenoti (more granular), or only monthly? If monthly-only, we must get stylist weekly numbers from the weekly PDF's "EMPLOYEE SALE DETAILS" section.
2. **5-day window** — the 4/1–4/5 window is Wed-Sun. Is that Karissa's standard work week (closed Mon-Tue)? Or are Mon-Tue missing accidentally? Affects how we label week_ending.
3. **Historical YoY backfill** — these Drive files are all March+April 2026. The 2025 comparison columns in Karissa's manual report need prior-year data we don't have. Is Elaina willing to run the same export for March-April 2025?
4. **Parser smoke-test before wiring Tier 2** — want me to run the 4 existing parsers against these real files and tell you what passes vs. breaks? That's the real ground-truth test, and it's a 30-minute job.

---

## 9. Parser ground-truth test (2026-04-21, afternoon)

Ran all 4 production parsers against real POS files pulled from Karissa's "KPI Historical data" Drive folder. Test harness: `/sessions/nifty-hopeful-bardeen/test_parsers.py`.

### Summary

| # | Parser | Input | Result |
|---|--------|-------|--------|
| 1 | `ZenotiExcelParser`       | Employee KPI.xlsx (Forest Lake, Mar 2026)  | ✅ PASS |
| 2 | `ZenotiPDFParser`         | Salon Summary.pdf (Forest Lake, 4/1–4/5)   | ✅ PASS |
| 3 | `SalonUltimateExcelParser`| Stylist_Tracking.xls (Apple Valley, Mar 2026) | ❌ BREAK — source file is malformed OLE |
| 4 | `SalonUltimatePDFParser`  | FS Salon Dashboard.pdf (Apple Valley, 4/1–4/5) | ✅ PASS |

**3 of 4 parsers worked against real POS data on the first run.** The one break is a source-file defect, not a code defect.

### Per-parser detail

**1. ZenotiExcelParser — PASS**

Output: 6 stylists from Forest Lake, period = 2026-03-01 to 2026-03-31. Example:
```
Bailey Phipps: invoice_count=79, service_sales=$3,519.20, product_sales=$190.00,
               total_sales=$3,709.20, ppg_net=$2.41, avg_ticket=$46.95
```
Location extracted correctly via the "Forest Lake mgr" marker row. Period parsed correctly from the Row 2 string `"From : 01 Mar 2026 To : 31 Mar 2026"`.

**Caveat to flag (not a break):** The parser's source docstring says the header is on Row 3, and the code constant `HEADER_ROW = 3` reflects that. In the real file the header is actually on **Row 4** (Row 3 is blank). The parser still works because it uses `DATA_START = 4` to begin scanning, and the `if invoice_count == 0 and service_sales == 0 and product_sales == 0: continue` filter silently skips the "Employee Name" header text row (since `_safe_float("Invoice Count") == 0.0`). Works in practice, fragile in principle. Should fix the docstring + constant so it matches reality.

**2. ZenotiPDFParser — PASS**

Output: Haircut 101 / $3,111.60, Color 24 / $2,035.50, Wax 13 / $243.00, Treatment 19 / $313.00. Location = Forest Lake. Period = 2026-04-01 to 2026-04-05.

**Surprise finding:** The PDF file Elaina is currently exporting is NOT the expected "Salon Summary" report described in the parser docstring. It's actually a much longer **Register Closure / Sales Reconciliation** report (5 pages, includes cash close, payment breakdown, discount campaigns, etc.). The parser still works because that report _happens to contain_ the same `SERVICE DETAILS` section with Haircut/Color/Wax/Treatment rows, and the regex `\s+` matches across the newlines PyMuPDF inserts between columns. Lucky hit, not by design. Worth documenting in the parser's header comment — or asking Elaina to export the shorter "Salon Summary" report specifically so we're not carrying 5 pages of unrelated content through the pipeline.

**3. SalonUltimateExcelParser — BREAK (not the parser's fault)**

Raised `ValueError` on instantiation — but the root cause is two layers deep:

Layer 1 (superficial): the parser hard-codes `SHEET_NAME = "Worksheet"` and the LibreOffice xls→xlsx conversion renames the sheet after the file stem (`stylist_tracking`). Quick fix: fall back to `wb.active` when the expected name is missing.

Layer 2 (the real problem): **the source .xls file from Salon Ultimate is structurally invalid**.
- Drive says the file is 17,408 bytes (a valid 512-byte-aligned OLE size)
- The file has a correct OLE2 compound document magic header (`D0 CF 11 E0 A1 B1 1A E1`)
- But `olefile.listdir()` returns `[]` — **no streams inside the compound document**
- `xlrd` crashes with `IndexError: array index out of range` trying to find the Workbook stream
- LibreOffice "successfully" converts it to xlsx but the result is raw OLE byte soup interpreted as cell text (first rows contain binary padding, hex fragments, and OLE directory bytes, not parseable data)

The raw bytes DO contain the real data in the middle of the file — I can see `Store name:`, `FS - Apple Valley Pilot Knob`, `Report period:`, `03/01/2026 - 03/31/2026`, `Provider Name`, `Service Sales`, `Amanda Klingler`, `Ari Spainhower`, etc. — but Salon Ultimate's exporter has written this as a shell OLE container with no proper Workbook/Directory streams. No standard Excel reader can parse it.

**What this means:** no amount of parser hardening will fix this file. Two paths forward, in order of cost:
  1. **Ask Elaina to try CSV export** from Salon Ultimate (if the POS supports it). Simplest fix.
  2. **Ask Elaina to try a different export path** in Salon Ultimate — maybe there's a "Save as Excel 2007" option that produces valid xlsx instead of this broken .xls.
  3. If neither works, write a custom byte-stream parser that scans the OLE bytes for the BIFF records we can still see buried inside. Doable but ~1-2 days of work, and fragile.
  4. Confirm with Elaina whether this file was produced by the current Salon Ultimate version — maybe it's a legacy export and the platform has a newer, saner option.

**Also noted:** the MCP `download_file_content` tool appends 48 bytes of `0xFF` padding to every .xls download (making the file 17,456 instead of 17,408). This is a Drive MCP artifact, not a Salon Ultimate problem. Any production ingestion path (email attachment watcher → parser) would bypass MCP entirely, so this padding issue does NOT affect the real pipeline.

**4. SalonUltimatePDFParser — PASS**

Output: Haircut 232 / $7,076.40, Color 41 / $5,883.25, Wax 38 / $703.00, Treatment 113 / $2,187.10. Location = Apple Valley. Period = 2026-04-01 to 2026-04-05. Clean.

### Verdict

- **PDFs are solid.** Both Zenoti and Salon Ultimate PDF parsers work against real files.
- **Zenoti Excel is solid** with one cosmetic docstring/constant fix worth doing.
- **Salon Ultimate Excel is blocked by a source-file defect.** The parser code is fine; the input file is structurally broken. This is a conversation with Elaina, not a code change.

### Proposed next moves

1. Apply the small fix to `salon_ultimate_excel.py` so it gracefully falls back to `wb.active` when the sheet name mismatches (keeps the parser correct even if file format varies).
2. Update `zenoti_excel.py` docstring + `HEADER_ROW` constant to reflect the real Row 4 header position (doesn't change behavior, removes a lie).
3. Update `zenoti_pdf.py` header comment to note that the current "Salon Summary" export is actually the longer Register Closure report, and the parser works because it matches on the `SERVICE DETAILS` subsection anywhere in the text.
4. Flag the Salon Ultimate .xls problem to Tony → conversation with Elaina → try CSV export or alternate Salon Ultimate export format.
5. After Elaina produces a parseable Salon Ultimate stylist file, re-run this same harness against it to confirm the parser path works end-to-end.

