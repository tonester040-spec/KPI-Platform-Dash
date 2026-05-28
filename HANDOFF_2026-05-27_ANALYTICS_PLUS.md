# HANDOFF — Analytics Plus Migration (2026-05-27)

**For the next Claude session.** Read in this order:
1. `CLAUDE.md` — project basics
2. **`KARISSA_GOLDEN_RULES.md` — the formula bible. Do not write a single line of parser code without reading this. It captures every Karissa-confirmed rule from 2026-05-27 including the PPG reconciliation exception that surprises every model.**
3. This doc — Analytics Plus migration plan

---

## Context: what just shipped this session

Today (2026-05-27) was a marathon. Commits `dc6083c` through `d5cb47b`. State of the dashboard right now:

| Area | Status |
|------|--------|
| Per-stylist `product_pct` | ✅ Live (Karissa formula `net_product / (net_service + net_product)`) |
| Per-stylist `ppg` | ✅ Live |
| Per-stylist `req_pct` (Request %, the loyalty metric replacing rebook per stylist) | ✅ Live, 114/143 non-zero |
| Per-stylist `avg_service_time_min` | ✅ SU stylists; Zenoti deferred (in-svc vs prod hours ambiguity) |
| Salon-level Rebook % on Locations tab | ✅ Live, 12/12 locations populated for Apr+May 2026 |
| Coaching tab → real STYLIST_DATA (no more "Katelyn Moore" seed) | ✅ Live (118 real stylists) |
| Coaching composite multi-KPI classifications | ✅ Live (Top/NeedsAttn/Stable bands from p25/p75 thresholds across 6 KPIs) |
| Per-KPI sort in Coaching tab | ✅ Live (PPH/PPG/Product/Request/Ticket/Services) |
| Stylist drawer overflow fixed + mobile layouts verified | ✅ Live |
| DATA tab dedup (60 → 12 rows) + idempotency bug fixed | ✅ Done — future re-runs are safe |
| Monthly Trend reframed as honest YoY (was fake "WK 1 5/24 × 6") | ✅ Live with YoY % column |
| Sandbox 10/10 PASS, full test suite 420/420 PASS | ✅ |

Per-stylist `rebook` and per-stylist `color` stay at 0 — no per-stylist source exists on either platform. Rebook column was removed from per-stylist UI; salon-level Rebook lives on Locations tab. Color stays as 0 with a TODO.

---

## The Big Architectural Shift Coming Next

**Tony got Analytics Plus access on Zenoti + Salon Ultimate.** That unlocks custom Excel report exports from both platforms.

### Decisions already made (don't re-litigate)

| Decision | Choice |
|----------|--------|
| Cadence | **Daily / on-demand for any date range** → full historical weekly backfill possible |
| Delivery | **Same Gmail inbox watcher** (`karissaperformanceintelligence@gmail.com`) — already accepts `.xlsx` per `config/inbox_config.json` `allowed_extensions` |
| Excel vs PDF | **Excel replaces ~98% of PDF parsing.** PDFs stay for **`production_hours` only** (single field; nothing else extracted from PDFs in the new architecture) |
| Tier 2 batch processor | Add an Excel branch alongside the PDF branch in `parsers/tier2_pdf_batch.py`; route by file extension |
| Manifest contract | Existing `data/inbox/manifest.json` shape works as-is; `trust_layer_flags[]` per-record stays |

### What Tony will drop in the new chat

1. **Field mapping doc** — every Karissa KPI mapped to its exact Analytics Plus path (which report, which column, which row position). Tony spent the morning with another Claude session deriving these — they're authoritative. Don't redo them.
2. **Sample Excel export** — one location, one period. Look at it FIRST before writing any parser code. Two things you need from inspection:
   - Header row position (row 1? preamble rows above?)
   - Whether salon-level + per-stylist data live in the same file or separate reports
3. **Production-hours PDF trim spec** — what minimal subset of the existing PDF parser still runs. Probably just `_extract_production_hours_total` from each platform's parser.

### Integration plan (refine once you see the artifacts)

**Phase 1 — Parser**
- New `parsers/excel_analytics_plus.py`
- Output shape mirrors existing PDF parsers exactly so downstream code doesn't notice:
  ```python
  {
    "location": str,
    "period_start": "YYYY-MM-DD",
    "period_end": "YYYY-MM-DD",
    "platform": "zenoti" | "salon_ultimate",
    "karissa": { ...salon-level KPIs per Karissa's golden rules... },
    "employees": [ ...per-stylist dicts... ],
    "flags": [ ...trust-layer flags... ],
    "source": "analytics_plus_xlsx",
  }
  ```
- Apply Karissa's golden formulas at parse time. Reject (or flag) values that violate the rules — same contract as the PDF parsers.

**Phase 2 — Trim the PDF parsers**
- Keep only `_extract_production_hours_total` from `pdf_zenoti_v2.py` and `pdf_salon_ultimate_v2.py`. Everything else becomes dead code candidates for deletion in a follow-up.
- Or: leave full PDF parsers intact as a fallback, and have a `prod_hours_from_pdf()` helper that's called separately. Recommended — less destructive.

**Phase 3 — Hybrid loader**
- For each `(location, period)`: read Excel for everything, read PDF for production_hours, merge before writing.
- Cross-validate: Excel-derived PPH should match `service_net / production_hours_from_pdf` within tolerance. Flag mismatches.

**Phase 4 — Tier 2 routing**
- `parsers/tier2_pdf_batch.py` currently consumes the manifest and processes PDFs. Rename to `tier2_batch.py` and add an Excel branch.
- Routing logic: if `filename.endswith('.xlsx')` → excel parser; if `.pdf` → prod-hours-only PDF parser. Pair them by `(location, period_start, period_end)` for hybrid merge.

**Phase 5 — Weekly backfill**
- `scripts/backfill/weekly_excel_loader.py` — walks a folder of weekly Excel exports, parses each, writes to DATA tab at weekly grain.
- Idempotent on `(loc_name, week_ending)`. The idempotency fix in commit `d5cb47b` (sheets_writer.py uses UNFORMATTED_VALUE + _to_date_str) is in place, so re-runs are safe.
- Karissa generates ~104 weekly exports per location × 12 locations = 1,250 files. Loader runs in batch.
- End state: real 12-week sparklines on the dashboard, real Retention Risk / Most Improved Coaching classifications, real weekly Monthly Trend (instead of monthly aggregates with the honest "weekly fills in" footnote).

**Phase 6 — Monday pipeline update**
- Currently the Monday pipeline reads CURRENT tab (Karissa's team types into it). With Tier 2 auto-populating CURRENT from the previous Sunday's Excel export, the manual entry step goes away.
- Schema unchanged. Just changes the source of truth from "Karissa's team" to "Tier 2 batch processor."

---

## Karissa's Golden Rules — apply at any grain

These MUST hold for both Excel and PDF data. Don't trust pre-computed stats in the Analytics Plus report if they conflict — compute from first principles.

| KPI | Formula |
|-----|---------|
| `total_sales` | `service_net + product_net` (pre-tax, never tax-inclusive) |
| `product_pct` | `product_net / total_sales` (denominator is TOTAL sales, not service) |
| `color_pct` | `color_sales / service_net` (denominator is SERVICE revenue, not total — different from product_pct) |
| `ppg` | `product_net / guest_count` |
| `pph` | `service_net / production_hours` (the one field still coming from PDF) |
| `wax_pct` | `wax_count / guest_count` (penetration rate, not revenue share) |
| `treatment_pct` | `treatment_count / guest_count` (penetration rate) |
| `avg_ticket` | `total_sales / guest_count` |
| `guest_count` (Zenoti) | invoice count (NOT unique guests) |
| `guest_count` (SU) | `serviced_guests + retail_only_guests` |
| `projection_eom` | `(total_sales / 7) * 24` (weekly only) |
| `req_pct` per stylist (Zenoti) | `req_services_count / service_qty` (computed from counts; the PDF's bracketed % is share-of-role-group, not what we want) |
| `req_pct` per stylist (SU) | Direct from "Req %" column |

Full contract in CLAUDE.md under "KPI formulas — Karissa's canonical definitions (MISSION CRITICAL)".

---

## Files the next session will most likely touch

| File | What changes |
|------|-------------|
| `parsers/excel_analytics_plus.py` | **New file** — primary parser |
| `parsers/tier2_pdf_batch.py` | Rename to `tier2_batch.py`, add Excel branch |
| `parsers/pdf_zenoti_v2.py`, `parsers/pdf_salon_ultimate_v2.py` | Trim or wrap — keep only production_hours extraction in active use |
| `scripts/backfill/weekly_excel_loader.py` | **New file** — historical weekly backfill |
| `core/sheets_writer.py` | Schema columns may grow (req_pct, svc_time, rebook_pct are already there — added today). Add `source` provenance if not present. |
| `core/data_source.py` | If schema grows, extend `COL` and reader ranges |
| `tests/test_excel_analytics_plus.py` | **New** — parser tests, validate against Karissa's golden rules |
| CLAUDE.md | Update architecture section once Excel pipeline is live; describe the hybrid Excel+PDF model |

---

## What NOT to do without checking first

1. Don't delete `parsers/pdf_zenoti_v2.py` or `parsers/pdf_salon_ultimate_v2.py` — they still serve production_hours and may serve as fallback. Plan a deprecation path, don't bulldoze.
2. Don't change `LOCATION_POS_MAP` keys (`config/locations.py`) — naming asymmetry between bare names (Andover) and canonical names (Andover FS) is intentional. The `FILENAME_TO_LOCATION` map in `scripts/backfill/patch_new_fields.py` handles the bridge.
3. Don't change Karissa's golden formula contracts. If the Analytics Plus report shows a "Product %" that uses service-net denominator, override it and compute from primitives per Karissa's rule.
4. Don't write directly to DATA tab without the idempotency check — the fixed `append_to_historical` in sheets_writer.py is the canonical write path.
5. Don't touch the inbox watcher's archive-before-inbox invariant (described in CLAUDE.md under "Gmail Attachment Watcher").

---

## Suggested first messages in the new chat

```
Read HANDOFF_2026-05-27_ANALYTICS_PLUS.md first. Then I'll drop:
1. The Analytics Plus field mapping spec
2. A sample Excel export from one location/period
3. The production-hours PDF trim notes

Don't write any code until you've seen all three.
```

---

## Open questions to resolve in the new chat

1. **Salon-level + per-stylist in one Excel file, or separate reports?** (Determines whether parser produces one dict per file or N dicts.)
2. **Date range labeling** — does the Excel export include `period_start` / `period_end` cells, or do we derive from the filename / a header row?
3. **Service categories breakdown** — does the export have separate Color / Wax / Treatment rows like the PDF Service Categories table, or are those KPIs missing and we need to keep parsing PDFs for them too?
4. **Production hours pairing** — what's the cleanest way to pair an Excel weekly export with its corresponding production-hours PDF? Same `(location, period)` key? Filename convention?
5. **Backfill order of operations** — Karissa exports all historical weekly files first, then we batch-process? Or stream-process as she exports?

---

Good luck. The hard part (parser shape, golden rules, idempotency, schema, UI plumbing) is already solved. This is mostly: write a new parser, route it through existing pipes.

— previous session, 2026-05-27
