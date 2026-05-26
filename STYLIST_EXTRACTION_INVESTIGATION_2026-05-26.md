# STYLIST / EMPLOYEE EXTRACTION INVESTIGATION

**Status:** 🟢 RESOLVED — see [`STYLIST_EXTRACTION_ROOT_CAUSE_2026-05-26.md`](STYLIST_EXTRACTION_ROOT_CAUSE_2026-05-26.md) for diagnosis and `fix-stylist-extraction-2026-05-26` branch for the fix.
**Resolution summary:** Day-1 bug since v2 parser commit `7abe5c3` (2026-04-21). PyMuPDF renders the per-stylist tables in vertical column order (one field per line); both parsers' per-row regexes assumed a columnar layout (`pdftotext -layout` style) and walked `splitlines()` + `.match()` per line — which can never see a full row when each field is on its own line. Golden tests never asserted employee presence so the bug shipped silently. Fix replaces the per-line walker with `re.finditer()` over the section + multi-line name lookback + role-keyword tagging. Tests go from 207 → 215 (8 new in `tests/test_pdf_employee_extraction.py`). Validation confirms both frozen Apr 21 fixtures and fresh May 26 MTD PDFs now produce non-zero employees per location.
**Started by:** Tony Grant, 2026-05-26 (end of audit session)
**Severity:** MASSIVE per Tony's framing — without stylist data, coach cards / Visit Prep / per-stylist dashboards all break. (Resolved before launch — no production impact.)
**This doc lives on main** so any worktree can read the history.

---

## 0. New session: read this first

The previous session (2026-05-26) completed a multi-branch parser audit and shipped 3 spec-compliance branches to main. At the very end Tony reported:

> "The Parser did NOT pick up employees or Stylists. This is a MASSIVE problem that will require a whole new chat."

**That's the problem this doc exists to solve.** The previous session did not investigate further because of context limits.

**Recommended opening message for this new session:**
> "Read STYLIST_EXTRACTION_INVESTIGATION_2026-05-26.md. Confirm what's on main and that 207 tests still pass. Then we'll diagnose where employee extraction is breaking."

---

## 1. What Tony observed (TODO — Tony to fill in or paste here at start of new session)

> _Placeholder. Tony, please describe in the new chat:_
>
> 1. What did you run? (e.g., `parse_file('data/inbox/X.pdf')`, the sandbox script, looked at Sheets after a manual workflow dispatch, etc.)
> 2. What did you see? (empty `employees` list? error? a specific dashboard with missing stylists? STYLISTS_CURRENT / STYLISTS_DATA empty in Sheets?)
> 3. Which locations / PDFs were affected? (all 12 fixtures? just the fresh ones you just received? Zenoti only? SU only?)
> 4. Any exact command you ran or output you captured?

The new Claude session should ASK these questions first if Tony doesn't volunteer the info.

---

## 2. State of main when this investigation began

**HEAD:** `0ac005a Add truth_mediation_log serializer (FINAL_SPEC §10, Branch 3) (#7)`

**Most recent commits (newest first):**
```
0ac005a Add truth_mediation_log serializer (FINAL_SPEC §10, Branch 3) (#7)
2e0d651 Document OAuth refresh token as Branch 4 prerequisite + close spawn-task #1 (#6)
d59349f Detect SU Top Product Lines header vs detail mismatch (FINAL_SPEC §6.2, Branch 1 — rebased) (#5)
7891cd4 Add partial-week alert hook (FINAL_SPEC §6.1, Branch 2) (#3)
472fc30 Merge pull request #2 from tonester040-spec/product-mismatch-detection-2026-05-26
0e4cb6c Document Bug #2 magnitude in audit doc — internal record
5bd213a Add parser audit deliverables and Vocabulary Map
465432d Fix Zenoti production_hours per FINAL_SPEC §6.6 and delete buggy HOURLY WORK extraction
7be79bf Fix Color % to use service_net denominator per FINAL_SPEC §3.2
```

**Tests:** 207 passed (180 baseline + 7 Branch 2 + 6 Branch 1 + 14 Branch 3). None of these specifically assert employee/stylist presence — that's part of the gap (see §6).

**Pipeline:** Intentionally dormant. CURRENT tab was wiped pre-launch. Failing every Monday with `ERROR: No location data found in CURRENT tab — aborting pipeline.` This is EXPECTED for current pre-launch state.

**Branches:** Just `main`. No stale branches on origin (cleaned up at end of audit session).

---

## 3. What the parser SHOULD do (per existing code + CLAUDE.md)

### Zenoti — `parsers/pdf_zenoti_v2.py`

`_extract_employees()` (around lines 800–862) merges TWO tables joined by stylist name:

1. **EMPLOYEE SALE DETAILS** — money, qty, invoice count, tips, discount. Regex: `_RE_EMPLOYEE_SALE_INDIV` (~line 276).
2. **EMPLOYEE PERFORMANCE DETAILS** — hours, per-hour rates, requested services. Regex: `_RE_EMPLOYEE_PERF_INDIV` (~line 332).

Output: list of merged stylist dicts under `parsed["employees"]`. Each dict has fields like:
```
name, role_group, net_service, comm_service, comm_disc, tips,
service_qty, invoice_count, net_product, comm_product, net_prod_per_pi,
avg_invoice_value, disc_dollars, disc_pct,
in_service_productivity, in_service_hours, actual_hours, production_hours,
non_production_hours, blocked_hours, net_service_per_hr, gross_service_per_hr,
service_comm_prod_per_hr, req_services_count, req_services_pct
```

Multi-word names (e.g. "Alexandria Costello Martinez" in Roseville) wrap across lines — handled by `pending_name_prefix` logic in both `_parse_sale_section` and `_parse_perf_section`.

### Salon Ultimate — `parsers/pdf_salon_ultimate_v2.py`

Employee Summary table — single section parsed via `_EMPLOYEE_DATA_ROW` regex (~line 145). Row shape (13 numeric fields after name):
```
<Name> $<net_service> $<net_retail> <total_hours> <prod_hours>
$<pph> $<retail_per_hour> <guests> <requests> <req_pct>%
$<ppg> <avg_service_time> $<avg_ticket>
```

TOTALS row (`_EMPLOYEE_TOTALS_ROW` ~line 177) marks end of table.

### Downstream — `parsers/tier2_pdf_batch.py`

- `transform_to_stylist_rows(parsed, platform, display_name, loc_id)` — around line 332 — converts parser `employees[]` to STYLISTS_CURRENT row dicts (12-column schema: week_ending, name, loc_name, loc_id, status, tenure_yrs, cur_pph, cur_rebook, cur_product, cur_ticket, services, color)
- `_write_stylists_tabs(customer_config, fresh_stylists, week_ending, dry_run)` — writes the rows to STYLISTS_CURRENT and appends to STYLISTS_DATA via `core/sheets_writer.py`

### Sheets writer — `core/sheets_writer.py`

- `write_stylists_current(...)` — overwrites STYLISTS_CURRENT
- `append_to_stylists_historical(...)` — appends to STYLISTS_DATA (idempotent — checks week_ending exists, skips if so)

---

## 4. Hypotheses for the new session to test

In rough order of likelihood, given what we know:

### H1 — Parser regex isn't matching the PDF format

The 12 frozen fixtures in `data/inbox/` are from week ending 2026-04-05. The PDFs that prompted Tony's discovery may be NEWER (the fresh PDFs Tony received this week, week ending 2026-05-XX). POS systems quietly change layouts. If Zenoti or SU shipped a format change between April and May, our regexes might fail silently.

**To test:** Run the parser on a fresh PDF (NOT one of the 12 frozen fixtures) and compare to a frozen-fixture run. If frozen fixtures still return populated `employees[]` but fresh ones don't → format drift.

### H2 — Section anchors not firing

`_RE_EMP_SALE_START` / `_RE_EMP_PERF_START` / `_RE_HOURLY_START` are used to bound the employee sections. If the new PDFs have slightly different section headers (e.g. "EMPLOYEE SALES" instead of "EMPLOYEE SALE"), the anchors fail and the loop never enters. `_extract_employees` would return `[]`.

**To test:** Print `_RE_EMP_SALE_START.search(text)` and `_RE_EMP_PERF_START.search(text)` against a problematic fresh PDF. If either returns None → section header changed.

### H3 — Wiring drops the data downstream

`_process_one_pdf` returns 5-tuple `(row, stylist_rows, flags, display_name, parsed)`. If `transform_to_stylist_rows` is returning `[]` because of a stylist-filter rule (e.g. the `is_phantom_house` skip, or the "all-zero rows" filter at line ~435), real stylists could be wrongly excluded.

**To test:** Add a print statement in `transform_to_stylist_rows` showing the raw `employees[]` count before filtering, and the `rows[]` count after filtering. Difference = filter dropped something.

### H4 — Sheets writer silently no-ops

`_write_stylists_tabs` calls `write_stylists_current` and `append_to_stylists_historical`. Either could fail silently if the sheet API call has a permissions issue or the tab structure changed.

**To test:** Check `data/logs/tier2_pdf_batch_*.json` for `stylist_rows_written` field — if it's `0` but `parsed_ok > 0`, the write is failing. If `stylist_rows_written > 0` but Sheets is empty, then the write is going to the wrong sheet ID or tab.

### H5 — Sheet was wiped (intentional pre-launch) and observation is just confirming that

Per `PARSER_SPEC_v1.0.1_ADDENDUM.md` §G item 1, Tony intentionally wiped CURRENT. He may have ALSO wiped STYLISTS_CURRENT / STYLISTS_DATA. In that case "Parser didn't pick up stylists" might be conflating "sheet is empty" with "extraction failed." 

**To test:** Run the parser locally (not through the workflow), inspect the returned dict. If `parsed["employees"]` has entries, extraction works — the empty sheet is just the intentional wipe.

---

## 5. Where to start (suggested first-hour plan for the new session)

1. **Confirm state.** `git status` (should be clean), `git log --oneline -5` (should show `0ac005a` as HEAD), `python -m pytest tests/` (should be 207 passed).
2. **Ask Tony for the specific observation** if he hasn't volunteered it at the top of the chat. The investigation depends on knowing exactly what he saw.
3. **Run the parser locally against a frozen fixture** to establish a baseline:
   ```python
   from parsers.pdf_zenoti_v2 import parse_file
   r = parse_file('data/inbox/1232b9_Andover.pdf')
   print('Andover employees count:', len(r.get('employees') or []))
   if r.get('employees'):
       print('first employee:', r['employees'][0])
   ```
   Repeat for one SU fixture. If both return non-empty `employees[]`, extraction works on the frozen fixtures.
4. **If frozen fixtures work** → format-drift hypothesis (H1). Need to inspect a fresh PDF vs a frozen one to see what changed.
5. **If frozen fixtures DON'T work** → something on main broke extraction. Bisect: `git log -- parsers/pdf_zenoti_v2.py parsers/pdf_salon_ultimate_v2.py` and look for any commits that might have broken extraction. The Branch 3 commit added the truth_mediation_log dispatch but didn't touch extraction code — verify by reading the diff: `git show 51b0761 -- parsers/`.

---

## 6. Existing test coverage gap

`tests/test_pdf_parsers_golden.py` does NOT assert employee/stylist presence in any of the 12 fixtures. The golden tests only check location-level KPIs (guest_count, service_net, product_net, color_pct, etc.). **If extraction broke, the golden tests would not have caught it.**

This is a real gap. Recommended fix once root cause is found:

```python
# Add to TestZenotiGolden:
def test_andover_has_at_least_one_employee(self):
    parsed = zenoti_parse_file(_fixture_path("1232b9_Andover.pdf"))
    self.assertGreater(len(parsed.get("employees") or []), 0,
                       "Andover Zenoti fixture should have at least one employee")
```

And similar for the other 11 fixtures. Ideally pin the exact count per fixture so a parser change can't silently drop stylists again.

---

## 7. Files most likely involved

| File | Lines / Methods |
|---|---|
| `parsers/pdf_zenoti_v2.py` | Regex constants (~256-345) + `_extract_employees` (~800-862) + `_parse_sale_section` (~880) + `_parse_perf_section` (~940) |
| `parsers/pdf_salon_ultimate_v2.py` | `_EMPLOYEE_DATA_ROW` (~145), `_EMPLOYEE_TOTALS_ROW` (~177), and the parse loop that uses them |
| `parsers/tier2_pdf_batch.py` | `transform_to_stylist_rows` (~332), `_write_stylists_tabs` (call site in `process_manifest`) |
| `core/sheets_writer.py` | `write_stylists_current`, `append_to_stylists_historical` |
| `tests/test_pdf_parsers_golden.py` | Currently silent on employee extraction — see §6 |

---

## 8. Do NOT

- Delete or move the 12 frozen fixtures in `data/inbox/` — they're the golden reference
- Modify parser code before establishing the baseline (run frozen fixtures first)
- Push directly to main — open a debugging branch (e.g. `fix-stylist-extraction-2026-05-XX`) and PR
- Skip writing tests for any fix — the test coverage gap (§6) is part of why this wasn't caught
- Touch the OAuth refresh token / Karissa email config / wipe state — those are separate concerns
- Conflate "STYLISTS sheet is empty" with "extraction failed" — they have different root causes

---

## 9. What the previous session shipped (so you know what's already done)

**Spec compliance — DONE on main:**
- FINAL_SPEC §3.2 Color % formula (corrected, was using `total_sales` denominator)
- FINAL_SPEC §6.6 Zenoti `production_hours` (Bug #2 — Roseville was reporting 15.17h vs correct 166.05h)
- FINAL_SPEC §6.2 Top Product Lines mismatch detection (Branch 1)
- FINAL_SPEC §6.1 Unclosed-day detection + alert hook, Phase 1 (Branch 2)
- FINAL_SPEC §10 Truth Mediation Log serializer (Branch 3)
- v1.0.1 addendum sections A–J

**Spec compliance — deferred per addendum §E:**
- §6.1 Phase 1.1 (automated rerun + Mon-EOD blank-out)
- §7 Tolerance threshold tier adoption
- §8 Chunked architecture refactor
- §11 Single config.json

**Operational gaps (separate from this investigation):**
- Spawn-task #1: pipeline silence diagnosed + closed
- Spawn-task #2: fill placeholder emails — chip dispatched, gated on fresh-PDF validation
- Spawn-task #3: ledger persistence decision — chip dispatched, decision doc only
- Spawn-task #4: legacy tier2 file rename — chip dispatched, parity check + rename only

---

## 10. Once root cause found

1. Open debugging branch: `git checkout -b fix-stylist-extraction-2026-05-XX`
2. Apply the fix
3. **Add tests** that pin the expected employee count + spot-check field values per fixture
4. Run `pytest tests/` — must be ≥207 passing + however many new tests
5. Commit (2 commits if logical: fix + tests/docs)
6. Push, open PR, wait for Tony's "merge it"
7. Once merged: update this doc's status to RESOLVED + close it out OR delete it
8. Update `STYLIST_EXTRACTION_INVESTIGATION_2026-05-26.md` to capture root cause + fix summary, OR fold the finding into a new addendum section

---

## 11. Reference docs

- `CLAUDE.md` — full project context (has a top-of-file callout pointing back to this doc)
- `PARSER_AUDIT_2026-05-26.md` — the audit that just landed
- `PARSER_SPEC_v1.0.1_ADDENDUM.md` — spec status (sections A–J)
- `KPI_LIVE_INVENTORY_2026-05-06.md` — ground-truth project state from earlier
- `KPI_NOTION_TRACKER_IMPORT.md` — Tony's working tracker (untracked in repo)

---

**End of handoff doc. New session: please read §0 first, then §1 (which Tony will fill in or paste in chat).**
