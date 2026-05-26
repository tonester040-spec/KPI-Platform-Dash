# STYLIST / EMPLOYEE EXTRACTION — ROOT CAUSE

**Status:** 🟢 DIAGNOSED — fix in progress on branch `fix-stylist-extraction-2026-05-26`
**Diagnosed:** 2026-05-26 (the same day Tony reported it; previous-session handoff was the trigger)
**Severity:** Day-1 bug, not a recent regression. Has been silently broken since the v2 parsers landed in commit `7abe5c3` (2026-04-21).

---

## TL;DR

PyMuPDF (the text extractor `parse_file` uses) renders the per-stylist tables in **vertical column order** — one field per line. Both parsers' per-row regexes were written for a **columnar layout** (`pdftotext -layout` style) and use `splitlines()` + `.match()` per line. That approach can never see a full row when each field is on its own line.

The regex authors evidently never tested employee extraction against PyMuPDF output. Golden tests assert location-level KPIs (which work because their regexes use `\s+` between fields and `re.search()` across newlines) but never asserted employee presence, so the bug shipped silently and stayed shipped for ~5 weeks.

---

## What the bug looked like in practice

Tony ran two things on 2026-05-26 and got 0 employees from all 12 PDFs:

1. `python -m parsers.tier2_pdf_batch --dry-run --manifest data/inbox/manifest_validation_2026-05-26.json`
   → run log `data/logs/tier2_pdf_batch_20260526_170928.json` shows `stylist_rows_written: 0`
2. `python scripts/dump_validation_kpis.py`
   → `len(parsed.get("employees") or []) == 0` for every fixture, no exception

Initial suspicion was that the May 26 fresh PDFs were a different format (MTD 24-day exports vs. weekly 7-day exports). But running the comparison script (`scripts/compare_frozen_vs_fresh.py`) against BOTH the Apr 21 frozen fixtures and the May 26 fresh PDFs returned 0 employees in every case — ruling out format drift.

---

## Evidence

### Section anchors fire correctly

`scripts/compare_frozen_vs_fresh.py` confirms the section-header regexes hit exactly once per file on both frozen and fresh:

```
Andover        (zenoti )  frozen text=9805  _RE_EMP_SALE_START=1  _RE_EMP_PERF_START=1  _RE_HOURLY_START=1
Andover        (zenoti )  fresh  text=15329 _RE_EMP_SALE_START=1  _RE_EMP_PERF_START=1  _RE_HOURLY_START=1
Apple Valley   (su     )  frozen text=5410  _EMPLOYEE_DATA_ROW=18 _EMPLOYEE_TOTALS_ROW=1
Apple Valley   (su     )  fresh  text=6556  _EMPLOYEE_DATA_ROW=18 _EMPLOYEE_TOTALS_ROW=1
```

The SU row regex finds **18 matches via `.findall()` against the full text**, yet `parse_file()` returns `employees=[]`. That's the smoking gun — the regex works; the walker doesn't.

### The text PyMuPDF actually produces

`scripts/trace_extract_employees.py` dumps the EMP PERF section first 50 lines of `1232b9_Andover.pdf`:

```
  0: 'EMPLOYEE PERFORMANCE DETAILS'
  1: 'IN-'
  2: 'SERVIC'
  3: 'E'
  ...
 41: 'MANAGER'
 42: '0.05'
 43: '0.83'
 44: '18.30'
 45: '18.30'
...
```

Header words are split across lines. Each numeric field is on its own line. Names appear on their own line, with the row's numeric fields each on subsequent lines.

For SU, Apple Valley first ~30 lines of the Employee Summary section:

```
Ari Spainhower
$386.75
$67.00
11h 21m
11h 21m $34.07
$5.90
14
2 15.38%
$4.79
24.47
$32.41
Brittany Gold
$0.00
$0.00
...
```

### Why the SU row regex still hits 18 via findall

`_EMPLOYEE_DATA_ROW` (parsers/pdf_salon_ultimate_v2.py:169) uses `\s+` between every field. Since `\s` matches `\n`, `re.findall` walks across newlines and finds the 18 rows. But `_extract_employees` (line 536) does:

```python
section = self._locate_employee_section()
lines = section.splitlines()
for line in lines:
    m = _EMPLOYEE_DATA_ROW.match(line)
    if m:
        ...
```

`.match("Ari Spainhower")` fails because there's no `$<amount>` on that single line. Every line fails. Returns `[]`.

### Why the Zenoti row regex fails even via findall

`_RE_EMPLOYEE_SALE_INDIV` (parsers/pdf_zenoti_v2.py:284):

```python
r"^ {4,6}"                          # literal 4-6 leading spaces
r"(?P<name>[A-Za-z][A-Za-z\.\'\-_ ]+?)"
r"\s{2,}"                            # 2+ whitespace between name and first number
r"(?P<net_service>-?[\d,]+\.\d+)\s+"
... 12 numeric fields total ...
```

Two reasons this fails on PyMuPDF output:
1. `^ {4,6}` requires literal 4-6 spaces of indentation. PyMuPDF's vertical layout has no such indent — the name is on its own line, no leading spaces.
2. `\s{2,}` between name and first number requires 2+ whitespace characters. Between `"Katelyn Kuchinski"` and `"694.00"` in PyMuPDF output there's exactly one `\n` — fails.

`_RE_EMPLOYEE_PERF_INDIV` (line 340) and `_RE_EMPLOYEE_SALE_GROUP` (line 307) have the same two structural problems.

### Why `_extract_production_hours_total` works on the same text

It uses `re.search()` with `\s+` separators against the full perf section:

```python
re.search(
    r"^\s*Total\s+(-?[\d,]+\.\d+)\s+(-?[\d,]+\.\d+)\s+...",
    section, re.MULTILINE | re.IGNORECASE)
```

The `\s+` matches `\n`, so the regex traverses vertical layout across multiple lines. This is the pattern that the per-row regexes need.

### Test coverage gap

`grep -in 'employees\|stylist\|net_service' tests/test_pdf_parsers_golden.py` returns 0 hits. The golden tests assert:
- `service_net`, `product_net`, `total_sales`, `guest_count`, `production_hours`, `color_pct`, `wax_count`, `treatment_count`, `pph`, etc.

But nothing about `parsed["employees"]`. So when the bug landed, no test caught it; it has stayed broken for ~5 weeks.

---

## The fix

### Salon Ultimate — minor
Replace the line-by-line walker with `_EMPLOYEE_DATA_ROW.finditer(section)`. The regex already supports vertical layout. The multi-line-name case (Magdalene York: "Magdalene\nYork\n$1,543.50...") needs a lookback: for each match, check whether the line immediately preceding the matched name is pure letters with no `$` — if so, prepend it to the name.

### Zenoti — moderate
1. Rewrite `_RE_EMPLOYEE_SALE_INDIV`, `_RE_EMPLOYEE_PERF_INDIV`, `_RE_EMPLOYEE_SALE_GROUP` to use `^\s*` + `\s+` separators (same pattern as `_extract_production_hours_total`'s regex).
2. Replace the splitlines-walker in `_parse_sale_section` / `_parse_perf_section` with `.finditer()` over the section.
3. Adjust role-group detection — role keywords (MANAGER / STYLIST / Shift Leader) appear on their own line with no indent, not at `^ {2,3}`.
4. Multi-word name handling ("Alexandria Costello Martinez", "Rebecca Follansbee") — same lookback approach as SU.

### Tests
Add `tests/test_pdf_employee_extraction.py` with per-fixture assertions:
- Every fixture: `assertGreater(len(employees), 0)`
- Spot checks on known stylists: Apple Valley contains "Ari Spainhower", Andover contains "Katelyn Kuchinski", etc.
- Field-shape assertions: `net_service` is float, `name` is non-empty string.

These tests would have caught the original bug.

---

## What was NOT the cause (ruled out during diagnosis)

- ❌ **MTD vs weekly export format drift** — both formats fail identically.
- ❌ **`465432d` HOURLY WORK deletion** — that commit explicitly preserved `_RE_HOURLY_START` and never touched `_extract_employees`. Verified via `git show 465432d`.
- ❌ **`d59349f` SU product-mismatch detection** — only added a new regex, didn't touch employee extraction. Verified via `git show d59349f`.
- ❌ **Downstream filter dropping rows** — `transform_to_stylist_rows` never sees rows because the parser returns `employees=[]` upstream of it.
- ❌ **Sheets writer no-op** — never reached; the failure is purely in parse time.
- ❌ **Pipeline dormancy / intentional sheet wipe** — Tony's observation came directly from `len(parsed["employees"])`, not from observing an empty sheet.

---

## Files referenced

| Path | What it shows |
|---|---|
| `parsers/pdf_zenoti_v2.py:284-301` | `_RE_EMPLOYEE_SALE_INDIV` — broken regex |
| `parsers/pdf_zenoti_v2.py:340-355` | `_RE_EMPLOYEE_PERF_INDIV` — broken regex |
| `parsers/pdf_zenoti_v2.py:799-860` | `_extract_employees` — entry point |
| `parsers/pdf_zenoti_v2.py:864-939` | `_parse_sale_section` — broken splitlines walker |
| `parsers/pdf_zenoti_v2.py:943-1049` | `_parse_perf_section` — broken splitlines walker |
| `parsers/pdf_salon_ultimate_v2.py:169-197` | `_EMPLOYEE_DATA_ROW` — regex is fine |
| `parsers/pdf_salon_ultimate_v2.py:536-605` | `_extract_employees` — broken splitlines walker |
| `parsers/pdf_zenoti_v2.py:592-629` | `_extract_production_hours_total` — works (reference pattern) |
| `scripts/compare_frozen_vs_fresh.py` | Frozen-vs-fresh comparison (rules out format drift) |
| `scripts/trace_extract_employees.py` | Line-by-line trace showing vertical layout |
| `scripts/check_total_row_layout.py` | Confirms Total-row regex works via cross-line `\s+` |
| `tests/test_pdf_parsers_golden.py` | Golden tests with NO employee assertions (the coverage gap) |

---

## Resolution

This doc captures the diagnosis. The fix lands on branch `fix-stylist-extraction-2026-05-26` with:
1. New `tests/test_pdf_employee_extraction.py` (intentionally fails on main first)
2. SU `_extract_employees` rewrite
3. Zenoti regex + walker rewrite
4. This doc

After the fix lands, `STYLIST_EXTRACTION_INVESTIGATION_2026-05-26.md` should be deleted (handoff complete) and the top-of-file callout in `CLAUDE.md` should be removed.
