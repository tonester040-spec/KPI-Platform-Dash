# Tier 2 Parity Diff — `tier2_batch_processor.py` (legacy) ↔ `tier2_pdf_batch.py` (canonical)

**Date:** 2026-05-26
**Author:** Cowork audit per Tony's directive
**Purpose:** Verify whether the legacy Tier 2 processor can safely be renamed `.DEPRECATED.py`. Per Tony: do NOT delete — keep for diff comparison.

---

## TL;DR — Decision Gate Result

**Parity is INCOMPLETE by design.** Three legacy features are not in the canonical:

1. **Excel ingestion path** (Zenoti + Salon Ultimate `.xlsx` / `.xls`) — *intentionally deferred*; canonical's own docstring (lines 31–36) names the legacy file as the reference for Excel reintroduction.
2. **`DataMerger` Excel+PDF stylist distribution** — legacy's core value-add: combining Excel's per-stylist lines with PDF's salon-level totals via Karissa-approved proportional adjustment. Canonical's `transform_to_stylist_rows()` reads stylist data directly from PDF employee tables instead — narrower scope.
3. **Heavyweight trust-layer integration** (`ConfidenceScorer`, `IntegrityReporter`, `BatchProcessingError`, batch-abort on LOW tier) — canonical uses the lighter `trust_layer_flags`-in-manifest + truth-mediation-log pattern.

**Production import status:** **ZERO production or test imports** of `parsers.tier2_batch_processor` anywhere in the codebase. Only references are docstring comments in `pdf_zenoti_v2.py`, `pdf_salon_ultimate_v2.py`, and `tier2_pdf_batch.py` itself.

**The prior audit's claim that `scripts/backfill_master_table.py` imports the legacy file is stale** — that script now only imports `from trust_layer import StylistIdentityResolver`. No `tier2_batch_processor` reference.

**Per Tony's stated rule** (matrix in 2026-05-26 task brief): *"If parity is INCOMPLETE: do NOT rename. Report the missing functionality to Tony."* → **No rename performed.** Tony decides whether to (a) port Excel + DataMerger into canonical first, or (b) accept that legacy stays unrenamed as the documented Excel reference, or (c) override and rename anyway since canonical's own docstring already names the legacy file as the deferred-feature reference.

---

## Per-Symbol Parity Table

For every public function/class/constant in `parsers/tier2_batch_processor.py`, this table identifies the canonical equivalent or marks it MISSING.

### Module-level constants

| Legacy symbol | Canonical equivalent | Status |
|---|---|---|
| `REQUIRED_STYLIST_KEYS` (set of required dict keys for stylist validation) | None — canonical uses `transform_to_stylist_rows()` schema directly, no separate validation set | **MISSING by design** — replaced by transform-time filtering. No need to port unless re-adding `validate_stylist_data()`. |
| `_TRUST_LAYER_AVAILABLE` (internal flag) | None (canonical doesn't import trust_layer at module level) | **MISSING** — see "Heavyweight trust layer" below. |

### Module-level functions

| Legacy symbol | Canonical equivalent | Status |
|---|---|---|
| `detect_system(file_path) -> Optional[str]` — classifies `.xls`/`.xlsx`/`.pdf` by filename heuristic into `zenoti_excel`/`zenoti_pdf`/`su_excel`/`su_pdf` | `parsers/pdf_detect.py::detect_pos_from_file()` — classifies PDFs by **content signature** (inspects PDF text), not filename. Returns `ZENOTI` / `SALON_ULTIMATE` constants. | **PARTIAL** — content-detection is strictly better than filename heuristics. **Excel branches (`.xls`, `.xlsx`) have no canonical equivalent** because canonical is PDF-only. |
| `group_files_by_location(files: List[str]) -> Dict[str, List[str]]` — buckets a flat file list into the four categories | None | **MISSING by design** — canonical reads pre-classified manifest from `gmail_attachment_watcher.py` (`data/inbox/manifest.json`). Different I/O paradigm — manifest-driven, not directory-scan-driven. |
| `validate_stylist_data(stylists) -> Tuple[List[Dict], List[str]]` — validates required keys, non-negative numerics, `total_sales ≈ service_net + product_net` | None | **MISSING** — canonical's `transform_to_stylist_rows()` filters at transform time (drops phantom House, empty names, all-zero rows) but doesn't validate field invariants. Not currently a problem because the v2 PDF parsers produce well-formed dicts. |
| `_import_parsers()` (private) — lazy import of Excel + PDF parsers + DataMerger | Direct `from parsers.pdf_zenoti_v2 import parse_file as parse_zenoti_pdf` (top of canonical module) | **PARTIAL** — eager imports of PDF parsers only. Excel parsers + DataMerger not imported (they remain in the repo, just not used by canonical Tier 2). |
| `_build_arg_parser()` (private) | Argparse setup inline at top of `main()` | **PARTIAL** — different CLI surface. See "CLI surface" section below. |
| `main(argv=None)` | `main(argv=None)` | **PARTIAL** — different CLI args; different exit-code contract. |

### Class `Tier2BatchProcessor`

| Legacy member | Canonical equivalent | Status |
|---|---|---|
| `Tier2BatchProcessor(input_dir, dry_run, sheets_writer)` (class instantiation) | None — canonical is function-based, no class | **MISSING by design** — `process_manifest()` is the replacement orchestrator. Function-based is simpler for the manifest-in/dict-out contract. |
| `.process(write_mode="append") -> Dict` — full orchestrator: discover → classify → parse Excel → parse PDF → merge → trust-validate → flatten → validate → write | `process_manifest(manifest_path, customer_config, dry_run=False) -> Dict` — load manifest → filter ready PDFs → resolve location → parse each PDF → transform → merge with existing CURRENT → write CURRENT + STYLISTS_CURRENT + STYLISTS_DATA → update manifest → fire partial-week alert | **PARTIAL** — same intent (orchestrate parse → write), different I/O: directory-driven vs manifest-driven. Different return dict schema. Canonical writes CURRENT (legacy didn't); legacy wrote stylists only. |
| `.process(write_mode="overwrite")` branch | None — canonical always overwrites CURRENT and STYLISTS_CURRENT, always appends to STYLISTS_DATA (idempotent on `week_ending`) | **MISSING** — canonical doesn't expose a `--mode` knob; write semantics are fixed. Idempotent appends remove most of the reason for the legacy mode flag. |
| `._discover_files()` | None | **MISSING by design** — manifest IS the file list. |
| `._safe_parse(parser_key, file_path, summary)` | `_process_one_pdf(record, location_lookup)` | **PARTIAL** — same intent (per-file try/except), different signature. Legacy returns a parsed dict; canonical returns a 5-tuple `(row, stylist_rows, flags, display_name, parsed)`. |
| `._get_writer()` — instantiates / returns `GoogleSheetsWriter` | Uses `core.sheets_writer._build_service()` + `write_current` / `write_stylists_current` / `append_to_stylists_historical` directly | **PARTIAL** — different writer abstraction. Canonical uses `core/sheets_writer.py` (which `main.py` also uses); legacy used `utils/sheets_writer.py::GoogleSheetsWriter`. These are two separate modules — both still in the repo. |
| `._run_trust_layer(excel_results, merged_locations, pdf_by_location)` — calls `run_trust_validation()`, scores per location with `ConfidenceScorer`, aggregates with `aggregate_batch_score()`, generates report with `IntegrityReporter`, **hard-aborts on LOW-tier batch via `BatchProcessingError`** | `_write_truth_mediation_events(parsed, flags, display_name)` — writes selected reconciliation events (`PRODUCT_TOTAL_MISMATCH`, `PARTIAL_WEEK`) to NDJSON log via `trust_layer/truth_mediation_log.py::write_event` | **MISSING (architectural shift)** — canonical does NOT do batch confidence scoring or hard-abort. Each parser populates its own `flags[]`; Tier 2 writes them into manifest's `trust_layer_flags` and selected events into `truth_mediation_log.json`. Downstream consumers (`main.py`, `drift_checker`, coach cards) read CURRENT and decide independently. **This is a deliberate lighter architecture** — see canonical's `_write_truth_mediation_events` (lines 896–978). |
| `.format_summary(summary) -> str` (staticmethod) — pretty multi-line human-readable summary with trust tier icons | None — canonical writes `_write_run_log(payload)` JSON to `data/logs/tier2_pdf_batch_*.json` and uses structured `logger.info()` calls | **MISSING** — JSON run log + structured logging replaces the human pretty-print. If Tony wants the legacy pretty summary in CI, easy to re-port. |

---

## Architecture comparison — beyond per-symbol parity

### Excel + PDF merge (legacy's primary value-add)

| Aspect | Legacy | Canonical |
|---|---|---|
| Excel parsers | `ZenotiExcelParser`, `SalonUltimateExcelParser` | Not imported (still in repo as `parsers/zenoti_excel.py`, `parsers/salon_ultimate_excel.py`; tested by `test_parsers.py`) |
| Excel+PDF merge | `utils/data_merger.py::DataMerger.merge_multiple_locations()` — combines Excel stylist lines with PDF salon-level totals via Karissa-approved proportional distribution | None — PDF v2 parsers emit stylist rows directly from PDF employee tables (less rich than Excel-based stylist data, but no Excel dep) |
| Stylist row source | Excel rows distributed proportionally against PDF salon-level | PDF employee tables only (`parsed["employees"]`) |
| Locked-in scope | Excel + PDF | PDF only (per docstring line 31–36) |

**Implication:** The canonical Tier 2 ships less rich stylist data than the legacy could (no salon-level supremacy adjustment because no Excel input). If/when Karissa starts shipping Excel exports alongside PDFs, this gap matters. Until then, PDF-only is fine.

### Trust layer integration

| Aspect | Legacy | Canonical |
|---|---|---|
| Per-location confidence score | Yes — `ConfidenceScorer.calculate_score(checks)` returns 0–100% + tier (high/moderate/low) | No |
| Batch aggregate score | Yes — `aggregate_batch_score(all_scores)` | No |
| Hard-abort on LOW tier | Yes — `raise BatchProcessingError` | No — flags pass through, downstream decides |
| Hard-abort on `ValueError` from `CrossFileVerifier` / `HistoricalBaselineValidator` | Yes | No — orchestrator never raises |
| `IntegrityReporter` markdown report | Yes — embedded in summary | No |
| Per-record `trust_layer_flags[]` in manifest | Not used | Yes — primary mechanism (FINAL_SPEC §10) |
| Truth Mediation Log NDJSON | Not present | Yes — `trust_layer/truth_mediation_log.py` (Branch 3) |
| Partial-week alert email | Not present | Yes — `send_partial_week_alert` (Branch 2) |

**Implication:** The two are different trust philosophies. Legacy: "validate before write, refuse if uncertain." Canonical: "annotate during write, let consumers decide." Per FINAL_SPEC §10 the canonical's flag-based approach is the locked-in pattern.

### CLI surface

| Aspect | Legacy CLI | Canonical CLI |
|---|---|---|
| Required arg | `--input-dir PATH` | None (defaults to `data/inbox/manifest.json`) |
| Optional args | `--mode {append,overwrite}`, `--dry-run`, `--verbose` | `--manifest PATH`, `--customer ID`, `--dry-run` |
| Env-var overrides | None | `DRY_RUN`, `ACTIVE_CUSTOMER_ID` |
| Exit code on parse errors | 1 | 0 (parse errors are non-fatal; result["status"] reflects severity) |
| Exit code on `BatchProcessingError` | 1 | n/a (no such raise) |
| Exit code on no files | 0 | 0 (`status="no_files"`) |

### Notification paths

| Notification | Legacy | Canonical |
|---|---|---|
| Success email | None (would have been from `format_summary()` printed to stdout) | None directly from Tier 2 — main.py sends the Excel email after the whole pipeline |
| Error email | None | `send_partial_week_alert` (Branch 2) — only fires on PARTIAL_WEEK flag |
| Inbox ingestion failure email | n/a (legacy doesn't touch inbox) | `email_sender.send_inbox_notification` — fired by `gmail_attachment_watcher.py`, not Tier 2 |

### Manifest schema expectations

| Manifest field | Legacy uses? | Canonical uses? |
|---|---|---|
| `filename`, `safe_filename`, `inbox_path` | No (directory-driven) | Yes — `inbox_path` + `safe_filename` fallback |
| `processing_status` | No | Yes — filters to `"ready"` only |
| `hash`, `archived_path`, `sender`, `date_received` | No | No (used by watcher, not Tier 2) |
| `trust_layer_flags` (write back) | No | Yes — written via `_update_manifest()` |

---

## All imports of `tier2_batch_processor` in the codebase

Grep run: `Grep "tier2_batch_processor" --type=py`

**Python imports — actual code dependencies:**

| File | Line | Type |
|---|---|---|
| (none) | — | No file imports `parsers.tier2_batch_processor` |

**Python files mentioning the name in comments / docstrings only (no import):**

| File | Line | Context |
|---|---|---|
| [parsers/tier2_pdf_batch.py:34](parsers/tier2_pdf_batch.py:34) | docstring | *"see the legacy `tier2_batch_processor.py` for the Excel+PDF merge path"* |
| [parsers/pdf_salon_ultimate_v2.py:9](parsers/pdf_salon_ultimate_v2.py:9) | docstring | *"flow in `tier2_batch_processor.py`; new PDF-only ingestion uses THIS module"* |
| [parsers/pdf_zenoti_v2.py:11](parsers/pdf_zenoti_v2.py:11) | docstring | *"merge flow in `tier2_batch_processor.py`; new PDF-only ingestion uses"* |
| [parsers/tier2_batch_processor.py:9,32,37,618](parsers/tier2_batch_processor.py) | self-references | docstring usage examples |

**Documentation mentions (no executable code impact):**

- `CLAUDE.md:633` — *stale* — says "Tier 2 (`parsers/tier2_batch_processor.py`) reads `data/inbox/manifest.json`". This is incorrect — the canonical (`tier2_pdf_batch.py`) is what reads the manifest. **Should be fixed regardless of rename decision.**
- `KPI_AUDIT_REPORT_2026-04-20.md` — multiple references; describes legacy as the Phase 2 processor.
- `KPI_LIVE_INVENTORY_2026-05-06.md:128` — says "UNCLEAR. Older variant. Wired through trust_layer but not referenced in current workflow. May be obsolete."
- `KPI_LIVE_INVENTORY_2026-05-06.md:154` — says "Imports verified in `parsers/tier2_batch_processor.py`, `scripts/backfill_master_table.py`, and the test suite." **This claim is STALE for both `backfill_master_table.py` (no longer imports tier2_batch_processor) and the test suite (no test imports the legacy file).**
- `KPI_NOTION_TRACKER_IMPORT.md:142,293` — tracks as LEGACY component with find-007 finding.
- `PARSER_AUDIT_2026-05-26.md:60,333` — tracks rename as pending.
- `PARSER_SPEC_v1.0.1_ADDENDUM.md:118` — names spawn-task #4 pending.

**Workflow / CI mentions:**

- `.github/workflows/weekly_pipeline.yml` — runs `tier2_pdf_batch.py` (canonical), no reference to legacy.

**Test imports:**

- `tests/test_pdf_parsers_golden.py:50` — imports from `parsers.tier2_pdf_batch` (canonical).
- `tests/test_truth_mediation_log.py:38` — imports `_write_truth_mediation_events` from `parsers.tier2_pdf_batch` (canonical).
- `scripts/test_stylist_transform.py:24` — imports `transform_to_stylist_rows` from `parsers.tier2_pdf_batch` (canonical).
- **No test imports `parsers.tier2_batch_processor`.**

---

## Recommendation

Per the strict reading of Tony's decision matrix:

> **If parity is INCOMPLETE:** do NOT rename. Report the missing functionality to Tony. He decides whether to port the missing features to the canonical file first or keep the legacy file in active use.

Parity is incomplete. **No rename performed.** Three options for Tony:

### Option A — Rename anyway (the chat-directive intent)

Strongest argument: canonical's own docstring (line 33) already names the legacy file as the documented reference for the deferred Excel path. Renaming to `.DEPRECATED.py` preserves that reference and signals current status, exactly per Tony's chat directive *"Mark `tier2_batch_processor.py` (the legacy version) as deprecated — rename to `tier2_batch_processor.DEPRECATED.py`"*. The "missing features" are intentionally deferred, not regressions.

Required follow-up if Tony picks this:
1. Update `parsers/tier2_pdf_batch.py:34` docstring to say `tier2_batch_processor.DEPRECATED.py`.
2. Update `parsers/pdf_zenoti_v2.py:11` and `parsers/pdf_salon_ultimate_v2.py:9` docstrings similarly.
3. Fix the stale `CLAUDE.md:633` line.
4. Update `KPI_LIVE_INVENTORY_2026-05-06.md:128` status from UNCLEAR → DEPRECATED-RENAMED.
5. Close `KPI_NOTION_TRACKER_IMPORT.md:293` finding `find-007-tier2-old-variant`.
6. PR for review.

### Option B — Port Excel + DataMerger into canonical first

Strongest argument: clean future. After porting, parity is full and the legacy file is genuinely obsolete (not just deferred). But this is real engineering work, not a one-PR rename:

- Port `_import_parsers()` Excel imports into canonical
- Port Excel branch of `detect_system()` (or add Excel detection to `pdf_detect.py`)
- Port `DataMerger.merge_multiple_locations()` integration
- Decide on trust-layer philosophy — keep canonical's flag-based, or port legacy's hard-abort
- Decide whether `validate_stylist_data()` invariants still matter (probably not, given parser maturity)

Risk: this is substantial work without an immediate use case — Karissa isn't shipping Excel today.

### Option C — Leave legacy unrenamed (status quo)

Strongest argument: no risk; we know exactly where the Excel reference lives; the file is dead code with zero imports. Cost: the audit findings stay open (`find-007-tier2-old-variant`), `KPI_LIVE_INVENTORY_2026-05-06.md` keeps saying UNCLEAR.

---

## Decision summary

| Option | Effort | Risk | Future-proofing | Audit clean-up |
|---|---|---|---|---|
| A — Rename to `.DEPRECATED.py` + doc updates | Small (1 PR, ~6 file edits) | Low (no production imports to break) | Good — preserves the legacy as reference | Closes find-007 |
| B — Port Excel+merge to canonical, then delete legacy | Large (parser refactor + merge logic + tests) | Medium (new code paths, new tests) | Best — full parity, single source | Closes find-007 + unblocks Excel ingestion |
| C — Leave as-is | Zero | Zero | None — finding stays open | find-007 stays open |

**Cowork recommends Option A** unless Tony plans to re-introduce Excel ingestion in the next sprint, in which case Option B becomes worth the effort.

---

## Appendix: complete public surface of legacy file

For reference when porting/diffing:

```
parsers/tier2_batch_processor.py
├── REQUIRED_STYLIST_KEYS                                    (constant, set of 10 strings)
├── _TRUST_LAYER_AVAILABLE                                   (private bool)
├── _import_parsers()                                        (private)
├── detect_system(file_path) -> Optional[str]                (public function)
├── group_files_by_location(files) -> Dict                   (public function)
├── validate_stylist_data(stylists) -> Tuple[List, List]     (public function)
├── _build_arg_parser() -> ArgumentParser                    (private)
├── main(argv=None)                                          (CLI entry)
└── class Tier2BatchProcessor
    ├── __init__(input_dir, dry_run, sheets_writer)
    ├── process(write_mode="append") -> Dict                 (public method)
    ├── _discover_files() -> List[str]                       (private)
    ├── _safe_parse(parser_key, file_path, summary) -> Dict  (private)
    ├── _get_writer() -> GoogleSheetsWriter                  (private)
    ├── _run_trust_layer(excel, merged, pdf_by_loc) -> Tuple (private)
    └── format_summary(summary) -> str                       (staticmethod, public)
```

## Appendix: complete public surface of canonical file

```
parsers/tier2_pdf_batch.py
├── FLAG_UNKNOWN_PLATFORM                                    (constant str)
├── FLAG_DETECT_FAILED                                       (constant str)
├── FLAG_PARSE_FAILED                                        (constant str)
├── FLAG_LOCATION_NOT_IN_CONFIG                              (constant str)
├── FLAG_DUPLICATE_LOCATION_OVERWRITTEN                      (constant str)
├── FLAG_MISSING_WEEK_ENDING                                 (constant str)
├── FLAG_NO_LOCATION_RESOLVED                                (constant str)
├── _utc_now_stamp()                                         (private)
├── _ensure_dir(path)                                        (private)
├── _safe_load_json(path)                                    (private)
├── _safe_write_json_atomic(path, payload)                   (private)
├── _load_customer_config(customer_id)                       (private)
├── _build_location_lookup(customer_config)                  (private)
├── transform_to_current_row(parsed, platform, name) -> Dict (public function)
├── transform_to_stylist_rows(parsed, platform, name, id)    (public function)
├── _process_one_pdf(record, location_lookup) -> Tuple5      (private)
├── _read_existing_current(service, sheet_id) -> List        (private)
├── _merge_rows(existing, fresh, config) -> List             (private)
├── _write_current_tab(config, rows, dry_run) -> bool        (private)
├── _write_stylists_tabs(config, stylists, week, dry) -> bool(private)
├── _update_manifest(path, flags, processed) -> bool         (private)
├── _write_run_log(payload) -> Optional[Path]                (private)
├── _write_truth_mediation_events(parsed, flags, name)       (private — Branch 3)
├── process_manifest(manifest_path, config, dry_run) -> Dict (public function — main orchestrator)
└── main(argv=None) -> int                                   (CLI entry)
```
