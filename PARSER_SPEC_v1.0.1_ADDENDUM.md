# PARSER_SPEC v1.0.1 — ADDENDUM (LIVE)

**Status:** ADDENDUM (additive only)
**Created:** 2026-05-26
**Applies to:** "KPI Platform — PDF Parser Final Spec" v1.0.0 (LOCKED)
**Maintained by:** Tony Grant
**Audit basis:** PARSER_AUDIT_2026-05-26.md (repo root)

---

## 0. How to read this file

FINAL_SPEC v1.0.0 is locked and immutable. This addendum is its only mutable companion. New clarifications, corrections, vocabulary mappings, and amendments live here.

If FINAL_SPEC v1.0.0 and this addendum disagree on any item explicitly listed below, **the addendum wins for that item.** Everything else, the base spec stands.

When the accumulated addenda justify a new base spec, v1.1.0 will start by folding selected entries here back into a new base — at which point this file is archived as `PARSER_SPEC_v1.0.1_ADDENDUM.ARCHIVED.md` and a new v1.1.1 addendum begins.

---

## Section A — Vocabulary Map (2026-05-26)

**Source:** PARSER_AUDIT_2026-05-26.md §6.2; Tony's decision matrix Q2 (2026-05-26 chat).

The spec uses terminology that maps to existing code under different names. The single source of truth for this mapping lives in `CLAUDE.md` under the heading "**Vocabulary Map — spec terminology ↔ existing modules**". This addendum cross-references that table rather than duplicating it.

**Implication:** implementations satisfying the spec terms via the modules listed in CLAUDE.md's Vocabulary Map are SPEC COMPLIANT. No code rewrite required for naming alone.

**Key mappings (excerpted; full table in CLAUDE.md):**

- "Salon-level supremacy" → `utils/data_merger.py`
- "Reconciliation engine" → `trust_layer/completeness_validator.py::_check_cross_file_totals`
- "Truth Mediation Log" → hybrid: in-memory CompletenessCheck + on-disk JSON via `trust_layer/truth_mediation_log.py` (follow-up branch)
- "Same-week file verification" → `trust_layer/cross_file_verifier.py`

---

## Section B — Color % formula correction (2026-05-26)

**Source:** PARSER_AUDIT_2026-05-26.md §3; Karissa voice memo Round 1 Q5; Karissa tracker F19=E19/D3; FINAL_SPEC v1.0.0 §3.2.

FINAL_SPEC v1.0.0 §3.2 row for Color % reads "Color Net / Service Net". This is the only valid formula.

**Previous parser implementation defect:** `pdf_zenoti_v2.py` and `pdf_salon_ultimate_v2.py` used `color_sales / total_sales` (share of TOTAL revenue) instead of `color_sales / service_net` (share of SERVICE revenue). Magnitude of error: ~8–13% relative under-reporting per location.

**Resolution (2026-05-26 on branch `parser-audit-2026-05-26`):**
- Both parsers updated to use `service_net` denominator
- All 12 golden test fixtures recomputed and updated
- `test_color_pct_is_revenue_share_not_penetration` retained the same NAME (correct intent: "revenue share" here means SHARE OF SERVICE REVENUE, the opposite of `color_count / guest_count` "penetration") with internal math corrected
- `utils/data_merger.py:96` was already correct — no change needed downstream

**Production impact:** None. Weekly pipeline had not auto-committed dashboards since 2026-04-22. No incorrect Color % values ever reached managers or Karissa.

---

## Section C — Zenoti production_hours source (2026-05-26)

**Source:** PARSER_AUDIT_2026-05-26.md §6.1 amendment (Gap A finding); FINAL_SPEC v1.0.0 §6.6.

FINAL_SPEC v1.0.0 §6.6 specifies EMPLOYEE PERFORMANCE Total row → PRODUCTION_HOURS column (field 4) as the canonical Zenoti source.

**Defects found and fixed 2026-05-26:**

1. The HOURLY WORK DETAILS path in the old parser was spec-non-compliant (not the source the spec named).
2. The HOURLY WORK regex was also broken: PyMuPDF renders each hourly bucket on its own line, so `^Production Hours\s+([0-9].+)$` captured only the FIRST hourly bucket via `\s+` consuming the newline. For 8/9 Zenoti locations the first bucket was "0" (single char) and the regex returned None — parser silently fell through to EMP PERF (correct answer by accident). For Roseville (opens 9AM, first bucket = "15.17") the regex captured 15.17 and shipped that as the location total. Real Roseville total = 166.05.
3. Roseville golden test fixture had been pinned to `production_hours=15.17, pph=404.88` with a misleading "verified against the raw PDF" comment.

**Resolution:**
- HOURLY WORK extraction (`_RE_HOURLY_PRODUCTION_HOURS` + the HOURLY branch of `_extract_production_hours_total`) deleted entirely from `pdf_zenoti_v2.py`
- EMP PERF Total field 4 is now the sole source for Zenoti location-level `production_hours`
- Roseville golden fixture corrected: `production_hours: 15.17 → 166.05`, `pph: 404.88 → 36.99`
- Other 8/9 Zenoti locations: no value change (they were already using EMP PERF via the silent fallback)

**Production impact:** None. Same dormancy as Section B above.

---

## Section D — Location count clarification (2026-05-26)

FINAL_SPEC v1.0.0 §1 and §13 reference "13 weekly PDFs" in passing. The §2 table correctly lists 12 locations. The accurate count is **12** (Woodbury removed). Read "13" in §1 and §13 as "12". Source of truth: `config/customers/karissa_001.json`.

---

## Section E — Open spec items deferred to v1.1.0 candidates

The following FINAL_SPEC items are real but deferred past 2026-05-26 launch readiness. Tracked for a future v1.1.0 base spec revision rather than addended in-flight:

- **§6.1 unclosed-day workflow** — **Phase 1 (detection + alert) IMPLEMENTED** in branch `unclosed-day-alert-hook-2026-05-26`, commit `c73ecaa`. See new Section I below. Phase 1.1 (automated rerun + Mon-EOD blank-out) still deferred.
- **§6.2 product line-item mismatch detection** — **IMPLEMENTED** in branch `product-mismatch-detection-2026-05-26`, commit `d5dbd4c`. See new Section H below.
- **§10 literal `truth_mediation_log.json` file** — **IMPLEMENTED** in branch `truth-mediation-log-serializer-2026-05-26`, see new Section J below. NDJSON format; existing in-memory `CompletenessCheck` pattern continues in parallel (hybrid model per Tony's Q1).
- **§11 single config.json** — current implementation splits across `config/inbox_config.json`, `config/zenoti_schema.json`, `config/salon_ultimate_schema.json`, `config/drift_config.json`. Decision: leave split, no v1.1.0 change planned (each file has a single clear concern).
- **§8 chunked architecture (4 explicit chunks)** — current parsers are functionally chunked but not structurally gated. Decision: leave as-is unless future test surface area justifies the refactor cost.

---

## Section F — Tolerance threshold tier adoption (deferred)

FINAL_SPEC §7 defines a 3-tier tolerance system (rounding ≤ $0.01 silent, > 0.01 & ≤ 1% soft-warn, > 1% hard-fail). Current parser uses a single $0.01 threshold for `TOTAL_SALES_MISMATCH`. Soft-warn middle tier not implemented.

**Status:** Deferred. The current behavior is stricter than the spec, not laxer, so no false negatives. Adoption of the 3-tier model is a v1.1.0 candidate.

---

## Section G — Activation gaps (separate from spec compliance)

Tracked in PARSER_AUDIT_2026-05-26.md §7. Not spec items per se, but blockers for going live with a spec-compliant parser:

1. **Weekly pipeline failing every Monday since 2026-04-27** — DIAGNOSED 2026-05-26. Pipeline IS firing on schedule but the `Run KPI Pipeline` step (main.py) fails with `ERROR: No location data found in CURRENT tab — aborting pipeline.` Two upstream causes, both expected for the current pre-launch state:
   - (a) Intentional CURRENT-tab wipe (Tony cleared Sheets data so dashboards wouldn't reflect anything until Tier 2 activates with fresh PDFs). main.py correctly refuses to publish over empty data — exactly the safety behavior we want.
   - (b) Gmail OAuth refresh token dead (see item 5). Step 0 (inbox watcher) fails first with `invalid_grant`, but `continue-on-error: true` lets the workflow proceed until main.py's hard failure.

   Will resolve naturally when both items are addressed at Branch 4 activation. **Spawn-task #1 marked closed** (diagnosis complete 2026-05-26, no fix needed pre-launch).

2. `config/inbox_config.json` has 2 `[REPLACE_BEFORE_GO_LIVE]` placeholder emails — to be filled with Karissa's real address after fresh-PDF validation confirms parser accuracy. Branch 2's alert function defensively handles the placeholder state, so this is a one-line config update, not a code change. **Spawn-task #2 pending.**

3. `data/processed_attachments.json` ledger persistence — **RESOLVED 2026-05-26** on branch `inbox-ledger-persistence-2026-05-26`. Decision: git-commit the ledger via a new workflow step (`Commit inbox ledger`) inserted between the watcher and Tier 2. The ledger is un-gitignored (`.gitignore` exception), the workflow stages + commits any change, and the existing Step 7 push picks it up alongside the dashboard commit. No new dependencies, no `actions/cache` eviction risk. See `INBOX_LEDGER_PERSISTENCE_DECISION.md` for the full options evaluation. **Spawn-task #3 closed.**

4. `parsers/tier2_batch_processor.py` (legacy) needs rename → `.DEPRECATED.py` after parity verification. **Spawn-task #4 pending.**

5. **Gmail OAuth refresh token** (`KPI_INBOX_REFRESH_TOKEN` GitHub Secret) — revoked or expired. Surfaced 2026-05-26 in production run logs as `RuntimeError: INBOX WATCHER AUTH FAILURE — 400: invalid_grant`. Google revokes refresh tokens after ~6 months of inactivity or on user revocation. Required for Tier 2 to fetch attachments from the dedicated inbox `karissaperformanceintelligence@gmail.com`. **Branch 4 (`tier2-go-live-activation`) prerequisite — regenerate via `email_assistant/get_token.py` immediately before activation.** Per Tony's 2026-05-26 chat: won't regenerate early because premature regeneration would just expire again if launch slips further. The same OAuth identity (or a parallel one) is also used by the Email Assistant pipeline (`GMAIL_REFRESH_TOKEN`), which may also need regeneration at the same time.

Spawn-tasks status: #1 and #3 closed (2026-05-26); #2 / #4 pending. None impact spec correctness directly.

---

## Section H — Product header vs detail mismatch detection (2026-05-26)

**Source:** PARSER_AUDIT_2026-05-26.md §6.3 (resolved) + Branch 1 completion record in §12; FINAL_SPEC v1.0.0 §6.2; Tony's Branch 1 decision matrix (2026-05-26 chat).

FINAL_SPEC v1.0.0 §6.2 specifies that SU PDFs occasionally show a discrepancy between the Sales-block "Total Retail" header and the "Top Product Lines" detail table, and that:

1. The header stays canonical (`product_net`).
2. The line-item sum is a cross-check.
3. A flag fires when they disagree by more than the rounding tolerance.

**Resolution (2026-05-26 on branch `product-mismatch-detection-2026-05-26`):**

- New regex `_RE_PRODUCT_LINES_TOTALS` in `parsers/pdf_salon_ultimate_v2.py` extracts the existing PDF-provided TOTALS row sales value (no need to re-sum line items — the PDF computes it).
- New flag `FLAG_PRODUCT_TOTAL_MISMATCH = "PRODUCT_TOTAL_MISMATCH"` fires from `_compute_karissa_kpis()` when `|product_net - product_lines_sum| > $0.01`.
- `product_net` continues to be the header value (`raw["total_retail"]`) per spec — even when mismatch is detected.
- 6 new tests in `TestProductTotalMismatch` covering Lakeville (positive case, where the mismatch lives) plus Apple Valley and Farmington (negative cases, no false positives).

**Lakeville is the canonical real-world example:** header $534.50 vs TOTALS row $623.25 (delta $88.75, likely refund timing — header post-refund, line items pre-refund). The PDF prints the TOTALS row's "% Sales" as 116.60% (= 623.25 / 534.50), itself a signal of the discrepancy.

**Tolerance:** $0.01 — matches FINAL_SPEC §7 rounding tier and the parser's existing `TOTAL_SALES_MISMATCH` threshold.

**Production impact:** None. Pipeline still dormant. When the pipeline goes live (Branch 4 — `tier2-go-live-activation`), Lakeville will display $534.50 as `product_net` and the trust layer will see `PRODUCT_TOTAL_MISMATCH` in the manifest's `trust_layer_flags`, available for downstream surfacing.

**Cross-references:**
- Vocabulary Map (CLAUDE.md): new row added for "Product header vs detail mismatch detection (FINAL_SPEC §6.2)".
- Section E above: entry status flipped from "UNBUILT" to "IMPLEMENTED in commit `d5dbd4c`".
- Branch 3 (`truth-mediation-log-serializer`) will wire `PRODUCT_TOTAL_MISMATCH` events into `data/logs/truth_mediation_log.json` per FINAL_SPEC §10.

---

## Section I — Unclosed-day detection + alert (2026-05-26)

**Source:** PARSER_AUDIT_2026-05-26.md §6.4 (Phase 1 resolved) + §12 Branch 2 completion record; FINAL_SPEC v1.0.0 §6.1; Karissa voice memo Round 1 Q7; Tony's Q3 decision matrix (2026-05-26 chat).

FINAL_SPEC v1.0.0 §6.1 specifies that when a PDF contains an unclosed day, the system must alert Karissa immediately so she can decide whether to rerun the POS export. Per Karissa's voice memo Q7, this is a hard requirement for go-live.

**Resolution — Phase 1 (2026-05-26 on branch `unclosed-day-alert-hook-2026-05-26`):**

- Detection already existed (`parsers/pdf_common.py::detect_unclosed_days` + `PARTIAL_WEEK` parser flag).
- New alert hook: `core/email_sender.py::send_partial_week_alert()`. Same SMTP plumbing, same `[REPLACE` placeholder filter, same try/except discipline as `send_inbox_notification`.
- New HTML body builder: `_build_partial_week_alert_html()`. Mobile-readable.
- `parsers/tier2_pdf_batch.py::_process_one_pdf` extended to return a 5-tuple (added `parsed` dict) so the orchestrator can extract `unclosed_days` without re-parsing.
- `process_manifest` collects records during the per-PDF loop and fires the alert once after `_update_manifest` succeeds.
- 7 new tests in `TestPartialWeekAlert` — all SMTP-mocked, no network calls.

**Lakeville is the canonical real-world fixture** — the golden test already asserts the `PARTIAL_WEEK` flag fires for the 2026-04-05 week.

**Defensive recipient handling:** if `config/inbox_config.json::notification_recipients` contains only `[REPLACE_BEFORE_GO_LIVE]` placeholders, the alert function logs WARN and skips the email send. This is the current state (pre-go-live) — the alert code is correct today and starts firing the moment Karissa's real email is added to the config.

**Production impact:** None. Pipeline still dormant (CURRENT tab empty, awaiting OAuth refresh + fresh-PDF validation).

**Phase 1.1 — explicitly deferred:**

- Automated rerun-request workflow (today's alert is one-way; rerun is manual)
- Auto-blank-out-by-Mon-EOD (today: parse what we have, flag PARTIAL_WEEK, dashboard surfaces the flag)
- Alert retry on SMTP failure
- Cross-run alert deduplication

These will move from "deferred" to "implemented" if/when Karissa's operational experience surfaces a real need.

**Cross-references:**
- Vocabulary Map (CLAUDE.md): "Unclosed-day detection + alert" row updated to add the alert hook.
- Section E above: entry status flipped from "detection + alert only built for v1.0 launch" to "Phase 1 IMPLEMENTED in commit `c73ecaa`".
- Branch 3 (`truth-mediation-log-serializer`) will wire `PARTIAL_WEEK` alert events into `data/logs/truth_mediation_log.json` per FINAL_SPEC §10.

---

## Section J — Truth Mediation Log serializer (2026-05-26)

**Source:** PARSER_AUDIT_2026-05-26.md §6.2 (Gap B Truth Mediation Log resolution) + §12 Branch 3 completion record; FINAL_SPEC v1.0.0 §10; Tony's Q1 hybrid decision (2026-05-26 chat).

FINAL_SPEC v1.0.0 §10 specifies a durable on-disk audit trail of reconciliation events as JSON. Per Tony's Q1 hybrid choice, the in-memory `CompletenessCheck` primitive (existing in `trust_layer/severity.py`) is preserved and a new serializer drains events to disk.

**Resolution (2026-05-26 on branch `truth-mediation-log-serializer-2026-05-26`):**

- New module `trust_layer/truth_mediation_log.py` with `write_event(...)` and `read_events(...)` functions.
- **NDJSON format** — one JSON object per line at `data/logs/truth_mediation_log.json`. Despite the `.json` extension, the file is newline-delimited so writes can append without parsing the whole file. Gitignored via the existing `data/logs/` rule.
- **Schema per FINAL_SPEC §10 exactly:** 12 fields — `timestamp, location_id, week_start, rule_applied, field, salon_level_value, stylist_sum_before, drift_amount, drift_pct, hypothesis, action, human_review_required`. A schema-lock test asserts every event has exactly these keys.
- **Recognized `rule_applied` values** (constants in the module — free-form strings also accepted):
  - `salon_level_supremacy` — the spec's primary use case (stylist proportional adjustment); not yet emitted by any callsite, available for future use.
  - `product_total_mismatch` — Branch 1 / FINAL_SPEC §6.2. `salon_level_value` = header (canonical), `stylist_sum_before` = line-item sum (observed).
  - `partial_week_detected` — Branch 2 / FINAL_SPEC §6.1. Reconciliation-numeric fields stay null; `hypothesis` carries the unclosed-day date(s); `human_review_required=True`.
  - `cross_file_reconciled` — placeholder for Excel↔PDF reconciliation events from `trust_layer/completeness_validator.py` (not yet wired in this branch).
- **Tier 2 dispatch:** `parsers/tier2_pdf_batch.py::_write_truth_mediation_events` is called inside the per-PDF loop of `process_manifest` whenever a parsed PDF carries recognized flags. It bridges flag detection to log writes. Never raises — defensive try/except so a broken log path can't crash the pipeline.
- 14 new tests in `tests/test_truth_mediation_log.py` — 9 module unit tests + 5 tier2 wiring tests. All use `tempfile` for isolation; no real disk writes outside the temp dir.

**Override the log path** via the `TRUTH_MEDIATION_LOG_PATH` environment variable (used in tests; useful for operators who want to redirect during debugging).

**Tolerance + atomicity:** writes are append-only with explicit flush. POSIX guarantees atomic appends for writes under `PIPE_BUF` (4KB) — well above our typical event size (~500 bytes). On Windows we accept the slightly weaker guarantee since the alternative (temp-file-rename per event) is wasteful for low-volume runs (≤12 events/week).

**Production impact:** None — pipeline still dormant. When Tier 2 activates (Branch 4), the log file will start accumulating one row per reconciliation event per weekly run.

**Cross-references:**
- Vocabulary Map (CLAUDE.md): "Truth Mediation Log" row updated — removed "(follow-up branch)" qualifier, added the four recognized `rule_applied` constants.
- Section E above: `§10 literal truth_mediation_log.json file` entry flipped from "hybrid build planned on follow-up branch" to "IMPLEMENTED in commit `<sha>`".
- PARSER_AUDIT_2026-05-26.md §12 — Branch 3 completion record.

**Future work (not in Branch 3):**

1. Wire `cross_file_reconciled` events from `trust_layer/completeness_validator.py::_check_cross_file_totals` (currently produces CompletenessCheck objects only; doesn't write to the log).
2. Emit `salon_level_supremacy` events when stylist proportional adjustment is implemented (the spec's primary use case — currently nothing in the codebase performs this adjustment).
3. Log rotation when the file gets large (not a concern at ≤12 events/week, but worth revisiting if event volume grows).

---

## END OF v1.0.1 ADDENDUM
