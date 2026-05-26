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

- **§6.1 unclosed-day automated rerun + leave-blank workflow** — detection + alert only built for v1.0 launch per Tony's decision; full automation deferred to Phase 1.1. Follow-up branch: `unclosed-day-alert-hook-2026-05-XX`.
- **§6.2 product line-item mismatch detection** — **IMPLEMENTED** in branch `product-mismatch-detection-2026-05-26`, commit `d5dbd4c`. See new Section H below.
- **§10 literal `truth_mediation_log.json` file** — hybrid build planned on follow-up branch `truth-mediation-log-serializer-2026-05-XX`; existing in-memory `CompletenessCheck` pattern continues in parallel.
- **§11 single config.json** — current implementation splits across `config/inbox_config.json`, `config/zenoti_schema.json`, `config/salon_ultimate_schema.json`, `config/drift_config.json`. Decision: leave split, no v1.1.0 change planned (each file has a single clear concern).
- **§8 chunked architecture (4 explicit chunks)** — current parsers are functionally chunked but not structurally gated. Decision: leave as-is unless future test surface area justifies the refactor cost.

---

## Section F — Tolerance threshold tier adoption (deferred)

FINAL_SPEC §7 defines a 3-tier tolerance system (rounding ≤ $0.01 silent, > 0.01 & ≤ 1% soft-warn, > 1% hard-fail). Current parser uses a single $0.01 threshold for `TOTAL_SALES_MISMATCH`. Soft-warn middle tier not implemented.

**Status:** Deferred. The current behavior is stricter than the spec, not laxer, so no false negatives. Adoption of the 3-tier model is a v1.1.0 candidate.

---

## Section G — Activation gaps (separate from spec compliance)

Tracked in PARSER_AUDIT_2026-05-26.md §7. Not spec items per se, but blockers for going live with a spec-compliant parser:

1. Weekly pipeline has not auto-committed dashboards since 2026-04-22
2. `config/inbox_config.json` has 2 `[REPLACE_BEFORE_GO_LIVE]` placeholder emails
3. `data/processed_attachments.json` ledger is per-runner ephemeral on GitHub Actions
4. `parsers/tier2_batch_processor.py` (legacy) needs rename → `.DEPRECATED.py` after parity verification

Spawn-tasks pending. None impact spec correctness directly.

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

## END OF v1.0.1 ADDENDUM
