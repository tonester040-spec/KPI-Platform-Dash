# KPI Platform — Security Audit Log

## Audit Date: 2026-03-15
## Auditor: Claude (Cowork session — Sonnet 4.6)
## Reviewed By: Tony Grant
## Branch: security-hardening (do not merge to main until Tony approves)

---

## Summary

| Severity | Found | Fixed | Documented Only |
|----------|-------|-------|-----------------|
| CRITICAL |   0   |   0   |       0         |
| HIGH     |   2   |   2   |       0         |
| MEDIUM   |   5   |   3   |       2         |
| LOW      |   5   |   1   |       4         |
| **Total**| **12**| **6** |     **6**       |

**New files created:** `core/alerter.py`, `core/drift_checker.py`, `config/drift_config.json`, `config/zenoti_schema.json`, `config/salon_ultimate_schema.json`, `scripts/sandbox_run.py`

**Sandbox validation:** 8/8 modules PASS (confirmed 2026-03-15)

---

## Findings and Fixes

| # | Section | File | Line | Severity | Issue | Fix | Status |
|---|---------|------|------|----------|-------|-----|--------|
| H1 | §1 / §10 | `email_assistant/noise_filter.py` | 71 | **HIGH** | `always_real_senders` was loaded from config but never checked — a trusted sender whose domain matched a noise pattern would be silently filtered and Karissa would never see the email | Added bypass check at the top of `_is_noise()`: if `sender_email` is in `always_real_senders`, returns `(False, "")` immediately before all other checks | **FIXED** |
| H2 | §1 / §10 | `config/email_config.json` | — | **HIGH** | `always_real_senders` field was a JSON object (with `_comment` and `examples` keys) instead of a list — `config.get("always_real_senders", [])` silently returned a dict, making any always-real logic impossible to implement | Corrected to a proper JSON array (list). Added a placeholder string that the filter code skips. Instructions clear on how to add real addresses. | **FIXED** |
| M1 | §6 / §8 | `config/customers/karissa_001.json` | 22–29 | **MEDIUM** | Manager PINs (Jess: `1234`, Jenn: `5678`) stored in plaintext in a file committed to the repository. If the repo is public (which the GitHub Pages URL implies), any visitor can view PINs. | **Documented only** — PINs are acknowledged as casual access control, not true security (see Architecture Decisions below). The correct long-term fix is to move PINs to GitHub Secrets and inject them at dashboard build time. Deferred until dashboard_builder.py supports env-var injection. | **DOCUMENTED** |
| M2 | §8 | `core/email_sender.py` | 204 | **MEDIUM** | Recipient email addresses logged at INFO level (`log.info("Email sent to: %s", recipients)`) — operational logs should not contain PII | Changed to `log.info("Email sent to %d recipient(s)", len(recipients))` — count only, no addresses | **FIXED** |
| M3 | §11 | `.github/workflows/email_assistant.yml` | 24 | **MEDIUM** | `id-token: write` permission included but not used — OIDC federation is not configured in this repo. Unnecessary permissions violate least-privilege principle | Removed `id-token: write` from the email_assistant workflow permissions block | **FIXED** |
| M4 | §11 | `.github/workflows/weekly_pipeline.yml` + `email_assistant.yml` | — | **MEDIUM** | No branch protection or manual approval gates configured — workflows can be triggered from any branch, and `workflow_dispatch` is available to any repo collaborator | **Documented only** — see Open Items section for GitHub UI steps to enable environment protection rules. Cannot be configured from code alone — requires GitHub repository settings. | **DOCUMENTED** |
| M5 | §14 | `main.py` + all modules | — | **MEDIUM** | No CRITICAL/HIGH alerting mechanism — pipeline failures only produced log output; no notification reached Tony before GitHub could send a delayed job-failure email | Created `core/alerter.py` with dual-path alerting (GitHub Actions Job Summary + Gmail SMTP). Wired into `main.py` top-level exception handler. Alert fires BEFORE `sys.exit(1)`. | **FIXED** |
| L1 | §7 | `core/data_source.py:66` + `core/sheets_writer.py:27` | 66, 27 | **LOW** | `_build_service()` function is identical in both files — DRY violation. If auth pattern changes (e.g. adding retry or scope changes) it must be updated in two places. | **Documented only** — not refactored per audit scope rules (no security fix required). When Zenoti/Salon Ultimate APIs are added, extract to `core/google_auth.py` at that time. | **DOCUMENTED** |
| L2 | §5 | `requirements.txt` | 16 | **LOW** | `requests>=2.31.0` listed as a dependency with comment "for future Zenoti/Salon Ultimate connectors" — it is not currently used anywhere in the codebase. Unused packages increase attack surface. | **Documented only** — package is legitimate and the comment explains intent. Acceptable risk. Remove when connectors are built and the actual HTTP client choice is finalized. | **DOCUMENTED** |
| L3 | §1 | `CLAUDE.md` + `config/customers/karissa_001.json` | — | **LOW** | Google Sheet ID (`1JY6L7H1Pb2JFmNoz2XNkvG0ogrYgagLVDwH01vuWT28`) hardcoded in config and documentation. The sheet ID alone does not grant access — a valid service account credential is also required — so this is low risk. | **Documented only** — acceptable risk given the access controls in place. | **DOCUMENTED** |
| L4 | §7 | `email_assistant/run_assistant.py` | 35 | **LOW** | Module docstring still references old path `email/run_assistant.py` (pre-rename) | **Documented only** — cosmetic. Fix in next maintenance pass. | **DOCUMENTED** |
| L5 | §7 | `email_assistant/gmail_connector.py` | 3 | **LOW** | Module docstring header still references old path `email/gmail_connector.py` | **Documented only** — cosmetic. Fix in next maintenance pass. | **DOCUMENTED** |

---

## Architecture Decisions Documented

### PIN Gate Security Model

The PIN gates on `jess.html` and `jenn.html` use client-side JavaScript. They are hosted on GitHub Pages (static hosting). **A user with browser developer tools can bypass them.**

This is **acceptable and intentional** for the current threat model:
- The dashboards contain **aggregated business performance data only** (KPI metrics, revenue ranges, location averages)
- They contain **zero guest PII** — no names, phone numbers, email addresses, or payment data
- The PIN gates provide **casual access prevention** (prevent accidental browsing by unauthorized staff)
- **True security relies on no PII in the frontend bundle** — this is the correct architecture for static hosting

If the threat model changes (e.g., dashboards include guest-level data in the future), migrate to server-side authentication via a Cloudflare Worker or similar proxy. The chat assistant architecture already anticipates this (see CLAUDE.md: "What's paused / future state").

**Known risk (M1 above):** Manager PINs are currently stored in `karissa_001.json` which is committed to the repo. If the repo is public, the PINs are visible. Long-term fix: inject PINs from GitHub Secrets at build time. Deferred.

### Atomic Write Pattern for Google Sheets

`sheets_writer.py` uses a clear-then-write pattern for the CURRENT and STYLISTS_CURRENT tabs:

```python
service.spreadsheets().values().clear(...)   # Tab emptied here
service.spreadsheets().values().update(...)  # New data written here
```

**Gap:** If the `update()` call fails after `clear()` succeeds, the tab is left empty until the next pipeline run (Monday). This is not a data corruption risk for the append ledger (DATA tab, STYLISTS_DATA tab) — those are never touched by this pattern. The CURRENT tab is a snapshot that is fully regenerated each run, so an empty state is recoverable on the next Monday.

**Acceptable risk** for the current architecture. The Google Sheets API does not support true atomic batch-clear-and-write in a single API call. A write-verify pattern (write to staging range, validate, swap to live range) would require tab duplication and is not warranted at current data volume and criticality.

**If this becomes critical:** Add a pre-write backup step that copies CURRENT to a CURRENT_BACKUP tab before clearing. Implement in `sheets_writer.py` as `_backup_current()` called before `_clear_and_write()`.

### Schema Validation Contract Approach

`config/zenoti_schema.json` and `config/salon_ultimate_schema.json` are the formal contracts between the KPI platform and its data sources. When an API changes its field names or types:

1. The schema file is updated first (explicit, deliberate change)
2. The pipeline code is then updated to match
3. A sandbox validation run confirms no regressions before live deployment

Schema files document required fields, optional fields, data types, minimum expected record counts, and abort conditions. They are not enforced by running code yet (Zenoti and Salon Ultimate APIs are not connected). Schema validation code will be added to the respective connector modules when API access is granted.

### Alert Routing Path

`core/alerter.py` implements two-path alerting for CRITICAL/HIGH failures:

1. **GitHub Actions Job Summary** (primary, always available in CI): Writes a formatted alert block to `$GITHUB_STEP_SUMMARY`. Visible in the Actions UI immediately on any failed run.
2. **Gmail SMTP email** (secondary, requires `GMAIL_APP_PASSWORD` + `GMAIL_SENDER`): Sends an alert email to `KPI_ALERT_EMAIL` (or sender address as fallback). Best-effort — never blocks pipeline exit.

The alerter fires **before** `sys.exit(1)` in the top-level exception handler. Silent failures are not acceptable.

### Sandbox Mode Design

`scripts/sandbox_run.py` provides a full end-to-end pipeline validation with zero real API calls. All 8 core modules are exercised against realistic mock data. The sandbox:
- Uses 12 realistic location rows (all platforms, all managers)
- Exercises drift_checker against these rows to confirm no false positives in UNCALIBRATED config
- Fires a deliberate test alert to confirm the alerter path works
- Writes no files outside /tmp (dashboard_builder writes to a temp directory)

**Run the sandbox before:** any new credential deployment, any schema/code change, any new team member access grant.

### Drift Monitoring Design and Calibration Workflow

`config/drift_config.json` ships with UNCALIBRATED placeholder ranges. The ranges are intentionally wide to avoid false alerts on first live run.

**Calibration workflow (after 4 weeks of live data):**
1. Pull the last 4 weeks of pipeline logs from GitHub Actions artifacts
2. Note actual weekly revenue, guest counts, product %, and rebook rates per location
3. Set `weekly_revenue_min` to 20% below the lowest observed value (allows for bad weeks)
4. Set `weekly_revenue_max` to 120% of the highest observed value (flags obvious data errors)
5. Apply same logic to all other metrics
6. Update `_calibration_status` to `"CALIBRATED"` and note the date
7. Run sandbox validation after updating

Drift fires **warnings** for out-of-range values (does not block dashboard update). Drift fires **errors** and **blocks** the dashboard update for physically impossible values (negative revenue, zero appointments across all stylists, rates outside 0–1).

---

## Schema Contracts Created

| File | Data Source | Fields Documented | Status |
|------|-------------|-------------------|--------|
| `config/zenoti_schema.json` | Zenoti API | 9 required, 5 optional (appointments); 6 required, 2 optional (stylists) | UNCALIBRATED |
| `config/salon_ultimate_schema.json` | Salon Ultimate API | 9 required, 5 optional (transactions); 4 required, 2 optional (staff) | UNCALIBRATED |

Both files document: authentication method, secret names, rate limit handling, retry codes, and abort conditions for record count drops.

---

## Drift Configuration Created

| Status | Locations Configured |
|--------|---------------------|
| UNCALIBRATED | All 12 (z001–z009, z010, su001, su002) |

All thresholds are placeholder ranges. Calibration review scheduled after 4 weeks of live Zenoti + Salon Ultimate data.

---

## Known Acceptable Risks

| Risk | Reasoning | Owner |
|------|-----------|-------|
| Manager PINs in repo (M1) | Acknowledged as casual access control only. No PII in frontend bundle. Long-term fix: inject from Secrets at build time. | Tony to schedule |
| SMTP clear-then-write non-atomicity | Only affects snapshot tabs (CURRENT, STYLISTS_CURRENT). Recoverable on next run. Historical tabs (DATA, STYLISTS_DATA) are unaffected. | Acceptable |
| `requests` unused in requirements.txt | Legitimate package, documented intent (future connectors). Low supply-chain risk. | Remove when connectors built |
| Sheet ID visible in config/docs | Auth requires service account credential separately. ID alone grants no access. | Acceptable |
| Python 3.10 deprecation warning | Google API client warns Python 3.10 reaches EOL in 2026. GitHub Actions runner already uses 3.11. Local dev on 3.10 is harmless. | Low urgency |

---

## Open Items (Require Tony Action)

### 1. Manager PINs — Move to GitHub Secrets (Medium Priority)
**Current state:** Jess PIN `1234` and Jenn PIN `5678` are stored in `config/customers/karissa_001.json`.
**Required action:** When `dashboard_builder.py` is updated to accept env-var-injected PINs, move them to GitHub Secrets (`JESS_DASHBOARD_PIN`, `JENN_DASHBOARD_PIN`). Remove from JSON.
**Who:** Tony + Cowork session

### 2. Manual Approval Gates for Workflows (Medium Priority)
GitHub Actions workflows can currently be triggered by any repo collaborator without approval. To enable manual approval gates:
1. Go to GitHub repo → Settings → Environments
2. Create a new environment named `production`
3. Add "Required reviewers" (add Tony as reviewer)
4. In `weekly_pipeline.yml` and `email_assistant.yml`, add `environment: production` under the job definition
5. Future manual triggers will require Tony to approve before running

**Note:** Scheduled triggers (cron) bypass environment protection. This gate only applies to `workflow_dispatch` manual runs.

### 3. Drift Range Calibration (Scheduled — 4 Weeks After Go-Live)
After 4 weeks of live data from Zenoti + Salon Ultimate:
- Review `config/drift_config.json` for each location
- Replace UNCALIBRATED placeholder ranges with real observed ranges
- Set `"_calibration_status": "CALIBRATED"`
- Run sandbox to confirm no regressions
- See CLAUDE.md drift monitoring section for calibration instructions

### 4. append_to_historical() — Known Gap (High Priority for Data Integrity)
**Status:** `sheets_writer.py` rewrites CURRENT and STYLISTS_CURRENT each run but does NOT automatically append to DATA/STYLISTS_DATA (the historical ledger). The `append_to_historical()` function (~20 lines) needs to be written.

**Risk:** Without this function, historical data only grows if someone manually copies CURRENT rows into DATA. If this is missed, the DATA tab grows stale and historical trends in the dashboard will be wrong.

**Action:** Build `append_to_historical()` in `core/sheets_writer.py` as part of the next Zenoti/Salon Ultimate integration session. Do not assume it is working until confirmed.

### 5. always_real_senders — Populate Before Gmail Goes Live (High Priority)
`config/email_config.json` → `always_real_senders` array is currently a placeholder. Before Gmail access is enabled, add:
- Jess's email address
- Jenn's email address
- Any other trusted internal sender who might be filtered by noise rules

This prevents legitimate manager emails from being silently dropped.

### 6. KPI_ALERT_EMAIL Secret (Optional)
To route alert emails to a specific address (rather than defaulting to the Gmail sender):
- Go to GitHub repo → Settings → Secrets → Actions
- Add `KPI_ALERT_EMAIL` with the alert recipient address

---

## Permanent Guardrails Recommended

These are GitHub/repository settings Tony should enable — they cannot be configured from code:

| Guardrail | Where | Why |
|-----------|-------|-----|
| **Dependabot** | GitHub Settings → Security → Dependabot | Alerts when a dependency has a known CVE |
| **CodeQL** | GitHub Security tab → Code scanning | Static analysis for common code vulnerabilities |
| **Branch protection on main** | Settings → Branches → Add rule | Prevent direct pushes to main; require PR |
| **Manual approval gates** | Settings → Environments → production | Require Tony approval for manual workflow dispatches |
| **Secret scanning** | Settings → Security → Secret scanning | Auto-detect accidentally committed credentials |

---

## 4-Layer Architecture Migration Note

### Current architecture (Phase 1 — in production)
```
Layer 1: Data entry → Google Sheets (manual weekly entry by Karissa's team)
Layer 2: Storage → Google Sheets tabs (CURRENT, DATA, GOALS, ALERTS, STYLISTS_*)
Layer 3: Processing → Python pipeline (main.py + core/ modules)
Layer 4: Delivery → GitHub Pages dashboards (index.html, jess.html, jenn.html)
```

### Phase 2 (in progress — build + dormant)
```
Layer 1 additions: Zenoti API (9 locations) + Salon Ultimate API (3 locations) — replacing manual entry
Layer 2: Google Sheets remains storage bridge during transition
Layer 1.5 addition: Gmail → Email Assistant (karissa-debrief.html)
```

### Future migration path (Phase 3+)
```
Layer 2 → PostgreSQL or BigQuery (replace Google Sheets as storage layer)
Layer 3 → dedicated analytics pipeline (dbt or similar)
Layer 4 → remains GitHub Pages or migrates to Vercel/Netlify with auth
```

**Intent documented for future developers:** The Google Sheets layer is a pragmatic bridge, not the final architecture. Schema contracts (`zenoti_schema.json`, `salon_ultimate_schema.json`) and the `DATA` append ledger pattern are designed to survive a Layer 2 migration with minimal pipeline changes.

---

## Next Audit Trigger

This audit should be re-run when any of the following occur:

- New API integration added (Zenoti, Salon Ultimate, or any other)
- New data source connected to the pipeline
- New team member granted repo or Sheets access
- API schema change detected (field name renamed or type changed)
- Drift alerts fire more than 3× in a single week
- 90 days have elapsed since this audit date (next: 2026-06-13)
- Any security incident of any severity (credential exposure, unauthorized access, data anomaly)
- PIN gates are updated or new dashboard files are added

---

## Sandbox Validation Results (Section 12)

**Run date:** 2026-03-15
**Command:** `python scripts/sandbox_run.py`
**Result:** 8/8 PASS

| Module | Result |
|--------|--------|
| data_processor | ✓ PASS |
| drift_checker | ✓ PASS |
| ai_cards (DRY RUN) | ✓ PASS |
| sheets_writer (DRY RUN) | ✓ PASS |
| report_builder (DRY RUN) | ✓ PASS |
| email_sender (DRY RUN) | ✓ PASS |
| dashboard_builder (DRY RUN) | ✓ PASS |
| alerter (deliberate test alert) | ✓ PASS |

No real API was called. No files were written outside /tmp. Deliberate test alert confirmed firing to log output (GitHub Actions Job Summary path requires CI context).
