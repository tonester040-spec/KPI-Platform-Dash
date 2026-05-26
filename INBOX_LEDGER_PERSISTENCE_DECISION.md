# Inbox Ledger Persistence — Decision Doc

**Status:** Awaiting Tony's approval — this doc proposes a fix; implementation has not started.
**Owner:** Tony
**Spawn-task:** Audit gap #3 (`PARSER_SPEC_v1.0.1_ADDENDUM.md` §G item 3, `PARSER_AUDIT_2026-05-26.md` §7.3)
**Blocks:** Branch 4 (`tier2-go-live-activation`)
**Created:** 2026-05-26

---

## Summary (tldr)

1. The SHA256 dedup ledger in `data/processed_attachments.json` is **gitignored** and lives only on the ephemeral GitHub Actions runner, so every Monday the watcher starts with `ledger = {}` and any attachment still within the 2-day Gmail search window re-processes.
2. **Recommendation: Option 1 — git-commit the ledger** via a new workflow step that runs between the watcher and the existing push step. Fits the pipeline's existing "commit-data-and-push" pattern, no new dependencies, no cache eviction risk.
3. Approval required from Tony on one item before implementation: un-gitignoring the ledger file. The ledger contains hashes, filenames, ISO timestamps, and Gmail message IDs — no PHI, but filenames may reveal operational cadence. Mitigation available (hash-only persisted form) if Tony prefers tighter privacy.

---

## Current state

### Ledger schema (`data/processed_attachments.json`)

Written by `parsers/gmail_attachment_watcher.py:668-672`:

```json
{
  "<sha256_hex>": {
    "filename": "Karissa_Salon_Weekly_Report.xlsx",
    "processed_at": "2026-05-26T12:03:17.482912+00:00",
    "message_id": "18f5d2a0c3e1b4f7"
  }
}
```

Per-entry size: ~150-250 bytes. Annual upper bound: ~52 weeks × ~12 locations × 2 formats ≈ 1,250 entries ≈ 300 KB. Trivial for git.

### Read/write surface in the watcher

| Function                       | Lines       | Behavior                                                                                             |
|-------------------------------- |-------------|------------------------------------------------------------------------------------------------------|
| `load_processed_ledger()`       | 349-357     | Returns `{}` if file missing OR JSON corrupt — **already handles cold start gracefully**.            |
| `save_processed_ledger(ledger)` | 360-369     | Atomic write (tempfile + `os.replace`) into `data/processed_attachments.json`.                       |
| `is_duplicate(hash, ledger)`    | 372-373     | `return content_hash in ledger` — **only the key is consulted; values are metadata-only**.            |
| Ledger load call site           | 597         | Single load at start of `main()`, after auth.                                                        |
| Ledger update call site         | 668-672     | Append on every new (non-duplicate, non-rejected) attachment.                                        |
| Ledger save call site           | 690         | Single save after the message loop, before manifest write.                                            |

### Gap

- **GitHub Actions runners are ephemeral** — every workflow run starts on a fresh filesystem.
- `.github/workflows/weekly_pipeline.yml` has **no `actions/cache` step** for `data/processed_attachments.json`.
- The ledger file is **gitignored** (`.gitignore:48`) — Step 7 push never includes it.
- `data/inbox/`, `data/archive/`, `data/logs/` are also gitignored — but they're either consumed within the same run (inbox → Tier 2) or treated as artifact (logs → uploaded via `actions/upload-artifact`). The ledger has no equivalent escape hatch.

### Blast radius today

If Elaina sends a file at 06:00 Monday and the watcher runs at 07:00 Monday:
- This run: file is new, gets archived + written to inbox + entered into ledger + Tier 2 parses + Sheets updated.
- **Next Monday's run:** ledger is gone. If the same email is still within the 2-day search window (it won't be — 7 days later), no impact. **If `search_window_days` is ever bumped to 7+ or if a partial-week alert causes Elaina to re-send, the same file re-processes.**
- More common case today: a Monday holiday or workflow_dispatch retry within the same week → 2-day window catches the same email, ledger is empty, file re-processes.
- **Downstream impact:** Tier 2 re-parses the same PDF, calls `update_sheet_with_rows` again on CURRENT, and `append_to_historical` would attempt a duplicate append (mitigated by its own week_ending check, but defense-in-depth at the watcher layer is the right place).

---

## Options evaluation

| # | Option                                           | Pro                                                                                                                                              | Con                                                                                                                                                                                                                       | Est. LOC | Risk    |
|---|--------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|---------|
| 1 | **Git-commit the ledger** (workflow step)        | Small structured JSON, exactly what git is good at. Survives all eviction scenarios. Visible audit trail via `git log`. Fits the existing "watcher → main.py → commit docs/ → push" pattern. No new dependencies, no new secrets. Step 7 push logic already handles concurrent main updates via rebase. | Adds ~one commit per Monday on top of the existing dashboard commit (low noise; existing pipeline already commits weekly). Requires un-gitignoring the ledger file — needs Tony's call on the filename/message-id privacy. Possible rebase conflict if a manual edit lands during the run (rare; `-X theirs` resolves correctly). | ~20      | **Low** |
| 2 | **`actions/cache@v4`** keyed on stable name      | Built-in, no commit noise on main. Standard GitHub pattern.                                                                                      | Cache **7-day eviction** policy — a single skipped Monday + holiday can evict the entire ledger. No human-visible audit trail. Cache restore not 100% deterministic during eviction races. Adds two workflow steps (restore + save) and slight ordering complexity around `continue-on-error`. The watcher already handles cold-start gracefully, so eviction isn't catastrophic — but it silently re-opens the same gap on holidays. | ~15      | Medium  |
| 3 | **Persist to Google Sheets** (`INBOX_LEDGER` tab) | Same data plane as everything else. Human-inspectable in the Sheet. Survives indefinitely.                                                       | Adds `gspread` + service-account dependency to the watcher (currently `urllib` only — deliberately minimal). Tab grows unbounded — needs trim policy. Adds a Sheets round-trip to every watcher run (latency + quota cost). Mixes idempotency state with operational data in the same store. | ~50-80   | Medium  |
| 4 | **External object storage** (S3 / GCS / Azure Blob) | Maximally durable, independent of GitHub.                                                                                                       | New cloud provider, new secret, new client library, new failure mode. Massive overkill for ~300 KB/year of state. Increases auth surface area. Adds opex.                                                                                                                                | ~80-150  | High    |

---

## Recommendation: **Option 1 — git-commit the ledger**

**Rationale (3 sentences):**

The ledger is small, structured, append-only state — git is the right substrate, and the workflow already has the commit/push machinery wired up for `docs/`. Option 2 looks attractive on paper but the 7-day cache eviction reopens the same gap on the first holiday Monday, defeating the point. Options 3 and 4 add dependencies (Sheets client library, cloud provider) that aren't justified at this scale and erode the watcher's deliberate "urllib-only, minimal-blast-radius" design posture.

**One condition for Tony to confirm before implementation:**

The ledger contains attachment **filenames** (e.g. `Karissa_Salon_Weekly_Report.xlsx`) and **Gmail message IDs**. None of this is PHI. Filenames may, however, leak operational cadence (when reports arrive, what they're called). The repo is currently public. If Tony wants stricter privacy, the watcher can write a **hash-only persisted form** (`{hash: processed_at}`) to a committed file while keeping the full audit ledger gitignored — small extra change, fully reversible. **Default recommendation: commit the full ledger as-is** — filenames duplicate info already public in `config/customers/karissa_001.json` (location names) and `docs/*.html`.

---

## Implementation outline (pending approval)

> **Do not start any of this until Tony approves both the option and the privacy-mode choice.**

### Step 1 — Privacy mode decision
- [ ] **Tony confirms:** commit full ledger (default) OR commit hash-only form (privacy mode).
- If hash-only: `save_processed_ledger` writes two files — full ledger to `data/processed_attachments.json` (stays gitignored) and a `{hash: iso_timestamp}` projection to `data/processed_hashes.json` (committed).

### Step 2 — Un-gitignore the persisted file
- `.gitignore:48` — change `data/processed_attachments.json` to `!data/processed_attachments.json` (or, in hash-only mode, add `!data/processed_hashes.json` while leaving line 48 intact).
- Verify other `*.json` rules (lines 1-4) don't shadow the unignore.

### Step 3 — Add a workflow step in `.github/workflows/weekly_pipeline.yml`
Insert between Step 5 (watcher) and Step 5.5 (Tier 2), or between Step 6 (main.py) and Step 7 (push). The latter is simpler — the existing push step then sweeps up both `docs/` commits from main.py AND the ledger commit:

```yaml
- name: Commit inbox ledger
  if: ${{ github.event.inputs.dry_run != 'true' }}
  run: |
    if [ -f data/processed_attachments.json ]; then
      git add data/processed_attachments.json
      git diff --cached --quiet || git commit -m "chore(inbox): update SHA256 dedup ledger [skip ci]"
    else
      echo "No ledger file to commit — watcher may have skipped (missing OAuth secrets)."
    fi
```

Notes:
- `[skip ci]` in the commit message prevents this commit from triggering `deploy.yml` / `static.yml` (which fire on push to main).
- `git diff --cached --quiet` is the idiomatic "only commit if something changed" guard — avoids empty commits on weeks where the watcher found nothing new.
- Runs only on real runs (matches existing Step 7 dry-run guard).
- Placed **before** Step 7 so the existing rebase-and-push logic handles the ledger commit alongside the dashboard commit.

### Step 4 — Do NOT modify `parsers/gmail_attachment_watcher.py`
Per Tony's constraints. The watcher already writes the ledger to disk; the workflow handles persistence. Keeps the watcher CI-agnostic and locally testable.

### Step 5 — Update `.gitignore` comment block
Add a one-line comment near the un-ignored entry explaining why this single JSON is committed.

### Step 6 — Update `CLAUDE.md`
Under "Gmail Attachment Watcher" section, add a sub-section "Ledger persistence" linking to this doc and explaining that the ledger now persists via the workflow's commit step.

### Step 7 — Update `PARSER_SPEC_v1.0.1_ADDENDUM.md` §G item 3
Mark the item resolved with the implementation date and a link to the merged PR.

### Step 8 — Open PR
Single PR with: `.github/workflows/weekly_pipeline.yml`, `.gitignore`, `CLAUDE.md`, `PARSER_SPEC_v1.0.1_ADDENDUM.md`, this decision doc.

---

## Test plan

### Pre-merge (local + dry-run)
- [ ] `python parsers/gmail_attachment_watcher.py` with `dry_run: true` in `inbox_config.json` — confirms watcher still writes manifest + run summary cleanly. Ledger should NOT change in dry-run mode.
- [ ] Flip `dry_run: false`, run watcher locally against a test inbox with one fake attachment. Confirm `data/processed_attachments.json` is created and contains one entry.
- [ ] Re-run watcher with the same attachment still in inbox. Confirm `duplicates_skipped == 1` and ledger is unchanged (same single entry).
- [ ] Run `git status` — confirm ledger appears as a tracked change. Run `git diff --cached data/processed_attachments.json` after `git add` to confirm content.

### CI smoke test (workflow_dispatch with dry_run=false on a non-Monday)
- [ ] Trigger `weekly_pipeline.yml` manually. Confirm the new "Commit inbox ledger" step runs and produces either a "no changes" or "1 file changed" outcome.
- [ ] If a commit was created, confirm it has `[skip ci]` in the message and didn't trigger `deploy.yml` / `static.yml`.
- [ ] Confirm Step 7 successfully pushes the ledger commit + the dashboard commit in a single rebase+push.

### CI second-run idempotency test (same Monday, manual re-trigger within the 2-day window)
- [ ] Trigger workflow_dispatch a second time within 2 hours of the first.
- [ ] Confirm second run reports `duplicates_skipped > 0` in `data/logs/inbox_run_*.json` (uploaded as artifact).
- [ ] Confirm Tier 2 step processes zero new manifest records (manifest entries should have `processing_status: "duplicate_skipped"`).
- [ ] Confirm no new commit was created in the "Commit inbox ledger" step (`git diff --cached --quiet` short-circuits).

### Rollback drill
- [ ] If anything misbehaves: revert the PR. The watcher's cold-start handling (`load_processed_ledger` returns `{}` on missing file) means a missing ledger never crashes anything — worst case is one cycle of pre-fix behavior.

---

## Operational concerns (post-deploy monitoring)

1. **Ledger size growth.** Watch `git ls-files -s data/processed_attachments.json` over time. Alert (e.g., add to drift checker) if file exceeds 5 MB. At expected growth (~300 KB/year) we shouldn't hit this for ~15 years; faster growth would indicate the watcher is mis-deduping.
2. **Empty-commit prevention.** Confirm the `git diff --cached --quiet` guard is firing on weeks with no new attachments — `git log --since='1 month ago' -- data/processed_attachments.json` should match the count of weeks where Elaina actually sent a file.
3. **Rebase conflicts in Step 7.** If a manual edit to the ledger ever lands on `main` between watcher runs (shouldn't happen, but defensive), the existing `git rebase -X theirs origin/main` in Step 7 will resolve in favor of the watcher's commit — correct behavior, since the watcher's ledger is by definition the newest.
4. **Watcher cold-start when OAuth secrets are still missing.** Today the watcher exits with no ledger file. The new commit step's `if [ -f ... ]` guard short-circuits cleanly — no spurious failures.
5. **First-run-after-merge.** The repo currently has no ledger file in git history. First Monday after merge will create the file from scratch (committed), then every subsequent Monday appends. No special migration needed.
6. **Long-term archive trim.** Out of scope for this doc, but flagged: the ledger never trims. After 5+ years of operation, consider a yearly cron that prunes entries older than `archive_retention_days` (90 days). Not urgent — at 300 KB/year, deferred indefinitely is fine.

---

## Appendix — what was explicitly considered and rejected

- **In-process SQLite at `data/ledger.db`** — same persistence gap as `processed_attachments.json`; would just move the problem.
- **Storing ledger inside `data/inbox/` so it rides with the manifest** — ruled out because `data/inbox/` is correctly gitignored (contains real attachments). Co-locating would force the same un-ignore decision plus surrounding files.
- **Letting Tier 2 own the dedup** — wrong layer. Tier 2 reads the manifest the watcher wrote; if the watcher already declared a record `ready`, Tier 2's only sane response is to process it. Dedup must live at the inbox boundary, not downstream.
- **Bumping `search_window_days` down to 1** — fragile; loses Saturday-evening sends to a Monday-morning run. Doesn't solve the root cause anyway (any same-day workflow_dispatch retry still re-processes).
