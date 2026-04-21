# KPI Platform — Google Sheets v2 (Parallel Build)

**Status:** Built, syntax-verified, dry-run smoke-tested. Not yet initialized against a real Sheet. Not yet wired into production. Shadow-write hook exists but is **OFF by default**.

**What this doc is:** Everything a future developer (or future Tony) needs to:
1. Understand what v2 is and why we built it.
2. Initialize the new parallel Sheets.
3. Verify they're healthy.
4. Turn on dual-write in production.
5. Cut over from legacy to v2.
6. Roll back if anything goes sideways.

---

## Why v2 exists

The legacy Sheets layer (`core/sheets_writer.py` + `config/sheets_schema.py`) has five specific problems:

1. **Magic-row hack** — Coach briefs live at `ALERTS!A100` and `ALERTS!A101`. If the alert log ever grows past 99 rows, we corrupt the briefs.
2. **No schema versioning** — If someone renames a column in the UI, the pipeline silently writes to the wrong place.
3. **No composite-key idempotency** — `append_to_historical()` dedupes by `week_ending` only. If the first run writes 11 rows instead of 12 (one location missed), we can't partially patch.
4. **No audit trail** — We can't prove what was written or when.
5. **Legacy-keyed dicts all the way through** — `loc_name`/`week_ending`/`guests` are fine inside the pipeline, but they make a future Postgres migration harder than it needs to be.

v2 addresses all five. It is a **parallel build** — the legacy pipeline keeps running unchanged. v2 writes happen as a **shadow write** after legacy succeeds. Once we've verified v2 for a few weeks, we flip the cutover.

---

## Architecture

```
┌───────────────────────────────┐        Legacy (production, unchanged)
│  main.py → sheets_writer.py   │◀───────┐
│  writes to legacy Sheet       │        │
│  (1JY6L7H1Pb2JFmNoz2XNkvG0…)  │        │
└───────────┬───────────────────┘        │
            │                            │
            │ DUAL_WRITE_V2=true         │ core/sheets_writer.py
            ▼                            │ write_all() → _dual_write_v2()
┌──────────────────────────────────────┐ │
│  core/schema_mapper.py               │ │  translates legacy dict shape
│  (legacy dicts → v2 dicts)           │ │  to v2 schema before writing
└───────────┬──────────────────────────┘ │
            ▼
┌──────────────────────────────────────┐
│  core/google_sheets_store.py         │  implements DataStore interface
│  (10 + 6 safety layers)              │  retry, idempotency, schema version,
│                                      │  safe overwrite, audit log, dry-run,
│                                      │  write_anomaly, bounded key reads
└───────────┬──────────────────────────┘
            ▼
┌──────────────────────────────────────┐
│  NEW parallel master Sheet (v2)      │  8 tabs:
│  (Sheet ID: set via V2_MASTER_       │    LOCATIONS_CURRENT, LOCATIONS_DATA,
│   SHEET_ID env var)                  │    STYLISTS_CURRENT,  STYLISTS_DATA,
│                                      │    GOALS, ALERTS,
│                                      │    COACH_BRIEFS, AUDIT_LOG
└──────────────────────────────────────┘
```

**Key principle:** the legacy write is the source of truth until cutover. Any v2 failure is logged at WARNING, never raised. Production cannot break because of v2.

---

## Files in this build

| Path | Purpose |
|------|---------|
| `config/sheets_schema_v2.py` | Single source of truth for v2 schema — tab definitions, columns, types, formatting, composite-key helpers, A1 helpers. Version `2.0.0`. |
| `core/data_store.py` | Abstract `DataStore` interface. v2 backend (and future Postgres/BigQuery backends) implement this. |
| `core/google_sheets_store.py` | `GoogleSheetsStore(DataStore)` — the production v2 backend with all 10 + 6 safety layers. |
| `core/schema_mapper.py` | Translates `data_processor` output (legacy dicts) into v2 schema dicts. Handles the percent convention (`18.5` → `0.185`). |
| `core/sheets_writer.py` | **Modified.** Legacy `write_all()` now calls `_dual_write_v2()` at the end. Off by default; fails silently. |
| `scripts/initialize_sheets_v2.py` | One-time initializer — creates tabs, stamps schema version note, applies formatting. Idempotent. |
| `scripts/verify_sheets_v2.py` | Health check — auth, tabs present, headers match, schema note present, store instantiates, all 6 write methods dry-run cleanly. |
| `README_SHEETS_V2.md` | This file. |

Nothing under `main.py`, `data_processor.py`, or `ai_*.py` was touched.

---

## The 10 safety layers (Tony's spec)

1. **Retry with exponential backoff** on 429/500/502/503/504.
2. **Composite-key idempotency** — `LOCATION|<loc>|<period_end>` and `STYLIST|<loc>|<name>|<period_end>`. Row-type discriminator prevents collisions.
3. **Schema version HARD-FAIL** at init if `AUDIT_LOG!A1` doesn't carry the correct note.
4. **Safe-write mode** — backup → clear → write → on failure, restore from backup; if restore also fails, **CRITICAL alert via `core.alerter`**.
5. **Input validation** per column (type coercion, warns on missing keys).
6. **Atomic batch_update** — data + formatting in the same API call where possible.
7. **AUDIT_LOG row** for every write, with SHA256 `batch_hash`.
8. **Dry-run mode** matches existing `DRY_RUN=true` convention.
9. **`write_anomaly()`** method for drift_checker / ai_cards auto-write.
10. **Bounded `_get_existing_keys()`** — reads only up to `ws.row_count` to avoid scanning empty rows.

### The 6 bonus features (confirmed)

- **A.** SHA256 batch_hash written into AUDIT_LOG for integrity verification.
- **B.** Row-type discriminator in composite keys (no STYLIST-vs-LOCATION collisions).
- **C.** Backup-restore failure escalates to CRITICAL via `core.alerter`.
- **D.** Bounded existing-key reads (see #10).
- **E.** Dry-run mode shares code paths with real writes.
- **F.** Plain-text date columns (`@` format) to prevent locale drift.

---

## Initialization (one-time)

### Step 1 — Create the Sheets

In Google Drive, create a new empty Sheet (title something like `KPI Platform — v2 (parallel)`). Grab its ID from the URL:

```
https://docs.google.com/spreadsheets/d/<THIS_IS_THE_ID>/edit
```

Optional: create a second Sheet for the human-readable coach cards archive.

**Share both Sheets with the service account email** (the `client_email` field inside your `GOOGLE_SERVICE_ACCOUNT_JSON`) as an Editor. The pipeline cannot touch Sheets it wasn't invited to.

### Step 2 — Run the initializer

From the repo root, with `GOOGLE_SERVICE_ACCOUNT_JSON` set in your environment:

```bash
# Dry run first — shows what it would do, writes nothing.
python scripts/initialize_sheets_v2.py \
    --master-sheet-id <master_id> \
    --coach-cards-sheet-id <coach_id> \
    --dry-run

# For real:
python scripts/initialize_sheets_v2.py \
    --master-sheet-id <master_id> \
    --coach-cards-sheet-id <coach_id>
```

The initializer:
- Creates every missing tab (`LOCATIONS_CURRENT`, `LOCATIONS_DATA`, `STYLISTS_CURRENT`, `STYLISTS_DATA`, `GOALS`, `ALERTS`, `COACH_BRIEFS`, `AUDIT_LOG`).
- Writes the header row (frozen) with the correct column names.
- Applies per-column formatting (date columns forced to plain text, currencies, percentages).
- Sets column widths.
- Stamps `Schema Version: 2.0.0` as a cell note on A1 of every tab.

**Idempotent.** Running it a second time updates headers/formatting in place but preserves rows.

**Safety guardrail:** Refuses to run against the legacy production Sheet ID (`1JY6L7H1Pb2JFmNoz2XNkvG0ogrYgagLVDwH01vuWT28`) unless you pass `--allow-legacy-sheet-id`. Don't pass that flag.

### Step 3 — Verify

```bash
python scripts/verify_sheets_v2.py \
    --master-sheet-id <master_id> \
    --coach-cards-sheet-id <coach_id>
```

Expected output (all 10 checks pass):

```
✓ auth — auth OK
✓ master: reachable — reachable: 'KPI Platform — v2 (parallel)'
✓ master: all tabs present — all 8 tabs present
✓ master: headers match schema — headers match on all 8 tabs
✓ master: schema version note on AUDIT_LOG!A1 — schema note present: '2.0.0'
✓ coach: reachable — reachable: '...'
✓ coach: all tabs present — all 1 tabs present
✓ coach: headers match schema — headers match on all 1 tabs
✓ GoogleSheetsStore.health_check() — health OK: reachable + schema version OK
✓ GoogleSheetsStore dry-run writes (6 methods) — dry-run writes for all 6 store methods succeeded
=== 10/10 checks passed ===
```

If any check fails, **do not proceed to the cutover**. Fix the underlying issue (rerun init, re-share sheet, check auth) and re-verify.

---

## GitHub Secrets to add before turning on dual-write

Add these to the `KPI-Platform-Dash` repo's Actions secrets:

| Secret | Value |
|--------|-------|
| `V2_MASTER_SHEET_ID` | Sheet ID from Step 1 (master). |
| `V2_COACH_SHEET_ID` | Sheet ID from Step 1 (coach cards). Optional. |
| `DUAL_WRITE_V2` | `true` to enable shadow write. Leave unset or `false` to disable. |
| `V2_DRY_RUN` | Optional. `true` means v2 writes are simulated only (useful for a first real-pipeline run). Defaults to following the legacy `DRY_RUN`. |

`GOOGLE_SERVICE_ACCOUNT_JSON` is already configured and reused — no changes needed.

Update `.github/workflows/weekly_pipeline.yml` to thread those env vars into the `python main.py` step:

```yaml
env:
  DUAL_WRITE_V2:        ${{ secrets.DUAL_WRITE_V2 }}
  V2_MASTER_SHEET_ID:   ${{ secrets.V2_MASTER_SHEET_ID }}
  V2_COACH_SHEET_ID:    ${{ secrets.V2_COACH_SHEET_ID }}
  V2_DRY_RUN:           ${{ secrets.V2_DRY_RUN }}
```

(Add only; don't remove any existing env vars.)

---

## Cutover plan

### Phase A — Shadow write (2–4 weeks)

1. Set `V2_MASTER_SHEET_ID` + `V2_COACH_SHEET_ID` in GitHub Secrets.
2. Set `DUAL_WRITE_V2=true`.
3. Leave `V2_DRY_RUN` unset (so v2 does real writes alongside legacy).
4. Run the pipeline one Monday.
5. Open the new Sheet. Confirm:
   - Every tab has the expected rows for the week.
   - `AUDIT_LOG` has one row per operation with `status=SUCCESS` and a `batch_hash`.
   - `ALERTS` has any drift/AI flag entries.
   - `COACH_BRIEFS` has one row per active manager (jess + jenn) with the brief JSON.
6. Compare against the legacy Sheet. Counts should match (12 locations, N stylists).
7. Repeat for 2–4 Mondays. Any shadow-write failure appears in the workflow log as `WARNING DUAL_WRITE_V2: shadow write failed...` — investigate but don't panic (legacy is still source of truth).

### Phase B — Dashboard readers migrate (1–2 weeks)

Once v2 has been correct for 2+ weeks:

1. Point `core/data_source.py` at the v2 Sheet for reads. This is the biggest code change — the reader needs to translate v2 column names back to what `data_processor.py` expects (or, better, update `data_processor.py` to consume v2 names directly).
2. Keep legacy writes ON during this phase — rollback path is still available.
3. Run one pipeline. If dashboards render correctly, move to Phase C.

### Phase C — Full cutover

1. Swap `sheets_writer.py.write_all()` so v2 writes happen FIRST and legacy becomes the shadow.
2. Run one Monday, verify.
3. Remove the legacy write calls from `write_all()`. Delete `config/sheets_schema.py` and prune the legacy column lists out of `sheets_writer.py` — or, safer, keep them as dead code behind a `LEGACY_WRITE=false` env flag for one more month.
4. Update `CLAUDE.md` with the new truth.
5. Celebrate.

---

## Rollback plan

**At any point during Phase A or B:**

1. Set `DUAL_WRITE_V2=false` in GitHub Secrets (or remove the secret).
2. The next pipeline run is a pure legacy run. Zero code changes needed.

**During Phase C (after swap):**

1. Restore `sheets_writer.py` from the git history (`git revert` the cutover commit).
2. Set `DUAL_WRITE_V2=false`.
3. Pipeline runs against legacy again.

The legacy Sheet (`1JY6L7H1Pb2JFmNoz2XNkvG0ogrYgagLVDwH01vuWT28`) is untouched by any of this work. Rollback is always free.

---

## Monitoring

Every write operation appends a row to `AUDIT_LOG` with:

- `timestamp` (UTC ISO-8601)
- `operation` (e.g. `write_locations_current`, `append_stylists_historical`)
- `target_tab`
- `row_count`, `duplicates_skipped`
- `status` (`SUCCESS` / `PARTIAL` / `FAILURE` / `DRY_RUN`)
- `batch_hash` (SHA256 of the row grid — can be used to verify what landed matches what was intended)
- `schema_version`
- `error_message` (empty on success)
- `caller` (Python module.function that initiated the call)

**Health checks:**

- Sort `AUDIT_LOG` by `timestamp` desc. Any `FAILURE` rows? Read `error_message`.
- Any `DRY_RUN` rows on a day you thought wasn't a dry run? `V2_DRY_RUN` is set somewhere it shouldn't be.
- `schema_version` column ever shows a value other than `2.0.0`? Someone deployed a newer schema without running the initializer. Either roll back the deploy or re-run init.

`core/alerter.py` fires a CRITICAL alert if a safe-write's backup AND restore both fail — which means the target tab may be partially missing data. That's the only code path that escalates v2 failures beyond a log line.

---

## Things not to do

1. **Don't modify the v2 Sheets by hand in the UI.** Every tab is rebuilt from schema definitions; manual edits get overwritten on the next write.
2. **Don't delete the schema version note on A1** of any tab. If it disappears, `GoogleSheetsStore.__init__()` hard-fails at `SchemaMismatchError`.
3. **Don't rename tabs or columns** in the v2 Sheet. Renames break the reader. If you need a change, bump `SCHEMA_VERSION` in `config/sheets_schema_v2.py` and rerun the initializer.
4. **Don't touch the AUDIT_LOG** tab manually. It's append-only and the only audit trail we have.
5. **Don't run `initialize_sheets_v2.py` against the legacy production Sheet** (`1JY6L7H1Pb2JFmNoz2XNkvG0ogrYgagLVDwH01vuWT28`). The script refuses unless you pass `--allow-legacy-sheet-id`; don't pass it.
6. **Don't remove the `except Exception` around `_dual_write_v2()` in `sheets_writer.py`.** Production must never break because of v2.

---

## Testing checklist before turning on `DUAL_WRITE_V2=true`

- [ ] Sheets created and shared with the service account.
- [ ] `python scripts/initialize_sheets_v2.py --master-sheet-id ... --coach-cards-sheet-id ...` ran cleanly.
- [ ] `python scripts/verify_sheets_v2.py --master-sheet-id ... --coach-cards-sheet-id ...` shows 10/10 checks pass.
- [ ] GitHub Secrets configured: `V2_MASTER_SHEET_ID`, `V2_COACH_SHEET_ID` (optional), `DUAL_WRITE_V2=true`.
- [ ] Workflow env vars threading those secrets into `main.py`.
- [ ] First shadow run is a weekday dispatch (not a Monday live run) so you can eyeball it without pressure.

---

## Open threads (future work)

1. **Bump `main.py` + `data_source.py` to consume v2 directly** (Phase B above). This is the real cutover.
2. **Stylist reader parity** — `data_source.py` currently reads the legacy `STYLISTS_DATA` tab with its `tenure_yrs`/`cur_pph` keys. The v2 reader needs to map `tenure_years`/`pph` back or (better) update `data_processor.py` to consume v2 names end-to-end.
3. **GOALS tab population** — `GOALS` is defined in the schema but nothing writes to it yet. When goals enter the pipeline (Phase 2, Zenoti API feeds), add `write_goals()` on `DataStore`.
4. **Postgres / BigQuery backend** — the `DataStore` abstract class is the migration path. Implementing `PostgresStore(DataStore)` with the same method surface means `main.py` doesn't change at all; only the orchestrator wiring picks a different backend. That's the whole point of the abstract layer.
5. **COACH_BRIEFS REPLACE semantics** — `write_coach_briefs()` currently reads keys, deletes matching rows from the bottom, then appends. That's correct but could be tightened into a single batch_update with `rowIndex` lookups. Fine for N=2 managers; revisit if manager count grows.

---

## Contacts

- **Tony** (owner): `tonester60@hotmail.com`
- **Karissa** (customer): production Sheet lives in her Drive.
- **Elaina** (data entry): submits POS exports to `karissaperformanceintelligence@gmail.com`.

When the cutover happens, update `CLAUDE.md` so future sessions know v2 is the source of truth.
