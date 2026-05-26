"""
trust_layer/truth_mediation_log.py
──────────────────────────────────
On-disk audit trail of reconciliation events. Per FINAL_SPEC v1.0.0 §10
and addendum v1.0.1 §J (Branch 3).

This module is the "Truth Mediation Log" the spec describes. It is the
on-disk half of the hybrid model Tony chose for Q1 in the 2026-05-26
audit chat:

  - In-memory primitive:   `trust_layer/severity.py::CompletenessCheck`
                            (existing — produced by all validators)
  - On-disk durable log:   this module's NDJSON file
                            (new — append-only, one event per line)

Schema per FINAL_SPEC v1.0.0 §10
────────────────────────────────
One JSON object per line in `data/logs/truth_mediation_log.json`:

    {
      "timestamp":              ISO-8601 UTC,
      "location_id":            short ID (e.g. "z001") or display name,
      "week_start":             ISO date (YYYY-MM-DD),
      "rule_applied":           snake_case event type
                                (see RULE_* constants below),
      "field":                  free-form descriptor of what was reconciled,
      "salon_level_value":      authoritative value, or null,
      "stylist_sum_before":     observed / divergent value, or null,
      "drift_amount":           absolute diff, or null,
      "drift_pct":              relative diff (0.0-1.0), or null,
      "hypothesis":             one-line cause guess,
      "action":                 snake_case description of what was done,
      "human_review_required":  bool,
    }

Schema interpretation across event types
────────────────────────────────────────
The spec's example uses `rule_applied = "salon_level_supremacy"` and
fields tuned to stylist-proportional adjustment. Branch 3 extends
`rule_applied` to a small set of values so other reconciliation events
fit the same schema:

  RULE_SALON_LEVEL_SUPREMACY   — stylist sums adjusted to salon totals
                                  (the spec's primary use case)
  RULE_PRODUCT_TOTAL_MISMATCH  — Branch 1 / FINAL_SPEC §6.2
                                  salon_level_value  = header value (canonical)
                                  stylist_sum_before = line-item sum (observed)
  RULE_PARTIAL_WEEK_DETECTED   — Branch 2 / FINAL_SPEC §6.1
                                  salon_level_value  = null (no reconciliation)
                                  stylist_sum_before = null
                                  hypothesis         = unclosed-day dates
  RULE_CROSS_FILE_RECONCILED   — Excel ↔ PDF reconciliation events
                                  (trust_layer/completeness_validator.py)

Free-form strings are also accepted — the constants exist as the canonical
set but the function does not enforce them.

File location + format
──────────────────────
Default: `data/logs/truth_mediation_log.json`. Despite the .json extension,
the file is NDJSON (newline-delimited JSON) — one event per line, not a
JSON array. This lets us append safely without parsing the whole file.

The file is gitignored via `data/logs/` in .gitignore (runtime artifact,
not source). Override the path via the `TRUTH_MEDIATION_LOG_PATH` env
var (used in tests).

Never raises
────────────
Both `write_event` and `read_events` swallow exceptions and return a
defensive fallback (False / []). A broken log path must NEVER crash
the pipeline. Callers can choose to log a warning on False, but should
not propagate.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


# Default log location, repo-root-relative. Override with
# TRUTH_MEDIATION_LOG_PATH env var (e.g. for tests).
_DEFAULT_LOG_PATH = (
    Path(__file__).resolve().parent.parent / "data" / "logs" / "truth_mediation_log.json"
)


# Canonical rule_applied values. Not enforced — strings accepted — but
# provided so callers can avoid typos and grep for usages.
RULE_SALON_LEVEL_SUPREMACY  = "salon_level_supremacy"
RULE_PRODUCT_TOTAL_MISMATCH = "product_total_mismatch"
RULE_PARTIAL_WEEK_DETECTED  = "partial_week_detected"
RULE_CROSS_FILE_RECONCILED  = "cross_file_reconciled"


def _resolve_log_path() -> Path:
    """Return the truth-mediation log path. Honors TRUTH_MEDIATION_LOG_PATH env var."""
    override = os.environ.get("TRUTH_MEDIATION_LOG_PATH")
    if override:
        return Path(override)
    return _DEFAULT_LOG_PATH


def write_event(
    rule_applied: str,
    location_id: str,
    week_start: str,
    field: str,
    salon_level_value: Optional[float] = None,
    stylist_sum_before: Optional[float] = None,
    drift_amount: Optional[float] = None,
    drift_pct: Optional[float] = None,
    hypothesis: str = "",
    action: str = "logged",
    human_review_required: bool = False,
    log_path: Optional[Path] = None,
) -> bool:
    """
    Append one reconciliation event to the truth-mediation log.

    Schema per FINAL_SPEC v1.0.0 §10. Returns True on success, False on
    any error. Never raises — a broken log path must not crash the
    pipeline. The caller decides what to do on False (usually: log a
    warning and continue).

    Args:
        rule_applied:           snake_case event type. Prefer the RULE_*
                                constants but free-form strings work.
        location_id:            customer-config short ID (preferred) or
                                display name (acceptable fallback).
        week_start:             ISO date string (YYYY-MM-DD).
        field:                  free-form description of the field being
                                reconciled (used for human-readable filtering).
        salon_level_value:      authoritative value, or None.
        stylist_sum_before:     observed / divergent value, or None.
        drift_amount:           absolute difference, or None.
        drift_pct:              relative difference (0.0-1.0 = 0%-100%), or None.
        hypothesis:             one-line cause guess (e.g. "Likely refund timing").
        action:                 snake_case description of what was done.
        human_review_required:  True if a human needs to act on this entry.
        log_path:               Override target file (default: env-var or
                                data/logs/truth_mediation_log.json).
    """
    target = log_path or _resolve_log_path()

    event: Dict[str, Any] = {
        "timestamp":             dt.datetime.utcnow().isoformat() + "Z",
        "location_id":           location_id,
        "week_start":            week_start,
        "rule_applied":          rule_applied,
        "field":                 field,
        "salon_level_value":     salon_level_value,
        "stylist_sum_before":    stylist_sum_before,
        "drift_amount":          drift_amount,
        "drift_pct":             drift_pct,
        "hypothesis":            hypothesis,
        "action":                action,
        "human_review_required": human_review_required,
    }

    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(event, ensure_ascii=False))
            fh.write("\n")
            fh.flush()
        return True
    except Exception as exc:
        logger.error(
            "truth_mediation_log.write_event failed (rule=%s, loc=%s): %s",
            rule_applied, location_id, exc,
        )
        return False


def read_events(
    log_path: Optional[Path] = None,
    rule_filter: Optional[str] = None,
    location_filter: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Read events from the log. Returns a list of event dicts.

    Args:
        log_path:        Override source file.
        rule_filter:     If set, only return events whose rule_applied matches.
        location_filter: If set, only return events whose location_id matches.

    Returns an empty list if the file is missing or unreadable. Never raises.
    Lines that fail to parse as JSON are skipped with a logged warning.
    """
    target = log_path or _resolve_log_path()
    if not target.exists():
        return []

    events: List[Dict[str, Any]] = []
    try:
        with target.open("r", encoding="utf-8") as fh:
            for lineno, line in enumerate(fh, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    event = json.loads(line)
                except json.JSONDecodeError as exc:
                    logger.warning(
                        "truth_mediation_log: skipping malformed JSON at line %d: %s",
                        lineno, exc,
                    )
                    continue

                if rule_filter and event.get("rule_applied") != rule_filter:
                    continue
                if location_filter and event.get("location_id") != location_filter:
                    continue
                events.append(event)
    except Exception as exc:
        logger.error("truth_mediation_log.read_events failed: %s", exc)
        return []

    return events


__all__ = [
    "RULE_SALON_LEVEL_SUPREMACY",
    "RULE_PRODUCT_TOTAL_MISMATCH",
    "RULE_PARTIAL_WEEK_DETECTED",
    "RULE_CROSS_FILE_RECONCILED",
    "write_event",
    "read_events",
]
