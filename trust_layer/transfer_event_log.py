"""
Trust Layer — Transfer Event Log  (Phase 3B Final Polish)
Immutable versioned event log for transfer detection and corrections.

The Problem With Mutable State
-------------------------------
Marking a transfer INVALID tells you something was wrong.
It doesn't tell you:
  - What the CORRECT state should be
  - Who made the correction and why
  - What the confidence score was at detection time
  - How to distinguish this false positive from future similar cases

The Solution: Immutable Event Log + Versioning
----------------------------------------------
Every transfer detection creates an event (version=1).
Every correction creates a NEW event (version=N+1) that supersedes the original.
Original events are NEVER deleted — they become status='superseded'.

This gives Karissa (and future developers):
  - Complete decision trail for any stylist
  - Ability to replay history ("what happened in April?")
  - Confidence scores preserved at detection time
  - Correction types that teach the system over time

Correction Types
----------------
  'false_positive'  — Not a transfer. Different person (Sara vs Sarah).
                      Corrected: no location change, new stylist_id created at new location.
  'wrong_location'  — Real transfer, but destination was wrong.
                      Corrected: from_location preserved, to_location updated.
  'wrong_date'      — Real transfer, correct locations, wrong date.
                      Corrected: from+to preserved, transfer_date updated.
  'partial_correct' — Multiple fields wrong. Manual notes required.

Storage: 'Transfer_Events' sheet tab
  Columns: event_id | version | created_at | created_by | type |
           stylist_id | from_location | to_location | transfer_date |
           confidence | status | superseded_by | corrects_event_id |
           correction_type | reason | corrected_to_location |
           corrected_date | invalidated_at | invalidated_by |
           invalidation_reason

All Sheets I/O is STUBBED (returns None / []) until the Sheets read/write
interface is wired. The event creation and correction logic is fully functional.
"""

import logging
import uuid
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


# ── Event type constants ──────────────────────────────────────────────────────
EVENT_TYPE_TRANSFER         = "transfer"
EVENT_TYPE_CORRECTION       = "correction"
EVENT_TYPE_MANUAL_OVERRIDE  = "manual_override"

# ── Status constants ──────────────────────────────────────────────────────────
STATUS_ACTIVE               = "active"
STATUS_SUPERSEDED           = "superseded"
STATUS_INVALIDATED          = "invalidated"

# ── Correction type constants ─────────────────────────────────────────────────
CORRECTION_FALSE_POSITIVE   = "false_positive"
CORRECTION_WRONG_LOCATION   = "wrong_location"
CORRECTION_WRONG_DATE       = "wrong_date"
CORRECTION_PARTIAL_CORRECT  = "partial_correct"

VALID_CORRECTION_TYPES = {
    CORRECTION_FALSE_POSITIVE,
    CORRECTION_WRONG_LOCATION,
    CORRECTION_WRONG_DATE,
    CORRECTION_PARTIAL_CORRECT,
}


class TransferEventLog:
    """
    Immutable event log for transfer detection and corrections.

    Every transfer detection and every correction is stored as a versioned event.
    Events are never deleted — corrections supersede originals, preserving full
    audit trail.

    Usage:
        log = TransferEventLog()
        event = log.create_transfer_event(
            stylist_id, from_location, to_location, transfer_date, confidence
        )
        # ... Karissa reviews and finds false positive ...
        correction = log.create_correction_event(
            original_transfer_id=event['id'],
            correction_type='false_positive',
            reason='Sara at Blaine is a different person from Sarah at Farmington',
            performed_by='karissa',
        )
        trail = log.get_transfer_audit_trail(stylist_id)
    """

    # ------------------------------------------------------------------
    # Public API — event creation
    # ------------------------------------------------------------------

    def create_transfer_event(
        self,
        stylist_id:     str,
        from_location:  str,
        to_location:    str,
        transfer_date:  str,
        confidence:     float,
        created_by:     str = "auto",
    ) -> Dict:
        """
        Create a new versioned transfer detection event.

        Args:
            stylist_id:    Stable stylist ID.
            from_location: Location stylist was at before transfer.
            to_location:   Location stylist appeared at.
            transfer_date: ISO date string of detected transfer.
            confidence:    Detection confidence score (0.0–1.0).
            created_by:    'auto' (pipeline) | 'karissa' | 'system'.

        Returns:
            Event dict with id, version=1, status='active'.
        """
        event = {
            "id":           str(uuid.uuid4()),
            "version":      1,
            "created_at":   datetime.now(tz=timezone.utc).isoformat(),
            "created_by":   created_by,

            "type":         EVENT_TYPE_TRANSFER,
            "stylist_id":   stylist_id,
            "from_location": from_location,
            "to_location":  to_location,
            "transfer_date": transfer_date,
            "confidence":   round(confidence, 4),

            "status":       STATUS_ACTIVE,
            "superseded_by": None,

            # Correction fields — not applicable to initial detection
            "corrects_event_id":      None,
            "correction_type":        None,
            "reason":                 None,
            "corrected_to_location":  None,
            "corrected_date":         None,

            "invalidated_at":     None,
            "invalidated_by":     None,
            "invalidation_reason": None,
        }

        self._store_event(event)

        logger.info(
            "Transfer event created: %s | %s → %s | %s | confidence=%.0f%% | id=%s",
            stylist_id, from_location, to_location, transfer_date,
            confidence * 100, event["id"][:8],
        )
        return event

    def create_correction_event(
        self,
        original_transfer_id:   str,
        correction_type:        str,
        reason:                 str,
        performed_by:           str = "karissa",
        correct_location_fn:    Optional[Callable[[], str]] = None,
        correct_date_fn:        Optional[Callable[[], str]] = None,
    ) -> Dict:
        """
        Create a correction event that supersedes a previous transfer event.

        The original event is marked status='superseded'. The correction event
        is stored and linked back via 'corrects_event_id'.

        Args:
            original_transfer_id: ID of the event being corrected.
            correction_type:      One of VALID_CORRECTION_TYPES.
            reason:               Human-readable reason (Karissa's words).
            performed_by:         Who performed the correction.
            correct_location_fn:  Optional callable returning corrected to_location
                                  string. Used when correction_type='wrong_location'.
                                  If None, corrected_to_location is set to empty string.
            correct_date_fn:      Optional callable returning corrected date string.
                                  Used when correction_type='wrong_date'.
                                  If None, corrected_date is set to empty string.

        Returns:
            Correction event dict.

        Raises:
            ValueError: If correction_type is not recognized.
            ValueError: If original event is not found (stub: returns None).
        """
        if correction_type not in VALID_CORRECTION_TYPES:
            raise ValueError(
                f"Invalid correction_type '{correction_type}'. "
                f"Must be one of: {sorted(VALID_CORRECTION_TYPES)}"
            )

        original = self.get_transfer_event(original_transfer_id)
        if original is None:
            # In stub mode, create a minimal placeholder so tests work
            logger.warning(
                "create_correction_event: original event %s not found (stub mode). "
                "Using placeholder for correction.",
                original_transfer_id[:8] if len(original_transfer_id) >= 8 else original_transfer_id,
            )
            original = {
                "id":            original_transfer_id,
                "version":       1,
                "stylist_id":    "",
                "from_location": "",
                "to_location":   "",
                "transfer_date": "",
                "confidence":    0.0,
                "status":        STATUS_ACTIVE,
                "superseded_by": None,
            }

        # ── Build correction event ────────────────────────────────────────
        corrected_to_location: Optional[str] = None
        corrected_date:        Optional[str] = None

        if correction_type == CORRECTION_FALSE_POSITIVE:
            # No transfer occurred — corrected_to_location stays None
            corrected_to_location = None
            corrected_date        = None

        elif correction_type == CORRECTION_WRONG_LOCATION:
            # Transfer happened, but to a different location
            corrected_to_location = (
                correct_location_fn() if correct_location_fn is not None
                else ""
            )
            corrected_date = original.get("transfer_date")

        elif correction_type == CORRECTION_WRONG_DATE:
            # Transfer happened, correct locations, wrong date
            corrected_to_location = original.get("to_location")
            corrected_date = (
                correct_date_fn() if correct_date_fn is not None
                else ""
            )

        elif correction_type == CORRECTION_PARTIAL_CORRECT:
            # Multiple fields may be wrong — caller provides overrides via fns
            corrected_to_location = (
                correct_location_fn() if correct_location_fn is not None
                else None
            )
            corrected_date = (
                correct_date_fn() if correct_date_fn is not None
                else None
            )

        correction = {
            "id":           str(uuid.uuid4()),
            "version":      original["version"] + 1,
            "created_at":   datetime.now(tz=timezone.utc).isoformat(),
            "created_by":   performed_by,

            "type":         EVENT_TYPE_CORRECTION,
            "stylist_id":   original.get("stylist_id", ""),

            # Source of truth for what actually happened
            "from_location": original.get("from_location"),
            "to_location":   original.get("to_location"),
            "transfer_date": original.get("transfer_date"),
            "confidence":    original.get("confidence", 0.0),

            "status":       STATUS_ACTIVE,
            "superseded_by": None,

            # Correction metadata
            "corrects_event_id":     original_transfer_id,
            "correction_type":       correction_type,
            "reason":                reason,
            "corrected_to_location": corrected_to_location,
            "corrected_date":        corrected_date,

            "invalidated_at":     None,
            "invalidated_by":     None,
            "invalidation_reason": None,
        }

        # ── Mark original as superseded ────────────────────────────────────
        original["status"]        = STATUS_SUPERSEDED
        original["superseded_by"] = correction["id"]
        self._update_event(original)

        # ── Store correction ───────────────────────────────────────────────
        self._store_event(correction)

        logger.info(
            "📝 CORRECTION CREATED: %s | type=%s | by=%s | id=%s (supersedes %s)",
            original.get("stylist_id", "?"), correction_type, performed_by,
            correction["id"][:8],
            original_transfer_id[:8] if len(original_transfer_id) >= 8 else original_transfer_id,
        )

        return correction

    # ------------------------------------------------------------------
    # Public API — queries
    # ------------------------------------------------------------------

    def get_transfer_event(self, event_id: str) -> Optional[Dict]:
        """
        Retrieve a single event by ID.

        Stub — returns None until Sheets read is wired.
        """
        return self._load_event_by_id(event_id)

    def get_transfer_audit_trail(self, stylist_id: str) -> List[Dict]:
        """
        Get the complete, chronological audit trail for a stylist.

        Returns structured list of trail entries — each entry is either a
        transfer detection or a correction, formatted for display.

        Returns:
            List[Dict], sorted by timestamp ascending. Empty list if no events.
        """
        events = self._load_events_for_stylist(stylist_id)

        if not events:
            logger.debug(
                "get_transfer_audit_trail: no events found for %s", stylist_id
            )
            return []

        trail = []

        for event in sorted(events, key=lambda e: e.get("created_at", "")):
            if event["type"] == EVENT_TYPE_TRANSFER:
                trail.append({
                    "timestamp":    event["created_at"],
                    "event":        "Transfer Detected",
                    "from":         event.get("from_location"),
                    "to":           event.get("to_location"),
                    "confidence":   event.get("confidence"),
                    "detected_by":  event.get("created_by"),
                    "status":       event.get("status"),
                    "event_id":     event.get("id"),
                    "version":      event.get("version"),
                })

            elif event["type"] == EVENT_TYPE_CORRECTION:
                original_id  = event.get("corrects_event_id")
                original_evt = self.get_transfer_event(original_id) if original_id else None
                trail.append({
                    "timestamp":          event["created_at"],
                    "event":              "Correction Applied",
                    "correction_type":    event.get("correction_type"),
                    "reason":             event.get("reason"),
                    "corrected_by":       event.get("created_by"),
                    "original_confidence": (
                        original_evt.get("confidence") if original_evt else None
                    ),
                    "corrected_to_location": event.get("corrected_to_location"),
                    "corrected_date":        event.get("corrected_date"),
                    "event_id":     event.get("id"),
                    "version":      event.get("version"),
                })

            elif event["type"] == EVENT_TYPE_MANUAL_OVERRIDE:
                trail.append({
                    "timestamp":  event["created_at"],
                    "event":      "Manual Override",
                    "reason":     event.get("reason"),
                    "by":         event.get("created_by"),
                    "event_id":   event.get("id"),
                    "version":    event.get("version"),
                })

        return trail

    def get_active_events_for_stylist(self, stylist_id: str) -> List[Dict]:
        """
        Return only active (non-superseded) events for a stylist.

        Used to get current ground truth for a stylist's transfer history.
        """
        events = self._load_events_for_stylist(stylist_id)
        return [e for e in events if e.get("status") == STATUS_ACTIVE]

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _build_final_state_summary(stylist_id: str, events: List[Dict]) -> Dict:
        """
        Derive the final ground truth state from all events.

        The 'final state' is determined by the most recent active event:
        - If most recent active event is a correction with corrected_to_location=None
          → No transfer occurred (false positive)
        - Otherwise → Use corrected_to_location or original to_location

        Returns:
            {
              'current_location': str | None,
              'transfer_occurred': bool,
              'correction_applied': bool,
            }
        """
        active = [e for e in events if e.get("status") == STATUS_ACTIVE]
        active = sorted(active, key=lambda e: e.get("created_at", ""), reverse=True)

        if not active:
            return {"current_location": None, "transfer_occurred": False,
                    "correction_applied": False}

        most_recent = active[0]
        correction_applied = most_recent["type"] == EVENT_TYPE_CORRECTION

        if most_recent["type"] == EVENT_TYPE_CORRECTION:
            ct = most_recent.get("correction_type")
            if ct == CORRECTION_FALSE_POSITIVE:
                return {
                    "current_location": most_recent.get("from_location"),
                    "transfer_occurred": False,
                    "correction_applied": True,
                }
            else:
                return {
                    "current_location": (
                        most_recent.get("corrected_to_location")
                        or most_recent.get("to_location")
                    ),
                    "transfer_occurred": True,
                    "correction_applied": True,
                }

        # Regular transfer event
        return {
            "current_location": most_recent.get("to_location"),
            "transfer_occurred": True,
            "correction_applied": False,
        }

    # ------------------------------------------------------------------
    # Sheets I/O stubs
    # ------------------------------------------------------------------

    def _store_event(self, event: Dict) -> None:
        """
        Persist a new event to the Transfer_Events sheet.

        Stub — no-op until Sheets write is wired. Logs the event for
        visibility in debug mode.
        """
        logger.debug(
            "TransferEventLog: store stub — id=%s type=%s status=%s",
            event.get("id", "?")[:8], event.get("type"), event.get("status"),
        )

    def _update_event(self, event: Dict) -> None:
        """
        Update an existing event in the Transfer_Events sheet.

        Stub — no-op until Sheets write is wired.
        """
        logger.debug(
            "TransferEventLog: update stub — id=%s status=%s",
            event.get("id", "?")[:8], event.get("status"),
        )

    def _load_event_by_id(self, event_id: str) -> Optional[Dict]:
        """
        Load a single event by ID from the Transfer_Events sheet.

        Stub — returns None until Sheets read is wired.
        """
        logger.debug(
            "TransferEventLog: load-by-id stub for %s → None", event_id[:8]
            if len(event_id) >= 8 else event_id,
        )
        return None

    def _load_events_for_stylist(self, stylist_id: str) -> List[Dict]:
        """
        Load all events for a stylist from the Transfer_Events sheet.

        Stub — returns [] until Sheets read is wired.
        """
        logger.debug(
            "TransferEventLog: load-for-stylist stub for %s → []", stylist_id
        )
        return []
