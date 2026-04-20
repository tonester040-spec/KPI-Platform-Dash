"""
Trust Layer — Transfer Detector  (Phase 3B / Final Polish)
Detects when a stylist moves from one salon location to another across weekly uploads.

Architecture
------------
StylistIdentityResolver assigns stable stylist_ids across name variations.
TransferDetector uses those IDs to detect when a known stylist appears at a
different location than the one on record.

Six-layer defence:
  1. Revert instruction check  — respects previously flagged false positives
  2. Multi-location guard      — excludes stylists known to split time across sites
  3. Fast-track bypass         — confidence ≥90% + absent from old loc → skip grace period
  4. Grace period (2 weeks)    — prevents premature detection from scheduling overlap
  5. Confidence 2.0 (≥80%)    — 4 factors: name + performance + overlap + volume continuity
  6. Return-transfer tagging   — recognises "Amelia → Blaine → Farmington" as a return

Sheets tables used (all I/O stubbed until Sheets interface is wired):
  Stylist_Master       — authoritative identity + transfer history + location info
  Pending_Transfers    — candidates waiting for grace-period confirmation
  Revert_Instructions  — active orders to create a new stylist at a location

All interactive prompt methods accept an injectable input_fn parameter so tests
don't block on stdin.
"""

import hashlib
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher
from typing import Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data containers
# ---------------------------------------------------------------------------

@dataclass
class TransferConfidence:
    """
    Confidence score for a detected transfer candidate.

    Confidence 2.0 (Phase 3B Final Polish) — four weighted factors:
      30% name similarity
      25% performance similarity (avg ticket + guest count within 30% variance)
      20% no overlapping weeks (clean location transition)
      25% volume continuity (workload footprint moved with the stylist)
    """
    score:                  float           # 0.0 – 1.0
    name_similarity:        float           # SequenceMatcher ratio
    performance_similarity: Optional[float] # None if no historical perf available
    overlap_detected:       bool            # Stylist appeared at old loc in recent weeks
    volume_continuity:      Optional[float] # None if no historical volume available
    requires_confirmation:  bool            # score < CONFIDENCE_THRESHOLD


@dataclass
class PendingTransfer:
    """A transfer candidate waiting for grace-period confirmation."""
    id:             str            # UUID
    stylist_id:     str
    canonical_name: str
    from_location:  str
    to_location:    str
    first_seen:     str            # ISO date — first week at new location
    last_seen:      str            # ISO date — most recent week at new location
    weeks_count:    int            # Consecutive weeks seen at new location
    status:         str = "pending"  # 'pending' | 'confirmed' | 'expired'


# ---------------------------------------------------------------------------
# TransferDetector
# ---------------------------------------------------------------------------

class TransferDetector:
    """
    Detects, confirms, and manages stylist location transfers.

    Usage:
        detector = TransferDetector()
        confirmed_transfers = detector.detect(stylists, location_id, period_end)

    Each confirmed transfer dict contains:
        stylist_id, canonical_name, from_location, to_location,
        transfer_date, transfer_type ('new_transfer' | 'return_transfer'),
        detected_by, confidence, weeks_at_new_location

    Phase 3B — all Sheets I/O is stubbed.
    """

    CONFIDENCE_THRESHOLD            = 0.80   # ≥80% → auto-approve; <80% → needs confirmation
    FAST_TRACK_CONFIDENCE_THRESHOLD = 0.90   # ≥90% + absent from old loc → skip grace period
    GRACE_PERIOD_WEEKS              = 2      # Consecutive weeks at new location before confirmation
    FUZZY_THRESHOLD                 = 0.85   # Same threshold as StylistIdentityResolver
    PENDING_EXPIRY_DAYS             = 14     # Pending transfers older than this are expired

    # Soft transfer types (used when classifying manually)
    TRANSFER_TYPES = (
        "promotion", "lateral", "performance",
        "temporary", "personal", "unknown",
    )

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    def detect(
        self,
        stylists:    List[Dict],
        location_id: str,
        period_end:  str,
    ) -> List[Dict]:
        """
        Run the full transfer detection pipeline for one location's weekly upload.

        Pipeline:
          1. Check revert instructions (skip if active revert order for this person)
          2. Load master record — new stylist if not found
          3. Skip if already at this location
          4. Multi-location guard — skip if known secondary location
          5. Grace period — accumulate weeks, confirm at GRACE_PERIOD_WEEKS
          6. Confidence score — flag low-confidence for manual review
          7. Confirm transfer and update master

        Args:
            stylists:    List of stylist dicts (from StylistIdentityResolver.resolve_batch).
                         Each dict must have 'stylist_id' and 'canonical_name'.
            location_id: Snake-case location identifier for this upload.
            period_end:  ISO date string 'YYYY-MM-DD' of the period end date.

        Returns:
            List of confirmed transfer dicts (empty if none confirmed this week).
        """
        confirmed: List[Dict] = []
        pending_transfers = self._load_pending_transfers()

        for stylist in stylists:
            stylist_id     = stylist.get("stylist_id", "")
            canonical_name = stylist.get("canonical_name", stylist.get("stylist_name", "?"))
            employee_name  = stylist.get("stylist_name", canonical_name)

            # --- Step 1: revert instruction check -------------------------
            revert_signal = self.check_revert_instructions(employee_name, location_id)
            if revert_signal == "create_new":
                logger.warning(
                    "Revert instruction active — skipping transfer check for %s at %s",
                    canonical_name, location_id,
                )
                continue

            # --- Step 2: load master record --------------------------------
            master = self._get_master_record(stylist_id)
            if not master:
                logger.debug("New stylist %s — no master record", canonical_name)
                continue

            current_known = master.get("current_location", "")
            if not current_known or current_known == location_id:
                continue   # No change

            # --- Step 3: multi-location guard ------------------------------
            if not self._is_real_transfer(stylist_id, location_id, master):
                continue

            # --- Step 3b: fast-track bypass --------------------------------
            # If confidence is very high AND stylist is absent from old location,
            # skip the grace period entirely and confirm immediately.
            fast_confidence = self._calculate_transfer_confidence(
                stylist, master, location_id
            )
            not_at_old = self._check_absence_from_old_location(
                stylist_id,
                master.get("current_location", ""),
                period_end,
            )
            if (
                fast_confidence.score >= self.FAST_TRACK_CONFIDENCE_THRESHOLD
                and not_at_old
            ):
                transfer = self._build_transfer_event(
                    stylist_id, canonical_name,
                    from_location=master["current_location"],
                    to_location=location_id,
                    period_end=period_end,
                    confidence=fast_confidence,
                    weeks_at_new=1,
                    master=master,
                )
                transfer["fast_tracked"] = True
                self._update_master_location(stylist_id, location_id, transfer, master)
                confirmed.append(transfer)
                logger.info(
                    "⚡ FAST-TRACK TRANSFER: %s  %s → %s  "
                    "(confidence %.0f%%, not seen at old location)",
                    canonical_name, master["current_location"],
                    location_id, fast_confidence.score * 100,
                )
                continue

            # --- Step 4: grace period logic --------------------------------
            pending = self._find_pending(stylist_id, location_id, pending_transfers)

            if pending:
                new_count = pending["weeks_count"] + 1
                if new_count >= self.GRACE_PERIOD_WEEKS:
                    # --- Step 5 + 6: confidence check before confirming ----
                    confidence = self._calculate_transfer_confidence(
                        stylist, master, location_id
                    )

                    transfer = self._build_transfer_event(
                        stylist_id, canonical_name,
                        from_location=master["current_location"],
                        to_location=location_id,
                        period_end=period_end,
                        confidence=confidence,
                        weeks_at_new=new_count,
                        master=master,
                    )

                    if confidence.requires_confirmation:
                        logger.warning(
                            "⚠️  LOW-CONFIDENCE TRANSFER: %s  %s → %s  "
                            "(confidence %.0f%%) — MANUAL REVIEW REQUIRED",
                            canonical_name, master["current_location"],
                            location_id, confidence.score * 100,
                        )
                        transfer["requires_confirmation"] = True
                        # Don't auto-confirm; leave pending until manual action
                    else:
                        # Auto-confirm
                        self._update_master_location(stylist_id, location_id, transfer, master)
                        self._remove_pending(pending["id"])
                        confirmed.append(transfer)
                        logger.info(
                            "✅ TRANSFER CONFIRMED: %s  %s → %s  "
                            "(confidence %.0f%%, %d weeks)",
                            canonical_name, master["current_location"],
                            location_id, confidence.score * 100, new_count,
                        )
                else:
                    # More weeks needed
                    self._update_pending_weeks(pending["id"], new_count, period_end)
                    logger.info(
                        "⏳ PENDING TRANSFER: %s at %s  (week %d/%d)",
                        canonical_name, location_id, new_count, self.GRACE_PERIOD_WEEKS,
                    )
            else:
                # First appearance at new location
                self._create_pending(
                    stylist_id, canonical_name,
                    from_location=master["current_location"],
                    to_location=location_id,
                    period_end=period_end,
                )
                logger.info(
                    "🔍 POSSIBLE TRANSFER: %s appeared at %s (week 1/%d)",
                    canonical_name, location_id, self.GRACE_PERIOD_WEEKS,
                )

        return confirmed

    # ------------------------------------------------------------------
    # Confidence scoring
    # ------------------------------------------------------------------

    def _calculate_transfer_confidence(
        self,
        current_record: Dict,
        master_record:  Dict,
        new_location:   str,
    ) -> TransferConfidence:
        """
        Confidence 2.0 — four-factor scoring (Phase 3B Final Polish).

        Factors (rebalanced weights):
          30% — Name similarity (exact vs fuzzy match quality)
          25% — Performance similarity (avg ticket + guest count within 30% variance)
          20% — Overlap check (absent from old location = clean transition)
          25% — Volume continuity (workload footprint moved with stylist) ← NEW

        Volume continuity is the primary false-positive killer: a new hire with a
        matching name will have 4 guests vs a veteran's 15 → 27% continuity → LOW.
        A real transfer retains most of the same workload → HIGH continuity.
        """
        score = 0.0

        # Factor 1: name similarity (30%)
        from difflib import SequenceMatcher as SM
        current_name = current_record.get("stylist_name", "").lower().strip()
        master_name  = master_record.get("normalized_name", "").lower().strip()
        name_sim     = SM(None, current_name, master_name).ratio()

        if name_sim > 0.95:
            score += 0.30
        elif name_sim > 0.85:
            score += 0.20
        else:
            score += 0.05

        # Factor 2: performance similarity (25%)
        old_perf = self._get_recent_performance(
            master_record["stylist_id"],
            master_record.get("current_location", ""),
        )
        perf_sim: Optional[float] = None
        if old_perf:
            perf_sim = self._calculate_performance_similarity(old_perf, current_record)
            if perf_sim > 0.70:
                score += 0.25
            elif perf_sim > 0.50:
                score += 0.15
            else:
                score += 0.05
        else:
            score += 0.10   # Neutral — no history to compare

        # Factor 3: overlap check (20%)
        overlap = self._check_recent_overlap(
            master_record["stylist_id"],
            master_record.get("current_location", ""),
            current_record.get("period_end", ""),
        )
        score += 0.02 if overlap else 0.20

        # Factor 4: volume continuity (25%) — Confidence 2.0 addition
        volume_cont: Optional[float] = None
        if old_perf:
            volume_cont = self._calculate_volume_continuity(old_perf, current_record)
            if volume_cont > 0.80:
                score += 0.25    # High continuity — workload moved
            elif volume_cont > 0.60:
                score += 0.15    # Medium continuity
            else:
                score += 0.05    # Low continuity — likely different person
        else:
            score += 0.10   # Neutral — no baseline volume data

        return TransferConfidence(
            score=round(score, 3),
            name_similarity=round(name_sim, 3),
            performance_similarity=round(perf_sim, 3) if perf_sim is not None else None,
            overlap_detected=overlap,
            volume_continuity=round(volume_cont, 3) if volume_cont is not None else None,
            requires_confirmation=(score < self.CONFIDENCE_THRESHOLD),
        )

    @staticmethod
    def _calculate_performance_similarity(old: Dict, new: Dict) -> float:
        """
        Calculate similarity between two performance snapshots.

        Compares avg_ticket and guest_count; allows 30% variance.
        Returns 0.0–1.0.
        """
        def _ratio(a, b):
            if not a or not b:
                return 0.5
            return min(a, b) / max(a, b) if max(a, b) > 0 else 0.5

        ticket_ratio = _ratio(
            old.get("avg_ticket", 0),
            new.get("avg_ticket", 0),
        )
        guest_ratio = _ratio(
            old.get("guest_count", 0),
            new.get("guest_count", 0),
        )

        ticket_sim = ticket_ratio if ticket_ratio > 0.70 else 0.30
        guest_sim  = guest_ratio  if guest_ratio  > 0.70 else 0.30

        return (ticket_sim + guest_sim) / 2.0

    @staticmethod
    def _calculate_volume_continuity(old_perf: Dict, new_perf: Dict) -> float:
        """
        Volume continuity signal (Confidence 2.0, Phase 3B Final Polish).

        Compares total workload footprint (guests + revenue) to determine
        whether the same volume of work "moved" with the stylist.

        A real transfer: veteran's 15 guests/week at Farmington → 14–16 guests
        at Blaine (continuity ~93%).

        A false positive: veteran's 15 guests vs new hire's 4 guests at same
        location (continuity ~27%) — VERY different people.

        Returns:
            0.0–1.0 volume continuity score.
            0.5 if either side has no data (neutral — don't penalise stub).
        """
        old_guests  = old_perf.get("guest_count", 0) or 0
        new_guests  = new_perf.get("guest_count", 0) or 0
        old_revenue = old_perf.get("total_sales", old_perf.get("avg_ticket", 0)) or 0
        new_revenue = new_perf.get("total_sales", new_perf.get("avg_ticket", 0)) or 0

        def _sim(a, b):
            if not a or not b:
                return 0.5   # Neutral — insufficient data
            ratio = min(a, b) / max(a, b)
            return ratio if ratio > 0.70 else 0.30

        guest_sim   = _sim(old_guests, new_guests)
        revenue_sim = _sim(old_revenue, new_revenue)

        # Revenue weighted slightly higher (harder to fake)
        return round((guest_sim * 0.40) + (revenue_sim * 0.60), 4)

    # ------------------------------------------------------------------
    # Multi-location guard
    # ------------------------------------------------------------------

    def _is_real_transfer(
        self,
        stylist_id:   str,
        new_location: str,
        master:       Dict,
    ) -> bool:
        """
        Return True if location change should be treated as a transfer.

        Returns False (not a transfer) if:
          - new_location is an established secondary location for this stylist
          - Pattern detection over last 4 weeks shows regular multi-location work
        """
        secondary = master.get("secondary_locations", [])
        if new_location in secondary:
            logger.info(
                "📍 MULTI-LOCATION: %s working at secondary location %s",
                master.get("canonical_name", stylist_id), new_location,
            )
            return False

        pattern = self._detect_multi_location_pattern(stylist_id, lookback_weeks=4)
        if pattern["is_multi_location"] and new_location in pattern["secondary_locations"]:
            self._update_master_secondary_locations(stylist_id, pattern, master)
            return False

        return True

    def _detect_multi_location_pattern(
        self,
        stylist_id:     str,
        lookback_weeks: int = 4,
    ) -> Dict:
        """
        Detect if a stylist regularly works multiple locations.

        Returns:
            {
              'is_multi_location': bool,
              'primary_location':  str,
              'secondary_locations': List[str],
              'split': Dict[str, float],
            }
        Stub — returns single-location result until Sheets read is wired.
        """
        logger.debug(
            "TransferDetector: multi-location pattern stub for %s → single-location",
            stylist_id,
        )
        return {
            "is_multi_location":    False,
            "primary_location":     "",
            "secondary_locations":  [],
            "split":                {},
        }

    # ------------------------------------------------------------------
    # Transfer event builder + master update
    # ------------------------------------------------------------------

    def _build_transfer_event(
        self,
        stylist_id:    str,
        canonical_name: str,
        from_location: str,
        to_location:   str,
        period_end:    str,
        confidence:    TransferConfidence,
        weeks_at_new:  int,
        master:        Dict,
    ) -> Dict:
        """Build a transfer event dict (not yet committed to master)."""
        # Return detection — was this stylist at to_location before?
        previous_locations = [
            t.get("to", "") for t in (master.get("transfer_history") or [])
        ] + [t.get("from", "") for t in (master.get("transfer_history") or [])]

        transfer_type = "return_transfer" if to_location in previous_locations else "new_transfer"

        event = {
            "stylist_id":          stylist_id,
            "canonical_name":      canonical_name,
            "from_location":       from_location,
            "to_location":         to_location,
            "transfer_date":       period_end,
            "timing":              "week_ending",   # Week-precision; manual correction via update_effective_date()
            "effective_date":      None,            # Set manually if exact date known
            "type":                transfer_type,
            "transfer_type":       "unknown",       # Classified manually; see prompt_transfer_classification()
            "manual_classification": False,
            "detected_by":         "auto",
            "confirmed_by":        None,
            "confidence":          confidence.score,
            "confidence_breakdown": {
                "name_similarity":        confidence.name_similarity,
                "performance_similarity": confidence.performance_similarity,
                "overlap_detected":       confidence.overlap_detected,
                "volume_continuity":      confidence.volume_continuity,
            },
            "fast_tracked":        False,
            "weeks_at_new_location": weeks_at_new,
            "requires_confirmation": confidence.requires_confirmation,
            "notes":               None,
        }

        if transfer_type == "return_transfer":
            logger.info(
                "🔙 RETURN TRANSFER detected: %s returning to %s",
                canonical_name, to_location,
            )

        return event

    def _update_master_location(
        self,
        stylist_id:   str,
        new_location: str,
        transfer:     Dict,
        master:       Dict,
    ) -> None:
        """
        Append confirmed transfer to master record and update current_location.
        """
        if master.get("transfer_history") is None:
            master["transfer_history"] = []

        master["transfer_history"].append(transfer)
        master["current_location"] = new_location
        self._write_master_record(master)

    # ------------------------------------------------------------------
    # Manual revert
    # ------------------------------------------------------------------

    def manual_revert_transfer(
        self,
        stylist_id:    str,
        transfer_date: str,
        reason:        str,
        performed_by:  str = "karissa",
    ) -> None:
        """
        Safely revert a false-positive transfer.

        Strategy:
          1. Mark the transfer event as INVALID in master (not deleted — audit trail)
          2. Revert master.current_location to from_location
          3. Write a Revert_Instruction so future uploads create a NEW stylist
             entry if the same name appears at to_location again

        Does NOT split historical records — too risky.
        """
        master = self._get_master_record(stylist_id)
        if not master:
            logger.error("manual_revert_transfer: no master record for %s", stylist_id)
            return

        history = master.get("transfer_history") or []
        transfer = next(
            (t for t in history if t.get("transfer_date") == transfer_date),
            None,
        )
        if not transfer:
            logger.error(
                "manual_revert_transfer: no transfer at %s for %s",
                transfer_date, stylist_id,
            )
            return

        from_location = transfer["from_location"]
        to_location   = transfer["to_location"]

        # Mark invalid
        transfer["status"]              = "INVALID"
        transfer["invalidated_by"]      = performed_by
        transfer["invalidated_at"]      = datetime.now(tz=timezone.utc).isoformat()
        transfer["invalidation_reason"] = reason

        # Revert location
        master["current_location"] = from_location
        self._write_master_record(master)

        # Create revert instruction
        self._save_revert_instruction({
            "id":                  str(uuid.uuid4()),
            "created_at":          datetime.now(tz=timezone.utc).isoformat(),
            "reverted_stylist_id": stylist_id,
            "destination_location": to_location,
            "action":              "create_new_stylist_on_next_upload",
            "reason":              f"False positive transfer reverted: {reason}",
            "status":              "active",
            "completed_at":        None,
        })

        logger.warning(
            "⚠️  TRANSFER REVERTED: %s  %s → %s  (reason: %s) by %s",
            master.get("canonical_name", stylist_id),
            to_location, from_location, reason, performed_by,
        )

        print(
            f"\n{'='*60}\n"
            f"✅  TRANSFER REVERTED\n"
            f"{'='*60}\n"
            f"Stylist : {master.get('canonical_name', stylist_id)}\n"
            f"Reverted to: {from_location}\n\n"
            f"IMPORTANT:\n"
            f"  • If this person appears at {to_location} in future uploads,\n"
            f"    a NEW stylist entry will be created.\n"
            f"  • Historical data at {to_location} remains under current ID.\n"
            f"  • This decision is logged and cannot be undone automatically.\n"
            f"{'='*60}\n"
        )

    def check_revert_instructions(
        self,
        employee_name: str,
        location_id:   str,
    ) -> Optional[str]:
        """
        Check for an active revert instruction before resolving identity.

        Returns:
            'create_new' — caller should skip transfer matching and create new ID
            None         — no revert instruction; proceed normally
        """
        instructions = self._load_revert_instructions()

        for instr in instructions:
            if instr.get("destination_location") != location_id:
                continue
            if instr.get("status") != "active":
                continue

            reverted_id   = instr.get("reverted_stylist_id", "")
            reverted_name = self._get_master_canonical_name(reverted_id)
            if not reverted_name:
                continue

            sim = SequenceMatcher(None, employee_name.lower(), reverted_name.lower()).ratio()
            if sim >= self.FUZZY_THRESHOLD:
                logger.warning(
                    "⚠️  REVERT INSTRUCTION ACTIVE: creating NEW stylist for %s at %s "
                    "(previous match reverted)",
                    employee_name, location_id,
                )
                # Mark completed
                instr["status"]       = "completed"
                instr["completed_at"] = datetime.now(tz=timezone.utc).isoformat()
                self._update_revert_instruction(instr)
                return "create_new"

        return None

    def update_transfer_effective_date(
        self,
        stylist_id:     str,
        transfer_date:  str,
        effective_date: str,
        notes:          Optional[str] = None,
    ) -> None:
        """
        Manually correct a transfer effective date to a more precise value.

        Args:
            stylist_id:     Stable ID of the stylist.
            transfer_date:  The week_ending date recorded at detection time.
            effective_date: The known exact date (ISO format).
            notes:          Optional free-text context.
        """
        master = self._get_master_record(stylist_id)
        if not master:
            return

        for t in (master.get("transfer_history") or []):
            if t.get("transfer_date") == transfer_date:
                t["effective_date"] = effective_date
                t["timing"]         = "exact"
                if notes:
                    t["notes"] = notes
                self._write_master_record(master)
                logger.info(
                    "Transfer date updated: %s effective %s (was %s)",
                    stylist_id, effective_date, transfer_date,
                )
                return

        logger.warning(
            "update_transfer_effective_date: no transfer at %s for %s",
            transfer_date, stylist_id,
        )

    # ------------------------------------------------------------------
    # Interactive prompt helpers (injectable input_fn for testability)
    # ------------------------------------------------------------------

    @staticmethod
    def prompt_transfer_confirmation(
        transfer:  Dict,
        input_fn:  Callable[[str], str] = input,
    ) -> str:
        """
        Show a low-confidence transfer to Karissa for manual confirmation.

        Args:
            transfer: Transfer dict (from detect()).
            input_fn: Callable for user input — override in tests to avoid stdin.

        Returns:
            'confirm' | 'reject' | 'skip'
        """
        bd = transfer.get("confidence_breakdown", {})
        perf_line = ""
        if bd.get("performance_similarity") is not None:
            perf_line = f"  • Performance similarity : {bd['performance_similarity']:.0%}\n"

        print(
            f"\n{'='*60}\n"
            f"⚠️   LOW-CONFIDENCE TRANSFER DETECTED\n"
            f"{'='*60}\n"
            f"Stylist : {transfer.get('canonical_name')}\n"
            f"From    : {transfer.get('from_location')}\n"
            f"To      : {transfer.get('to_location')}\n"
            f"Week    : {transfer.get('transfer_date')}\n\n"
            f"Confidence : {transfer.get('confidence', 0):.0%}\n"
            f"  • Name similarity        : {bd.get('name_similarity', 0):.0%}\n"
            f"{perf_line}"
            f"  • Overlap detected       : {bd.get('overlap_detected', False)}\n\n"
            f"Is this the SAME person moving locations?\n"
            f"{'='*60}"
        )
        while True:
            choice = input_fn(
                "\n(Y) Yes, same person   (N) No, different person   (S) Skip: "
            ).strip().upper()
            if choice == "Y":
                return "confirm"
            if choice == "N":
                return "reject"
            if choice == "S":
                return "skip"
            print("Enter Y, N, or S.")

    @staticmethod
    def prompt_transfer_classification(
        transfer: Dict,
        input_fn: Callable[[str], str] = input,
    ) -> str:
        """
        Ask Karissa to classify a confirmed transfer type.

        Returns:
            One of: 'promotion' | 'lateral' | 'performance' |
                    'temporary' | 'personal' | 'unknown'
        """
        print(
            f"\n🔄 Transfer detected: {transfer.get('canonical_name')}\n"
            f"   {transfer.get('from_location')} → {transfer.get('to_location')}\n\n"
            f"How would you classify this transfer?\n"
            f"  1) Promotion\n"
            f"  2) Lateral move\n"
            f"  3) Performance (struggling stylist)\n"
            f"  4) Temporary coverage → permanent\n"
            f"  5) Personal request\n"
            f"  6) Unknown / Skip"
        )
        classification_map = {
            "1": "promotion",
            "2": "lateral",
            "3": "performance",
            "4": "temporary",
            "5": "personal",
            "6": "unknown",
        }
        while True:
            choice = input_fn("\nChoice (1-6): ").strip()
            if choice in classification_map:
                return classification_map[choice]
            print("Enter a number between 1 and 6.")

    # ------------------------------------------------------------------
    # Manual confirm
    # ------------------------------------------------------------------

    def manual_confirm_transfer(
        self,
        stylist_id:    str,
        transfer_date: str,
        confirmed_by:  str = "karissa",
        notes:         Optional[str] = None,
    ) -> bool:
        """
        Explicitly confirm a pending or already-auto-confirmed transfer.

        Used when Karissa or a manager reviews the transfer queue and says
        "yes, this one is correct."  Marks the transfer entry with
        ``confirmed_by`` and ``confirmed_at`` so the audit trail shows
        whether a transfer was auto-detected or human-verified.

        If the transfer is still in the Pending_Transfers queue, it is
        immediately promoted and the master record is updated — bypassing
        the remaining grace-period weeks.

        Args:
            stylist_id:    Stable ID of the stylist.
            transfer_date: The ``transfer_date`` (week_ending) recorded at
                           detection time.
            confirmed_by:  Who is confirming — 'karissa' | 'jess' | 'jenn' | 'system'.
            notes:         Optional free-text context for the audit trail.

        Returns:
            True if a matching transfer was found and confirmed.
            False if no matching record was found.
        """
        now_iso = datetime.now(tz=timezone.utc).isoformat()

        # ── 1. Check master transfer_history first (auto-confirmed transfers) ──
        master = self._get_master_record(stylist_id)
        if master:
            for t in (master.get("transfer_history") or []):
                if t.get("transfer_date") == transfer_date:
                    t["confirmed_by"] = confirmed_by
                    t["confirmed_at"] = now_iso
                    if notes:
                        t["confirmation_notes"] = notes
                    self._write_master_record(master)
                    logger.info(
                        "✅ TRANSFER CONFIRMED: %s  %s → %s  (by %s)",
                        master.get("canonical_name", stylist_id),
                        t.get("from_location"), t.get("to_location"),
                        confirmed_by,
                    )
                    return True

        # ── 2. Check Pending_Transfers (grace-period queue) ──
        pending_list = self._load_pending_transfers()
        pending = next(
            (
                p for p in pending_list
                if p.get("stylist_id") == stylist_id
                and (
                    p.get("first_seen") == transfer_date
                    or p.get("last_seen")  == transfer_date
                )
                and p.get("status") == "pending"
            ),
            None,
        )

        if pending:
            # Promote immediately — build a transfer event and write to master
            from_location = pending["from_location"]
            to_location   = pending["to_location"]

            confirmed_master = self._get_master_record(stylist_id) or {
                "stylist_id":       stylist_id,
                "canonical_name":   pending.get("canonical_name", ""),
                "current_location": from_location,
                "transfer_history": [],
            }

            transfer_event = {
                "transfer_date":   transfer_date,
                "from_location":   from_location,
                "to_location":     to_location,
                "transfer_type":   pending.get("transfer_type", "new_transfer"),
                "detected_by":     "auto",
                "confirmed_by":    confirmed_by,
                "confirmed_at":    now_iso,
                "confidence":      pending.get("confidence", 0.0),
                "status":          "CONFIRMED",
            }
            if notes:
                transfer_event["confirmation_notes"] = notes

            self._update_master_location(stylist_id, to_location, transfer_event,
                                         confirmed_master)
            self._remove_pending(pending["id"])

            logger.info(
                "✅ PENDING TRANSFER CONFIRMED (manual): %s  %s → %s  (by %s)",
                pending.get("canonical_name", stylist_id),
                from_location, to_location, confirmed_by,
            )
            return True

        logger.warning(
            "manual_confirm_transfer: no matching transfer at %s for %s",
            transfer_date, stylist_id,
        )
        return False

    # ------------------------------------------------------------------
    # Tenure and performance enrichment
    # ------------------------------------------------------------------

    @staticmethod
    def calculate_tenure_weeks(hire_date: str, as_of: Optional[str] = None) -> Optional[int]:
        """
        Calculate tenure in whole weeks from ``hire_date`` to ``as_of``.

        Tenure is always measured from the stylist's ORIGINAL hire date
        (i.e., first appearance across all locations), never from the
        date they joined their current location.  This preserves continuity
        through transfers.

        Args:
            hire_date: ISO date string (YYYY-MM-DD) — first_seen date from master.
            as_of:     ISO date string to measure tenure against.
                       Defaults to today if None.

        Returns:
            Tenure in complete weeks (≥0), or None if hire_date is invalid.
        """
        if not hire_date:
            return None
        try:
            start = datetime.fromisoformat(hire_date).date()
            end   = (
                datetime.fromisoformat(as_of).date()
                if as_of
                else datetime.now(tz=timezone.utc).date()
            )
            delta = (end - start).days
            return max(0, delta // 7)
        except (ValueError, TypeError):
            logger.debug(
                "calculate_tenure_weeks: invalid date(s) hire_date=%s as_of=%s",
                hire_date, as_of,
            )
            return None

    def get_performance_enrichment(
        self,
        stylist_id: str,
        period_end: str,
    ) -> Dict:
        """
        Return transfer-aware fields for a weekly performance record.

        Call this when writing a STYLISTS_DATA row.  The returned dict
        should be merged into the performance record before writing:

            perf_record.update(detector.get_performance_enrichment(sid, week_end))

        Fields returned:
            ``is_transfer_week``  — bool: True if the stylist's first appearance
                                    at their current location was this week.
            ``transferred_from``  — str | None: previous location if transfer week.
            ``tenure_weeks``      — int | None: complete weeks since original hire.

        Stub-safe: if master record is unavailable, returns safe defaults.
        """
        master = self._get_master_record(stylist_id)

        if not master:
            return {
                "is_transfer_week":  False,
                "transferred_from":  None,
                "tenure_weeks":      None,
            }

        hire_date = master.get("hire_date") or master.get("first_seen") or ""
        tenure    = self.calculate_tenure_weeks(hire_date, as_of=period_end)

        # Check if this period_end is the transfer date for the most recent transfer
        history = master.get("transfer_history") or []
        is_transfer_week = False
        transferred_from = None

        if history:
            last = history[-1]
            if (
                last.get("transfer_date") == period_end
                and last.get("status") != "INVALID"
            ):
                is_transfer_week = True
                transferred_from = last.get("from_location")

        return {
            "is_transfer_week":  is_transfer_week,
            "transferred_from":  transferred_from,
            "tenure_weeks":      tenure,
        }

    # ------------------------------------------------------------------
    # Edge case handlers
    # ------------------------------------------------------------------

    def handle_network_departure(
        self,
        stylist_id:     str,
        last_seen_week: str,
        absent_weeks:   int,
        performed_by:   str = "auto",
    ) -> bool:
        """
        Mark a stylist as ``inactive`` when they've been absent from ALL
        locations for a sustained period.

        This prevents false attrition signals — a stylist who simply
        didn't appear in uploads for 3+ weeks should be marked inactive,
        not silently dropped from analysis.

        Args:
            stylist_id:     Stable stylist ID.
            last_seen_week: ISO date of the last week they appeared.
            absent_weeks:   How many consecutive weeks they've been absent.
            performed_by:   'auto' (pipeline) | 'karissa' | 'system'.

        Returns:
            True if record was found and updated; False if no master record.
        """
        master = self._get_master_record(stylist_id)
        if not master:
            logger.warning(
                "handle_network_departure: no master record for %s", stylist_id
            )
            return False

        if master.get("status") == "inactive":
            logger.debug(
                "handle_network_departure: %s already inactive — skipping", stylist_id
            )
            return True   # Already handled

        master["status"]              = "inactive"
        master["last_seen"]           = last_seen_week
        master["departed_at"]         = datetime.now(tz=timezone.utc).isoformat()
        master["departed_by"]         = performed_by
        master["absent_weeks_at_departure"] = absent_weeks
        self._write_master_record(master)

        logger.info(
            "🚪 NETWORK DEPARTURE: %s marked inactive (absent %d weeks, last seen %s)",
            master.get("canonical_name", stylist_id), absent_weeks, last_seen_week,
        )
        return True

    def handle_reactivation(
        self,
        stylist_id:    str,
        location_id:   str,
        period_end:    str,
        performed_by:  str = "auto",
    ) -> bool:
        """
        Reactivate an inactive stylist when they reappear at any location.

        Called during normal transfer/identity detection when a previously
        inactive stylist shows up in a new upload.  Resets status to 'active'
        and updates ``current_location``.

        Args:
            stylist_id:    Stable stylist ID.
            location_id:   Location where they reappeared.
            period_end:    Week-ending date of the reappearance.
            performed_by:  'auto' | 'karissa'.

        Returns:
            True if reactivated; False if no master record or already active.
        """
        master = self._get_master_record(stylist_id)
        if not master:
            logger.warning(
                "handle_reactivation: no master record for %s", stylist_id
            )
            return False

        if master.get("status") == "active":
            return False   # Already active — nothing to do

        prev_location = master.get("current_location", "")
        master["status"]            = "active"
        master["current_location"]  = location_id
        master["last_seen"]         = period_end
        master["reactivated_at"]    = datetime.now(tz=timezone.utc).isoformat()
        master["reactivated_by"]    = performed_by
        master["reactivated_at_location"] = location_id

        # Record reactivation as a special history entry (not a full transfer)
        if master.get("transfer_history") is None:
            master["transfer_history"] = []
        master["transfer_history"].append({
            "transfer_date":   period_end,
            "from_location":   prev_location,
            "to_location":     location_id,
            "transfer_type":   "reactivation",
            "detected_by":     performed_by,
            "status":          "CONFIRMED",
        })

        self._write_master_record(master)

        logger.info(
            "🔄 REACTIVATION: %s returned to active at %s (week %s)",
            master.get("canonical_name", stylist_id), location_id, period_end,
        )
        return True

    def is_temporary_coverage(
        self,
        stylist_id:  str,
        location_id: str,
        period_end:  str,
    ) -> bool:
        """
        Determine whether a stylist's presence at a non-home location is
        temporary coverage (1 isolated week) rather than a genuine transfer.

        A 1-week appearance at a non-home location creates a pending transfer
        but should NOT be auto-confirmed until the grace period (2 weeks)
        is reached.  This method is a lighter pre-check that can be used
        to annotate the performance record with ``coverage=True``.

        Heuristic:
          - If the stylist has a master record at a different location AND
          - There is no existing pending transfer to ``location_id`` AND
          - The pending transfer list is empty for this stylist
          → Classify as potential temporary coverage (single-week signal)

        Returns:
            True  — looks like single-week coverage; treat cautiously.
            False — master shows this as home location, or a multi-week
                    presence is already pending.

        Note: This method does NOT update any records — call-site decides
        whether to create a pending entry or annotate the row.
        """
        master = self._get_master_record(stylist_id)
        if not master:
            return False

        home_location = master.get("current_location", "")
        if home_location == location_id:
            return False   # This IS the home location

        # Check if there's already a multi-week pending transfer
        pending_list = self._load_pending_transfers()
        existing = self._find_pending(stylist_id, location_id, pending_list)

        if existing and existing.get("weeks_count", 0) >= 2:
            return False   # Already confirmed as more than a single week

        # Single-week presence at non-home location → candidate for temp coverage
        return True

    def merge_name_change(
        self,
        primary_stylist_id:   str,
        secondary_stylist_id: str,
        reason:               str,
        performed_by:         str = "karissa",
        input_fn:             Callable[[str], str] = input,
    ) -> bool:
        """
        Merge two stylist IDs into one when Karissa confirms they are the
        same person (e.g., name changed after marriage, initial vs full name).

        Merge strategy:
          1. Keep ``primary_stylist_id`` — all secondary history is adopted.
          2. Copy secondary's ``transfer_history`` entries into primary's.
          3. Preserve earliest ``hire_date`` across both records.
          4. Mark secondary record as ``status='merged'`` and set
             ``merged_into = primary_stylist_id``.
          5. Write a merge event to transfer_history on primary record.
          6. Sheets: any STYLISTS_DATA rows for secondary → relabel to primary
             (stub — caller handles the backfill).

        Args:
            primary_stylist_id:   The ID to keep (usually the older record).
            secondary_stylist_id: The ID to absorb and retire.
            reason:               Human-readable explanation.
            performed_by:         'karissa' | 'jess' | 'system'.
            input_fn:             Injectable for tests.

        Returns:
            True if merge was performed; False on validation failure.
        """
        if primary_stylist_id == secondary_stylist_id:
            logger.error("merge_name_change: primary and secondary IDs are identical")
            return False

        primary   = self._get_master_record(primary_stylist_id)
        secondary = self._get_master_record(secondary_stylist_id)

        # In stub mode either may be None — build minimal placeholders so the
        # merge logic can still be exercised and tested.
        if primary is None:
            logger.warning(
                "merge_name_change: primary %s not found (stub mode). "
                "Using placeholder.", primary_stylist_id[:12],
            )
            primary = {
                "stylist_id":       primary_stylist_id,
                "canonical_name":   primary_stylist_id,
                "hire_date":        "",
                "transfer_history": [],
                "status":           "active",
            }

        if secondary is None:
            logger.warning(
                "merge_name_change: secondary %s not found (stub mode). "
                "Using placeholder.", secondary_stylist_id[:12],
            )
            secondary = {
                "stylist_id":       secondary_stylist_id,
                "canonical_name":   secondary_stylist_id,
                "hire_date":        "",
                "transfer_history": [],
                "status":           "active",
            }

        # ── Adopt secondary history ───────────────────────────────────────
        if primary.get("transfer_history") is None:
            primary["transfer_history"] = []

        for entry in (secondary.get("transfer_history") or []):
            entry_copy              = dict(entry)
            entry_copy["source"]    = f"merged_from:{secondary_stylist_id}"
            primary["transfer_history"].append(entry_copy)

        # ── Preserve earliest hire_date ───────────────────────────────────
        dates = [
            d for d in [primary.get("hire_date"), secondary.get("hire_date")]
            if d
        ]
        if dates:
            primary["hire_date"] = min(dates)

        # ── Record the merge event ────────────────────────────────────────
        now_iso = datetime.now(tz=timezone.utc).isoformat()
        primary["transfer_history"].append({
            "transfer_date": now_iso[:10],
            "transfer_type": "name_change_merge",
            "merged_from":   secondary_stylist_id,
            "reason":        reason,
            "performed_by":  performed_by,
            "merged_at":     now_iso,
        })

        # ── Mark secondary as merged ──────────────────────────────────────
        secondary["status"]      = "merged"
        secondary["merged_into"] = primary_stylist_id
        secondary["merged_at"]   = now_iso
        secondary["merged_by"]   = performed_by

        # ── Write both records ────────────────────────────────────────────
        self._write_master_record(primary)
        self._write_master_record(secondary)

        logger.info(
            "🔗 NAME-CHANGE MERGE: %s ← %s  (reason: %s)  by %s",
            primary_stylist_id[:16], secondary_stylist_id[:16], reason, performed_by,
        )
        return True

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------

    def cleanup_expired_pending_transfers(self) -> int:
        """
        Expire pending transfers that haven't confirmed within PENDING_EXPIRY_DAYS.

        Intended to run at the start of each weekly batch.

        Returns:
            Number of pending transfers expired.
        """
        pending = self._load_pending_transfers()
        now     = datetime.now(tz=timezone.utc).date()
        expired = 0

        for p in pending:
            last_seen = datetime.fromisoformat(p["last_seen"]).date()
            if (now - last_seen).days > self.PENDING_EXPIRY_DAYS:
                self._expire_pending(p["id"])
                expired += 1
                logger.info(
                    "🔙 PENDING TRANSFER EXPIRED: %s did not continue at %s",
                    p.get("canonical_name", p["stylist_id"]),
                    p.get("to_location", "?"),
                )

        return expired

    # ------------------------------------------------------------------
    # Pending transfer helpers
    # ------------------------------------------------------------------

    def _find_pending(
        self,
        stylist_id:  str,
        to_location: str,
        pending_list: List[Dict],
    ) -> Optional[Dict]:
        """Return the active pending transfer for a stylist→location pair."""
        return next(
            (
                p for p in pending_list
                if p.get("stylist_id") == stylist_id
                and p.get("to_location") == to_location
                and p.get("status") == "pending"
            ),
            None,
        )

    # ------------------------------------------------------------------
    # Sheets I/O stubs
    # ------------------------------------------------------------------

    def _get_master_record(self, stylist_id: str) -> Optional[Dict]:
        """
        Fetch master record for a stylist from Stylist_Master sheet.
        Stub — returns None until Sheets read interface is wired.
        """
        logger.debug("TransferDetector: master record stub for %s → None", stylist_id)
        return None

    def _write_master_record(self, record: Dict) -> None:
        """
        Write updated master record to Stylist_Master sheet.
        Stub — no-op until Sheets write interface is wired.
        """
        logger.debug(
            "TransferDetector: write master stub — %s",
            record.get("stylist_id", "?"),
        )

    def _get_master_canonical_name(self, stylist_id: str) -> Optional[str]:
        """Return canonical_name from master for a given stylist_id. Stub → None."""
        record = self._get_master_record(stylist_id)
        return record.get("canonical_name") if record else None

    def _load_pending_transfers(self) -> List[Dict]:
        """Load all pending transfer records. Stub → []."""
        logger.debug("TransferDetector: load pending transfers stub → []")
        return []

    def _create_pending(
        self,
        stylist_id:    str,
        canonical_name: str,
        from_location: str,
        to_location:   str,
        period_end:    str,
    ) -> None:
        """Create a new pending transfer record. Stub — no-op."""
        logger.debug(
            "TransferDetector: create pending stub — %s %s → %s",
            canonical_name, from_location, to_location,
        )

    def _update_pending_weeks(
        self, pending_id: str, weeks_count: int, period_end: str
    ) -> None:
        """Update week count on a pending transfer. Stub — no-op."""
        logger.debug(
            "TransferDetector: update pending stub — id=%s weeks=%d",
            pending_id, weeks_count,
        )

    def _remove_pending(self, pending_id: str) -> None:
        """Remove (confirm) a pending transfer. Stub — no-op."""
        logger.debug("TransferDetector: remove pending stub — id=%s", pending_id)

    def _expire_pending(self, pending_id: str) -> None:
        """Mark a pending transfer as expired. Stub — no-op."""
        logger.debug("TransferDetector: expire pending stub — id=%s", pending_id)

    def _load_revert_instructions(self) -> List[Dict]:
        """Load active revert instructions. Stub → []."""
        logger.debug("TransferDetector: load revert instructions stub → []")
        return []

    def _save_revert_instruction(self, instruction: Dict) -> None:
        """Write a new revert instruction. Stub — no-op."""
        logger.debug(
            "TransferDetector: save revert instruction stub — %s at %s",
            instruction.get("reverted_stylist_id"), instruction.get("destination_location"),
        )

    def _update_revert_instruction(self, instruction: Dict) -> None:
        """Update existing revert instruction. Stub — no-op."""
        logger.debug(
            "TransferDetector: update revert instruction stub — id=%s status=%s",
            instruction.get("id"), instruction.get("status"),
        )

    def _update_master_secondary_locations(
        self,
        stylist_id: str,
        pattern:    Dict,
        master:     Dict,
    ) -> None:
        """Update master with detected secondary locations. Stub — no-op."""
        logger.debug(
            "TransferDetector: update secondary locations stub — %s → %s",
            stylist_id, pattern.get("secondary_locations"),
        )

    def _check_absence_from_old_location(
        self,
        stylist_id:   str,
        old_location: str,
        period_end:   str,
    ) -> bool:
        """
        Check whether the stylist was ABSENT from their old location this week.

        Used by the fast-track bypass: if confidence ≥90% AND stylist did not
        appear at old location this period → immediate confirmation (no grace period).

        Returns:
            True  — stylist not seen at old_location this week (supports fast-track)
            False — stylist still appeared at old location, or data unavailable

        Stub — returns False (conservative: don't fast-track until data confirms
        absence) until Sheets read interface is wired.
        """
        logger.debug(
            "TransferDetector: absence-check stub for %s @ %s → False",
            stylist_id, old_location,
        )
        return False

    def _get_recent_performance(
        self,
        stylist_id:  str,
        location_id: str,
    ) -> Optional[Dict]:
        """
        Fetch most recent performance record for a stylist at a location.
        Stub — returns None until Sheets read is wired.
        """
        logger.debug(
            "TransferDetector: recent performance stub for %s @ %s → None",
            stylist_id, location_id,
        )
        return None

    def _check_recent_overlap(
        self,
        stylist_id:  str,
        location_id: str,
        period_end:  str,
    ) -> bool:
        """
        Check if stylist appeared at location_id in recent weeks.
        Stub — returns False (no overlap assumed) until Sheets read is wired.
        """
        logger.debug(
            "TransferDetector: overlap check stub for %s @ %s → False",
            stylist_id, location_id,
        )
        return False
