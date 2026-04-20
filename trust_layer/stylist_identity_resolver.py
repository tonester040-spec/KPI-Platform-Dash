"""
Trust Layer — Stylist Identity Resolver  (Phase 3B)
Assigns stable stylist IDs across name variations.

The problem: "Sarah Ross", "S. Ross", "Sarah (PT)", "Sarah Ross Jr" are
the same person. Without stable IDs, every longitudinal feature breaks:
  - Retention tracking thinks Sarah left
  - Tenure is always 0 (each variation is a "new" stylist)
  - "Most Improved" cards have no historical baseline

Strategy:
  1. Normalize name (strip suffixes, punctuation, extra words)
  2. Fuzzy match against existing canonical names (85% threshold)
  3. Generate stable ID: {location}_{normalized}_{8-char-hash}
  4. Track all name variants under the canonical name
  5. Allow manual override (Karissa can fix mis-matches)

Storage: 'Stylist_Master' sheet tab
  Columns: stylist_id | canonical_name | normalized_name | location_id |
           first_seen | name_variants | manual_override | status
"""

import hashlib
import logging
import re
from difflib import SequenceMatcher
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


# Suffixes and qualifiers to strip during normalization
_STRIP_SUFFIXES = re.compile(
    r"\b(jr|sr|ii|iii|pt|ft|mgr|manager|lead|asst|assistant)\b",
    re.IGNORECASE,
)
_STRIP_PUNCT = re.compile(r"[^a-z\s]")


class StylistIdentityResolver:
    """
    Resolves raw stylist names to stable canonical IDs.

    Phase 3B — Sheets I/O is stubbed pending implementation.
    The normalization and fuzzy-matching logic is fully functional.
    """

    FUZZY_MATCH_THRESHOLD = 0.85   # ≥85% similarity → same person

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def resolve(self, stylist_name: str, location_id: str) -> Dict:
        """
        Resolve a raw stylist name to a stable identity record.

        Args:
            stylist_name: Raw name as extracted from the Excel file.
            location_id:  Snake-case location identifier.

        Returns:
            {
              'stylist_id':     str,   # stable cross-week ID
              'canonical_name': str,   # authoritative display name
              'is_new':         bool,  # True if first time seen
              'matched_variant': str | None,  # previously seen variant
            }
        """
        normalized = self.normalize_name(stylist_name)
        existing   = self._load_master_records(location_id)

        # Try exact match on normalized name first
        for record in existing:
            if record["normalized_name"] == normalized:
                self._record_variant(record["stylist_id"], stylist_name)
                return {
                    "stylist_id":       record["stylist_id"],
                    "canonical_name":   record["canonical_name"],
                    "is_new":           False,
                    "matched_variant":  stylist_name,
                }

        # Try fuzzy match against all canonical normalized names
        best_match, best_score = self._fuzzy_match(normalized, existing)

        if best_match and best_score >= self.FUZZY_MATCH_THRESHOLD:
            logger.info(
                "Identity resolver: '%s' matched to '%s' (%.0f%%) at %s",
                stylist_name, best_match["canonical_name"],
                best_score * 100, location_id,
            )
            self._record_variant(best_match["stylist_id"], stylist_name)
            return {
                "stylist_id":       best_match["stylist_id"],
                "canonical_name":   best_match["canonical_name"],
                "is_new":           False,
                "matched_variant":  stylist_name,
            }

        # New stylist — create record
        # Pass a first_seen timestamp so two people with the same name at the
        # same location (hired at different times) receive distinct IDs.
        from datetime import datetime, timezone as _tz
        first_seen = datetime.now(tz=_tz.utc).isoformat()
        stylist_id = self._generate_id(location_id, normalized, first_seen=first_seen)
        self._create_master_record(
            stylist_id=stylist_id,
            canonical_name=stylist_name,
            normalized_name=normalized,
            location_id=location_id,
            first_seen=first_seen,
        )
        return {
            "stylist_id":      stylist_id,
            "canonical_name":  stylist_name,
            "is_new":          True,
            "matched_variant": None,
        }

    def resolve_batch(
        self, stylists: List[Dict], location_id: str
    ) -> List[Dict]:
        """
        Resolve all stylist names in a batch, augmenting each dict in-place.

        Adds 'stylist_id' and 'canonical_name' keys to each stylist dict.

        Args:
            stylists:    List of stylist dicts (from parsers).
            location_id: Location to resolve within.

        Returns:
            Updated list with stylist_id and canonical_name added.
        """
        resolved: List[Dict] = []
        for stylist in stylists:
            raw_name = stylist.get("stylist_name", "")
            identity = self.resolve(raw_name, location_id)
            augmented = stylist.copy()
            augmented["stylist_id"]     = identity["stylist_id"]
            augmented["canonical_name"] = identity["canonical_name"]
            resolved.append(augmented)
        return resolved

    # ------------------------------------------------------------------
    # Name normalization
    # ------------------------------------------------------------------

    def normalize_name(self, raw_name: str) -> str:
        """
        Normalize a stylist name for fuzzy comparison.

        Strips: suffixes (Jr, Sr, PT), punctuation, extra whitespace.
        Returns lowercase, whitespace-normalized string.

        Examples:
            "Sarah Ross Jr"   → "sarah ross"
            "S. Ross"         → "s ross"
            "Sarah (PT)"      → "sarah"   ← intentional; PT stripped
            "Sarah Ross Mgr"  → "sarah ross"
        """
        name = raw_name.strip()
        # Remove parenthetical content: "(PT)", "(part-time)"
        name = re.sub(r"\([^)]*\)", "", name)
        # Strip known suffixes
        name = _STRIP_SUFFIXES.sub("", name)
        # Strip non-alpha-space characters
        name = _STRIP_PUNCT.sub("", name.lower())
        # Collapse whitespace
        name = " ".join(name.split())
        return name

    # ------------------------------------------------------------------
    # Fuzzy matching
    # ------------------------------------------------------------------

    def _fuzzy_match(
        self, normalized_name: str, existing_records: List[Dict]
    ) -> Tuple[Optional[Dict], float]:
        """
        Find the best fuzzy match for a normalized name in existing records.

        Returns:
            (best_matching_record, similarity_score) or (None, 0.0)
        """
        best_record: Optional[Dict] = None
        best_score = 0.0

        for record in existing_records:
            score = SequenceMatcher(
                None, normalized_name, record["normalized_name"]
            ).ratio()
            if score > best_score:
                best_score = score
                best_record = record

        return best_record, best_score

    # ------------------------------------------------------------------
    # Stable ID generation
    # ------------------------------------------------------------------

    def _generate_id(
        self,
        location_id:    str,
        normalized_name: str,
        first_seen:     str = "",
    ) -> str:
        """
        Generate a collision-resistant stylist ID.

        Format: {location_id}_{name_slug}_{8-char-hash}

        The hash is derived from location + name + first_seen timestamp, so two
        different people with the same name at the same location (hired at
        different times) receive distinct IDs.

        Example:
            amelia_thompson_farm_a7f3bc12  (hired 2025-03-23)
            amelia_thompson_farm_b2e9f041  (different Amelia hired 2026-08-15)

        Paranoid fallback: if the 8-char candidate already exists in the master,
        extend to a 12-char hash (collision probability ~1 in 16 billion).

        Args:
            location_id:    Snake-case location identifier.
            normalized_name: Output of normalize_name().
            first_seen:     ISO date or datetime string of first appearance.
                            Empty string → hash same as location+name (legacy behaviour).
        """
        name_slug  = normalized_name.replace(" ", "_")[:20]
        hash_input = f"{location_id}|{normalized_name}|{first_seen}".encode("utf-8")
        short_hash = hashlib.sha256(hash_input).hexdigest()[:8]
        candidate  = f"{location_id}_{name_slug}_{short_hash}"

        # Paranoid collision check (only triggered when first_seen is non-empty,
        # i.e., we have enough entropy to generate an extended hash).
        if first_seen and self._id_exists(candidate):
            extended  = hashlib.sha256(hash_input).hexdigest()[:12]
            candidate = f"{location_id}_{name_slug}_{extended}"
            logger.warning(
                "ID collision detected for %s @ %s — extended to 12-char hash: %s",
                normalized_name, location_id, candidate,
            )

        return candidate

    # ------------------------------------------------------------------
    # Sheets I/O stubs
    # ------------------------------------------------------------------

    def _load_master_records(self, location_id: str) -> List[Dict]:
        """
        Load existing Stylist_Master records for a location.

        Stub — returns empty list until Sheets read interface is implemented.
        Each record should contain: stylist_id, canonical_name, normalized_name.
        """
        logger.debug(
            "StylistIdentityResolver: load stub for %s → [] (no master data)",
            location_id,
        )
        return []

    def _id_exists(self, stylist_id: str) -> bool:
        """
        Check whether a stylist_id already exists in the Stylist_Master sheet.

        Stub — returns False (no collision assumed) until Sheets read is wired.
        This is the paranoid collision guard called by _generate_id().
        """
        logger.debug(
            "StylistIdentityResolver: _id_exists stub for %s → False", stylist_id
        )
        return False

    def _create_master_record(
        self,
        stylist_id:     str,
        canonical_name: str,
        normalized_name: str,
        location_id:    str,
        first_seen:     str = "",
    ) -> None:
        """
        Write a new record to the Stylist_Master sheet.

        Stub — no-op until Sheets write interface is implemented.
        """
        logger.debug(
            "StylistIdentityResolver: create stub — %s → %s at %s (first_seen: %s)",
            canonical_name, stylist_id, location_id, first_seen,
        )

    def _record_variant(self, stylist_id: str, variant_name: str) -> None:
        """
        Add a name variant to an existing Stylist_Master record.

        Stub — no-op until Sheets write interface is implemented.
        """
        logger.debug(
            "StylistIdentityResolver: variant stub — %s → %s",
            stylist_id, variant_name,
        )
