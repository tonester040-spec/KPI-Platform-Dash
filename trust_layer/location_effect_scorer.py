"""
Trust Layer — Location Effect Scorer  (Phase 3B Final Polish — Strategic Layer)
Calculates the Location Effect Score and powers Talent Graph queries.

The Strategic Moat
------------------
Most BI tools answer "what happened?" KPI answers "why it happened — and what
to do about it."

Location Effect Score = the measured impact of a location's environment on
stylist development. Calculated from performance deltas of all stylists who
have transferred INTO a location.

If Blaine consistently grows avg tickets by 15% and guest counts by 18% after
transfers in, that's not luck — that's management quality + training + culture.
If Andover consistently drops performance by 8-12%, that's a signal no POS
system will ever surface.

Score Tiers
-----------
  80–100  TALENT ACCELERATOR 🌟  — significant gains across all metrics
  50–79   POSITIVE IMPACT    ✅  — meaningful improvement in most metrics
  30–49   NEUTRAL            🟡  — minimal impact on stylist performance
  0–29    TALENT DRAIN       🔴  — stylists consistently decline here

Composite Score Components (each 0–100 before weighting):
  25% — Avg ticket effect      (-20% to +20% normalized)
  25% — Guest count effect     (-20% to +20% normalized)
  25% — Rebooking effect       (-15pp to +15pp normalized, defaults to 0 if absent)
  25% — Retention rate         (% of transferred-in stylists still at this location)

Talent Graph Queries
--------------------
Additional methods on this class implement the Talent Graph query layer —
treating the salon network as a graph of people (nodes), movements (edges),
and environmental impacts (edge weights).

All Sheets I/O is STUBBED. The computation logic is fully functional once
data is supplied.
"""

import logging
import statistics
from dataclasses import dataclass
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


# ── Tier thresholds ───────────────────────────────────────────────────────────
TIER_ACCELERATOR_MIN = 70   # ≥70 → TALENT ACCELERATOR
TIER_POSITIVE_MIN    = 50   # 50–69 → POSITIVE IMPACT
TIER_NEUTRAL_MIN     = 30   # 30–49 → NEUTRAL
                             # <30 → TALENT DRAIN

# ── Normalization ranges ──────────────────────────────────────────────────────
TICKET_EFFECT_MIN    = -0.20   # -20% → 0
TICKET_EFFECT_MAX    =  0.20   # +20% → 100
GUEST_EFFECT_MIN     = -0.20
GUEST_EFFECT_MAX     =  0.20
REBOOKING_EFFECT_MIN = -0.15   # -15 percentage points → 0
REBOOKING_EFFECT_MAX =  0.15   # +15 percentage points → 100

# ── Talent flow thresholds ────────────────────────────────────────────────────
TALENT_FLOW_DESTINATION_MIN =  3   # net_flow ≥ 3 → destination
TALENT_FLOW_SOURCE_MAX      = -3   # net_flow ≤ -3 → source


# ---------------------------------------------------------------------------
# Data container
# ---------------------------------------------------------------------------

@dataclass
class LocationEffectScore:
    """
    Computed Location Effect Score for a single location.

    Attributes:
        location_id:         Snake-case location identifier.
        effect_score:        Composite score 0–100.
        tier:                Classification string.
        emoji:               Display emoji for the tier.
        transfers_analyzed:  Number of confirmed transfers analyzed.
        avg_ticket_effect:   Mean pct change in avg ticket (ratio, e.g. 0.15 = +15%).
        guest_count_effect:  Mean pct change in guest count.
        rebooking_effect:    Mean pp change in rebooking rate (absolute, e.g. 0.07 = +7pp).
                             Defaults to 0.0 when data is absent.
        retention_rate:      Fraction of transferred-in stylists still at this location.
    """
    location_id:         str
    effect_score:        int
    tier:                str   # talent_accelerator | positive_impact | neutral | talent_drain
    emoji:               str
    transfers_analyzed:  int
    avg_ticket_effect:   float
    guest_count_effect:  float
    rebooking_effect:    float
    retention_rate:      float


# ---------------------------------------------------------------------------
# LocationEffectScorer
# ---------------------------------------------------------------------------

class LocationEffectScorer:
    """
    Calculates Location Effect Scores and powers Talent Graph queries.

    Usage:
        scorer = LocationEffectScorer()
        score  = scorer.calculate_location_effect_score("blaine")
        ranked = scorer.rank_locations_by_effect(["blaine", "farmington", ...])
    """

    # ------------------------------------------------------------------
    # Location Effect Score
    # ------------------------------------------------------------------

    def calculate_location_effect_score(
        self, location_id: str
    ) -> Optional[LocationEffectScore]:
        """
        Calculate the Location Effect Score for a single location.

        Aggregates performance deltas from all confirmed transfers INTO this
        location. Returns None if insufficient transfer data.

        Args:
            location_id: Snake-case location identifier.

        Returns:
            LocationEffectScore or None.
        """
        transfers_in = self._get_transfers_to_location(location_id)

        if not transfers_in:
            logger.debug(
                "calculate_location_effect_score: no transfers to %s → None",
                location_id,
            )
            return None

        impacts:          List[Dict] = []
        retained_count:   int        = 0

        for transfer in transfers_in:
            impact = self._get_transfer_impact(
                transfer.get("stylist_id", ""),
                transfer.get("transfer_id", transfer.get("id", "")),
            )
            if not impact:
                continue

            impacts.append(impact)

            # Retention: is stylist still at this location?
            master = self._get_master_record(transfer.get("stylist_id", ""))
            if master and master.get("current_location") == location_id:
                retained_count += 1

        if not impacts:
            logger.debug(
                "calculate_location_effect_score: no impact data for %s → None",
                location_id,
            )
            return None

        avg_ticket_effect  = statistics.mean(
            [i["impact"]["avg_ticket_pct"]    for i in impacts]
        )
        guest_count_effect = statistics.mean(
            [i["impact"]["guest_count_pct"]   for i in impacts]
        )
        # rebooking_change may not exist in current impact dicts — default 0.0
        rebooking_effect   = statistics.mean(
            [i["impact"].get("rebooking_change", 0.0) for i in impacts]
        )
        retention_rate     = (
            retained_count / len(transfers_in) if transfers_in else 0.0
        )

        score = self.calculate_composite_score(
            avg_ticket_effect,
            guest_count_effect,
            rebooking_effect,
            retention_rate,
        )
        tier, emoji = self._classify_score(score)

        return LocationEffectScore(
            location_id=        location_id,
            effect_score=       score,
            tier=               tier,
            emoji=              emoji,
            transfers_analyzed= len(impacts),
            avg_ticket_effect=  round(avg_ticket_effect, 4),
            guest_count_effect= round(guest_count_effect, 4),
            rebooking_effect=   round(rebooking_effect, 4),
            retention_rate=     round(retention_rate, 4),
        )

    def rank_locations_by_effect(
        self,
        location_ids: List[str],
    ) -> List[LocationEffectScore]:
        """
        Rank all supplied locations by their Effect Score, best to worst.

        Locations with insufficient data (None result) are excluded from the
        ranking — they need more transfers before scoring.

        Args:
            location_ids: List of snake-case location IDs to evaluate.

        Returns:
            Sorted list of LocationEffectScore objects (best first).
        """
        scores: List[LocationEffectScore] = []

        for loc in location_ids:
            result = self.calculate_location_effect_score(loc)
            if result:
                scores.append(result)

        scores.sort(key=lambda s: s.effect_score, reverse=True)
        return scores

    # ------------------------------------------------------------------
    # Composite score math
    # ------------------------------------------------------------------

    @staticmethod
    def calculate_composite_score(
        avg_ticket_effect:  float,
        guest_count_effect: float,
        rebooking_effect:   float,
        retention_rate:     float,
    ) -> int:
        """
        Calculate composite Location Effect Score (0–100).

        Each factor is normalised to a 0–100 sub-score, then weighted equally.

        Weights:
          25% avg ticket effect
          25% guest count effect
          25% rebooking effect
          25% retention rate
        """
        ticket_score   = LocationEffectScorer.normalize_to_100(
            avg_ticket_effect,  TICKET_EFFECT_MIN,    TICKET_EFFECT_MAX,
        )
        guest_score    = LocationEffectScorer.normalize_to_100(
            guest_count_effect, GUEST_EFFECT_MIN,     GUEST_EFFECT_MAX,
        )
        rebooking_score = LocationEffectScorer.normalize_to_100(
            rebooking_effect,   REBOOKING_EFFECT_MIN, REBOOKING_EFFECT_MAX,
        )
        retention_score = retention_rate * 100.0  # already 0–1

        composite = (
            ticket_score    * 0.25 +
            guest_score     * 0.25 +
            rebooking_score * 0.25 +
            retention_score * 0.25
        )
        return int(round(composite))

    @staticmethod
    def normalize_to_100(value: float, min_val: float, max_val: float) -> float:
        """
        Normalise a value to a 0–100 scale given known min/max boundaries.

        Values at or below min_val → 0.0
        Values at or above max_val → 100.0
        Values in-between → linear interpolation.

        Args:
            value:   Raw metric value (e.g. 0.15 for +15% ticket improvement).
            min_val: Lower boundary of expected range.
            max_val: Upper boundary of expected range.

        Returns:
            Float in [0.0, 100.0].
        """
        if max_val <= min_val:
            return 50.0  # degenerate range — neutral
        if value <= min_val:
            return 0.0
        if value >= max_val:
            return 100.0
        return ((value - min_val) / (max_val - min_val)) * 100.0

    @staticmethod
    def _classify_score(score: int):
        """
        Return (tier_string, emoji) for a composite score.

        Thresholds:
          ≥70 → talent_accelerator 🌟
          ≥50 → positive_impact    ✅
          ≥30 → neutral            🟡
          <30 → talent_drain       🔴
        """
        if score >= TIER_ACCELERATOR_MIN:
            return "talent_accelerator", "🌟"
        if score >= TIER_POSITIVE_MIN:
            return "positive_impact", "✅"
        if score >= TIER_NEUTRAL_MIN:
            return "neutral", "🟡"
        return "talent_drain", "🔴"

    # ------------------------------------------------------------------
    # Talent Graph queries
    # ------------------------------------------------------------------

    def analyze_talent_flow(self, location_ids: List[str]) -> Dict:
        """
        Identify which locations are talent sources vs destinations.

        Answers: does a location net-gain or net-lose talent through transfers?

        Args:
            location_ids: Full list of active location IDs.

        Returns:
            Dict keyed by location_id, each value:
            {
              'transfers_in':   int,
              'transfers_out':  int,
              'net_flow':       int,   # positive = destination, negative = source
              'classification': str,   # 'destination' | 'source' | 'balanced'
            }
        """
        all_transfers = self._get_all_transfers()

        flow: Dict[str, Dict] = {
            loc: {"transfers_in": 0, "transfers_out": 0, "net_flow": 0}
            for loc in location_ids
        }

        for transfer in all_transfers:
            frm = transfer.get("from_location", "")
            to  = transfer.get("to_location",   "")

            if frm in flow:
                flow[frm]["transfers_out"] += 1
            if to in flow:
                flow[to]["transfers_in"] += 1

        for loc, data in flow.items():
            net = data["transfers_in"] - data["transfers_out"]
            data["net_flow"] = net

            if net >= TALENT_FLOW_DESTINATION_MIN:
                data["classification"] = "destination"
            elif net <= TALENT_FLOW_SOURCE_MAX:
                data["classification"] = "source"
            else:
                data["classification"] = "balanced"

        return flow

    def find_common_career_paths(
        self, min_frequency: int = 3
    ) -> List[Dict]:
        """
        Find career paths (sequences of locations) taken by ≥ min_frequency stylists.

        Returns:
            List of:
            {
              'path':                   str,          # e.g. "farmington → blaine → roseville"
              'locations':              List[str],
              'frequency':              int,
              'stylists':               List[str],    # stylist_ids
              'avg_tenure_at_each':     List[float],  # weeks
              'performance_trajectory': str,          # 'ascending' | 'flat' | 'declining'
            }
            sorted by frequency (most common first).

        Stub — returns [] until Sheets data is available.
        """
        all_stylists = self._get_all_stylists()
        path_groups: Dict[str, List] = {}

        for stylist in all_stylists:
            history = stylist.get("transfer_history") or []
            locations = [stylist.get("original_location", "")]
            locations += [t.get("to", "") for t in sorted(
                history, key=lambda t: t.get("transfer_date", "")
            )]
            # Filter empty segments
            locations = [loc for loc in locations if loc]
            if len(locations) < 2:
                continue

            path_key = " → ".join(locations)
            if path_key not in path_groups:
                path_groups[path_key] = []
            path_groups[path_key].append(stylist.get("stylist_id", ""))

        results = []
        for path_key, stylist_ids in path_groups.items():
            if len(stylist_ids) < min_frequency:
                continue
            results.append({
                "path":       path_key,
                "locations":  path_key.split(" → "),
                "frequency":  len(stylist_ids),
                "stylists":   stylist_ids,
                # tenure + trajectory analysis: future implementation
                "avg_tenure_at_each":     [],
                "performance_trajectory": "unknown",
            })

        results.sort(key=lambda r: r["frequency"], reverse=True)
        return results

    def analyze_manager_effect(self, location_id: str) -> Optional[Dict]:
        """
        Isolate a location manager's effect on stylist development.

        Compares incoming stylists' performance before and after arrival.
        Controls for stylist quality by using pre-transfer baseline as reference.

        Returns:
            {
              'location_id':          str,
              'stylists_analyzed':    int,
              'avg_ticket_effect':    float,
              'guest_count_effect':   float,
              'rebooking_effect':     float,
              'interpretation':       str,
            }
            or None if insufficient data.

        Stub — returns None until Sheets data is available.
        """
        transfers_in = self._get_transfers_to_location(location_id)
        deltas: List[Dict] = []

        for transfer in transfers_in:
            impact = self._get_transfer_impact(
                transfer.get("stylist_id", ""),
                transfer.get("transfer_id", transfer.get("id", "")),
            )
            if not impact:
                continue
            deltas.append({
                "stylist_id":       transfer.get("stylist_id"),
                "pre_performance":  impact.get("pre_transfer", {}),
                "post_performance": impact.get("post_transfer", {}),
                "delta":            impact.get("impact", {}),
            })

        if not deltas:
            logger.debug("analyze_manager_effect: no data for %s", location_id)
            return None

        avg_ticket_effect  = statistics.mean(
            [d["delta"].get("avg_ticket_pct", 0.0)    for d in deltas]
        )
        guest_count_effect = statistics.mean(
            [d["delta"].get("guest_count_pct", 0.0)   for d in deltas]
        )
        rebooking_effect   = statistics.mean(
            [d["delta"].get("rebooking_change", 0.0)  for d in deltas]
        )

        return {
            "location_id":          location_id,
            "stylists_analyzed":    len(deltas),
            "avg_ticket_effect":    round(avg_ticket_effect, 4),
            "guest_count_effect":   round(guest_count_effect, 4),
            "rebooking_effect":     round(rebooking_effect, 4),
            "interpretation":       self._interpret_manager_effect(
                avg_ticket_effect, guest_count_effect
            ),
        }

    @staticmethod
    def _interpret_manager_effect(ticket: float, guests: float) -> str:
        """
        Interpret combined metric deltas into a plain-English classification.
        """
        if ticket >= 0.10 and guests >= 0.10:
            return "TALENT DEVELOPER — stylists significantly improve under this management"
        if ticket >= 0.05 or guests >= 0.05:
            return "POSITIVE IMPACT — modest performance gains observed"
        if ticket <= -0.10 or guests <= -0.10:
            return "WARNING — stylists decline under this management"
        return "NEUTRAL — minimal measurable impact"

    # ------------------------------------------------------------------
    # Sheets I/O stubs
    # ------------------------------------------------------------------

    def _get_transfers_to_location(self, location_id: str) -> List[Dict]:
        """
        Fetch all confirmed transfers TO a location (with stylist_ids).
        Stub — returns [] until Sheets read is wired.
        """
        logger.debug(
            "LocationEffectScorer: transfers-to stub for %s → []", location_id
        )
        return []

    def _get_transfer_impact(
        self, stylist_id: str, transfer_id: str
    ) -> Optional[Dict]:
        """
        Fetch a pre-computed transfer impact dict for a specific transfer.
        Stub — returns None until Sheets read is wired.
        """
        logger.debug(
            "LocationEffectScorer: transfer-impact stub for %s / %s → None",
            stylist_id, transfer_id,
        )
        return None

    def _get_master_record(self, stylist_id: str) -> Optional[Dict]:
        """
        Fetch master record for a stylist (to check current_location for retention).
        Stub — returns None until Sheets read is wired.
        """
        logger.debug(
            "LocationEffectScorer: master record stub for %s → None", stylist_id
        )
        return None

    def _get_all_stylists(self) -> List[Dict]:
        """
        Fetch all stylist master records (for career path analysis).
        Stub — returns [] until Sheets read is wired.
        """
        logger.debug("LocationEffectScorer: all-stylists stub → []")
        return []

    def _get_all_transfers(self) -> List[Dict]:
        """
        Fetch all confirmed transfers across the network (for talent flow).
        Stub — returns [] until Sheets read is wired.
        """
        logger.debug("LocationEffectScorer: all-transfers stub → []")
        return []
