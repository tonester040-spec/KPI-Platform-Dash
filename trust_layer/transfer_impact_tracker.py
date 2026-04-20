"""
Trust Layer — Transfer Impact Tracker  (Phase 3B Addendum — Strategic Unlock)
Measures the performance impact of transfers and surfaces organizational intelligence.

This module is the analytical "unlock" that separates KPI from a reporting tool:
with stable stylist identities across locations, you can now answer questions no
POS system provides:

  • Which locations develop talent vs burn it out?
  • What happens to stylist performance after a transfer?
  • Where do top performers come from?
  • What career paths are common across the network?

Architecture
------------
All data queries are stubbed (return None / empty lists) until the Sheets read
interface is wired.  The computation logic is fully functional once data is present.

Output interpretation:
  avg_ticket_pct change ≥ 10% AND guest_count_pct ≥ 10%  → TALENT DEVELOPER
  either metric ≥  5%                                     → POSITIVE IMPACT
  either metric ≤ -10%                                    → WARNING
  otherwise                                               → NEUTRAL
"""

import logging
import statistics
from datetime import datetime, timedelta
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Transfer impact calculation
# ---------------------------------------------------------------------------

class TransferImpactTracker:
    """
    Calculates performance deltas before and after transfers, and generates
    network-level organizational intelligence.

    Usage:
        tracker = TransferImpactTracker()
        impact  = tracker.calculate_transfer_impact(stylist_id, transfer_event)
        org_intel = tracker.generate_organizational_intelligence()
    """

    LOOKBACK_WEEKS  = 4   # Weeks of data before/after transfer to compare
    MIN_TICKET_GAIN = 0.10  # 10%+ avg ticket improvement → talent developer threshold
    MIN_GUEST_GAIN  = 0.10  # 10%+ guest count improvement → talent developer threshold
    DECLINE_THRESHOLD = -0.10  # -10% or worse → warning

    # ------------------------------------------------------------------
    # Per-transfer impact
    # ------------------------------------------------------------------

    def calculate_transfer_impact(
        self,
        stylist_id: str,
        transfer:   Dict,
    ) -> Optional[Dict]:
        """
        Calculate performance delta for a single transfer event.

        Compares LOOKBACK_WEEKS before transfer at from_location to
        LOOKBACK_WEEKS after transfer at to_location.

        Args:
            stylist_id: Stable stylist ID.
            transfer:   Transfer event dict (must contain transfer_date,
                        from_location, to_location).

        Returns:
            Impact dict or None if insufficient data.

        Impact dict structure:
            {
              'transfer_id':   str,
              'from':          str,
              'to':            str,
              'transfer_date': str,
              'pre_transfer':  { avg_ticket, guest_count_weekly, product_pct },
              'post_transfer': { avg_ticket, guest_count_weekly, product_pct },
              'impact': {
                  'avg_ticket_change':      float (absolute $),
                  'avg_ticket_pct':         float (ratio),
                  'guest_count_change':     float (absolute),
                  'guest_count_pct':        float (ratio),
                  'product_pct_change':     float (percentage points),
              }
            }
        """
        transfer_date = datetime.fromisoformat(transfer["transfer_date"]).date()
        from_loc      = transfer["from_location"]
        to_loc        = transfer["to_location"]

        pre_start  = (transfer_date - timedelta(weeks=self.LOOKBACK_WEEKS)).isoformat()
        post_end   = (transfer_date + timedelta(weeks=self.LOOKBACK_WEEKS)).isoformat()

        pre_data  = self._get_performance_history(
            stylist_id, from_loc, pre_start, transfer["transfer_date"]
        )
        post_data = self._get_performance_history(
            stylist_id, to_loc, transfer["transfer_date"], post_end
        )

        if not pre_data or not post_data:
            logger.debug(
                "calculate_transfer_impact: insufficient data for %s (%s → %s)",
                stylist_id, from_loc, to_loc,
            )
            return None

        pre_avg  = self._average_performance(pre_data)
        post_avg = self._average_performance(post_data)

        def _pct_change(pre, post):
            if not pre or pre == 0:
                return 0.0
            return round((post - pre) / pre, 4)

        impact = {
            "avg_ticket_change":   round(post_avg["avg_ticket"] - pre_avg["avg_ticket"], 2),
            "avg_ticket_pct":      _pct_change(pre_avg["avg_ticket"], post_avg["avg_ticket"]),
            "guest_count_change":  round(post_avg["guest_count"] - pre_avg["guest_count"], 1),
            "guest_count_pct":     _pct_change(pre_avg["guest_count"], post_avg["guest_count"]),
            "product_pct_change":  round(
                post_avg["product_pct"] - pre_avg["product_pct"], 4
            ),
        }

        transfer_id = (
            f"{from_loc}_to_{to_loc}_{transfer['transfer_date']}"
        )

        return {
            "transfer_id":   transfer_id,
            "stylist_id":    stylist_id,
            "from":          from_loc,
            "to":            to_loc,
            "transfer_date": transfer["transfer_date"],
            "pre_transfer":  pre_avg,
            "post_transfer": post_avg,
            "impact":        impact,
        }

    # ------------------------------------------------------------------
    # Location-level aggregation
    # ------------------------------------------------------------------

    def analyze_location_transfer_impact(self, location_id: str) -> Optional[Dict]:
        """
        Aggregate transfer impact for all transfers INTO a location.

        Answers: does this location improve stylist performance?

        Returns:
            {
              'location_id':          str,
              'transfers_analyzed':   int,
              'avg_ticket_impact':    float (mean pct change),
              'guest_count_impact':   float (mean pct change),
              'product_pct_impact':   float (mean pp change),
              'interpretation':       str,
            }
            or None if no impact data available.
        """
        transfers_in = self._get_transfers_to_location(location_id)

        impacts: List[Dict] = []
        for transfer in transfers_in:
            impact = self.calculate_transfer_impact(
                transfer["stylist_id"], transfer
            )
            if impact:
                impacts.append(impact)

        if not impacts:
            logger.debug(
                "analyze_location_transfer_impact: no data for %s", location_id
            )
            return None

        ticket_changes  = [i["impact"]["avg_ticket_pct"]    for i in impacts]
        guest_changes   = [i["impact"]["guest_count_pct"]   for i in impacts]
        product_changes = [i["impact"]["product_pct_change"] for i in impacts]

        avg_ticket  = statistics.mean(ticket_changes)
        avg_guest   = statistics.mean(guest_changes)
        avg_product = statistics.mean(product_changes)

        return {
            "location_id":        location_id,
            "transfers_analyzed": len(impacts),
            "avg_ticket_impact":  round(avg_ticket, 4),
            "guest_count_impact": round(avg_guest, 4),
            "product_pct_impact": round(avg_product, 4),
            "interpretation":     self._interpret_impact(ticket_changes, guest_changes),
        }

    @staticmethod
    def _interpret_impact(
        ticket_changes: List[float],
        guest_changes:  List[float],
    ) -> str:
        """
        Classify location impact based on average performance deltas.

        Returns:
            'TALENT DEVELOPER'     — significant gains across both metrics
            'POSITIVE IMPACT'      — modest improvement in either metric
            'WARNING'              — decline in either metric
            'NEUTRAL'              — no meaningful change
        """
        avg_ticket = statistics.mean(ticket_changes) if ticket_changes else 0.0
        avg_guest  = statistics.mean(guest_changes)  if guest_changes  else 0.0

        if avg_ticket >= 0.10 and avg_guest >= 0.10:
            return "TALENT DEVELOPER — stylists significantly improve after transferring here"
        if avg_ticket >= 0.05 or avg_guest >= 0.05:
            return "POSITIVE IMPACT — modest performance gains after transfer"
        if avg_ticket <= -0.10 or avg_guest <= -0.10:
            return "WARNING — stylists decline after transferring here"
        return "NEUTRAL — minimal impact on stylist performance"

    # ------------------------------------------------------------------
    # Network-level organizational intelligence
    # ------------------------------------------------------------------

    def generate_organizational_intelligence(self) -> Dict:
        """
        Generate network-level insights from all transfer data.

        Returns a structured dict ready to power the Organizational Intelligence
        dashboard section:

            {
              'talent_flow': {
                'source_locations':      List[str],   # Net exporters
                'destination_locations': List[str],   # Net importers
                'net_talent_change':     Dict[str, int],
              },
              'location_development': {
                '<location_id>': { ... analyze_location_transfer_impact result ... },
              },
              'career_paths': [
                {
                  'path':                  List[str],
                  'frequency':             int,
                  'avg_tenure_at_each':    List[float],  # weeks
                  'performance_trajectory': str,
                }
              ],
            }

        Stub — data queries return empty; computation is fully wired.
        """
        all_transfers = self._get_all_transfers()
        all_locations = self._get_all_location_ids()

        # ── Talent flow ──────────────────────────────────────────────
        transfer_matrix: Dict[str, Dict[str, int]] = {}  # [from][to] = count
        for t in all_transfers:
            frm = t.get("from_location", "")
            to  = t.get("to_location",   "")
            transfer_matrix.setdefault(frm, {})
            transfer_matrix[frm][to] = transfer_matrix[frm].get(to, 0) + 1

        net_change: Dict[str, int] = {loc: 0 for loc in all_locations}
        for frm, destinations in transfer_matrix.items():
            for to, count in destinations.items():
                net_change[frm] = net_change.get(frm, 0) - count
                net_change[to]  = net_change.get(to,  0) + count

        source_locations      = [loc for loc, n in net_change.items() if n < 0]
        destination_locations = [loc for loc, n in net_change.items() if n > 0]

        # ── Location development ────────────────────────────────────
        location_development = {}
        for loc in all_locations:
            result = self.analyze_location_transfer_impact(loc)
            if result:
                location_development[loc] = result

        # ── Career paths ────────────────────────────────────────────
        career_paths = self._identify_common_career_paths(all_transfers)

        return {
            "talent_flow": {
                "source_locations":      source_locations,
                "destination_locations": destination_locations,
                "net_talent_change":     net_change,
            },
            "location_development": location_development,
            "career_paths":         career_paths,
        }

    @staticmethod
    def _identify_common_career_paths(transfers: List[Dict]) -> List[Dict]:
        """
        Group transfer sequences into common multi-step career paths.

        Stub — returns empty list until Sheets data is available.
        """
        # Future: group by stylist_id, sort by date, extract location sequences,
        # count common sequences, calculate average tenure at each stop.
        return []

    # ------------------------------------------------------------------
    # Performance computation helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _average_performance(records: List[Dict]) -> Dict:
        """
        Average a list of weekly performance records.

        Returns dict with avg_ticket, guest_count, product_pct keys.
        """
        if not records:
            return {"avg_ticket": 0.0, "guest_count": 0.0, "product_pct": 0.0}

        def _mean(key):
            vals = [r.get(key, 0) or 0 for r in records]
            return round(statistics.mean(vals), 2) if vals else 0.0

        return {
            "avg_ticket":         _mean("avg_ticket"),
            "guest_count_weekly": _mean("guest_count"),
            "guest_count":        _mean("guest_count"),
            "product_pct":        _mean("product_pct"),
        }

    # ------------------------------------------------------------------
    # Sheets I/O stubs
    # ------------------------------------------------------------------

    def _get_performance_history(
        self,
        stylist_id:  str,
        location_id: str,
        start_date:  str,
        end_date:    str,
    ) -> List[Dict]:
        """
        Fetch weekly performance records for a stylist at a location within a date range.
        Stub — returns [] until Sheets read is wired.
        """
        logger.debug(
            "TransferImpactTracker: perf history stub for %s @ %s → []",
            stylist_id, location_id,
        )
        return []

    def _get_transfers_to_location(self, location_id: str) -> List[Dict]:
        """
        Fetch all confirmed transfers TO a location (with their stylist_ids).
        Stub — returns [] until Sheets read is wired.
        """
        logger.debug(
            "TransferImpactTracker: transfers-to stub for %s → []", location_id
        )
        return []

    def _get_all_transfers(self) -> List[Dict]:
        """Fetch all confirmed transfers across the network. Stub → []."""
        logger.debug("TransferImpactTracker: all-transfers stub → []")
        return []

    def _get_all_location_ids(self) -> List[str]:
        """Fetch all active location IDs. Stub → []."""
        logger.debug("TransferImpactTracker: all-location-ids stub → []")
        return []
