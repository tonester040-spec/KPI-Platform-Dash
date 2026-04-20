"""
Trust Layer — Integrity Reporter
Generates human-readable data integrity reports for batch processing results.

Two formats:
  generate_report()       — full multi-location report (for logs / Karissa's review)
  generate_email_summary() — short notification (for automated email alerts)
"""

from typing import Dict, List

from trust_layer.confidence_scorer import ConfidenceScore, ConfidenceScorer


class IntegrityReporter:
    """
    Generates human-readable data integrity reports.

    Expects each batch_result dict to contain a 'trust_checks' key
    (list of CompletenessCheck) and a 'location' key.
    """

    def generate_report(self, batch_results: List[Dict]) -> str:
        """
        Generate a complete per-location integrity report.

        Args:
            batch_results: List of merged location dicts, each containing
                           'trust_checks' (List[CompletenessCheck]) and 'location'.

        Returns:
            Formatted multi-line string ready for display or logging.
        """
        scorer = ConfidenceScorer()
        lines: List[str] = []

        # ── Header ──────────────────────────────────────────────────────
        lines += [
            "=" * 70,
            "📊  DATA INTEGRITY REPORT",
            "=" * 70,
            "",
        ]

        location_scores: List[ConfidenceScore] = []

        # ── Per-location scores ──────────────────────────────────────────
        for loc in batch_results:
            checks     = loc.get("trust_checks", [])
            confidence = scorer.calculate_score(checks)
            location_scores.append(confidence)

            loc_name = loc.get("location", "Unknown Location")
            period   = loc.get("period", {})
            period_str = (
                f"{period.get('start_date', '?')} → {period.get('end_date', '?')}"
                if period else ""
            )

            lines.append(
                f"{confidence.emoji}  {loc_name.upper()}"
                + (f"  ({period_str})" if period_str else "")
            )
            lines.append(f"    Confidence: {confidence.score}%")
            lines.append(f"    {confidence.summary}")
            lines.append("")

            # Individual check lines
            for check in confidence.checks:
                if check.status == "pass":
                    prefix = "   ✓"
                elif check.status == "warn":
                    prefix = "   ⚠"
                else:
                    prefix = "   ✗"
                # Indent multi-line messages
                msg = check.message.replace("\n", "\n      ")
                lines.append(f"{prefix}  {check.name}: {msg}")

            lines += ["", "-" * 70, ""]

        # ── Batch summary ────────────────────────────────────────────────
        batch_score = scorer.aggregate_batch_score(location_scores)
        high_count     = sum(1 for s in location_scores if s.tier == "high")
        moderate_count = sum(1 for s in location_scores if s.tier == "moderate")
        low_count      = sum(1 for s in location_scores if s.tier == "low")

        lines += [
            "BATCH SUMMARY",
            f"  Locations processed  : {len(batch_results)}",
            f"  Minimum confidence   : {batch_score.score}%",
            f"  🟢 High confidence   : {high_count}",
            f"  🟡 Moderate          : {moderate_count}",
            f"  🔴 Low confidence    : {low_count}",
            "",
        ]

        # ── Overall verdict ──────────────────────────────────────────────
        if batch_score.tier == "high":
            lines.append("✅  HIGH CONFIDENCE — Data ready for decision-making")
        elif batch_score.tier == "moderate":
            lines.append("⚠️   MODERATE CONFIDENCE — Review warnings before use")
        else:
            lines.append("🚨  LOW CONFIDENCE — Manual review strongly recommended")

        lines += ["", "=" * 70]

        return "\n".join(lines)

    def generate_email_summary(self, batch_results: List[Dict]) -> str:
        """
        Generate a short email-friendly summary for automated notifications.

        Args:
            batch_results: Same format as generate_report().

        Returns:
            Short multi-line string suitable for email body or Slack message.
        """
        scorer = ConfidenceScorer()
        all_scores = [
            scorer.calculate_score(loc.get("trust_checks", []))
            for loc in batch_results
        ]
        batch_score = scorer.aggregate_batch_score(all_scores)

        total_warnings = sum(
            sum(1 for c in s.checks if c.status == "warn")
            for s in all_scores
        )
        total_failures = sum(
            sum(1 for c in s.checks if c.status == "fail")
            for s in all_scores
        )

        if batch_score.tier == "high":
            status_emoji = "✅"
            status_text  = "HIGH CONFIDENCE — auto-approved"
        elif batch_score.tier == "moderate":
            status_emoji = "⚠️"
            status_text  = "MODERATE CONFIDENCE — review recommended"
        else:
            status_emoji = "🚨"
            status_text  = "LOW CONFIDENCE — manual review required"

        # Lowest-scoring location (most likely culprit)
        worst = min(all_scores, key=lambda s: s.score)
        worst_name = (
            batch_results[all_scores.index(worst)].get("location", "?")
            if batch_results else "?"
        )

        return (
            f"{status_emoji}  Weekly Data Upload\n\n"
            f"Status     : {status_text}\n"
            f"Locations  : {len(batch_results)}\n"
            f"Min score  : {batch_score.score}% ({worst_name})\n"
            f"Warnings   : {total_warnings}\n"
            f"Failures   : {total_failures}\n\n"
            f"View full integrity report in the dashboard."
        )

    def generate_decision_prompt(self, batch_score: ConfidenceScore) -> str:
        """
        Generate a user-facing decision prompt for moderate-confidence batches.

        Args:
            batch_score: Aggregated batch ConfidenceScore.

        Returns:
            Multi-line string explaining the situation and asking for a decision.
        """
        return (
            f"\n{'─' * 60}\n"
            f"⚠️  BATCH REQUIRES REVIEW ({batch_score.score}% confidence)\n"
            f"{'─' * 60}\n"
            f"{batch_score.summary}\n\n"
            f"Choose an action:\n"
            f"  [A] Approve  — write data to Sheets despite warnings\n"
            f"  [R] Reject   — discard batch, fix issues and re-upload\n"
            f"  [S] Skip     — skip for now, come back later\n\n"
            f"Enter choice (A/R/S): "
        )
