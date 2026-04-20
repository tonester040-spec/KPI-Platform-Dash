"""
Trust Layer — Confidence Scorer
Aggregates validation check results into a 0–100 confidence score.

Tier thresholds (Phase 3B enhanced):
  🟢 High     : 85–100%  — auto-approve (reduced from 95% to be more pragmatic)
  🟡 Moderate : 70–84%   — review warnings before use
  🔴 Low      : <70%     — manual review required; do not write

The score starts at 100 and deducts points for each failed/warned check
based on the check's 'severity' value.
"""

from dataclasses import dataclass, field
from typing import List

from trust_layer.severity import CompletenessCheck


@dataclass
class ConfidenceScore:
    """Confidence score for a single location's data batch."""
    score:   int                      # 0–100
    tier:    str                      # 'high' | 'moderate' | 'low'
    checks:  List[CompletenessCheck]  # All checks that contributed
    summary: str                      # One-line human summary

    @property
    def emoji(self) -> str:
        return {"high": "🟢", "moderate": "🟡", "low": "🔴"}.get(self.tier, "⚪")

    @property
    def auto_approve(self) -> bool:
        return self.tier == "high"

    @property
    def requires_review(self) -> bool:
        # Both moderate and low require human review before use.
        # Only 'high' batches auto-approve.
        return self.tier in ("moderate", "low")


class ConfidenceScorer:
    """
    Calculate confidence score from a list of CompletenessCheck results.

    Usage:
        scorer = ConfidenceScorer()
        score  = scorer.calculate_score(checks)
        print(score.emoji, score.score, score.summary)
    """

    # Tier thresholds (Phase 3B enhanced — stricter auto-approval)
    HIGH_THRESHOLD     = 85   # >= 85% → auto-approve
    MODERATE_THRESHOLD = 70   # >= 70% → review warnings

    def calculate_score(self, checks: List[CompletenessCheck]) -> ConfidenceScore:
        """
        Calculate confidence score from validation checks.

        Score = 100 - sum(severity for all non-passing checks).
        Floored at 0.

        Args:
            checks: List of CompletenessCheck results from all validators.

        Returns:
            ConfidenceScore with tier classification and summary.
        """
        if not checks:
            return ConfidenceScore(
                score=100,
                tier="high",
                checks=[],
                summary="No checks performed",
            )

        deductions = sum(
            c.severity for c in checks if c.status in ("warn", "fail")
        )
        score = max(0, 100 - deductions)

        # Tier classification
        if score >= self.HIGH_THRESHOLD:
            tier = "high"
        elif score >= self.MODERATE_THRESHOLD:
            tier = "moderate"
        else:
            tier = "low"

        # Summary line
        pass_count = sum(1 for c in checks if c.status == "pass")
        warn_count = sum(1 for c in checks if c.status == "warn")
        fail_count = sum(1 for c in checks if c.status == "fail")
        summary = (
            f"{pass_count} passed, {warn_count} warning(s), {fail_count} failure(s)"
        )

        return ConfidenceScore(
            score=score,
            tier=tier,
            checks=checks,
            summary=summary,
        )

    def aggregate_batch_score(
        self, location_scores: List[ConfidenceScore]
    ) -> ConfidenceScore:
        """
        Aggregate per-location scores into a single batch-level score.

        Uses the minimum score (the weakest link drives the batch decision).
        Summary reports per-tier counts.
        """
        if not location_scores:
            return ConfidenceScore(
                score=100,
                tier="high",
                checks=[],
                summary="No locations scored",
            )

        min_score = min(s.score for s in location_scores)

        # Tier from min score
        if min_score >= self.HIGH_THRESHOLD:
            tier = "high"
        elif min_score >= self.MODERATE_THRESHOLD:
            tier = "moderate"
        else:
            tier = "low"

        high_count     = sum(1 for s in location_scores if s.tier == "high")
        moderate_count = sum(1 for s in location_scores if s.tier == "moderate")
        low_count      = sum(1 for s in location_scores if s.tier == "low")
        avg_score      = sum(s.score for s in location_scores) / len(location_scores)

        summary = (
            f"{len(location_scores)} locations — "
            f"avg {avg_score:.0f}%, min {min_score}% | "
            f"🟢 {high_count}  🟡 {moderate_count}  🔴 {low_count}"
        )

        # Flatten all checks from all locations
        all_checks = [c for score in location_scores for c in score.checks]

        return ConfidenceScore(
            score=min_score,
            tier=tier,
            checks=all_checks,
            summary=summary,
        )
