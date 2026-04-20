"""
Trust Layer — Severity Levels and Core Data Types
Shared across all trust_layer modules.

CompletenessCheck is the universal result object: every validator returns
a list of these, and the ConfidenceScorer aggregates them into a score.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import List


class Severity(Enum):
    """Standardized severity levels for validation issues."""

    INFO    = 0   # 🟢 Informational — no points deducted
    WARN    = 1   # 🟡 Warning — points deducted; review recommended
    BLOCKER = 2   # 🔴 Critical — stops processing

    @property
    def emoji(self) -> str:
        return {
            Severity.INFO:    "🟢",
            Severity.WARN:    "🟡",
            Severity.BLOCKER: "🔴",
        }[self]

    @property
    def label(self) -> str:
        return {
            Severity.INFO:    "INFO",
            Severity.WARN:    "WARNING",
            Severity.BLOCKER: "BLOCKER",
        }[self]


@dataclass
class CompletenessCheck:
    """
    Result of a single validation check.

    Attributes:
        name     : Short identifier for the check (e.g. "Category Completeness").
        status   : One of 'pass', 'warn', 'fail'.
        message  : Human-readable description of what was found.
        severity : Integer points to deduct from confidence score (0 = no deduction).
    """
    name:     str
    status:   str   # 'pass' | 'warn' | 'fail'
    message:  str
    severity: int   # points deducted from confidence score

    def __post_init__(self):
        if self.status not in ("pass", "warn", "fail"):
            raise ValueError(f"Invalid status '{self.status}' — must be 'pass', 'warn', or 'fail'")
        if self.severity < 0:
            raise ValueError(f"severity must be >= 0, got {self.severity}")
