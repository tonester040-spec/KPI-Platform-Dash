#!/usr/bin/env python3
"""
core/git_pusher.py
KPI Platform — commits and pushes the docs/ folder to GitHub.

SAFETY: Only stages files in docs/. Never touches the repo root,
scripts/, core/, config/, or any file containing credentials.

DRY_RUN=true → logs what would happen without actually committing.
"""

import subprocess
import logging
import datetime
from pathlib import Path

log = logging.getLogger(__name__)


def _run(cmd: list[str], cwd: Path, check: bool = True) -> tuple[int, str, str]:
    """Run a git command. Returns (returncode, stdout, stderr)."""
    result = subprocess.run(
        cmd,
        cwd=cwd,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.stdout.strip():
        log.debug("git stdout: %s", result.stdout.strip())
    if result.stderr.strip():
        log.debug("git stderr: %s", result.stderr.strip())
    if check and result.returncode != 0:
        raise RuntimeError(
            f"Git command failed: {' '.join(cmd)}\n"
            f"Exit code: {result.returncode}\n"
            f"stderr: {result.stderr.strip()}"
        )
    return result.returncode, result.stdout.strip(), result.stderr.strip()


def push_dashboard(
    repo_root: Path,
    week_ending: str = "",
    dry_run: bool = False,
) -> bool:
    """
    Stage docs/ only, commit, and push to main.
    Returns True on success.
    """
    if not week_ending:
        week_ending = datetime.date.today().strftime("%Y-%m-%d")

    commit_msg = f"KPI auto-update: week ending {week_ending}"

    if dry_run:
        log.info("DRY RUN: Would commit and push docs/ with message: '%s'", commit_msg)
        return True

    # Check git status of docs/ only
    _, status_out, _ = _run(["git", "status", "--short", "docs/"], repo_root)
    if not status_out.strip():
        log.info("No changes in docs/ — nothing to commit")
        return True

    log.info("Changes detected in docs/:\n%s", status_out)

    # Stage ONLY docs/
    _run(["git", "add", "docs/"], repo_root)
    log.info("Staged docs/")

    # Verify nothing outside docs/ is staged
    _, staged_out, _ = _run(["git", "diff", "--cached", "--name-only"], repo_root)
    bad_files = [f for f in staged_out.splitlines() if not f.startswith("docs/")]
    if bad_files:
        # Emergency unstage — never commit credentials or non-docs files
        _run(["git", "reset", "HEAD"], repo_root)
        raise RuntimeError(
            f"SAFETY CHECK FAILED: Files outside docs/ were staged: {bad_files}. "
            f"Nothing was committed. Review git status and retry."
        )

    # Commit
    _run(["git", "commit", "-m", commit_msg], repo_root)
    log.info("Committed: %s", commit_msg)

    # Pull --rebase first in case other workflows pushed since we checked out
    # (static.yml deploy runs concurrently and can create commits on main)
    rc, _, stderr = _run(["git", "pull", "--rebase", "origin", "main"], repo_root, check=False)
    if rc != 0:
        log.warning("git pull --rebase had issues (rc=%d): %s — attempting push anyway", rc, stderr)

    # Push to main
    _run(["git", "push", "origin", "main"], repo_root)
    log.info("Pushed to origin/main")

    return True
