"""
scripts/backfill/render_review.py
─────────────────────────────────
Pretty-prints per-location KPI tables and validation summaries for
Tony's eyeball review during a backfill dry-run.

All output goes to stdout. Designed to fit in a normal terminal window
(no wider than 120 chars).
"""

from __future__ import annotations


def render_kpi_table(rows: list[dict], *, title: str = "") -> str:
    """Build a readable per-location table from DATA_MONTHLY row dicts."""
    if not rows:
        return f"{title}\n  (no rows)\n"

    lines: list[str] = []
    if title:
        lines.append(title)
        lines.append("=" * len(title))

    header = (
        f"{'Location':<15s} {'P':1s} {'Guests':>6s} "
        f"{'Service$':>11s} {'Product$':>10s} {'Total$':>11s} "
        f"{'PPG':>5s} {'PPH':>5s} {'AT':>6s} "
        f"{'Wax%':>5s} {'Col%':>5s} {'Trt%':>5s}"
    )
    lines.append(header)
    lines.append("-" * len(header))

    for r in rows:
        plat = "Z" if r.get("platform") == "zenoti" else "S"
        lines.append(
            f"{r.get('loc_name', ''):<15s} {plat:1s} {int(r.get('guests') or 0):>6d} "
            f"${float(r.get('service') or 0):>10,.2f} ${float(r.get('product') or 0):>9,.2f} "
            f"${float(r.get('total_sales') or 0):>10,.2f} "
            f"{float(r.get('ppg') or 0):>5.2f} {float(r.get('pph') or 0):>5.2f} "
            f"${float(r.get('avg_ticket') or 0):>5.2f} "
            f"{float(r.get('wax_pct') or 0) * 100:>4.1f}% "
            f"{float(r.get('color_pct') or 0) * 100:>4.1f}% "
            f"{float(r.get('treat_pct') or 0) * 100:>4.1f}%"
        )

    # Totals row
    sums = {
        "guests": sum(int(r.get("guests") or 0) for r in rows),
        "service": sum(float(r.get("service") or 0) for r in rows),
        "product": sum(float(r.get("product") or 0) for r in rows),
        "total": sum(float(r.get("total_sales") or 0) for r in rows),
    }
    lines.append("-" * len(header))
    lines.append(
        f"{'TOTAL':<15s} {' ':1s} {sums['guests']:>6d} "
        f"${sums['service']:>10,.2f} ${sums['product']:>9,.2f} "
        f"${sums['total']:>10,.2f}"
    )
    lines.append("")
    return "\n".join(lines)


def render_validation_summary(issues: list[dict], *, title: str = "Validation") -> str:
    """Render a validation summary with issues grouped by severity."""
    lines: list[str] = []
    lines.append(title)
    lines.append("=" * len(title))

    by_sev: dict[str, list[dict]] = {"error": [], "warning": [], "info": []}
    for i in issues:
        sev = i.get("severity", "info")
        by_sev.setdefault(sev, []).append(i)

    err_count = len(by_sev["error"])
    warn_count = len(by_sev["warning"])
    info_count = len(by_sev["info"])

    if err_count == 0 and warn_count == 0 and info_count == 0:
        lines.append("  All clean — no issues raised.")
        lines.append("")
        return "\n".join(lines)

    lines.append(
        f"  {err_count} error(s)   "
        f"{warn_count} warning(s)   "
        f"{info_count} info"
    )
    lines.append("")

    for sev_name, sev_label in (("error", "ERRORS"), ("warning", "WARNINGS"), ("info", "INFO")):
        bucket = by_sev[sev_name]
        if not bucket:
            continue
        lines.append(f"  {sev_label}:")
        for i in bucket:
            lines.append(
                f"    [{i.get('code', '?')}] {i.get('location', '?')}: {i.get('message', '')}"
            )
        lines.append("")

    return "\n".join(lines)


def render_cross_check(diff_findings: list[dict], *, title: str = "Cross-check: tracker vs xlsx") -> str:
    """Render the tracker-vs-xlsx diff findings (March only)."""
    return render_validation_summary(diff_findings, title=title)
