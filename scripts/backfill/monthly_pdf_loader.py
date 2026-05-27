"""
scripts/backfill/monthly_pdf_loader.py
──────────────────────────────────────
Parses monthly POS Salon Dashboard PDFs (Zenoti + Salon Ultimate) and emits
DATA_MONTHLY-ready row dicts.

The v2 parsers (pdf_zenoti_v2 / pdf_salon_ultimate_v2) were validated
2026-05-26 against weekly and monthly PDFs. Both shapes parse cleanly
(same field layout, different date range). For monthly inputs we just trust
the parser's ``karissa`` block and skip the only weekly-specific KPI
(``projection_eom``).

Validation per PDF:
  1. Location resolves to a known config entry.
  2. ``flags`` list is empty (or contains only informational
     ``GUEST_COUNT_MISMATCH`` — Apple Valley's known spec-correct case).
  3. Stylist sum reconciliation: sum(emp.net_service) ≈ raw.service_net
     within ``tolerance`` (default $0.01). Zenoti always reconciles
     exactly; SU may not always have employee data.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from config.locations import normalize_location, LOCATION_POS_MAP
from parsers import pdf_salon_ultimate_v2, pdf_zenoti_v2

log = logging.getLogger(__name__)


INFORMATIONAL_FLAGS = {"GUEST_COUNT_MISMATCH"}
DEFAULT_RECONCILIATION_TOLERANCE = 0.01


@dataclass
class ValidationIssue:
    location: str
    severity: str  # 'error' | 'warning' | 'info'
    code: str
    message: str
    details: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "location": self.location,
            "severity": self.severity,
            "code": self.code,
            "message": self.message,
            "details": self.details,
        }


def _build_bare_to_config(customer_config: dict) -> dict[str, dict]:
    """Build a map from filename-bare location names to config location dicts.

    Config has names like ``"Andover FS"`` and ``"Apple Valley"`` while PDF
    filenames are like ``Andover.pdf`` and ``Apple Valley.pdf``. We strip
    the trailing ``" FS"`` to produce the bare lookup form.
    """
    out: dict[str, dict] = {}
    for loc in customer_config["locations"]:
        canonical = loc["name"]
        out[canonical] = loc  # accept canonical form too
        bare = canonical[:-3] if canonical.endswith(" FS") else canonical
        out[bare] = loc
    return out


def discover_pdfs(
    pdf_dir: str | Path,
    customer_config: dict,
) -> dict[str, Path]:
    """Walk a directory and return ``{config_canonical_name: pdf_path}``.

    Raises:
      FileNotFoundError if pdf_dir doesn't exist.
      ValueError if any config location lacks a matching PDF, or if a PDF
        in the directory can't be mapped to a known location.
    """
    pdf_dir = Path(pdf_dir)
    if not pdf_dir.exists():
        raise FileNotFoundError(f"PDF directory not found: {pdf_dir}")

    bare_to_config = _build_bare_to_config(customer_config)

    found: dict[str, Path] = {}
    unmapped: list[Path] = []
    for path in sorted(pdf_dir.glob("*.pdf")):
        stem = path.stem.strip()  # e.g. "Andover" from Andover.pdf
        # Try canonical form first (via normalize), then bare lookup
        canonical_bare = normalize_location(stem)
        loc = bare_to_config.get(canonical_bare) or bare_to_config.get(stem)
        if not loc:
            unmapped.append(path)
            continue
        found[loc["name"]] = path

    missing = [loc["name"] for loc in customer_config["locations"] if loc["name"] not in found]
    if missing:
        raise ValueError(
            f"PDF directory {pdf_dir} is missing files for locations: {missing}. "
            f"Found {len(found)}/{len(customer_config['locations'])} expected PDFs."
        )
    if unmapped:
        raise ValueError(
            f"PDF directory {pdf_dir} contains files that don't map to any config "
            f"location: {[p.name for p in unmapped]}"
        )

    log.info("Discovered %d PDFs in %s", len(found), pdf_dir)
    return found


def _parse_zenoti(pdf_path: Path) -> dict:
    return pdf_zenoti_v2.parse_file(str(pdf_path))


def _parse_su(pdf_path: Path) -> dict:
    return pdf_salon_ultimate_v2.parse_file(str(pdf_path))


def _validate_parse_result(
    parsed: dict,
    config_loc: dict,
    *,
    tolerance: float,
) -> list[ValidationIssue]:
    """Return a list of issues for a single parsed PDF. Empty list = clean."""
    issues: list[ValidationIssue] = []
    loc_name = config_loc["name"]

    # Flag check — fail on any non-informational flag
    raw_flags = parsed.get("flags") or []
    bad_flags = [f for f in raw_flags if f not in INFORMATIONAL_FLAGS]
    if bad_flags:
        issues.append(ValidationIssue(
            location=loc_name,
            severity="error",
            code="PARSER_FLAGS",
            message=f"Parser raised non-informational flags: {bad_flags}",
            details={"flags": raw_flags},
        ))
    for info_flag in (set(raw_flags) & INFORMATIONAL_FLAGS):
        issues.append(ValidationIssue(
            location=loc_name,
            severity="info",
            code=info_flag,
            message=f"Informational: {info_flag}",
        ))

    # Sum reconciliation — sum(emp.net_service) vs karissa.service_net
    karissa = parsed.get("karissa") or {}
    employees = parsed.get("employees") or []
    karissa_service_net = float(karissa.get("service_net") or 0)
    emp_sum = sum(float(e.get("net_service") or 0) for e in employees)
    delta = round(emp_sum - karissa_service_net, 2)
    if employees and abs(delta) > tolerance:
        # Karissa 2026-05-26 Q9 confirmed: salon-total corrections don't always
        # propagate to per-stylist breakdowns. This is normal POS quirk drift,
        # not a parser bug. Downgrade from error → warning so it doesn't block
        # writes but is still surfaced for transparency.
        issues.append(ValidationIssue(
            location=loc_name,
            severity="warning",
            code="STYLIST_SUM_MISMATCH",
            message=(
                f"sum(employees.net_service)={emp_sum:,.2f} does not match "
                f"karissa.service_net={karissa_service_net:,.2f} "
                f"(delta={delta:+,.2f}, tolerance=${tolerance:.2f}). "
                f"Karissa Q9: salon-level corrections don't always reach per-stylist totals."
            ),
            details={"emp_sum": emp_sum, "service_net": karissa_service_net, "delta": delta},
        ))

    return issues


def _build_stylist_rows(
    parsed: dict,
    config_loc: dict,
    *,
    year_month: str,
    period_start: str,
    period_end: str,
    source: str,
) -> list[dict]:
    """Map parsed['employees'] entries into STYLISTS_DATA_MONTHLY row dicts.

    Zenoti employee dict has invoice_count + net_product + production_hours.
    SU employee dict has guests + net_retail (as product) + production_hours + ppg + pph + avg_ticket.

    Per Karissa's spec, Zenoti uses invoice_count as guest_count denominator,
    so for Zenoti stylists we copy invoice_count into BOTH `invoices` and
    `guests` so downstream stylist-PPG math works either way.
    """
    employees = parsed.get("employees") or []
    out: list[dict] = []
    platform = config_loc["platform"]
    for emp in employees:
        name = (emp.get("name") or "").strip()
        if not name:
            continue
        net_service = float(emp.get("net_service") or 0)
        if platform == "zenoti":
            invoices = int(emp.get("invoice_count") or 0)
            guests = invoices  # per Karissa's invoice_count = guest_count convention
            net_product = float(emp.get("net_product") or 0)
            avg_ticket = float(emp.get("avg_invoice_value") or 0)
            production_hours = float(emp.get("production_hours") or 0)
            pph = float(emp.get("net_service_per_hr") or 0)
            ppg = (net_product / invoices) if invoices else 0
        else:  # salon_ultimate
            guests = int(emp.get("guests") or 0)
            invoices = guests  # SU doesn't separate invoice vs guest at stylist level
            net_product = float(emp.get("net_retail") or 0)
            avg_ticket = float(emp.get("avg_ticket") or 0)
            production_hours = float(emp.get("production_hours") or 0)
            pph = float(emp.get("pph") or 0)
            ppg = float(emp.get("ppg") or 0)
        out.append({
            "year_month": year_month,
            "name": name,
            "loc_name": config_loc["name"],
            "loc_id": config_loc["id"],
            "platform": platform,
            "invoices": invoices,
            "guests": guests,
            "net_service": net_service,
            "net_product": net_product,
            "avg_ticket": avg_ticket,
            "pph": pph,
            "ppg": ppg,
            "production_hours": production_hours,
            "source": source,
            "period_start": period_start,
            "period_end": period_end,
        })
    return out


def _build_row(
    parsed: dict,
    config_loc: dict,
    *,
    year_month: str,
    period_start: str,
    period_end: str,
    source: str,
) -> dict:
    """Map a parsed PDF dict into a DATA_MONTHLY row dict."""
    k = parsed.get("karissa") or {}
    total_sales = float(k.get("total_sales") or 0)
    product = float(k.get("product_net") or 0)
    product_pct = (product / total_sales) if total_sales else 0

    return {
        "loc_name": config_loc["name"],
        "year_month": year_month,
        "platform": config_loc["platform"],
        "guests": k.get("guest_count", 0) or 0,
        "total_sales": total_sales,
        "service": float(k.get("service_net") or 0),
        "product": product,
        "product_pct": product_pct,
        "ppg": float(k.get("ppg") or 0),
        "pph": float(k.get("pph") or 0),
        "avg_ticket": float(k.get("avg_ticket") or 0),
        "prod_hours": float(k.get("production_hours") or 0),
        "wax_count": int(k.get("wax_count") or 0),
        "wax": float(k.get("wax_sales") or 0),  # SU returns no wax_sales; will be 0
        "wax_pct": float(k.get("wax_pct") or 0),
        "color": float(k.get("color_sales") or 0),
        "color_pct": float(k.get("color_pct") or 0),
        "treat_count": int(k.get("treatment_count") or 0),
        "treat": float(k.get("treatment_sales") or 0),  # SU returns no treatment_sales
        "treat_pct": float(k.get("treatment_pct") or 0),
        "source": source,
        "period_start": period_start,
        "period_end": period_end,
    }


def load_monthly_pdfs(
    pdf_dir: str | Path,
    customer_config: dict,
    *,
    year_month: str,
    period_start: str,
    period_end: str,
    tolerance: float = DEFAULT_RECONCILIATION_TOLERANCE,
) -> tuple[list[dict], list[dict], list[dict]]:
    """Parse + validate every PDF in pdf_dir.

    Returns:
      (rows, stylist_rows, issues)
      rows         — list of DATA_MONTHLY row dicts (one per location).
      stylist_rows — list of STYLISTS_DATA_MONTHLY row dicts (one per stylist
                     per location).
      issues       — list of dicts (from ValidationIssue.to_dict()).

    On parse failure, a zero-row for the location is still emitted so the
    caller sees the gap; no stylist rows are produced for the failed location.
    """
    found = discover_pdfs(pdf_dir, customer_config)

    rows: list[dict] = []
    stylist_rows: list[dict] = []
    issues: list[ValidationIssue] = []

    for loc in customer_config["locations"]:
        pdf_path = found[loc["name"]]
        platform = loc["platform"]
        source = "zenoti_monthly_pdf" if platform == "zenoti" else "su_monthly_pdf"

        try:
            if platform == "zenoti":
                parsed = _parse_zenoti(pdf_path)
            elif platform == "salon_ultimate":
                parsed = _parse_su(pdf_path)
            else:
                raise ValueError(f"Unknown platform {platform!r} for {loc['name']}")
        except Exception as e:
            log.error("Parse failed for %s (%s): %s", loc["name"], pdf_path.name, e)
            issues.append(ValidationIssue(
                location=loc["name"],
                severity="error",
                code="PARSE_EXCEPTION",
                message=f"Parser raised {type(e).__name__}: {e}",
                details={"pdf": pdf_path.name},
            ))
            rows.append({
                "loc_name": loc["name"], "year_month": year_month, "platform": platform,
                "source": source, "period_start": period_start, "period_end": period_end,
            })
            continue

        loc_issues = _validate_parse_result(parsed, loc, tolerance=tolerance)
        issues.extend(loc_issues)

        rows.append(_build_row(
            parsed, loc,
            year_month=year_month, period_start=period_start, period_end=period_end,
            source=source,
        ))
        stylist_rows.extend(_build_stylist_rows(
            parsed, loc,
            year_month=year_month, period_start=period_start, period_end=period_end,
            source=source,
        ))

    log.info(
        "Parsed %d PDFs for %s; %d stylist rows; %d issues (errors=%d)",
        len(rows), year_month, len(stylist_rows), len(issues),
        sum(1 for i in issues if i.severity == "error"),
    )
    return rows, stylist_rows, [i.to_dict() for i in issues]
