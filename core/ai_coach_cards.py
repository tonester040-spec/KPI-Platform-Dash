#!/usr/bin/env python3
"""
core/ai_coach_cards.py
KPI Platform — generates per-manager coach cards via Claude API.

Produces:
  - 1 Jess brief:  coaching card for her 4-location territory
  - 1 Jenn brief:  coaching card for her 5-location territory
  - Karissa's 3 direct locations (Forest Lake, Hudson, New Richmond)
    do NOT generate a coach card — they flow to Karissa's Ops dashboard only.

Uses the hardened prompt spec from KPI_Coach_Card_AI_Prompt_Hardened.docx.
Model: claude-sonnet-4-6 (quality matters — this drives real coaching calls)
DRY_RUN=true → returns placeholder JSON without calling the API.
"""

import os
import json
import logging
import time
from typing import Any

log = logging.getLogger(__name__)

COACH_CARD_MODEL = "claude-sonnet-4-6"

# ─── Hardened system prompt (from spec Section 4) ─────────────────────────────

SYSTEM_PROMPT = """You are KPI — a business intelligence advisor built for professional salon networks.
You are generating a weekly coaching brief for a business coach who manages a portfolio of salon locations. Your output will be read on an iPhone at 7:45am Monday, before the coach's first client call. Every word must earn its place.

WHAT YOU ARE GENERATING:
A structured weekly brief that lets the coach walk into every call prepared and credible, without opening a spreadsheet.

OUTPUT RULES — NON-NEGOTIABLE:
1. BE SPECIFIC. Never say "performance declined." Say "PPH dropped $4.20 to $38.12, now $4.72 below network average." Metrics, deltas, and comparisons are mandatory.
2. TALKING POINTS MUST FOLLOW THIS FORMAT:
Observation (specific number + vs what) → Context (probable cause) → Question (open-ended, surfaces cause, cannot be answered with "fine").
BAD: "Discuss retail attachment with your manager."
GOOD: "Product % dropped 6 points to 12% this week. Ask the manager whether there was a staffing gap Friday or if the team has stopped offering at checkout. Which stylists are still hitting 18%+?"
3. COACHING QUESTIONS MUST SURFACE CAUSE, NOT RESTATE SYMPTOM.
BAD: "How are things going at Elk River?"
GOOD: "PPH has dropped 3 weeks running — walk me through a typical Tuesday. Is this a scheduling gap, a stylist-specific issue, or something else?"
4. TERRITORY HEADLINE: Lead with a number. Name the best and worst performer. Max 3 sentences. No preamble. No "mixed results."
5. ONE TO WATCH: Flag something that becomes a Priority Call in 2 weeks if ignored. Include the trend data (3 weeks is ideal) and a specific threshold to watch for. Do not flag things already in crisis — those belong in Priority Call.
6. STAR OF THE WEEK: One sentence the coach can say out loud to the team. Include the specific metric that earned it. Make it feel like recognition, not a report readout.
7. TONE: Knowledgeable colleague who already read the data. Not a report generator. No corporate language. No hedging. No "it is worth noting that." Write like you're handing the coach a sticky note before she walks in.
8. OUTPUT: Valid JSON only. No explanation text. No markdown fences. No preamble. If any field lacks data to support a specific claim, use the closest available proxy — do not leave fields vague or empty."""

# ─── Output schema hint appended to user message ──────────────────────────────

OUTPUT_SCHEMA = """{
  "coach_name": string,
  "week_ending": string,
  "territory_headline": string,
  "star_of_week": {
    "location": string,
    "pph": number,
    "recognition_line": string
  },
  "priority_call": {
    "location": string,
    "network_rank": number,
    "issue": string,
    "probable_cause": string,
    "coaching_question": string
  },
  "one_to_watch": {
    "location": string,
    "trend": string,
    "threshold": string,
    "weeks_until_critical": number
  },
  "location_cards": [
    {
      "name": string,
      "pph": number,
      "pph_vs_network": number,
      "avg_ticket": number,
      "guests": number,
      "product_pct": number,
      "revenue_to_goal_pct": number,
      "flag": "STAR" | "WATCH" | "SOLID",
      "talking_points": [string, string]
    }
  ],
  "stylist_spotlight": {
    "recognition": {
      "name": string,
      "location": string,
      "pph": number,
      "rebook_rate": number,
      "note": string
    },
    "concern": {
      "name": string,
      "location": string,
      "pph": number,
      "rebook_rate": number,
      "note": string
    } | null
  },
  "pph_comparison": [
    {
      "location": string,
      "this_week": number,
      "last_week": number,
      "delta": number
    }
  ]
}"""


# ─── Anthropic client (lazy) ──────────────────────────────────────────────────

def _get_client():
    """Return Anthropic client (lazy import so DRY_RUN never needs the package)."""
    try:
        import anthropic
        api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        if not api_key:
            raise EnvironmentError("ANTHROPIC_API_KEY env var is not set.")
        return anthropic.Anthropic(api_key=api_key)
    except ImportError:
        raise ImportError("anthropic package not installed. Run: pip install anthropic")


def _call(client, system_msg: str, user_msg: str, max_tokens: int = 2000) -> str:
    """Single API call with retry. Uses system + user message pattern."""
    for attempt in range(3):
        try:
            msg = client.messages.create(
                model=COACH_CARD_MODEL,
                max_tokens=max_tokens,
                system=system_msg,
                messages=[{"role": "user", "content": user_msg}],
            )
            return msg.content[0].text.strip()
        except Exception as exc:
            if attempt < 2:
                log.warning("Coach card API call failed (attempt %d): %s — retrying in 5s", attempt + 1, exc)
                time.sleep(5)
            else:
                raise


# ─── Data payload builders ────────────────────────────────────────────────────

def _build_location_payload(loc: dict, stylists: list[dict]) -> dict:
    """
    Map an enriched location dict to the coach card prompt payload schema.
    Cross-platform note: PPH is already normalized by data_processor before this
    function runs, so no additional normalization is needed here.
    """
    name = loc["loc_name"]
    loc_stylists = [s for s in stylists if s.get("loc_name") == name]
    active_stylists = [s for s in loc_stylists if s.get("status", "active") == "active"]

    # Sort stylists by PPH to find top and watch
    sorted_by_pph = sorted(active_stylists, key=lambda s: s.get("cur_pph", 0), reverse=True)
    top_stylist = sorted_by_pph[0] if sorted_by_pph else None
    watch_stylist = next(
        (s for s in sorted_by_pph if s.get("arch_label") == "Needs Coaching"), None
    )

    # PPH history for trend
    hist = loc.get("hist", {})
    pph_trend = hist.get("pph", [loc.get("pph", 0)])

    # Last week PPH
    pph_last = loc.get("pph_prev")
    if pph_last is None:
        delta = loc.get("pph_delta", 0) or 0
        pph_last = round(loc.get("pph", 0) - delta, 2)

    # 3-week average
    recent = pph_trend[-3:] if len(pph_trend) >= 3 else pph_trend
    pph_3wk = round(sum(recent) / len(recent), 2) if recent else loc.get("pph", 0)

    # Rebook rate — stored at location level if available, else derive from stylists
    rebook = loc.get("rebook_rate", 0)
    if not rebook and active_stylists:
        rebook_vals = [s.get("cur_rebook", 0) for s in active_stylists if s.get("cur_rebook", 0) > 0]
        rebook = round(sum(rebook_vals) / len(rebook_vals), 1) if rebook_vals else 0

    return {
        "name": name,
        "platform": loc.get("platform", "zenoti"),
        "pph_this_week": round(loc.get("pph", 0), 2),
        "pph_last_week": round(pph_last, 2),
        "pph_3wk_avg": pph_3wk,
        "revenue_this_week": round(loc.get("total_sales", 0), 0),
        "revenue_last_week": round(loc.get("total_sales_prev") or 0, 0),
        "revenue_goal_weekly": round(loc.get("weekly_goal", 0), 0),
        "guests_this_week": loc.get("guests", 0),
        "avg_ticket_this_week": round(loc.get("avg_ticket", 0), 2),
        "product_pct_this_week": round(loc.get("product_pct", 0), 1),
        "product_pct_last_week": round(loc.get("product_pct_prev") or loc.get("product_pct", 0), 1),
        "rebook_rate_this_week": round(rebook, 1),
        "stylist_count": len(active_stylists),
        "top_stylist": {
            "name": top_stylist["name"],
            "pph": round(top_stylist.get("cur_pph", 0), 2),
            "rebook_rate": round(top_stylist.get("cur_rebook", 0), 1),
        } if top_stylist else {"name": "N/A", "pph": 0.0, "rebook_rate": 0.0},
        "watch_stylist": {
            "name": watch_stylist["name"],
            "pph": round(watch_stylist.get("cur_pph", 0), 2),
            "rebook_rate": round(watch_stylist.get("cur_rebook", 0), 1),
        } if watch_stylist else None,
        "12wk_trend_pph": [round(v, 2) for v in pph_trend[-12:]],
        "network_pph_rank": loc.get("rank_pph", 0),
    }


def _build_network_summary(network: dict, all_locations: list[dict]) -> dict:
    """Build the network context payload for benchmarking."""
    pph_values = [loc.get("pph", 0) for loc in all_locations if loc.get("pph", 0) > 0]

    # Network-level rebook rate from stylists if stored
    avg_rebook = network.get("avg_rebook_rate", 0)

    return {
        "network_avg_pph": round(network.get("avg_pph", 0), 2),
        "network_avg_product_pct": round(network.get("avg_product_pct", 0), 1),
        "network_avg_rebook_rate": round(avg_rebook, 1),
        "network_top_pph": round(max(pph_values), 2) if pph_values else 0.0,
        "network_bottom_pph": round(min(pph_values), 2) if pph_values else 0.0,
        "total_locations": 12,
    }


# ─── Dry run fallback ─────────────────────────────────────────────────────────

def _dry_run_brief(manager_name: str, locations: list[dict], network: dict) -> dict:
    """Return a placeholder coach card dict without calling the API."""
    if not locations:
        return {}

    top = max(locations, key=lambda l: l.get("pph", 0))
    bot = min(locations, key=lambda l: l.get("pph", 0))
    week = network.get("week_ending", "N/A")
    avg_pph = network.get("avg_pph", 0)

    return {
        "coach_name": manager_name,
        "week_ending": week,
        "territory_headline": (
            f"[DRY RUN] Your territory has {len(locations)} location(s) this week. "
            f"{top['loc_name']} leads at ${top['pph']:.2f} PPH; "
            f"{bot['loc_name']} is lowest at ${bot['pph']:.2f} PPH."
        ),
        "star_of_week": {
            "location": top["loc_name"],
            "pph": round(top.get("pph", 0), 2),
            "recognition_line": (
                f"[DRY RUN] {top['loc_name']} hit ${top['pph']:.2f} PPH this week — strong work."
            ),
        },
        "priority_call": {
            "location": bot["loc_name"],
            "network_rank": bot.get("rank_pph", 0),
            "issue": f"[DRY RUN] PPH ${bot['pph']:.2f} — placeholder issue.",
            "probable_cause": "[DRY RUN] Requires live API call for real analysis.",
            "coaching_question": "[DRY RUN] Walk me through a typical Tuesday.",
        },
        "one_to_watch": {
            "location": locations[0]["loc_name"],
            "trend": "[DRY RUN] 3-week trend placeholder.",
            "threshold": "[DRY RUN] Threshold placeholder.",
            "weeks_until_critical": 2,
        },
        "location_cards": [
            {
                "name": loc["loc_name"],
                "pph": round(loc.get("pph", 0), 2),
                "pph_vs_network": round(loc.get("pph", 0) - avg_pph, 2),
                "avg_ticket": round(loc.get("avg_ticket", 0), 2),
                "guests": loc.get("guests", 0),
                "product_pct": round(loc.get("product_pct", 0), 1),
                "revenue_to_goal_pct": 100.0,
                "flag": loc.get("flag", "solid").upper(),
                "talking_points": [
                    f"[DRY RUN] PPH ${loc['pph']:.2f} vs network avg ${avg_pph:.2f}.",
                    f"[DRY RUN] Product % {loc['product_pct']:.1f}% — real talking points require live run.",
                ],
            }
            for loc in locations
        ],
        "stylist_spotlight": {
            "recognition": {
                "name": "N/A",
                "location": top["loc_name"],
                "pph": 0.0,
                "rebook_rate": 0.0,
                "note": "[DRY RUN] Star stylist placeholder.",
            },
            "concern": None,
        },
        "pph_comparison": [
            {
                "location": loc["loc_name"],
                "this_week": round(loc.get("pph", 0), 2),
                "last_week": round(loc.get("pph_prev") or loc.get("pph", 0), 2),
                "delta": round(loc.get("pph_delta") or 0, 2),
            }
            for loc in locations
        ],
    }


# ─── Core generation ──────────────────────────────────────────────────────────

def generate_coach_card(
    manager_name: str,
    mgr_locations: list[dict],
    all_locations: list[dict],
    stylists: list[dict],
    network: dict,
    week_ending: str,
    dry_run: bool = False,
) -> dict:
    """
    Generate a single coach card for one manager's territory.
    Returns a dict matching the OUTPUT_SCHEMA.
    Falls back to _dry_run_brief on JSON parse failure (preserves pipeline stability).
    """
    if dry_run:
        brief = _dry_run_brief(manager_name, mgr_locations, network)
        log.info("DRY RUN: coach card placeholder for %s (%d locs)", manager_name, len(mgr_locations))
        return brief

    # Build data payloads
    loc_data = [_build_location_payload(loc, stylists) for loc in mgr_locations]
    net_summary = _build_network_summary(network, all_locations)

    # User message (from spec Section 5)
    user_msg = (
        f"Generate a weekly coaching brief for {manager_name}.\n\n"
        f"COACH TERRITORY — this week's data:\n"
        f"{json.dumps(loc_data, indent=2)}\n\n"
        f"NETWORK CONTEXT (all 12 locations) for benchmarking:\n"
        f"{json.dumps(net_summary, indent=2)}\n\n"
        f"Week ending: {week_ending}\n\n"
        f"Return ONLY valid JSON matching this schema. No explanation. No markdown fences.\n\n"
        f"{OUTPUT_SCHEMA}"
    )

    client = _get_client()
    log.info("Calling Claude for %s coach card (%d locations)...", manager_name, len(mgr_locations))
    raw = _call(client, SYSTEM_PROMPT, user_msg, max_tokens=2000)
    log.info("Coach card raw response: %d chars for %s", len(raw), manager_name)

    # Parse JSON — strip accidental markdown fences if present
    text = raw.strip()
    if text.startswith("```"):
        lines = text.splitlines()
        # Remove first and last fence lines
        lines = [l for l in lines if not l.strip().startswith("```")]
        text = "\n".join(lines).strip()

    try:
        result = json.loads(text)
        log.info("Coach card generated for %s ✓", manager_name)
        return result
    except json.JSONDecodeError as exc:
        log.error(
            "Failed to parse coach card JSON for %s: %s\nFirst 500 chars: %s",
            manager_name, exc, raw[:500]
        )
        log.warning("Falling back to dry-run placeholder for %s", manager_name)
        return _dry_run_brief(manager_name, mgr_locations, network)


# ─── Main entry ───────────────────────────────────────────────────────────────

def generate_all_coach_cards(
    config: dict,
    data: dict,
    dry_run: bool = False,
) -> dict:
    """
    Generate coach cards for all managers configured in karissa_001.json.
    Returns {manager_name: coach_card_dict}

    Skips managers with no location_ids.
    Karissa's direct locations are in the network context but don't get their own card.
    """
    managers = config.get("managers", [])
    all_locations = data["locations"]
    stylists = data["stylists"]
    network = data["network"]
    week_ending = network.get("week_ending", "")

    cards = {}

    for mgr in managers:
        name = mgr.get("name", "")
        loc_ids = mgr.get("location_ids", [])

        if not loc_ids:
            log.info("Skipping coach card for %s — no location_ids configured", name)
            continue

        mgr_locations = [loc for loc in all_locations if loc.get("loc_id") in loc_ids]

        if not mgr_locations:
            log.warning("No matching locations found for manager %s — skipping coach card", name)
            continue

        log.info(
            "Generating coach card: %s — %d locations: %s",
            name,
            len(mgr_locations),
            ", ".join(loc["loc_name"] for loc in mgr_locations),
        )

        card = generate_coach_card(
            manager_name=name,
            mgr_locations=mgr_locations,
            all_locations=all_locations,
            stylists=stylists,
            network=network,
            week_ending=week_ending,
            dry_run=dry_run,
        )
        cards[name] = card

        # Pace API calls — give a moment between Jess and Jenn
        if not dry_run:
            time.sleep(1.0)

    log.info("Coach cards complete: %d generated (dry_run=%s)", len(cards), dry_run)
    return cards
