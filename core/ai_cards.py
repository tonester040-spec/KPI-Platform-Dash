#!/usr/bin/env python3
"""
core/ai_cards.py
KPI Platform — generates AI performance cards via Claude API.

Produces:
  - 13 location cards: 2-3 sentence performance summary for each location
  - ~115 stylist cards: brief coaching insight per stylist
  - 1 coach briefing: network-wide summary + 3 action items for Karissa

Batches stylist cards to reduce API calls and stay within rate limits.
DRY_RUN=true → returns placeholder text without calling the API.
"""

import os
import logging
import time
from typing import Any

log = logging.getLogger(__name__)

STYLIST_BATCH_SIZE = 10   # stylists per API call for card generation
MODEL = "claude-haiku-4-5-20251001"  # fast + cheap for bulk card generation
COACH_MODEL = "claude-sonnet-4-6"    # smarter model for the coach briefing


def _client():
    """Return Anthropic client (lazy import so DRY_RUN never needs the package)."""
    try:
        import anthropic
        api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        if not api_key:
            raise EnvironmentError("ANTHROPIC_API_KEY env var is not set.")
        return anthropic.Anthropic(api_key=api_key)
    except ImportError:
        raise ImportError("anthropic package not installed. Run: pip install anthropic")


def _call(client, model: str, prompt: str, max_tokens: int = 512) -> str:
    """Single API call with simple retry on transient errors."""
    for attempt in range(3):
        try:
            msg = client.messages.create(
                model=model,
                max_tokens=max_tokens,
                messages=[{"role": "user", "content": prompt}],
            )
            return msg.content[0].text.strip()
        except Exception as exc:
            if attempt < 2:
                log.warning("API call failed (attempt %d): %s — retrying in 5s", attempt + 1, exc)
                time.sleep(5)
            else:
                raise


# ─── Location cards ───────────────────────────────────────────────────────────

def _loc_prompt(loc: dict, network: dict) -> str:
    delta = f"{loc['pph_delta']:+.2f}" if loc.get("pph_delta") is not None else "N/A"
    return (
        f"You are a salon business performance analyst. Write a 2-3 sentence performance "
        f"snapshot for the weekly KPI report. Be specific, use the numbers, and end with "
        f"one actionable coaching nudge. No fluff. No headers.\n\n"
        f"Location: {loc['loc_name']}\n"
        f"Week ending: {loc.get('week_ending', 'N/A')}\n"
        f"PPH: ${loc['pph']:.2f} (network avg: ${network['avg_pph']:.2f}, delta vs last week: {delta})\n"
        f"Product %: {loc['product_pct']:.1f}% (network avg: {network['avg_product_pct']:.1f}%)\n"
        f"Guest count: {loc['guests']} guests\n"
        f"Avg ticket: ${loc['avg_ticket']:.2f}\n"
        f"Network rank (PPH): #{loc.get('rank_pph', '?')} of {network['total_locations']}\n"
        f"Performance flag: {loc.get('flag', 'solid').upper()}"
    )


def generate_location_cards(locations: list[dict], network: dict, dry_run: bool = False) -> dict:
    """Returns {loc_name: card_text} for all locations."""
    cards = {}
    if dry_run:
        for loc in locations:
            cards[loc["loc_name"]] = (
                f"[DRY RUN] {loc['loc_name']}: PPH ${loc['pph']:.2f}, "
                f"Product {loc['product_pct']:.1f}%, {loc['guests']} guests. "
                f"Rank #{loc.get('rank_pph','?')} in network."
            )
        log.info("DRY RUN: Generated %d location card placeholders", len(cards))
        return cards

    client = _client()
    for i, loc in enumerate(locations):
        log.info("Generating location card %d/%d: %s", i + 1, len(locations), loc["loc_name"])
        prompt = _loc_prompt(loc, network)
        cards[loc["loc_name"]] = _call(client, MODEL, prompt, max_tokens=200)
        if i < len(locations) - 1:
            time.sleep(0.5)  # gentle rate limiting

    log.info("Generated %d location cards", len(cards))
    return cards


# ─── Stylist cards ────────────────────────────────────────────────────────────

def _stylist_batch_prompt(stylists_batch: list[dict], network: dict) -> str:
    lines = []
    for s in stylists_batch:
        delta = f"{s['pph_delta']:+.2f}" if s.get("pph_delta") is not None else "N/A"
        lines.append(
            f"- {s['name']} ({s['loc_name']}, {s.get('arch_label','')}) | "
            f"PPH: ${s['cur_pph']:.2f} (vs net avg ${network['avg_pph']:.2f}, delta {delta}) | "
            f"Rebook: {s['cur_rebook']:.1f}% | Product: {s['cur_product']:.1f}%"
        )

    return (
        f"You are a salon coaching assistant. For each stylist below, write exactly ONE sentence "
        f"(max 25 words) that is a specific, actionable coaching insight based on their metrics. "
        f"Focus on their biggest opportunity. Be direct and warm.\n\n"
        f"Format: STYLIST NAME: coaching insight\n\n"
        + "\n".join(lines)
    )


def generate_stylist_cards(stylists: list[dict], network: dict, dry_run: bool = False) -> dict:
    """Returns {stylist_name: card_text} for all stylists."""
    cards = {}
    if dry_run:
        for s in stylists:
            cards[s["name"]] = (
                f"[DRY RUN] {s['name']}: Focus on {'rebook' if s['cur_rebook'] < 60 else 'product'} "
                f"this week. PPH ${s['cur_pph']:.2f}."
            )
        log.info("DRY RUN: Generated %d stylist card placeholders", len(cards))
        return cards

    client = _client()
    batches = [stylists[i:i + STYLIST_BATCH_SIZE] for i in range(0, len(stylists), STYLIST_BATCH_SIZE)]

    for batch_num, batch in enumerate(batches):
        log.info(
            "Generating stylist cards batch %d/%d (%d stylists)",
            batch_num + 1, len(batches), len(batch)
        )
        prompt = _stylist_batch_prompt(batch, network)
        response = _call(client, MODEL, prompt, max_tokens=len(batch) * 50)

        # Parse response: "NAME: insight" lines
        for line in response.splitlines():
            line = line.strip()
            if ":" in line:
                name_part, _, insight = line.partition(":")
                name = name_part.strip(" -•*")
                # Match to actual stylist name (fuzzy)
                for s in batch:
                    if s["name"].lower() in name.lower() or name.lower() in s["name"].lower():
                        cards[s["name"]] = insight.strip()
                        break

        # Fill any misses with a default
        for s in batch:
            if s["name"] not in cards:
                cards[s["name"]] = f"Great work this week — keep the momentum going."

        if batch_num < len(batches) - 1:
            time.sleep(1.0)

    log.info("Generated %d stylist cards across %d batches", len(cards), len(batches))
    return cards


# ─── Coach briefing ───────────────────────────────────────────────────────────

def generate_coach_briefing(
    locations: list[dict],
    stylists: list[dict],
    network: dict,
    dry_run: bool = False,
) -> str:
    """Returns a 3-paragraph coach briefing for Karissa."""
    if dry_run:
        top = network.get("top_pph_loc", "N/A")
        low = network.get("low_pph_loc", "N/A")
        return (
            f"[DRY RUN] Coach Briefing — Week ending {network.get('week_ending','N/A')}\n\n"
            f"Network Performance: {len(locations)} locations, ${network['total_sales']:,.0f} total sales, "
            f"avg PPH ${network['avg_pph']:.2f}.\n\n"
            f"Highlights: {top} leads on PPH. {low} needs attention.\n\n"
            f"Action Items:\n"
            f"1. Check in with {low} manager this week.\n"
            f"2. Recognize {top} team in Monday standup.\n"
            f"3. Review product % trend across all locations."
        )

    stars = [s["name"] for s in stylists if s.get("is_star")]
    watches = [loc["loc_name"] for loc in locations if loc.get("flag") == "watch"]
    top_loc = network.get("top_pph_loc", "")
    low_loc = network.get("low_pph_loc", "")

    prompt = (
        f"You are a senior salon business coach. Write a concise coach briefing for Karissa, "
        f"the owner of a 13-location salon network. Use 3 short paragraphs:\n"
        f"1. Network headline (2-3 sentences with the key numbers)\n"
        f"2. Wins to celebrate (specific people/locations)\n"
        f"3. Three numbered action items for this week (urgent things Karissa should do)\n\n"
        f"Data:\n"
        f"Week ending: {network.get('week_ending','N/A')}\n"
        f"Total locations: {len(locations)}\n"
        f"Total sales: ${network['total_sales']:,.0f}\n"
        f"Total guests: {network['total_guests']:,}\n"
        f"Avg network PPH: ${network['avg_pph']:.2f}\n"
        f"Avg product %: {network['avg_product_pct']:.1f}%\n"
        f"Top PPH location: {top_loc}\n"
        f"Lowest PPH location: {low_loc}\n"
        f"Locations flagged as WATCH: {', '.join(watches) if watches else 'None'}\n"
        f"Star stylists this week: {', '.join(stars[:5]) if stars else 'None'} "
        f"{'(and more)' if len(stars) > 5 else ''}\n"
        f"Total star stylists: {len(stars)} of {len(stylists)}\n\n"
        f"Be specific. Use the numbers. Sound like a coach who knows this business."
    )

    client = _client()
    briefing = _call(client, COACH_MODEL, prompt, max_tokens=600)
    log.info("Generated coach briefing (%d chars)", len(briefing))
    return briefing


# ─── Main entry ───────────────────────────────────────────────────────────────

def generate_all(data: dict, dry_run: bool = False) -> dict:
    """
    Generate all AI cards. Returns:
      {
        "location_cards": {loc_name: text},
        "stylist_cards":  {stylist_name: text},
        "coach_briefing": text,
      }
    """
    locations = data["locations"]
    stylists  = data["stylists"]
    network   = data["network"]

    log.info("Starting AI card generation (dry_run=%s)", dry_run)
    loc_cards    = generate_location_cards(locations, network, dry_run)
    stylist_cards = generate_stylist_cards(stylists, network, dry_run)
    briefing     = generate_coach_briefing(locations, stylists, network, dry_run)

    return {
        "location_cards":  loc_cards,
        "stylist_cards":   stylist_cards,
        "coach_briefing":  briefing,
    }
