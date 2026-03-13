#!/usr/bin/env python3
"""
email/voice_profile.py
KPI Platform — Karissa's voice profile loader.

Loads the voice profile from voice/karissa_voice_profile.json and
builds Claude system prompts tuned by recipient type so drafts
actually sound like her.

If no profile exists yet, returns a generic fallback prompt so the
draft generator still works (just without the voice tuning).
"""

import json
import logging
from pathlib import Path

log = logging.getLogger(__name__)

REPO_ROOT    = Path(__file__).resolve().parent.parent
PROFILE_PATH = REPO_ROOT / "voice" / "karissa_voice_profile.json"

RECIPIENT_TYPES = ["coach", "location_manager", "vendor", "personal", "unknown"]


def load_profile() -> dict:
    """
    Load Karissa's voice profile.
    Returns the profile dict, or an empty dict if not yet built.
    """
    if not PROFILE_PATH.exists():
        log.warning(
            "Voice profile not found at %s. "
            "Run email/build_profile.py to generate it. "
            "Using generic fallback for now.",
            PROFILE_PATH,
        )
        return {}

    try:
        profile = json.loads(PROFILE_PATH.read_text())
        log.info("Voice profile loaded (%d fields)", len(profile))
        return profile
    except Exception as exc:
        log.error("Failed to load voice profile: %s", exc)
        return {}


def get_draft_system_prompt(recipient_type: str = "unknown", profile: dict = None) -> str:
    """
    Build a Claude system prompt for draft generation, tuned by recipient type.
    Falls back gracefully if no profile is loaded.
    """
    if profile is None:
        profile = load_profile()

    if not profile:
        return _fallback_prompt(recipient_type)

    # Core voice description from profile
    greeting     = profile.get("greeting_patterns", "Keep greetings brief and warm.")
    sign_off     = profile.get("sign_off_patterns", "Sign off with her name or initials.")
    sentence_len = profile.get("sentence_length", "Uses a mix of short and medium sentences.")
    formality    = profile.get("formality_level", "Professional but casual and direct.")
    punctuation  = profile.get("punctuation_style", "Uses standard punctuation, minimal exclamation marks.")
    vocab        = profile.get("vocabulary_patterns", "Clear, direct language. No corporate speak.")
    phrases      = profile.get("distinctive_phrases", [])
    para_style   = profile.get("paragraph_structure", "Short paragraphs. Gets to the point quickly.")

    phrases_text = ""
    if phrases:
        phrases_text = f"\nDistinctive phrases she uses: {', '.join(phrases[:5])}"

    # Recipient-type adjustments
    tone_notes = {
        "coach": "Tone: collaborative, open to feedback, slightly more formal. She respects her coaches.",
        "location_manager": "Tone: warm but direct. She's supportive and solution-focused with her managers.",
        "vendor": "Tone: professional, brief, clear about what she needs. She doesn't over-explain to vendors.",
        "personal": "Tone: casual and warm — this is personal email, sound like a real human.",
        "unknown": "Tone: neutral and professional, err toward warmth.",
    }.get(recipient_type, "Tone: neutral and professional.")

    return f"""You are writing an email reply in Karissa's voice. Karissa owns a 13-location salon network in Minneapolis.

VOICE PROFILE:
- Greetings: {greeting}
- Sign-offs: {sign_off}
- Sentence style: {sentence_len}
- Formality: {formality}
- Punctuation: {punctuation}
- Vocabulary: {vocab}
- Paragraph style: {para_style}{phrases_text}

{tone_notes}

RULES:
- Write ONLY the email body — no subject line, no metadata
- Do not start with "Dear" or "To Whom It May Concern"
- Do not include any preamble like "Here is the draft:" — just the email
- Match her voice exactly — if the profile says she's terse, be terse
- If she uses casual language, use casual language
- Never sound like a corporate AI wrote this"""


def _fallback_prompt(recipient_type: str) -> str:
    """Generic prompt used when no voice profile has been built yet."""
    log.info("Using generic voice fallback (no profile built yet)")
    tone = {
        "coach":            "professional and collaborative",
        "location_manager": "warm, direct, and supportive",
        "vendor":           "professional and concise",
        "personal":         "casual and friendly",
        "unknown":          "professional but warm",
    }.get(recipient_type, "professional but warm")

    return f"""You are writing an email reply on behalf of Karissa, owner of a 13-location salon network in Minneapolis.

Tone: {tone}. Direct, clear, no corporate fluff.

RULES:
- Write ONLY the email body — no subject line, no metadata
- Do not start with "Dear" or "To Whom It May Concern"
- Do not include preamble — just the email body
- Keep it concise. Karissa is busy."""


def infer_recipient_type(email: dict) -> str:
    """
    Infer recipient type from email category.
    Maps categorizer output to voice profile recipient types.
    """
    category_map = {
        "Location Issue": "location_manager",
        "Coach":          "coach",
        "Vendor":         "vendor",
        "Personal":       "personal",
        "Admin":          "unknown",
        "FYI Only":       "unknown",
    }
    return category_map.get(email.get("category", ""), "unknown")
