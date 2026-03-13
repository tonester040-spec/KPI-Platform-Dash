#!/usr/bin/env python3
"""
email/build_profile.py
KPI Platform — One-time script to build Karissa's voice profile.

Usage:
  1. Export 30-40 sent emails from Karissa's Gmail to voice/samples/
     Each file: plain text, named anything (e.g., 01.txt, 02.txt)
  2. Run: python email/build_profile.py
  3. Review the output at voice/karissa_voice_profile.json
  4. Manually check: does the profile sound like her?
  5. If anything is off, adjust the profile JSON directly

What it captures:
  - Greeting patterns (how she starts emails)
  - Sign-off patterns (how she ends them)
  - Sentence length preference (terse vs. expansive)
  - Formality level (casual to formal)
  - Punctuation style (exclamation marks, em-dashes, etc.)
  - Vocabulary patterns (words/phrases she uses or avoids)
  - Paragraph structure (how she organizes thoughts)
  - Distinctive phrases she uses regularly
"""

import json
import os
import sys
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger("build_profile")

REPO_ROOT    = Path(__file__).resolve().parent.parent
SAMPLES_DIR  = REPO_ROOT / "voice" / "samples"
PROFILE_PATH = REPO_ROOT / "voice" / "karissa_voice_profile.json"

ANALYSIS_PROMPT = """Analyze the email samples below. All emails were written by the same person (Karissa).

Your job: extract a detailed writing style profile that could be used to instruct an AI to write emails that sound exactly like her.

Analyze and document:
1. Greeting patterns — how does she typically start emails? (e.g., "Hey [name]," / "Hi," / straight into content / etc.)
2. Sign-off patterns — how does she close? (e.g., "Thanks," / "K" / "- Karissa" / etc.)
3. Sentence length — mostly short? Long? Mixed? Give a qualitative description.
4. Formality level — casual / semi-casual / professional / formal? Does it vary by recipient?
5. Punctuation style — uses exclamation marks frequently? Em-dashes? Ellipses? Minimal punctuation?
6. Vocabulary patterns — words/phrases she uses often, words she avoids, any notable patterns
7. Paragraph structure — does she use short punchy paragraphs? Long flowing ones? Bullet points?
8. Distinctive phrases — 3-5 phrases or expressions she uses that feel uniquely like her
9. Overall voice summary — one paragraph describing how you'd explain "how Karissa writes" to someone

Return a JSON object with these exact keys:
{
  "greeting_patterns": "description of how she starts emails",
  "sign_off_patterns": "description of how she ends emails",
  "sentence_length": "description of her sentence length preference",
  "formality_level": "description of formality level",
  "punctuation_style": "description of punctuation habits",
  "vocabulary_patterns": "description of word choice patterns",
  "paragraph_structure": "description of how she structures paragraphs",
  "distinctive_phrases": ["phrase 1", "phrase 2", "phrase 3"],
  "voice_summary": "one paragraph plain English description of her overall voice",
  "sample_count": <number of samples analyzed>
}"""


def load_samples() -> list[str]:
    """Load all .txt sample files from voice/samples/"""
    if not SAMPLES_DIR.exists():
        log.error("Samples directory not found: %s", SAMPLES_DIR)
        log.error("Create the directory and add 30-40 sent email samples as .txt files.")
        sys.exit(1)

    samples = sorted(SAMPLES_DIR.glob("*.txt"))
    if not samples:
        log.error("No .txt files found in %s", SAMPLES_DIR)
        log.error("Export Karissa's sent emails as plain text files and place them there.")
        sys.exit(1)

    texts = []
    for path in samples:
        try:
            texts.append(path.read_text(encoding="utf-8", errors="replace").strip())
        except Exception as exc:
            log.warning("Could not read %s: %s", path.name, exc)

    log.info("Loaded %d email samples from %s", len(texts), SAMPLES_DIR)
    return texts


def build_voice_profile(samples: list[str]) -> dict:
    """Call Claude to analyze samples and build the voice profile."""
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        log.error("ANTHROPIC_API_KEY environment variable not set.")
        sys.exit(1)

    try:
        import anthropic
    except ImportError:
        log.error("anthropic package not installed. Run: pip install anthropic")
        sys.exit(1)

    client = anthropic.Anthropic(api_key=api_key)

    # Build the prompt with all samples
    sample_text = "\n\n---EMAIL SAMPLE---\n\n".join(samples[:50])  # Cap at 50
    full_prompt  = f"{ANALYSIS_PROMPT}\n\n=== EMAIL SAMPLES ===\n\n{sample_text}"

    log.info("Sending %d samples to Claude for analysis...", len(samples))
    log.info("This may take 30-60 seconds...")

    msg = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1500,
        messages=[{"role": "user", "content": full_prompt}],
    )

    response = msg.content[0].text.strip()
    start    = response.find("{")
    end      = response.rfind("}") + 1

    if start == -1 or end == 0:
        log.error("Claude returned unexpected format:\n%s", response[:500])
        sys.exit(1)

    profile = json.loads(response[start:end])
    profile["sample_count"] = len(samples)
    return profile


def print_human_summary(profile: dict):
    """Print a readable summary of what was detected for manual review."""
    print("\n" + "="*60)
    print("VOICE PROFILE — HUMAN REVIEW SUMMARY")
    print("="*60)
    print(f"Samples analyzed: {profile.get('sample_count', '?')}")
    print(f"\nGreeting style:\n  {profile.get('greeting_patterns','')}")
    print(f"\nSign-off style:\n  {profile.get('sign_off_patterns','')}")
    print(f"\nSentence length:\n  {profile.get('sentence_length','')}")
    print(f"\nFormality:\n  {profile.get('formality_level','')}")
    print(f"\nPunctuation:\n  {profile.get('punctuation_style','')}")
    print(f"\nVocabulary:\n  {profile.get('vocabulary_patterns','')}")
    print(f"\nParagraph style:\n  {profile.get('paragraph_structure','')}")
    phrases = profile.get("distinctive_phrases", [])
    if phrases:
        print(f"\nDistinctive phrases:")
        for p in phrases:
            print(f"  - \"{p}\"")
    print(f"\nOverall voice:\n  {profile.get('voice_summary','')}")
    print("\n" + "="*60)
    print("Review the above. Does it sound like her?")
    print(f"Profile saved to: {PROFILE_PATH}")
    print("If anything is off, edit the JSON file directly before going live.")
    print("="*60 + "\n")


def main():
    print()
    print("╔═══════════════════════════════════════╗")
    print("║  KPI — Voice Profile Builder          ║")
    print("╚═══════════════════════════════════════╝")
    print()

    samples = load_samples()

    if len(samples) < 15:
        log.warning(
            "Only %d samples found. 30+ is recommended for a good profile. "
            "The profile will still be built, but may be less accurate.",
            len(samples),
        )

    profile = build_voice_profile(samples)

    # Save profile
    PROFILE_PATH.parent.mkdir(exist_ok=True)
    PROFILE_PATH.write_text(json.dumps(profile, indent=2))
    log.info("Profile saved to %s", PROFILE_PATH)

    print_human_summary(profile)


if __name__ == "__main__":
    main()
