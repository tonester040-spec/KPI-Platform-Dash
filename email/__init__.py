"""
email/
KPI Platform — Karissa Email Assistant

Modules:
  gmail_connector  — Gmail API auth + read/write
  noise_filter     — remove newsletters, automated noise
  categorizer      — Claude-powered email triage
  voice_profile    — load Karissa's writing style
  draft_generator  — generate reply drafts in her voice
  debrief_builder  — build the morning HTML debrief page
  friday_recap     — weekly summary (runs on Fridays)
  run_assistant    — main orchestrator (called by GitHub Actions)

One-time setup scripts:
  build_profile    — build voice profile from email samples
  get_token        — OAuth token generation helper
"""
