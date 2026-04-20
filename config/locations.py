"""
Location configuration and POS system mapping.
Source: KPI_Tier2_Intake_Complete_Spec.md
"""

# Location to POS system mapping
# Source of truth: config/customers/karissa_001.json
LOCATION_POS_MAP = {
    # Zenoti locations (9)
    "Andover": "zenoti",
    "Blaine": "zenoti",
    "Crystal": "zenoti",
    "Elk River": "zenoti",
    "Forest Lake": "zenoti",
    "Hudson": "zenoti",
    "New Richmond": "zenoti",
    "Prior Lake": "zenoti",     # FIX 2026-04-20: was salon_ultimate (wrong per karissa_001.json z006)
    "Roseville": "zenoti",

    # Salon Ultimate locations (3)
    "Apple Valley": "salon_ultimate",
    "Farmington": "salon_ultimate",
    "Lakeville": "salon_ultimate",
}

# Coach assignments
COACH_ASSIGNMENTS = {
    "Jess": ["Prior Lake", "Apple Valley", "Lakeville", "Farmington"],
    "Jenn": ["Andover", "Blaine", "Crystal", "Elk River", "Roseville"],
    "Karissa": ["Forest Lake", "Hudson", "New Richmond"],
}

# Zenoti location ID patterns (for non-standard Roseville format)
ZENOTI_LOCATION_IDS = {
    "Andover": "10278",
    "Blaine": "10279",
    "Crystal": "10280",
    "Elk River": "10281",
    "Roseville": "40098",
    "Forest Lake": None,   # Add when available
    "Hudson": None,
    "New Richmond": None,
}

# Canonical location name aliases — map file-level variations to canonical names
# Used by parsers to normalize location strings extracted from Excel headers and filenames
LOCATION_ALIASES = {
    # Zenoti aliases (as they appear in "Andover mgr" rows, or with "FS" suffix in filenames)
    "andover":        "Andover",
    "andover fs":     "Andover",
    "andover full service": "Andover",
    "blaine":         "Blaine",
    "crystal":        "Crystal",
    "crystal fs":     "Crystal",
    "crystal full service": "Crystal",
    "elk river":      "Elk River",
    "elk river fs":   "Elk River",
    "elk river full service": "Elk River",
    "forest lake":    "Forest Lake",
    "hudson":         "Hudson",
    "new richmond":   "New Richmond",
    "prior lake":     "Prior Lake",
    "roseville":      "Roseville",

    # Salon Ultimate aliases (as they appear in B1 store name cell)
    "apple valley":   "Apple Valley",
    "farmington":     "Farmington",
    "lakeville":      "Lakeville",
}


def get_pos_system(location_name: str) -> str:
    """Return POS system for a location. Returns 'unknown' if not found."""
    return LOCATION_POS_MAP.get(location_name, "unknown")


def get_coach(location_name: str) -> str:
    """Return assigned coach for a location. Returns 'Unassigned' if not found."""
    for coach, locations in COACH_ASSIGNMENTS.items():
        if location_name in locations:
            return coach
    return "Unassigned"


def normalize_location(raw_name: str) -> str:
    """
    Normalize a raw location string (from Excel) to the canonical location name.

    Examples:
        "andover" → "Andover"
        "Elk River" → "Elk River"
        "prior lake" → "Prior Lake"
    """
    if not raw_name:
        return "Unknown"
    return LOCATION_ALIASES.get(raw_name.strip().lower(), raw_name.strip())
