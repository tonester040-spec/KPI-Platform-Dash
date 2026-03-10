"""
load_stylist_dummy_data.py
Generates realistic stylist-level dummy data and loads it into the Google Sheet.

Writes:
  STYLISTS_DATA    — full 52-week history per stylist (append, never clears)
  STYLISTS_CURRENT — most recent week only (clears first, then writes)

Usage:
  $env:GOOGLE_SERVICE_ACCOUNT_JSON = [Convert]::ToBase64String(
    [IO.File]::ReadAllBytes("karissa-service-account.json"))
  python scripts/load_stylist_dummy_data.py
"""

import os, json, base64, random
from datetime import date, timedelta
from google.oauth2 import service_account
from googleapiclient.discovery import build

CONFIG_PATH = "config/customers/karissa_001.json"

# ── Name pools ────────────────────────────────────────────────────────────────

FIRST_NAMES = [
    "Emma", "Olivia", "Ava", "Isabella", "Sophia", "Mia", "Charlotte",
    "Amelia", "Harper", "Evelyn", "Abigail", "Emily", "Madison", "Avery",
    "Ella", "Scarlett", "Grace", "Chloe", "Riley", "Aria", "Lily", "Aubrey",
    "Zoey", "Nora", "Hannah", "Sarah", "Taylor", "Samantha", "Ashley",
    "Alexis", "Jessica", "Brittany", "Amanda", "Rachel", "Jennifer",
    "Stephanie", "Nicole", "Melissa", "Angela", "Tiffany", "Amber", "Crystal",
    "Whitney", "Jasmine", "Kayla", "Lauren", "Danielle", "Megan", "Courtney",
    "Brianna", "Destiny", "Tara", "Lindsey", "Shannon", "Heather", "Vanessa",
    "Kylie", "Alyssa", "Sierra", "Paige", "Brooke", "Natalie", "Leah",
    # A few male names (salon industry is mostly female but not exclusively)
    "Marcus", "Tyler", "Jordan", "Chris", "Ryan", "Derek", "Kyle", "James"
]

LAST_NAMES = [
    "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Wilson", "Taylor", "Anderson", "Thomas", "Jackson", "White", "Harris",
    "Martin", "Thompson", "Young", "Robinson", "Lewis", "Walker", "Hall",
    "Allen", "Hernandez", "King", "Wright", "Lopez", "Hill", "Green", "Baker",
    "Nelson", "Carter", "Mitchell", "Perez", "Roberts", "Turner", "Phillips",
    "Campbell", "Parker", "Evans", "Edwards", "Collins", "Stewart", "Sanchez",
    "Morris", "Rogers", "Reed", "Cook", "Morgan", "Bell", "Murphy", "Bailey",
    "Rivera", "Cooper", "Richardson", "Cox", "Howard", "Ward", "Torres",
    "Peterson", "Gray", "Ramirez", "James", "Watson", "Brooks", "Kelly",
    "Sanders", "Price", "Bennett", "Wood", "Barnes", "Ross", "Henderson"
]

# ── Archetype definitions ─────────────────────────────────────────────────────
# Each archetype: (weight, base_ranges_dict)
# Ranges are (min, max) for the stylist's personal base value.
# Weekly variance is ±8% of base.

ARCHETYPES = {
    "star": {
        "weight": 0.15,
        "pph":                  (60, 72),
        "rebook_pct":           (0.70, 0.82),
        "product_attachment_pct": (0.35, 0.45),
        "avg_ticket":           (88, 110),
        "total_services":       (32, 42),
        "color_pct":            (0.45, 0.65),
    },
    "underperformer": {
        "weight": 0.20,
        "pph":                  (28, 40),
        "rebook_pct":           (0.35, 0.50),
        "product_attachment_pct": (0.08, 0.18),
        "avg_ticket":           (42, 60),
        "total_services":       (18, 26),
        "color_pct":            (0.15, 0.30),
    },
    "product_champion": {
        "weight": 0.10,
        "pph":                  (45, 55),
        "rebook_pct":           (0.50, 0.65),
        "product_attachment_pct": (0.38, 0.45),
        "avg_ticket":           (65, 82),
        "total_services":       (24, 34),
        "color_pct":            (0.30, 0.50),
    },
    "rebook_machine": {
        "weight": 0.10,
        "pph":                  (45, 58),
        "rebook_pct":           (0.72, 0.82),
        "product_attachment_pct": (0.12, 0.25),
        "avg_ticket":           (60, 80),
        "total_services":       (28, 38),
        "color_pct":            (0.25, 0.50),
    },
    "solid_middle": {
        "weight": 0.45,
        "pph":                  (42, 60),
        "rebook_pct":           (0.52, 0.68),
        "product_attachment_pct": (0.15, 0.32),
        "avg_ticket":           (58, 85),
        "total_services":       (24, 36),
        "color_pct":            (0.25, 0.50),
    },
}

METRICS = ["pph", "rebook_pct", "product_attachment_pct",
           "avg_ticket", "total_services", "color_pct"]

VARIANCE = 0.08  # ±8% weekly noise

# Staffing target: 6-11 per location, ~110-120 total across 13 locations
LOCATION_COUNTS = [9, 8, 10, 9, 7, 9, 8, 10, 9, 8, 9, 7, 8]  # sums to 111


# ── Auth ──────────────────────────────────────────────────────────────────────

def get_service():
    raw = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")
    if not raw:
        raise ValueError("GOOGLE_SERVICE_ACCOUNT_JSON env var not set")
    key_data = json.loads(base64.b64decode(raw + "=="))
    creds = service_account.Credentials.from_service_account_info(
        key_data,
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return build("sheets", "v4", credentials=creds)


# ── Stylist generation ────────────────────────────────────────────────────────

def pick_archetype():
    archetypes = list(ARCHETYPES.keys())
    weights    = [ARCHETYPES[a]["weight"] for a in archetypes]
    return random.choices(archetypes, weights=weights, k=1)[0]


def assign_base_metrics(archetype, tenure_years):
    """Assign a stylist's stable base performance. Veterans trend slightly higher."""
    arch   = ARCHETYPES[archetype]
    tenure_boost = min(0.10, tenure_years * 0.01)  # up to +10% for 10yr vets
    bases  = {}
    for m in METRICS:
        lo, hi = arch[m]
        base   = random.uniform(lo, hi)
        # Veterans trend higher but not always (add some noise to the boost)
        if tenure_years > 1.0 and archetype != "underperformer":
            boost = tenure_boost * random.uniform(0.5, 1.5)
            base  = min(hi * 1.05, base * (1 + boost))
        bases[m] = base
    return bases


def generate_week_value(base, metric, status):
    """Apply weekly variance to a base metric."""
    noise = random.uniform(1 - VARIANCE, 1 + VARIANCE)
    val   = base * noise
    # New stylists are measurably worse
    if status == "new":
        val *= random.uniform(0.75, 0.90)
    # Clamp to realistic ranges
    clamps = {
        "pph":                    (28, 80),
        "rebook_pct":             (0.20, 0.90),
        "product_attachment_pct": (0.05, 0.55),
        "avg_ticket":             (35, 130),
        "total_services":         (10, 50),
        "color_pct":              (0.10, 0.75),
    }
    lo, hi = clamps[metric]
    return max(lo, min(hi, val))


def last_monday(ref_date=None):
    """Return the most recent Monday on or before ref_date."""
    d = ref_date or date.today()
    return d - timedelta(days=d.weekday())


def build_week_endings(num_weeks):
    """Return list of week-ending dates (Sundays) going back num_weeks from today."""
    anchor = last_monday()
    sundays = []
    for i in range(num_weeks - 1, -1, -1):
        monday = anchor - timedelta(weeks=i)
        sunday = monday + timedelta(days=6)
        sundays.append(sunday.isoformat())
    return sundays


def generate_stylists(locations):
    """Generate all stylists across locations."""
    used_names = set()
    stylists   = []

    for i, loc in enumerate(locations):
        count = LOCATION_COUNTS[i] if i < len(LOCATION_COUNTS) else 8
        for _ in range(count):
            # Unique name
            for attempt in range(100):
                fname = random.choice(FIRST_NAMES)
                lname = random.choice(LAST_NAMES)
                full  = f"{fname} {lname}"
                if full not in used_names:
                    used_names.add(full)
                    break

            # Tenure
            tenure = round(random.uniform(0.5, 9.0), 1)

            # Status
            if tenure < 0.5:
                status = "new"
            else:
                r = random.random()
                if r < 0.05:
                    status = "new"
                elif r < 0.10:
                    status = "part_time"
                else:
                    status = "active"

            archetype    = pick_archetype()
            base_metrics = assign_base_metrics(archetype, tenure)

            # How many weeks of history?
            tenure_weeks = int(tenure * 52)
            history_weeks = min(52, max(4, tenure_weeks))

            stylists.append({
                "name":          full,
                "location_name": loc["name"],
                "location_id":   loc["id"],
                "tenure_years":  tenure,
                "status":        status,
                "archetype":     archetype,
                "base_metrics":  base_metrics,
                "history_weeks": history_weeks,
            })

    return stylists


def build_rows_for_stylist(stylist):
    """Return list of row arrays (oldest → newest week)."""
    week_endings = build_week_endings(stylist["history_weeks"])
    rows = []
    for we in week_endings:
        row = [
            stylist["name"],
            stylist["location_name"],
            stylist["location_id"],
            we,
            stylist["tenure_years"],
        ]
        for m in METRICS:
            val = generate_week_value(stylist["base_metrics"][m], m, stylist["status"])
            if m in ("rebook_pct", "product_attachment_pct", "color_pct"):
                row.append(round(val, 4))          # store as decimal
            elif m == "pph":
                row.append(round(val, 2))
            elif m == "avg_ticket":
                row.append(round(val, 2))
            else:
                row.append(int(round(val)))        # total_services
        row.append(stylist["status"])
        rows.append(row)
    return rows


# ── Sheet writing ─────────────────────────────────────────────────────────────

def chunk(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def write_to_sheet(service, sheet_id, tab_name, rows, append=False):
    """Write rows to sheet. append=True uses append API, False clears first."""
    if not rows:
        print(f"  No rows to write to {tab_name}")
        return

    if not append:
        # Clear data rows (keep header)
        service.spreadsheets().values().clear(
            spreadsheetId=sheet_id,
            range=f"{tab_name}!A2:Z"
        ).execute()

    # Write in batches of 1000 to stay under API limits
    total = 0
    for batch in chunk(rows, 1000):
        if append:
            service.spreadsheets().values().append(
                spreadsheetId=sheet_id,
                range=f"{tab_name}!A2",
                valueInputOption="RAW",
                insertDataOption="INSERT_ROWS",
                body={"values": batch}
            ).execute()
        else:
            start_row = 2 + total
            service.spreadsheets().values().update(
                spreadsheetId=sheet_id,
                range=f"{tab_name}!A{start_row}",
                valueInputOption="RAW",
                body={"values": batch}
            ).execute()
        total += len(batch)

    print(f"  Wrote {total:,} rows to {tab_name}")
    return total


# ── Summary reporting ─────────────────────────────────────────────────────────

def print_summary(stylists, all_rows):
    print("\n" + "="*60)
    print("LOAD SUMMARY")
    print("="*60)
    print(f"Total stylists:    {len(stylists)}")
    print(f"Total rows loaded: {len(all_rows):,}")

    # By location
    print("\nHeadcount by Location:")
    loc_counts = {}
    for s in stylists:
        loc_counts[s["location_name"]] = loc_counts.get(s["location_name"], 0) + 1
    for loc, cnt in sorted(loc_counts.items(), key=lambda x: -x[1]):
        print(f"  {loc:<25} {cnt} stylists")

    # Top 5 by PPH (most recent week)
    print("\nTop 5 Stylists by PPH (most recent week):")
    # most recent row per stylist is the last in their rows
    latest = {}
    for row in all_rows:
        key = (row[0], row[1])  # name + location
        latest[key] = row       # overwrites → ends up with last (most recent)

    # PPH is column index 5
    ranked = sorted(latest.values(), key=lambda r: r[5], reverse=True)[:5]
    for i, r in enumerate(ranked, 1):
        print(f"  {i}. {r[0]:<22} {r[1]:<20} PPH: ${r[5]:.2f}")

    # Archetype breakdown
    print("\nArchetype Breakdown:")
    arch_counts = {}
    for s in stylists:
        arch_counts[s["archetype"]] = arch_counts.get(s["archetype"], 0) + 1
    for arch, cnt in sorted(arch_counts.items(), key=lambda x: -x[1]):
        pct = cnt / len(stylists) * 100
        print(f"  {arch:<20} {cnt:>3} ({pct:.0f}%)")
    print("="*60)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    random.seed(42)  # Reproducible results

    with open(CONFIG_PATH) as f:
        config = json.load(f)
    sheet_id  = config["sheet_id"]
    locations = config["locations"]

    print(f"Sheet ID:  {sheet_id}")
    print(f"Locations: {len(locations)}")

    service = get_service()
    print("Connected to Sheets API ✓\n")

    # Generate all stylists
    print("Generating stylist roster...")
    stylists = generate_stylists(locations)
    print(f"  {len(stylists)} stylists across {len(locations)} locations")

    # Build all rows
    print("Building historical rows...")
    all_historical_rows = []
    current_rows        = []

    for s in stylists:
        rows = build_rows_for_stylist(s)
        all_historical_rows.extend(rows)
        if rows:
            current_rows.append(rows[-1])  # most recent week only

    print(f"  {len(all_historical_rows):,} historical rows")
    print(f"  {len(current_rows)} current-week rows\n")

    # Write STYLISTS_DATA — append (never deletes existing history)
    print("Writing STYLISTS_DATA (append)...")
    write_to_sheet(service, sheet_id, "STYLISTS_DATA",
                   all_historical_rows, append=True)

    # Write STYLISTS_CURRENT — clear + rewrite
    print("Writing STYLISTS_CURRENT (clear + rewrite)...")
    write_to_sheet(service, sheet_id, "STYLISTS_CURRENT",
                   current_rows, append=False)

    print_summary(stylists, all_historical_rows)
    print("\n✅ Done. Stylist data loaded successfully.")


if __name__ == "__main__":
    main()
