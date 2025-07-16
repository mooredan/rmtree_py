
import re
from collections import defaultdict
from rmutils import get_connection

def normalize_place_name(name):
    if not name:
        return None
    name = name.strip()

    # Replace abbreviations (IL -> Illinois, etc.)
    replacements = {
        r'\bIL\b': 'Illinois',
        r'\bMO\b': 'Missouri',
        r'\bIN\b': 'Indiana',
        r'\bIA\b': 'Iowa',
        r'\bUSA\b': 'USA',
        r'\bU\.S\.A\.\b': 'USA',
        r'\bCo\.\b': 'County',
    }

    # Normalize spacing and commas
    name = re.sub(r'\s*,\s*', ', ', name)
    name = re.sub(r'\s+', ' ', name)

    for pattern, replacement in replacements.items():
        name = re.sub(pattern, replacement, name, flags=re.IGNORECASE)

    name = name.title()
    if not name.endswith("USA"):
        name += ", USA"

    return name.strip(", ")

def load_all_places(conn):
    query = "SELECT PlaceID, Name FROM PlaceTable WHERE Name IS NOT NULL"
    return conn.execute(query).fetchall()

def group_variants(rows):
    normalized = defaultdict(list)
    for row in rows:
        pid, raw_name = row
        canon = normalize_place_name(raw_name)
        normalized[canon].append((pid, raw_name))
    return normalized

def main():
    conn = get_connection()
    rows = load_all_places(conn)
    grouped = group_variants(rows)

    for canon, variants in grouped.items():
        if len(variants) <= 1:
            continue
        print(f"📍 Canonical: {canon}")
        for pid, variant in variants:
            print(f"   - [{pid}] {variant}")
        print()

if __name__ == "__main__":
    main()
