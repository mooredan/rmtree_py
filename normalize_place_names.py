import re
import argparse
from config import (
    STATE_ABBREVIATIONS,
    OLD_STYLE_ABBR,
    STATE_NAMES,
    FOREIGN_COUNTRIES,
    COMMON_PLACE_MAPPINGS,
    MEXICAN_STATES,
    CANADIAN_PROVINCES,
    HISTORICAL_US_TERRITORIES,
)
from rmutils import (
    get_connection,
    load_county_database,
    standardize_us_county_name,
    current_utcmoddate,
    reverse_place_name,
)


# One-time load of the U.S. counties list
COUNTY_DB = load_county_database("american_counties.csv")


def normalize_once(name):

    # Check for exact match in COMMON_PLACE_MAPPINGS
    key = name.strip()
    for pattern, replacement in COMMON_PLACE_MAPPINGS.items():
        if key == pattern:
            name = replacement
            break  # Exact match found, skip further mapping

    # If 4 fields and ends with USA, remove ' County' from second field
    parts = [p.strip() for p in name.split(",")]
    if len(parts) == 4 and parts[-1].upper() == "USA":
        if " County" in parts[1]:
            parts[1] = parts[1].replace(" County", "")
            name = ", ".join(parts)

    # Capitalize first character of each field
    parts = [part.strip() for part in name.split(",")]
    normalized_parts = [p[0].upper() + p[1:] if p else "" for p in parts]
    name = ", ".join(normalized_parts)

    name = name.strip()

    # Add missing comma before known state names (e.g., 'Twin Falls Idaho' → 'Twin Falls, Idaho')
    for state in STATE_NAMES:
        if name.endswith(" " + state) and not name.endswith(", " + state):
            name = re.sub(rf" (?!.*, ){state}$", f", {state}", name)
            break  # Only apply to one state match

    # Insert comma before state abbreviation if missing
    for abbr in STATE_ABBREVIATIONS:
        pattern = rf"\b(.+?)\s+{abbr}$"
        if re.search(pattern, name):
            name = re.sub(pattern, rf"\1, {abbr}", name)
            break  # only fix once

    # Ensure comma before historical U.S. territory names
    for territory in HISTORICAL_US_TERRITORIES:
        pattern = rf"\b(.+?)\s+{re.escape(territory)}$"
        if re.search(pattern, name):
            name = re.sub(pattern, rf"\1, {territory}", name)
            break  # only one match expected

    # Add comma before 'Mexico' unless it's part of 'New Mexico'
    if " Mexico" in name and "New Mexico" not in name:
        name = re.sub(r"(?<!New) Mexico", r", Mexico", name)

    # Remove 3–5 letter uppercase prefixes (e.g., "KYRO - ", "NYCA - ")
    name = re.sub(r"^[A-Z]{4,5} - ", "", name)
    name = re.sub(r"^[A-Z]{4}[0-9] - ", "", name)
    name = re.sub(r"^[A-Z]{3} - ", "", name)

    # Remove lone trailing period
    name = re.sub(r"\.\s*$", "", name)

    # Insert comma before known country names if missing
    for country in FOREIGN_COUNTRIES:
        if name.endswith(" " + country):
            name = name[: -len(country) - 1].rstrip(", ") + ", " + country

    # Fix names like 'highpoint IA' → 'Highpoint, Iowa, USA'
    tokens = name.split()
    if len(tokens) == 2 and tokens[1] in STATE_ABBREVIATIONS:
        name = f"{tokens[0]}, {STATE_ABBREVIATIONS[tokens[1]]}, USA"

    # Fix state-only names like 'KY' → 'Kentucky, USA'
    if name.strip() in STATE_ABBREVIATIONS:
        name = f"{STATE_ABBREVIATIONS[name.strip()]}, USA"

    # Fix state-only names like 'Virginia' → 'Virginia, USA'
    if name.strip() in STATE_NAMES:
        name += ", USA"

    # Ensure a comma precedes valid Mexican state names (excluding 'New Mexico')
    if not name.endswith("New Mexico, USA"):  # exclude legitimate U.S. state
        for state in MEXICAN_STATES:
            if (
                state == "México"
            ):  # special case to avoid conflict with 'Mexico' country
                continue
            pattern = rf"(?<!,),\s*{state}, Mexico$"
            fixed_pattern = rf", {state}, Mexico"
            if name.endswith(f" {state}, Mexico"):
                name = re.sub(rf" {state}, Mexico$", fixed_pattern, name)
                break

    # Ensure a comma precedes Canadian province names if missing
    if name.endswith(", Canada"):
        for province in CANADIAN_PROVINCES:
            if name.endswith(f" {province}, Canada"):
                name = re.sub(rf" {province}, Canada$", rf", {province}, Canada", name)
                break

    name = re.sub(r", United States of America$", ", USA", name)
    name = re.sub(r", U\.S\.A$", ", USA", name)
    name = re.sub(r", U\.S\.A\.$", ", USA", name)
    name = re.sub(r", U\.S\.$", ", USA", name)
    name = re.sub(r", United States$", ", USA", name)
    name = re.sub(r"^,\s*", "", name)
    name = re.sub(r",+$", "", name)
    name = re.sub(r",\s*,", ",", name)
    name = re.sub(r",\s*", ", ", name)
    name = re.sub(r" \d{5},", "", name)
    name = re.sub(r" Co\.", " County", name)
    name = re.sub(r"Co ", "County ", name)
    name = re.sub(r" Co,", " County,", name)
    name = re.sub(r" Co$", " County", name)
    name = re.sub(r"^County, ", "County ", name)
    name = re.sub(r"([A-Z,a-z,0-9])&([A-Z,a-z,0-9])", r"\1 & \2", name)
    name = re.sub(r"^Rural, ", "", name)

    # Get County in the middle fixed
    name = re.sub(r" County ", " County, ", name)

    # Remove lone periods (again)
    name = re.sub(r"\s+\.\s+", " ", name)

    # Remove double commas and extra spaces between them
    name = re.sub(r",\s*,", ", ", name)

    # Remove trailing lone periods (e.g., "Oklahoma.")
    name = re.sub(r"\.\s*$", "", name)

    # Remove trailing commas (if still remaining)
    name = re.sub(r",\s*$", "", name)

    # Collapse repeated whitespace
    name = re.sub(r"\s{2,}", " ", name)

    for abbr, full in OLD_STYLE_ABBR.items():
        name = re.sub(rf"\b{re.escape(abbr)}\b", full, name)

    for abbr, full in STATE_ABBREVIATIONS.items():
        name = re.sub(rf",\s*{abbr}(,|$)", rf", {full}\1", name)

    if any(name.endswith(", " + state) for state in STATE_NAMES) and not name.endswith(
        ", USA"
    ):
        name += ", USA"

    # If name ends with a historical US territory and does not already end in ', USA', append ', USA'
    if any(name.endswith(", " + territory) for territory in HISTORICAL_US_TERRITORIES):
        if not name.endswith(", USA"):
            name += ", USA"

    # Fix Canadian places missing a comma before the province
    for province in CANADIAN_PROVINCES:
        pattern = rf"\b({province})$"
        if re.search(pattern, name, re.IGNORECASE):
            if f", {province}" not in name:
                # Insert comma before the province
                name = re.sub(rf"\s+{province}$", rf", {province}", name)

    # If ends with a known Canadian province but not ", Canada", append it
    parts = [p.strip() for p in name.split(",")]
    if parts[-1] in CANADIAN_PROVINCES and not name.endswith(", Canada"):
        name += ", Canada"

    # If ends with a known Mexican state but not ", Mexico", append it
    parts = [p.strip() for p in name.split(",")]
    if parts[-1] in MEXICAN_STATES and not name.endswith(", Mexiso"):
        name += ", Mexiso"

    # If 4 fields and ends with USA, remove ' County' from second field
    parts = [p.strip() for p in name.split(",")]
    if len(parts) == 4 and parts[-1].upper() == "USA":
        if " County" in parts[1]:
            parts[1] = parts[1].replace(" County", "")
            name = ", ".join(parts)

    # Fix repeated state name before 'USA'
    parts = [p.strip() for p in name.split(",")]
    if len(parts) >= 4 and parts[-1] == "USA":
        # Check if the second-to-last and third-to-last parts are the same
        if parts[-2].lower() == parts[-3].lower():
            # Remove the redundant part
            del parts[-3]
            name = ", ".join(parts)

    # Standarize when only the county is known to a list
    # of known USA counties
    name = standardize_us_county_name(name, COUNTY_DB, STATE_NAMES)

    return name


def normalize_place_iteratively(name):
    previous = name
    while True:
        current = normalize_once(previous)
        if current == previous:
            break
        previous = current
    return current if current != name else None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--commit", action="store_true", help="Apply changes to the database"
    )
    parser.add_argument(
        "--logfile", default="normalize_place_names.log", help="Path to log file"
    )
    args = parser.parse_args()

    conn = get_connection()
    cursor = conn.execute("SELECT PlaceID, Name FROM PlaceTable")
    updates = []

    for row in cursor.fetchall():
        place_id, old_name = row["PlaceID"], row["Name"]
        new_name = normalize_place_iteratively(old_name)
        if new_name:
            updates.append((place_id, old_name, new_name))

    if not updates:
        print("✅ No changes needed.")
        return

    print(f"✏️  Found {len(updates)} places to normalize.")

    # Write log
    with open(args.logfile, "w", encoding="utf-8") as log:
        for pid, old, new in updates:
            print(f"[{pid:5}] {old:<80} → {new}")
            log.write(f"[{pid}] {old} → {new}\n")

    # Optional database commit
    if args.commit:
        for place_id, _, new_name in updates:
            # Update Reverse and UTCModDate when modifying the place name
            reverse = reverse_place_name(new_name)
            utcmoddate = current_utcmoddate()

            cursor.execute(
                """
                UPDATE PlaceTable
                SET Name = ?, Reverse = ?, UTCModDate = ?
                WHERE PlaceID = ?
                """,
                (new_name, reverse, utcmoddate, place_id),
            )
        conn.commit()
        print("✅ Changes committed to the database.")
    else:
        print("ℹ️  Dry run only. No changes made. Use --commit to apply.")

    conn.close()


if __name__ == "__main__":
    main()
