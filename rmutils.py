# rmutils.py
import sqlite3
import pandas as pd
import re
import os
import sys
import csv
# import argparse
from collections import defaultdict
from datetime import datetime, timezone

from config import (
    rmtree_path,
    extension_path,
    US_COUNTIES,
    STATE_ABBREVIATIONS,
    OLD_STYLE_ABBR,
    STATE_NAMES,
    FOREIGN_COUNTRIES,
    COMMON_PLACE_MAPPINGS,
    MEXICAN_STATES,
    CANADIAN_PROVINCES,
    HISTORICAL_US_TERRITORIES,
)

def get_config():
    import config  # This is your separate config.py file

    return {
        "rmtree_path": config.rmtree_path,
        "extension_path": config.extension_path,
    }


def get_connection(read_only=False):
    """Returns a SQLite connection with RMNOCASE extension loaded.
    Defaults to read-only access unless read_only is set to False.
    """
    if not os.path.isfile(rmtree_path):
        sys.exit(f"❌ Database file not found: {rmtree_path}")
    if not os.access(rmtree_path, os.R_OK):
        sys.exit(f"❌ Database file not readable: {rmtree_path}")
    if os.path.getsize(rmtree_path) == 0:
        sys.exit(f"❌ Database file is empty: {rmtree_path}")
    if not os.path.isfile(extension_path):
        sys.exit(f"❌ Extension file not found: {extension_path}")

    try:
        if read_only:
            uri = f"file:{rmtree_path}?mode=ro"
            conn = sqlite3.connect(uri, uri=True)
        else:
            conn = sqlite3.connect(rmtree_path)

        conn.row_factory = sqlite3.Row
        conn.enable_load_extension(True)
        conn.load_extension(extension_path)
        conn.execute("REINDEX RMNOCASE;")
        return conn

    except Exception as e:
        sys.exit(f"❌ SQLite initialization error: {e}")


def run_query(conn, sql, params=None):
    """Executes a SQL query and returns a pandas DataFrame."""
    try:
        return pd.read_sql_query(sql, conn, params=params or {})
    except Exception as e:
        sys.exit(f"❌ Query failed: {e}")


def get_primary_names(conn, person_ids=None):
    """
    Returns a dictionary mapping PersonID to (Given, Surname) for all primary names.
    If person_ids is provided, restricts results to those PersonIDs.
    """
    base_query = """
        SELECT OwnerID AS PersonID, Given, Surname
        FROM NameTable
        WHERE IsPrimary = 1
    """
    params = ()
    if person_ids:
        placeholders = ",".join("?" for _ in person_ids)
        base_query += f" AND OwnerID IN ({placeholders})"
        params = tuple(person_ids)

    cursor = conn.execute(base_query, params)
    return {
        row["PersonID"]: (row["Given"], row["Surname"]) for row in cursor.fetchall()
    }


def merge_place_records(conn, canonical_id, duplicate_id, dry_run=True):
    if canonical_id == duplicate_id:
        raise ValueError("Canonical and duplicate IDs must differ.")

    referencing_tables = {
        "EventTable": "PlaceID",
        "FANTable": "PlaceID",
    }

    conditional_tables = [
        ("TaskLinkTable", 5),
        ("URLTable", 5),
        ("MediaLinkTable", 14),
        ("TaskLinkTable", 14),
    ]

    cursor = conn.cursor()

    # Fetch both PlaceTable rows
    cursor.execute("SELECT * FROM PlaceTable WHERE PlaceID = ?", (canonical_id,))
    canonical_row = cursor.fetchone()
    cursor.execute("SELECT * FROM PlaceTable WHERE PlaceID = ?", (duplicate_id,))
    duplicate_row = cursor.fetchone()

    if not canonical_row or not duplicate_row:
        raise ValueError("One or both PlaceIDs do not exist.")

    print(f"\n🔁 Merging PlaceID {duplicate_id} → {canonical_id}")
    if dry_run:
        print("Dry-run mode: no changes will be written.")

    print("\n📋 Comparing PlaceTable fields:")
    differing_fields = []
    cursor.execute("PRAGMA table_info(PlaceTable)")
    columns = [col[1] for col in cursor.fetchall()]
    for idx, field in enumerate(columns):
        if field in ("PlaceID", "UTCModDate"):
            continue
        val1 = canonical_row[idx]
        val2 = duplicate_row[idx]
        if val1 != val2:
            differing_fields.append(field)
            print(f" ⚠️ {field}:")
            print(f"    Canonical: {repr(val1)}")
            print(f"    Duplicate: {repr(val2)}")

    if not differing_fields:
        print(" ✅ All fields match.")

    # Update referencing tables with direct PlaceID FK
    for table, col in referencing_tables.items():
        cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE {col} = ?", (duplicate_id,))
        count = cursor.fetchone()[0]
        if count > 0:
            print(f" → Would update {count} row(s) in {table}.{col}")
            if not dry_run:
                utcmoddate = current_utcmoddate()
                update_sql = f"""
                    UPDATE {table}
                    SET {col} = ?,
                        UTCModDate = ?
                    WHERE {col} = ?
                """
                cursor.execute(update_sql, (canonical_id, utcmoddate, duplicate_id))

    # Update referencing tables with conditional OwnerID = PlaceID
    for table, owner_type in conditional_tables:
        cursor.execute(
            f"SELECT COUNT(*) FROM {table} WHERE OwnerType = ? AND OwnerID = ?",
            (owner_type, duplicate_id),
        )
        count = cursor.fetchone()[0]
        if count > 0:
            print(f" → Would update {count} row(s) in {table} (OwnerType={owner_type})")
            if not dry_run:
                utcmoddate = current_utcmoddate()
                update_sql = f"""
                    UPDATE {table}
                    SET OwnerID = ?,
                        UTCModDate = ?
                    WHERE OwnerType = ? AND OwnerID = ?
                """
                cursor.execute(
                    update_sql, (canonical_id, utcmoddate, owner_type, duplicate_id)
                )

    # Delete the duplicate place
    if not dry_run:
        cursor.execute("DELETE FROM PlaceTable WHERE PlaceID = ?", (duplicate_id,))
        print(" ✅ Duplicate place deleted.")
        conn.commit()

    print("✅ Merge complete." if not dry_run else "ℹ️ Dry-run complete.")

    # Return whether a conflict was encountered
    return len(differing_fields) > 0  # True = conflict exists


def standardize_us_county_name(name, counties_db, state_list):
    """
    Normalize U.S. county-style place names to the form:
    <county> County, <state>, USA
    """
    parts = [p.strip() for p in name.split(",")]
    if len(parts) != 3:
        return name  # Not a candidate

    county_candidate, state, country = parts
    if country.upper() != "USA":
        return name
    if state not in state_list:
        return name

    lookup = (f"{county_candidate} county".lower(), state.lower())
    for county, county_state in counties_db:
        if (county, county_state) == lookup:
            # Return properly capitalized replacement
            return f"{county_candidate} County, {state}, USA"

    return name  # No match found


def show_place_details(conn: sqlite3.Connection, place_id: int) -> dict:
    """
    Show all fields from the PlaceTable row with the given PlaceID.
    """
    cursor = conn.execute(
        """
        SELECT * FROM PlaceTable
        WHERE PlaceID = ?
    """,
        (place_id,),
    )
    row = cursor.fetchone()
    if row is None:
        print(f"[{place_id}] No such PlaceID found.")
        return {}

    # Print details in a clean way
    print(f"Details for PlaceID {place_id}:")
    for key in row.keys():
        print(f"  {key}: {row[key]}")

    return dict(row)


def current_utcmoddate():
    """
    Return the current UTC time as a float value in the format used by RootsMagic UTCModDate,
    which is the number of days since 1899-12-30.
    """
    epoch = datetime(1899, 12, 30, tzinfo=timezone.utc)
    now = datetime.now(timezone.utc)
    delta = now - epoch
    return delta.total_seconds() / 86400.0  # Convert seconds to days


def reverse_place_name(name: str) -> str:
    """Reverse the order of comma-separated fields in a place name."""
    parts = [part.strip() for part in name.split(",")]
    return ", ".join(reversed(parts))


def find_duplicate_place_names(conn: sqlite3.Connection):
    """
    return a collection of PlaceIDs where the Name matches
    """
    cursor = conn.cursor()
    cursor.execute(
        "SELECT PlaceID, Name FROM PlaceTable ORDER BY Name COLLATE RMNOCASE"
    )

    name_to_ids = defaultdict(list)
    for place_id, name in cursor.fetchall():
        key = name.strip().lower()
        name_to_ids[key].append((place_id, name.strip()))

    duplicates = {k: v for k, v in name_to_ids.items() if len(v) > 1}
    return duplicates


def merge_places(conn: sqlite3.Connection, dupes, dry_run=True):
    """
    Merge PlaceTable records by replacing references to duplicates
    with a canonical PlaceID and removing the duplicates.

    Reports any critical conflicts where merge cannot proceed safely.
    """
    from rmutils import merge_place_records, get_place_details

    critical_conflicts = []

    print("🧭 Exact Duplicate Place Names:\n")
    for group in dupes.values():
        if len(group) < 2:
            continue  # skip trivial cases

        print(f"📍 {group[0][1]}")

        survivor = group[0]
        survivor_id = survivor[0]

        for victim in group[1:]:
            victim_id = victim[0]
            print(f"🧬 Merging into PlaceID {survivor_id} from {victim_id}")
            try:
                merge_place_records(conn, survivor_id, victim_id, dry_run)
            except RuntimeError as e:
                print(f"⚠️ Merge skipped due to critical differences:\n{e}")
                # Fetch Name and full row details
                columns, s_row = get_place_details(conn, survivor_id)
                _, v_row = get_place_details(conn, victim_id)

                if s_row is None or v_row is None:
                    print(
                        "❌ Unable to retrieve one or both rows for detailed comparison.\n"
                    )
                    continue

                name = s_row[columns.index("Name")]
                print(f"\n🛑 Conflict: PlaceID {victim_id} → {survivor_id}")
                print(f"   📍 Name: {name}")

                print(
                    "    Field            | Survivor                          | Victim"
                )
                print(
                    "    -------------------------------------------------------------------------------"
                )
                for i, field in enumerate(columns):
                    if field in ("PlaceID", "UTCModDate"):
                        continue
                    val1 = s_row[i]
                    val2 = v_row[i]
                    if val1 != val2:
                        print(f"    {field:<17} | {str(val1):<32} | {str(val2)}")

                critical_conflicts.append((survivor_id, victim_id))
                print()

        print()

    if critical_conflicts:
        print("🚫 Critical conflicts detected. The following merges were skipped:")
        for survivor_id, victim_id in critical_conflicts:
            print(f" - PlaceID {victim_id} → {survivor_id}")

    if dry_run:
        print("\n✅ Dry run complete — no changes committed.")
    else:
        conn.commit()
        print("\n✅ Merges committed to database.")


def find_placeid_references(conn: sqlite3.Connection):
    # conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    referencing = []

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [row[0] for row in cursor.fetchall()]

    for table in tables:
        cursor.execute(f"PRAGMA foreign_key_list({table})")
        for row in cursor.fetchall():
            ref_table = row[2]
            from_col = row[3]
            to_col = row[4]
            if ref_table == "PlaceTable" and to_col == "PlaceID":
                referencing.append((table, from_col))

    return referencing


def get_place_details(conn, place_id):
    """
    Retrieve a full row from PlaceTable given a PlaceID.
    Returns a tuple (columns, row) or ([], None) if not found.
    """
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM PlaceTable WHERE PlaceID = ?", (place_id,))
    row = cursor.fetchone()
    if row:
        columns = [desc[0] for desc in cursor.description]
        return columns, row
    else:
        return [], None


def fix_missing_commas_in_county_state(name: str) -> str:
    """
    Fix entries like "Edinburg Shenandoah, Virginia, USA" to "Edinburg, Shenandoah, Virginia, USA"
    by inserting a missing comma between city and county.
    """
    parts = name.split(",")
    if len(parts) < 2:
        return name  # Nothing to fix

    head = parts[0].strip()
    tail = ",".join(p.strip() for p in parts[1:])

    for county, state in US_COUNTIES:
        test_suffix = f"{county}, {state}"
        if tail.startswith(test_suffix):
            if head.endswith(county):
                # Already something like "Edinburg Shenandoah"
                fixed_head = head.replace(f" {county}", f", {county}")
                return f"{fixed_head}, {tail}"
    return name





def normalize_once(pid, name):

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

    normalized = name.strip()

    # Fix accidental splits in two-word U.S. state names
    us_two_word_states = {
        "West Virginia", "South Dakota", "North Dakota",
        "New York", "New Jersey", "New Mexico",
        "Rhode Island", "New Hampshire", "North Carolina", "South Carolina"
    }
    
    for state in us_two_word_states:
        parts = state.split()
        broken = f"{parts[0]}, {parts[1]}"
        fixed = state
        if broken in normalized:
            normalized = normalized.replace(broken, fixed)
            name = normalized
            break 



    # Strip an address if present
    # this needs to be done *before*
    # checking if an address in the "NOPLACENAME" routines below 
    name, addr = strip_address_if_present(name, pid)
    if addr:
        # Optionally: save the stripped address somewhere (log, dict, etc.)
        # handling this is future work
        print(f"📍 PlaceID {pid} had its address: \"{addr}\" stripped, new name: \"{name}\"")


    # when a place name is just a single character, remove it
    # making it a null string, another routine will delete the record
    # and update referencing records
    parts = [p.strip() for p in name.split(",")]
    if len(parts) == 1:
        if len(parts[0]) < 2:
           return "NOPLACENAME"

    original = name.strip()
    
    if len(original) <= 1:
        return "NOPLACENAME"
    
    # 1. Starts with number followed by common address pattern
    if re.match(r'^\d+\s+\w+', original):
        if re.search(r'\b(St|Ave|Blvd|Rd|Ln|Dr|Ct|Way|Circle|Pl|Terrace)\b', original, re.IGNORECASE):
            print(f"📍 PlaceID {pid} contains an address: \"{original}\", it will be marked for deletion")
            # maybe handle this later to prevent data bus
            return "NOPLACENAME"

    # 2. High digit ratio
    digit_ratio = sum(c.isdigit() for c in original) / max(len(original), 1)
    if digit_ratio > 0.5:
        return "NOPLACENAME"

    # 3. Obvious placeholders
    if original.lower() in {"unknown", "n/a", "-", ".", "..."}:
        return "NOPLACENAME"

    # 4. No alphabetic characters
    if not any(c.isalpha() for c in original):
        return "NOPLACENAME"



    # Normalize PLSS-like entries
    name = re.sub(
        r'\bT(?:wp)?\s*(\d+)([NS])\s*R\s*(\d+)([EW])\b',
        r'Township \1\2 Range \3\4',
        name
    )


    # Strip "Township" from first field if in valid US state (except NJ, PA) and not a survey-style township
    lines = name.split(",")
    if len(lines) >= 3:
        state = lines[-2].strip()
        country = lines[-1].strip()
        first_field = lines[0].strip()

        if (
            country == "USA"
            and state in STATE_NAMES
            and state not in {"New Jersey", "Pennsylvania"}
            and "Township" in first_field
            and not re.search(r'\bTownship\s+\d+.*\bRange\b', first_field, re.IGNORECASE)
        ):
            # Remove "Township" and any trailing comma
            new_first = re.sub(r'\bTownship\b[,]?\s*', '', first_field).strip()
            lines[0] = new_first
            name = ", ".join([part.strip() for part in lines])
            return name  # Safe early return



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

    name = fix_missing_commas_in_county_state(name)

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


    # ───────────────────────────────────────────────
    # 🧹 Strip parenthetical text like (Independent City), (new), (7 yrs), etc.
    # Apply only once, before other cleanup
    name = re.sub(r"\s*\([^()]*\)", "", name)  # remove parentheses and enclosed text
    name = re.sub(r"\s{2,}", " ", name).strip(",. ")  # clean excess spaces and trailing punctuation



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
    name = re.sub(r" Coun,", " County,", name)
    name = re.sub(r"^County, ", "County ", name)
    name = re.sub(r"([A-Z,a-z,0-9])&([A-Z,a-z,0-9])", r"\1 & \2", name)
    name = re.sub(r"^Rural, ", "", name)
    name = re.sub(r"\(Chicago\)", "", name)
    name = re.sub(r" Ward [0-9],", ",", name)
    name = re.sub(r" Ward [0-9][0-9],", ",", name)
    name = re.sub(r"^District [0-9], ", "", name)
    name = re.sub(r"^District [0-9][0-9], ", "", name)
    name = re.sub(r" Twp,", ",", name)
    name = re.sub(r" Twp.,", ",", name)
    name = re.sub(r"^Magisterial ", "", name)
    name = re.sub(r" Assembly District [0-9],", ",", name)
    name = re.sub(r" Assembly District [0-9][0-9],", ",", name)
    name = re.sub(r"^District No [0-9], ", "", name)
    name = re.sub(r"^District No [0-9][0-9], ", "", name)
    name = re.sub(r"^Township [0-9], ", "", name)
    name = re.sub(r"^Township [0-9][0-9], ", "", name)
    name = re.sub(r"^Precinct [0-9], ", "", name)
    name = re.sub(r"^Precinct [0-9][0-9], ", "", name)
    name = re.sub(r" Irland$", " Ireland", name)
    name = re.sub(r"^Mag District No [0-9], ", "", name)
    name = re.sub(r"^Mag District No [0-9][0-9], ", "", name)
    name = re.sub(r"^Mag Dist No [0-9], ", "", name)
    name = re.sub(r"^Mag Dist No [0-9][0-9], ", "", name)
    name = re.sub(r"^Mag D No [0-9], ", "", name)
    name = re.sub(r"^Mag D No [0-9][0-9], ", "", name)
    name = re.sub(r"^Mag District [0-9], ", "", name)
    name = re.sub(r"^Mag District [0-9][0-9], ", "", name)
    name = re.sub(r"^Mag Dist [0-9], ", "", name)
    name = re.sub(r"^Mag Dist [0-9][0-9], ", "", name)
    name = re.sub(r"^Mag D [0-9], ", "", name)
    name = re.sub(r"^Mag D [0-9][0-9], ", "", name)
    name = re.sub(r"^Mag District \# [0-9], ", "", name)
    name = re.sub(r"^Mag District \# [0-9][0-9], ", "", name)
    name = re.sub(r"^Mag Dist \# [0-9], ", "", name)
    name = re.sub(r"^Mag Dist \# [0-9][0-9], ", "", name)
    name = re.sub(r"^Mag D \# [0-9], ", "", name)
    name = re.sub(r"^Mag D \# [0-9][0-9], ", "", name)
    name = re.sub(r"^Mag District \#[0-9], ", "", name)
    name = re.sub(r"^Mag District \#[0-9][0-9], ", "", name)
    name = re.sub(r"^Mag Dist \#[0-9], ", "", name)
    name = re.sub(r"^Mag Dist \#[0-9][0-9], ", "", name)
    name = re.sub(r"^Mag D \#[0-9], ", "", name)
    name = re.sub(r"^Mag D \#[0-9][0-9], ", "", name)
    name = re.sub(r"Fraanklin, ", "Franklin, ", name)
    name = re.sub(r"Bethlehm, ", "Bethlehem, ", name)
    name = re.sub(r"Abingon, ", "Abingdon, ", name)
    name = re.sub(r"Grrenv", "Grenv", name)
    name = re.sub(r"Los Angles", "Los Angeles", name)
    name = re.sub(r"Indianapoli,", "Indianapolis,", name)
    name = re.sub(r"St Louis", "St. Louis", name)
    name = re.sub(r'\bSaint\s+(?=\w)', 'St. ', name)
    name = re.sub(r'\bSt\s+(?=\w)', 'St. ', name)
    name = re.sub(r'\bPrince Georges\b', "Prince George's", name)



    # Remove parenthetical prefixes like "City (Districts 1234-5678), ..."
    name = re.sub(r'^[^,]*\([^)]*\),\s*', '', name)

    # Remove leading parenthetical if followed by duplicate city
    name = re.sub(
        r'^\s*\((.*?)\),\s*(\w[\w\s.-]+?),\s*(.+)$',
        lambda m: f"{m.group(2)}, {m.group(3)}" if m.group(2).lower() in m.group(3).lower() else m.group(0),
        name
    )


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

    # # Standarize when only the county is known to a list
    # # of known USA counties
    # name = standardize_us_county_name(name, COUNTY_DB, STATE_NAMES)

    return name


def normalize_place_iteratively(pid, name):
    previous = name
    while True:
        current = normalize_once(pid, previous)
        if current == previous:
            break
        previous = current
    return current if current != name else None


def normalize_place_names(conn: sqlite3.Connection, dry_run=True):

    cursor = conn.execute("SELECT PlaceID, Name FROM PlaceTable")
    updates = []

    for row in cursor.fetchall():
        place_id, old_name = row["PlaceID"], row["Name"]
        new_name = normalize_place_iteratively(place_id, old_name)
        if new_name:
            if new_name == "NOPLACENAME":
                print(f"🧹 PlaceID {place_id} had an old name of \"{old_name}\" and will be deleted ...")
                delete_place_id(conn, place_id, dry_run)
            else:
                updates.append((place_id, old_name, new_name))
          

    if not updates:
        print("✅ No changes needed.")
        return

    print(f"✏️  Found {len(updates)} places to normalize.")

    # # Write log
    # with open(args.logfile, "w", encoding="utf-8") as log:
    #     for pid, old, new in updates:
    #         print(f"[{pid:5}] {old:<80} → {new}")
    #         log.write(f"[{pid}] {old} → {new}\n")

    # Optional database commit
    if not dry_run:
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
        print("ℹ️  Dry run only. No changes made. Use dry_run=False to apply.")



########################################################



def delete_place_id(conn, pid, dry_run=False):
    """
    Deletes a PlaceTable record and updates referencing tables.
    Sets PlaceID or OwnerID to 0 where applicable and updates UTCModDate.
    """
    cursor = conn.cursor()
    utc_now = current_utcmoddate()

    # Make sure pid is in the table
    cursor.execute("SELECT PlaceID FROM PlaceTable WHERE PlaceID = ?", (pid,))
    empty_id = [row[0] for row in cursor.fetchall()]

    if not empty_id:
        print("✅ No place with PlaceID = {pid} found.")
        return

    print(f"🧹 Deleting PlaceID {pid} (and cleaning referencing records) ...")

    # Direct references to PlaceID
    referencing_tables = {
        "EventTable": "PlaceID",
        "FANTable": "PlaceID",
    }

    for table, col in referencing_tables.items():
        if table_has_column(cursor, table, col):
            update_sql = f"UPDATE {table} SET {col} = 0"
            params = []
            if table_has_column(cursor, table, "UTCModDate"):
                update_sql += ", UTCModDate = ?"
                params.append(utc_now)
            update_sql += f" WHERE {col} = ?"
            params.append(pid)
            if not dry_run:
                print(f"🧹 Cleaning {table} record.\n{update_sql}, {params}")
                cursor.execute(update_sql, tuple(params))
                updated_rows = cursor.rowcount
                print(f"✅ Updated {updated_rows} rows in {table}")


    # Conditionally referencing tables
    conditional_refs = [
        ("TaskLinkTable", 5),
        ("URLTable", 5),
        ("MediaLinkTable", 14),
        ("TaskLinkTable", 14),
    ]


    for table, owner_type in conditional_refs:
        if not table_has_column(cursor, table, "OwnerType") or not table_has_column(cursor, table, "OwnerID"):
            continue

        update_sql = f"UPDATE {table} SET OwnerID = 0"
        params = []
        if table_has_column(cursor, table, "UTCModDate"):
            update_sql += ", UTCModDate = ?"
            params.append(utc_now)
        update_sql += " WHERE OwnerType = ? AND OwnerID = ?"
        params.extend([owner_type, pid])
        if not dry_run:
            print(f"🧹 Cleaning {table} record.\n{update_sql}, {params}")
            cursor.execute(update_sql, tuple(params))
            updated_rows = cursor.rowcount
            print(f"✅ Updated {updated_rows} rows in {table}")


    # Delete the PlaceTable row
    if not dry_run:
        cursor.execute("DELETE FROM PlaceTable WHERE PlaceID = ?", (pid,))
        print(f"🗑️ Deleted PlaceID {pid}")


def delete_blank_place_records(conn, dry_run=False):
    """
    Deletes PlaceTable rows where Name is an empty string.
    Updates referencing and conditionally referencing tables,
    setting PlaceID or OwnerID to 0 and updating UTCModDate if present.
    """
    cursor = conn.cursor()

    # Get all PlaceIDs with blank names
    cursor.execute("SELECT PlaceID FROM PlaceTable WHERE TRIM(Name) = ''")
    blank_place_ids = [row[0] for row in cursor.fetchall()]
    if not blank_place_ids:
        print("✅ No blank place names found.")
        return

    for pid in blank_place_ids:
        delete_place_id(conn, pid, dry_run=dry_run)

    if not dry_run:
        conn.commit()
        print(f"✅ Removed {len(blank_place_ids)} PlaceTable record(s) with empty names.")
    else:
        print(f"ℹ️ Dry run complete — {len(blank_place_ids)} PlaceTable record(s) would be removed.")



def table_has_column(cursor, table_name, column_name):
    """
    Returns True if the given table has a column with the given name.
    """
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = [row[1] for row in cursor.fetchall()]
    return column_name in columns




# Ensure longer suffixes match first
# STREET_SUFFIXES = sorted(STREET_SUFFIXES, key=lambda s: -len(s))


# DIRECTIONALS = ["N", "S", "E", "W", "NE", "NW", "SE", "SW"]

# # Compile regex for known address pattern
# ADDRESS_PATTERN = re.compile(
#     r"""^                    # start of string
#     \d+[\w\-]*               # street number (with optional letter/number)
#     (?:\s+[NESW]{{1,2}})?    # optional directional (escaped curly braces)
#     (?:\s+\w+)+              # street name
#     (?:\s+(?:{}))\.?         # street type
#     """.format("|".join(STREET_SUFFIXES)),
#     re.IGNORECASE | re.VERBOSE
# )


def strip_address_if_present(name: str, pid: int) -> tuple[str, str | None]:
    """
    Attempts to strip a street address from the beginning of a place name.
    Returns the updated name and the stripped address if found.
    """
    original_name = name

    # Sort suffixes longest-first to avoid partial matches (e.g. "St" matching "Street")
    from config import STREET_SUFFIXES
    suffixes_sorted = sorted(STREET_SUFFIXES, key=lambda s: -len(s))
    suffix_pattern = r'\b(?:' + '|'.join(re.escape(s) for s in suffixes_sorted) + r')\b'

    # Example matches: "123 Main St.", "1209 21st Ave, Rock Island", "40500 119th St, Genoa City, WI"
    pattern = re.compile(
        r'^\s*'                                 # Leading spaces
        r'(?P<addr>\d{1,6}(?:\s+\w+){0,4})\s+'  # Address number and street words (up to 4)
        + suffix_pattern +                     # A valid suffix
        r'(?:\s*\.\s*|\s*,\s*|\s+)'            # Separator (dot/comma/space)
        r'(?P<tail>.*)$',                      # Remainder of the name
        flags=re.IGNORECASE
    )

    match = pattern.match(name)
    if match:
        # Capture the address and remainder
        prefix = match.group('addr').strip()
        suffix_match = pattern.pattern[pattern.pattern.find('(?:'):]  # For clarity only
        # We extract from the match string directly
        full_address = match.group(0)[:match.end('addr')].strip()
        address = full_address
        tail = match.group('tail').strip()
        new_name = tail if tail else "NOPLACENAME"

        print(f"📍 PlaceID {pid} \"{original_name}\" had its address: \"{address}\" stripped, new name: \"{new_name}\"")
        return new_name, address

    return name, None


