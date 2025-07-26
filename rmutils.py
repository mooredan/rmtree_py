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
    UNIQUE_FACT_TYPES,
)

from normalizer import (
    normalize_once,
    strip_address_if_present,
    is_nonsensical_place_name
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


def merge_place_records(conn, canonical_id, duplicate_id, dry_run=True, brief=True):
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

    if not brief:
        print(f"    🔁 Merging PlaceID {duplicate_id} → {canonical_id}")
    if dry_run:
        print("Dry-run mode: no changes will be written.")

    if not brief:
       print("        📋 Comparing PlaceTable fields:")

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
            if not brief:
                print(f"           ⚠️ {field}:")
                print(f"              Canonical: {repr(val1)}")
                print(f"              Duplicate: {repr(val2)}")

    if not differing_fields:
        if not brief:
            print("        ✅ All fields match.")

    # Update referencing tables with direct PlaceID FK
    for table, col in referencing_tables.items():
        cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE {col} = ?", (duplicate_id,))
        count = cursor.fetchone()[0]
        if count > 0:
            if not brief:
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
        if not brief:
            print(" ✅ Duplicate place deleted.")

        conn.commit()

    if not brief:
        print("✅ Merge complete." if not dry_run else "ℹ️ Dry-run complete.")

    # Return whether a conflict was encountered
    return len(differing_fields) > 0  # True = conflict exists



def get_place_name_from_id(conn: sqlite3.Connection, place_id):
    """
    Get Name of a place from PlaceID.
    """
    cursor = conn.execute(
        """
        SELECT Name FROM PlaceTable
        WHERE PlaceID = ?
        """, (place_id,),
    )

    row = cursor.fetchone()

    if row is None:
        print(f"❌ No such PlaceID: {place_id} found.")
        return {}

    name = row[0]
    return name



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



def find_duplicate_place_names(conn: sqlite3.Connection, brief=True):
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


def merge_places(conn: sqlite3.Connection, dupes, dry_run=True, brief=True):
    """
    Merge PlaceTable records by replacing references to duplicates
    with a canonical PlaceID and removing the duplicates.

    Reports any critical conflicts where merge cannot proceed safely.
    """
    from rmutils import merge_place_records, get_place_details

    critical_conflicts = []

    print("\n🧭 Exact Duplicate Place Names:")
    for group in dupes.values():
        if len(group) < 2:
            continue  # skip trivial cases

        if not brief:
            print(f"    📍 {group[0][1]}")

        survivor = group[0]
        survivor_id = survivor[0]

        for victim in group[1:]:
            victim_id = victim[0]
            if not brief:
                print(f"    🧬 Merging into PlaceID {survivor_id} from {victim_id}")

            try:
                merge_place_records(conn, survivor_id, victim_id, dry_run, brief)
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
                # print()

        # print()

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




########################################################



def delete_place_id(conn: sqlite3.Connection, pid: int, dry_run=False, brief=True) -> bool:
    """
    Deletes a place from PlaceTable and removes all references to it from other tables.
    Skips deletion if PlaceType == 1 or if the place is still referenced.
    Sets PlaceID or OwnerID to 0 where applicable and updates UTCModDate.
    Returns True if deleted, False otherwise.
    """
    cursor = conn.cursor()

    # Check PlaceType first
    cursor.execute("SELECT PlaceType FROM PlaceTable WHERE PlaceID = ?", (pid,))
    row = cursor.fetchone()
    if not row:
        print(f"[delete_place_id] PlaceID {pid} not found.")
        return False
    if row[0] == 1:
        print(f"[delete_place_id] PlaceID {pid} has PlaceType == 1, skipping.")
        return False

    utc_now = current_utcmoddate()

    # Make sure pid is in the table
    cursor.execute("SELECT PlaceID FROM PlaceTable WHERE PlaceID = ?", (pid,))
    empty_id = [row[0] for row in cursor.fetchall()]

    if not empty_id:
        print("✅ No place with PlaceID = {pid} found.")
        return False

    if not brief:
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
                if not brief:
                    # print(f"    🧹 Cleaning {table} record.\n{update_sql}, {params}")
                    print(f"    🧹 Cleaning {table} record.")

                cursor.execute(update_sql, tuple(params))
                updated_rows = cursor.rowcount
                if updated_rows > 0:
                    if not brief:
                        print(f"    ✅ Updated {updated_rows} rows in {table}")


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
            if not brief:
                # print(f"    🧹 Cleaning {table} record.\n{update_sql}, {params}")
                print(f"    🧹 Cleaning {table} record.")
            cursor.execute(update_sql, tuple(params))
            updated_rows = cursor.rowcount
            if updated_rows > 0:
                if not brief:
                    print(f"    ✅ Updated {updated_rows} rows in {table}")


    # Delete the PlaceTable row
    if not dry_run:
        cursor.execute("DELETE FROM PlaceTable WHERE PlaceID = ?", (pid,))
        if not brief:
            print(f"🗑️ Deleted PlaceID {pid}")

    return True 


def delete_blank_place_records(conn, dry_run=False, brief=True):
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
        ret = delete_place_id(conn, pid, dry_run=dry_run, brief=brief)

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




def report_non_normalized_places(conn, limit: int = 1000, show_references: bool = False):
    """
    Scans PlaceTable and reports place names that appear non-normalized,
    such as:
      - all upper or all lower case
      - extra punctuation
      - trailing/leading whitespace
      - missing commas between jurisdiction levels
      - names with unusual characters or numeric-only
      - excessive repetition or patterns
    """
    cursor = conn.cursor()
    cursor.execute("SELECT PlaceID, Name FROM PlaceTable WHERE PlaceType != 1 ORDER BY Name COLLATE NOCASE")

    import re

    bad_places = []
    for pid, name in cursor.fetchall():
        reasons = []


        # Skip EE-acceptable place names
        name_lc = name.lower().strip()
        
        # Rule 1: Accept historical territories without 'USA'
        if name.endswith("Territory") and "USA" not in name:
            continue
        
        # Rule 2: Accept full country names alone
        if "," not in name and name in FOREIGN_COUNTRIES:
            continue
        
        # Rule 3: Accept "At sea"
        if name_lc == "at sea":
            continue
        

        parts = [p.strip() for p in name.split(",")]


        # detect things like "Clay County, Clay, Indiana, USA"
        if len(parts) == 4 and parts[-1].upper() == "USA":
            state = parts[2].strip()
            if state in STATE_NAMES and parts[0].endswith(" County"):
                reasons.append("county name misordered")


        if is_non_county_missing_county(name):
            reasons.append("USA place missing county")


        if not name.strip():
            reasons.append("empty or whitespace")

        if name != name.strip():
            reasons.append("leading/trailing whitespace")

        if name.isupper():
            reasons.append("all UPPERCASE")

        if name.islower():
            reasons.append("all lowercase")

        if re.match(r'^[0-9 ,.-]+$', name):
            reasons.append("numeric or punctuation only")




        if re.search(r'[!?@#$%^&*+=<>]', name):
            reasons.append("unusual characters")

        if re.search(r'\b(?:unknown|unkown|none|blank)\b', name, re.IGNORECASE):
            reasons.append("unknown placeholder")

        if name.count(',') == 0 and ' ' in name and not name.endswith('.'):
            reasons.append("missing commas between jurisdiction levels?")


        # if re.search(r'\b(\w+)\b(?:,\s*)?\s+\1\b', name, re.IGNORECASE):
            # reasons.append("duplicated word")


        # first two parts are the same 
        if len(parts) > 1:
            if parts[0] == parts[1]:
                reasons.append("first two fields identical")


        for part in parts:
            if re.match(r'^[0-9 ,.-]+$', part):
                reasons.append("numeric or punctuation only in any field")



        if '  ' in name:
            reasons.append("double spaces")

        if '()' in name or '(unknown)' in name.lower():
            reasons.append("empty or unknown parentheses")

        if reasons:
            bad_places.append((pid, name, "; ".join(reasons)))

        if len(bad_places) >= limit:
            break

    if bad_places:
        print(f"\n🚩 Found {len(bad_places)} potentially non-normalized place names:")
        for pid, name, reason in bad_places:
            print(f"  [{pid}] {name}  ⟶  {reason}")
            # dump_place_usage(conn, pid)
            if show_references:
                _print_event_references_for_place_id(conn, pid)
    else:
        print("✅ No suspicious place names detected.")




def dump_place_usage(conn: sqlite3.Connection, place_id: int):
    """
    Dump all database references to a given PlaceID.
    """
    print(f"\n==== PLACE USAGE REPORT FOR PlaceID: {place_id} ====")

    # 1. Show PlaceTable entry
    row = conn.execute("SELECT * FROM PlaceTable WHERE PlaceID = ?", (place_id,)).fetchone()
    if row:
        print("\n-- PlaceTable:")
        for k in row.keys():
            print(f"  {k}: {row[k]}")
    else:
        print("\n-- PlaceTable: No record found.")

    # 2. EventTable: direct PlaceID reference
    rows = conn.execute("SELECT EventID, OwnerID, EventType, Date, PlaceID FROM EventTable WHERE PlaceID = ?", (place_id,)).fetchall()
    if rows:
        print("\n-- EventTable:")
        for r in rows:
            print(dict(r))

    # 3. MediaLinkTable: OwnerType=14 means PlaceID
    rows = conn.execute("SELECT * FROM MediaLinkTable WHERE OwnerType = 14 AND OwnerID = ?", (place_id,)).fetchall()
    if rows:
        print("\n-- MediaLinkTable (OwnerType=14):")
        for r in rows:
            print(dict(r))

    # 4. TaskLinkTable: OwnerType=5 or 14 means PlaceID
    rows = conn.execute("SELECT * FROM TaskLinkTable WHERE OwnerType IN (5, 14) AND OwnerID = ?", (place_id,)).fetchall()
    if rows:
        print("\n-- TaskLinkTable (OwnerType=5 or 14):")
        for r in rows:
            print(dict(r))

    # 5. URLTable: OwnerType=5 means PlaceID
    rows = conn.execute("SELECT * FROM URLTable WHERE OwnerType = 5 AND OwnerID = ?", (place_id,)).fetchall()
    if rows:
        print("\n-- URLTable (OwnerType=5):")
        for r in rows:
            print(dict(r))

    print("\n==== END REPORT ====\n")



# 
# def print_event_references_for_place_id(conn: sqlite3.Connection, place_id: int):
#     cursor = conn.cursor()
#     cursor.execute("SELECT Name FROM PlaceTable WHERE PlaceID = ?", (place_id,))
#     row = cursor.fetchone()
#     place_name = row[0] if row else "(Unknown)"
# 
#     query = """
#     SELECT
#         e.EventID,
#         e.OwnerID,
#         e.OwnerType,
#         e.EventType,
#         p.PersonID,
#         n.Given || ' ' || n.Surname AS FullName
#     FROM EventTable e
#     LEFT JOIN PersonTable p
#         ON e.OwnerType = 0 AND e.OwnerID = p.PersonID
#     LEFT JOIN NameTable n
#         ON p.PersonID = n.OwnerID AND n.IsPrimary = 1
#     WHERE e.PlaceID = ?
#     ORDER BY e.EventID;
#     """
#     cursor.execute(query, (place_id,))
#     rows = cursor.fetchall()
# 
#     # print(f"\nPlaceID: {place_id}  Name: {place_name}")
#     print(f"\nPlaceID: {place_id}  Name: {place_name}")
#     print(f"{'EventID':<8} {'OwnerID':<8} {'OwnerType':<10} {'EventType':<9} {'FactName':<20} {'PersonID':<10} {'Full Name'}")
# 
#     for event_id, owner_id, owner_type, event_type, person_id, full_name in rows:
#         fact_name = UNIQUE_FACT_TYPES.get(event_type, f"Unknown ({event_type})")
#         print(f"{event_id:<8} {owner_id:<8} {owner_type:<10} {event_type:<9} {fact_name:<20} {person_id or '':<10} {full_name or ''}")
# 
# 

# def print_event_references_for_place_id(conn: sqlite3.Connection, place_id: int):
#     cursor = conn.cursor()
# 
#     # Get place name for display
#     cursor.execute("SELECT Name FROM PlaceTable WHERE PlaceID = ?", (place_id,))
#     row = cursor.fetchone()
#     place_name = row[0] if row else "(Unknown)"
# 
#     # Query for events referencing this place
#     query = """
#     SELECT
#         e.EventID,
#         e.OwnerID,
#         e.OwnerType,
#         e.EventType,
#         p.PersonID,
#         n.Given || ' ' || n.Surname AS FullName
#     FROM EventTable e
#     LEFT JOIN PersonTable p
#         ON e.OwnerType = 0 AND e.OwnerID = p.PersonID
#     LEFT JOIN NameTable n
#         ON p.PersonID = n.OwnerID AND n.IsPrimary = 1
#     WHERE e.PlaceID = ?
#     ORDER BY e.EventID;
#     """
#     cursor.execute(query, (place_id,))
#     rows = cursor.fetchall()
# 
#     # Print header
#     print(f"{'PlaceID':<8} {'PlaceName':<30} {'EventID':<8} {'OwnerID':<8} {'OwnerType':<10} {'EventType':<9} {'FactName':<20} {'PersonID':<10} {'Full Name'}")
# 
#     for event_id, owner_id, owner_type, event_type, person_id, full_name in rows:
#         fact_name = UNIQUE_FACT_TYPES.get(event_type, f"Unknown ({event_type})")
#         print(f"{place_id:<8} {place_name:<30} {event_id:<8} {owner_id:<8} {owner_type:<10} {event_type:<9} {fact_name:<20} {person_id or '':<10} {full_name or ''}")




# def print_event_references_for_place_id(conn: sqlite3.Connection, place_id: int):
#     """
#     For a given PlaceID, print: PlaceID, EventID, OwnerID, OwnerType, EventType, PersonID, and Person's primary name.
#     """
#     query = """
#     SELECT
#         e.PlaceID,
#         e.EventID,
#         e.OwnerID,
#         e.OwnerType,
#         e.EventType,
#         p.PersonID,
#         n.Given || ' ' || n.Surname AS FullName
#     FROM EventTable e
#     LEFT JOIN PersonTable p
#         ON e.OwnerType = 0 AND e.OwnerID = p.PersonID
#     LEFT JOIN NameTable n
#         ON p.PersonID = n.OwnerID AND n.IsPrimary = 1
#     WHERE e.PlaceID = ?
#     ORDER BY e.EventID;
#     """
#     cursor = conn.cursor()
#     cursor.execute(query, (place_id,))
#     rows = cursor.fetchall()
# 
#     print(f"{'PlaceID':<8} {'EventID':<8} {'OwnerID':<8} {'OwnerType':<10} {'EventType':<10} {'PersonID':<10} {'Full Name'}")
#     for row in rows:
#         print(f"{row[0]:<8} {row[1]:<8} {row[2]:<8} {row[3]:<10} {row[4]:<10} {row[5] if row[5] else '':<10} {row[6] or ''}")



def is_place_referenced(conn: sqlite3.Connection, place_id: int, quiet=True, debug=False) -> bool:
    """
    Check if a PlaceID is referenced by any other table in the database.
    Prints a report and returns True if found elsewhere, False if orphaned.
    """
    if debug:
        print(f"\n==== PLACE USAGE CHECK FOR PlaceID: {place_id} ====")
    referenced = False

    # 1. EventTable
    rows = conn.execute("SELECT EventID FROM EventTable WHERE PlaceID = ?", (place_id,)).fetchall()
    if rows:
        if debug:
            print(f"-- Referenced in EventTable: {len(rows)} rows")
        referenced = True

    # 2. MediaLinkTable (OwnerType = 14 = Place)
    rows = conn.execute("SELECT MediaID FROM MediaLinkTable WHERE OwnerType = 14 AND OwnerID = ?", (place_id,)).fetchall()
    if rows:
        if debug:
            print(f"-- Referenced in MediaLinkTable: {len(rows)} rows")
        referenced = True

    # 3. TaskLinkTable (OwnerType = 5 or 14 = Place)
    rows = conn.execute("SELECT TaskID FROM TaskLinkTable WHERE OwnerType IN (5, 14) AND OwnerID = ?", (place_id,)).fetchall()
    if rows:
        if debug:
            print(f"-- Referenced in TaskLinkTable: {len(rows)} rows")
        referenced = True


    # 4. URLTable (OwnerType = 5 = Place)
    rows = conn.execute("SELECT LinkID FROM URLTable WHERE OwnerType = 5 AND OwnerID = ?", (place_id,)).fetchall()
    if rows:
        if debug:
            print(f"-- Referenced in URLTable: {len(rows)} rows")
        referenced = True

    if not referenced:
        if debug:
            print("-- No external references found.")

    if debug:
        print("==== END CHECK ====\n")

    return referenced




def get_all_place_ids(conn: sqlite3.Connection) -> list[int]:
    """
    Returns a list of all PlaceID values from the PlaceTable.
    Ignore PlaceType == 1 - these are built-in LDS locations
    """
    rows = conn.execute("SELECT PlaceID FROM PlaceTable WHERE PlaceType != 1").fetchall()
    return [row[0] for row in rows]


############################################################
# place mapping checker
############################################################

def get_single_field_places(conn):
    cursor = conn.execute("""
        SELECT PlaceID, Name
        FROM PlaceTable
        WHERE PlaceType != 1
          AND Name NOT LIKE '%,%'
          AND Name IS NOT NULL
          AND TRIM(Name) != ''
        ORDER BY Name
    """)
    return cursor.fetchall()

def build_known_place_lookup(conn):
    cursor = conn.execute("""
        SELECT Name
        FROM PlaceTable
        WHERE PlaceType != 1
          AND Name LIKE '%,%'
        ORDER BY Name
    """)
    return cursor.fetchall()



def find_matches_against_known_segments(conn):
    single_field_places = get_single_field_places(conn)
    # known_segments = build_known_place_lookup(conn)

    full_names = []
    single_names = []

    cursor = conn.execute("""
        SELECT Name 
        FROM PlaceTable 
        WHERE PlaceType != 1 
          AND Name LIKE '%,%'
          AND TRIM(Name) != ''
        ORDER BY Name
    """)

    rows = cursor.fetchall()       

    for row in rows:
        full_name = row[0]
        full_names.append(full_name)
        # print(f"{full_name}")


    cursor = conn.execute("""
        SELECT PlaceID, Name
        FROM PlaceTable
        WHERE PlaceType != 1
          AND Name NOT LIKE '%,%'
          AND Name IS NOT NULL
          AND TRIM(Name) != ''
        ORDER BY Name
    """)

    rows = cursor.fetchall()       

    for row in rows:
        single_name = row[1]
        single_name_pid = row[0]
        if (is_foreign_country(single_name)):
            continue
        if (is_us_territory(single_name)):
            continue
        if (single_name == "Mexico"):
            continue
        single_names.append([single_name_pid, single_name])
        # print(f"{single_name}")
    
    show_matches = False

    no_match_pids = []

    for row in single_names:
        single_name = row[1]
        pid = row[0]
        match_found = False 
        for full_name in full_names:
            if re.search(single_name, full_name):
                match_found = True
                if show_matches:
                    print(f"    Found \"{single_name}\" in \"{full_name}\"")
        if not match_found:
            print(f"    No match found: {pid} \"{single_name}\"")
            # dump_place_usage(conn, pid)
            no_match_pids.append(pid)
            # print_event_references_for_place_id(conn, pid)


    print_event_references_for_place_ids(conn, no_match_pids)



def print_event_references_for_place_ids(conn: sqlite3.Connection, place_ids: list[int]):
    """
    Wrapper function that prints all event references for a list of PlaceIDs.
    """
    print(f"{'PlaceID':<8} {'PlaceName':<30} {'EventID':<8} {'OwnerID':<8} {'OwnerType':<10} {'EventType':<9} {'FactName':<20} {'PersonID':<10} {'Full Name'}")
    for place_id in place_ids:
        _print_event_references_for_place_id(conn, place_id)



def _print_event_references_for_place_id(conn: sqlite3.Connection, place_id: int):
    """
    Prints all events referencing a given PlaceID (internal use).
    """
    cursor = conn.cursor()

    # Get place name
    cursor.execute("SELECT Name FROM PlaceTable WHERE PlaceID = ?", (place_id,))
    row = cursor.fetchone()
    place_name = row[0] if row else "(Unknown)"

    # Query for event references
    query = """
    SELECT
        e.EventID,
        e.OwnerID,
        e.OwnerType,
        e.EventType,
        p.PersonID,
        n.Given || ' ' || n.Surname AS FullName
    FROM EventTable e
    LEFT JOIN PersonTable p
        ON e.OwnerType = 0 AND e.OwnerID = p.PersonID
    LEFT JOIN NameTable n
        ON p.PersonID = n.OwnerID AND n.IsPrimary = 1
    WHERE e.PlaceID = ?
    ORDER BY e.EventID;
    """
    cursor.execute(query, (place_id,))
    rows = cursor.fetchall()

    for event_id, owner_id, owner_type, event_type, person_id, full_name in rows:
        fact_name = UNIQUE_FACT_TYPES.get(event_type, f"Unknown ({event_type})")
        print(f"{place_id:<8} {place_name:<30} {event_id:<8} {owner_id:<8} {owner_type:<10} {event_type:<9} {fact_name:<20} {person_id or '':<10} {full_name or ''}")



def get_all_places(conn: sqlite3.Connection) -> list[tuple[int, str]]:
    """
    Returns a list of (PlaceID, Name) tuples from PlaceTable
    excluding entries with PlaceType == 1.
    """
    cursor = conn.cursor()
    cursor.execute("""
        SELECT PlaceID, Name
        FROM PlaceTable
        WHERE PlaceType != 1
        ORDER BY PlaceID
    """)
    return cursor.fetchall()



def update_place_name(conn: sqlite3.Connection, place_id: int, new_name: str) -> None:
    """
    Updates the Name field of the given PlaceID in PlaceTable.
    Also updates the UTCModDate to the current UTC time.
    """
    cursor = conn.cursor()
    mod_date = current_utcmoddate()
    cursor.execute("""
        UPDATE PlaceTable
        SET Name = ?, UTCModDate = ?
        WHERE PlaceID = ?
    """, (new_name, mod_date, place_id))
    conn.commit()




def split_place(name: str) -> list[str]:
    """
    Splits a place name string into components using commas.
    Strips leading/trailing whitespace from each component.
    Example: "Salt Lake City, Salt Lake, Utah, USA" → ["Salt Lake City", "Salt Lake", "Utah", "USA"]
    """
    return [part.strip() for part in name.split(",") if part.strip()]


def join_place(parts: list[str]) -> str:
    """
    Joins a list of place components into a standardized place string.
    Example: ["Salt Lake City", "Salt Lake", "Utah", "USA"] → "Salt Lake City, Salt Lake, Utah, USA"
    """
    return ", ".join(parts)



# def infer_and_insert_missing_county(conn, dry_run=True):
#     """
#     Find 3-part place names that are missing a county,
#     and expand them using matching 4-part entries and known US_COUNTIES.
#     If dry_run is True, only prints proposed updates.
#     """
#     three_field = {}
#     four_field = {}
# 
#     COUNTY_SET = set(US_COUNTIES)
# 
#     for pid, name in get_all_places(conn):
#         parts = split_place(name)
#         if len(parts) == 3 and parts[2] == "USA" and parts[1] in STATE_NAMES:
#             three_field[(parts[0], parts[1])] = (pid, parts)
#         elif len(parts) == 4 and parts[3] == "USA" and (parts[1], parts[2]) in COUNTY_SET:
#             four_field[(parts[0], parts[2])] = parts[1]
# 
#     for (city, state), (pid3, parts3) in three_field.items():
#         if (city, state) in four_field:
#             county = four_field[(city, state)]
#             new_parts = [city, county, state, "USA"]
#             new_name = join_place(new_parts)
#             if dry_run:
#                 print(f"📝 Would update PlaceID {pid3}: '{join_place(parts3)}' → '{new_name}'")
#             else:
#                 update_place_name(conn, pid3, new_name)
#                 print(f"✅ Updated PlaceID {pid3}: '{join_place(parts3)}' → '{new_name}'")
# 


def infer_and_insert_missing_county(conn, dry_run=True, brief=False):
    """
    Scan PlaceTable for 3-field US place names (City, State, USA) and see if there’s a
    corresponding 4-field (City, County, State, USA) match. If found, insert County into 3-field name.

    Skips updates where the 4-field name has the same City and County (e.g., "Kankakee, Kankakee, Illinois, USA").
    """
    places = get_all_places(conn)
    place_map = {pid: split_place(name) for pid, name in places}

    reverse_lookup = {}  # {(city, state): (county, pid)}

    # Build reference from 4-field names
    for pid, fields in place_map.items():
        if len(fields) == 4 and fields[3] == "USA":
            city, county, state, _ = fields
            if state in STATE_NAMES and (county, state) in US_COUNTIES:
                # Skip if city == county (e.g., "Kankakee, Kankakee, Illinois, USA")
                if city.strip().lower() == county.strip().lower():
                    continue
                reverse_lookup[(city.strip(), state.strip())] = (county.strip(), pid)


    count = 0

    # Check for 3-field names that could be enriched
    for pid, fields in place_map.items():
        if len(fields) == 3 and fields[2] == "USA":
            city, state, _ = fields
            key = (city.strip(), state.strip())
            if key in reverse_lookup:
                county, ref_pid = reverse_lookup[key]
                new_fields = [city.strip(), county, state.strip(), "USA"]
                new_name = join_place(new_fields)
                if not brief:
                    print(f"📝 Would update PlaceID {pid}: '{join_place(fields)}' → '{new_name}'")
                count += 1
                if not dry_run:
                    if not brief:
                        print(f"📝 Updating PlaceID {pid}: '{join_place(fields)}' → '{new_name}'")
                    update_place_name(conn, pid, new_name)

    if dry_run:
        print(f"✅ Would update {count} places\n")
    else:
        print(f"✅ Updated {count} places\n")





def is_non_county_missing_county(place: str) -> bool:
    parts = [p.strip() for p in place.split(",")]
    if len(parts) != 3:
        return False
    city, state, country = parts
    if country.upper() != "USA":
        return False
    if state not in STATE_NAMES:
        return False
    if city.endswith(" County"):
        return False
    if (city, state) in US_COUNTIES:
        return False
    return True





# Usage example:
# with sqlite3.connect('yourfile.rmtree') as conn:
#     find_matches_against_known_segments(conn)

def is_foreign_country(name: str) -> bool:
    normalized = name.strip().casefold()  # normalize whitespace and case
    return any(country.casefold() == normalized for country in FOREIGN_COUNTRIES)
    # return any(country == name for country in FOREIGN_COUNTRIES)

def is_us_territory(name: str) -> bool:
    normalized = name.strip().casefold()  # normalize whitespace and case
    return any(country.casefold() == normalized for country in HISTORICAL_US_TERRITORIES) 

