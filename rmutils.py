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




def report_non_normalized_places(conn, limit: int = 1000):
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
    cursor.execute("SELECT PlaceID, Name FROM PlaceTable ORDER BY Name COLLATE NOCASE")

    import re

    bad_places = []
    for pid, name in cursor.fetchall():
        reasons = []

        parts = [p.strip() for p in name.split(",")]



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
    else:
        print("✅ No suspicious place names detected.")


