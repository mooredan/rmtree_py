# rmutils.py
import sqlite3
import pandas as pd
import os
import sys
import csv
from config import rmtree_path, extension_path


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
        placeholders = ','.join('?' for _ in person_ids)
        base_query += f" AND OwnerID IN ({placeholders})"
        params = tuple(person_ids)

    cursor = conn.execute(base_query, params)
    return {row["PersonID"]: (row["Given"], row["Surname"]) for row in cursor.fetchall()}


# -------------------------------------
# RootsMagic Data Mutation Utilities
# -------------------------------------
def merge_places(conn, canonical_id, duplicate_id, dry_run=True):
    if canonical_id == duplicate_id:
        raise ValueError("Canonical and duplicate IDs must differ.")

    referencing_tables = {
        "EventTable": "PlaceID",
        "AddressTable": "PlaceID",
        "CitationTable": "PlaceID",
        "MediaLinkTable": "PlaceID",
        "PlaceLinkTable": "PlaceID",
    }

    cursor = conn.cursor()

    # Fetch both PlaceTable rows
    cursor.execute("SELECT * FROM PlaceTable WHERE PlaceID = ?", (canonical_id,))
    canonical_row = cursor.fetchone()
    cursor.execute("SELECT * FROM PlaceTable WHERE PlaceID = ?", (duplicate_id,))
    duplicate_row = cursor.fetchone()

    if not canonical_row or not duplicate_row:
        raise ValueError("One or both PlaceIDs do not exist.")

    print(f"🔁 Merging PlaceID {duplicate_id} → {canonical_id}")
    if dry_run:
        print("Dry-run mode: no changes will be written.")

    # Field-by-field comparison
    print("\n📋 Comparing PlaceTable fields:")
    differing_fields = []
    for idx, col in enumerate(cursor.description):
        field = col[0]
        val1 = canonical_row[idx]
        val2 = duplicate_row[idx]
        if val1 != val2:
            differing_fields.append(field)
            print(f" ⚠️ {field}:")
            print(f"    Canonical: {repr(val1)}")
            print(f"    Duplicate: {repr(val2)}")

    if not differing_fields:
        print(" ✅ All fields match.")

    # Update references in other tables
    for table, col in referencing_tables.items():
        update_sql = f"""
            UPDATE {table}
            SET {col} = ?
            WHERE {col} = ?
        """
        cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE {col} = ?", (duplicate_id,))
        count = cursor.fetchone()[0]
        if count > 0:
            print(f" → Would update {count} row(s) in {table}.{col}")
            if not dry_run:
                cursor.execute(update_sql, (canonical_id, duplicate_id))

    # Delete the duplicate place
    if not dry_run:
        cursor.execute("DELETE FROM PlaceTable WHERE PlaceID = ?", (duplicate_id,))
        print(" ✅ Duplicate place deleted.")
        conn.commit()

    print("✅ Merge complete." if not dry_run else "ℹ️ Dry-run complete.")



def load_county_database(path="american_counties.csv"):
    """Load valid U.S. counties from a CSV into a set of (county, state) pairs."""
    counties = set()
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) != 3:
                continue
            county, state, country = [r.strip() for r in row]
            if country.upper() == "USA":
                counties.add((county.lower(), state.lower()))
    return counties


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


def find_duplicate_place_names():
    if not os.path.exists(DB_PATH):
        print(f"❌ Database not found: {DB_PATH}")
        return
    if not os.path.exists(EXT_PATH):
        print(f"❌ Extension not found: {EXT_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    conn.enable_load_extension(True)
    conn.execute(f"SELECT load_extension('{EXT_PATH}')")
    conn.execute("REINDEX RMNOCASE;")
    conn.enable_load_extension(False)

    cursor = conn.cursor()
    cursor.execute("SELECT PlaceID, Name FROM PlaceTable ORDER BY Name COLLATE RMNOCASE")

    name_to_ids = defaultdict(list)
    for place_id, name in cursor.fetchall():
        key = name.strip().lower()
        name_to_ids[key].append((place_id, name.strip()))

    duplicates = {k: v for k, v in name_to_ids.items() if len(v) > 1}

    print("🧭 Exact Duplicate Place Names (Post-Normalization):\n")
    for group in duplicates.values():
        print(f"📍 {group[0][1]}")
        for place_id, _ in group:
            print(f"   - [PlaceID: {place_id}]")
        print()

    print(f"\nTotal duplicate groups: {len(duplicates)}")
    conn.close()
