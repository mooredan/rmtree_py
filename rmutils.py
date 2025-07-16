# rmutils.py
import sqlite3
import pandas as pd
import os
import sys
from config import rmtree_path, extension_path


def get_config():
    import config  # This is your separate config.py file

    return {
        "rmtree_path": config.rmtree_path,
        "extension_path": config.extension_path,
    }


def get_connection():
    """Returns a SQLite connection with RMNOCASE extension loaded."""
    if not os.path.isfile(rmtree_path):
        sys.exit(f"❌ Database file not found: {rmtree_path}")
    if not os.access(rmtree_path, os.R_OK):
        sys.exit(f"❌ Database file not readable: {rmtree_path}")
    if os.path.getsize(rmtree_path) == 0:
        sys.exit(f"❌ Database file is empty: {rmtree_path}")
    if not os.path.isfile(extension_path):
        sys.exit(f"❌ Extension file not found: {extension_path}")

    try:
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

