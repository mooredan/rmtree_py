# rmutils.py
import sqlite3
import pandas as pd
import os
import sys
from config import DB_PATH, EXT_PATH

def get_connection():
    """Returns a SQLite connection with RMNOCASE extension loaded."""
    if not os.path.isfile(DB_PATH):
        sys.exit(f"❌ Database file not found: {DB_PATH}")
    if not os.access(DB_PATH, os.R_OK):
        sys.exit(f"❌ Database file not readable: {DB_PATH}")
    if os.path.getsize(DB_PATH) == 0:
        sys.exit(f"❌ Database file is empty: {DB_PATH}")
    if not os.path.isfile(EXT_PATH):
        sys.exit(f"❌ Extension file not found: {EXT_PATH}")

    try:
        conn = sqlite3.connect(DB_PATH)
        conn.enable_load_extension(True)
        conn.load_extension(EXT_PATH)
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
