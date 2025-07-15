import sqlite3
import pandas as pd
import os
import sys
import platform
from config import DB_PATH, EXT_PATH


# --- Platform-aware extension path ---
system = platform.system()

# --- Platform-aware extension path ---
ext_path = EXT_PATH

# --- Set database path ---
db_path = DB_PATH

# --- Validate file presence and readability ---
for path, label in [(db_path, "Database"), (ext_path, "Extension")]:
    if not os.path.isfile(path):
        sys.exit(f"❌ {label} file not found: {path}")
    if not os.access(path, os.R_OK):
        sys.exit(f"❌ {label} file not readable: {path}")

# --- Check for empty database file ---
if os.path.getsize(db_path) == 0:
    sys.exit(f"❌ Database file is empty: {db_path}")

# --- Connect and initialize ---
try:
    conn = sqlite3.connect(db_path)
    conn.enable_load_extension(True)
    conn.load_extension(ext_path)
    conn.execute("REINDEX RMNOCASE;")
except sqlite3.OperationalError as e:
    sys.exit(f"❌ SQLite error: {e}")
except Exception as e:
    sys.exit(f"❌ Unexpected error: {e}")

# --- Get FactTypeID for 'Birth' and 'Death' ---
birth_fact_id = pd.read_sql_query(
    "SELECT FactTypeID FROM FactTypeTable WHERE LOWER(Name) = 'birth'", conn
).squeeze()
death_fact_id = pd.read_sql_query(
    "SELECT FactTypeID FROM FactTypeTable WHERE LOWER(Name) = 'death'", conn
).squeeze()

# --- Get birth and death events ---
births = pd.read_sql_query(f"""
SELECT OwnerID AS PersonID, Date AS BirthDate
FROM EventTable
WHERE OwnerType = 0 AND EventType = {birth_fact_id}
""", conn)

deaths = pd.read_sql_query(f"""
SELECT OwnerID AS PersonID, Date AS DeathDate
FROM EventTable
WHERE OwnerType = 0 AND EventType = {death_fact_id}
""", conn)

# --- Get child-mother links via FamilyTable ---
relations = pd.read_sql_query("""
SELECT
    c.ChildID,
    f.MotherID
FROM ChildTable c
JOIN FamilyTable f ON c.FamilyID = f.FamilyID
WHERE f.MotherID IS NOT NULL
""", conn)

# --- Get primary names using OwnerID (verified against schema) ---
names = pd.read_sql_query("""
SELECT OwnerID AS PersonID, Given, Surname
FROM NameTable
WHERE IsPrimary = 1
""", conn)

# --- Merge and analyze ---
df = relations \
    .merge(births.rename(columns={"PersonID": "ChildID", "BirthDate": "ChildBirth"}), on="ChildID", how="left") \
    .merge(births.rename(columns={"PersonID": "MotherID", "BirthDate": "MotherBirth"}), on="MotherID", how="left") \
    .merge(deaths.rename(columns={"PersonID": "MotherID", "DeathDate": "MotherDeath"}), on="MotherID", how="left") \
    .merge(names.rename(columns={"PersonID": "ChildID", "Given": "ChildGiven", "Surname": "ChildSurname"}), on="ChildID", how="left") \
    .merge(names.rename(columns={"PersonID": "MotherID", "Given": "MotherGiven", "Surname": "MotherSurname"}), on="MotherID", how="left")

# --- Convert to datetime using custom RootsMagic parser ---
def parse_rm_date(date_str):
    try:
        if isinstance(date_str, str) and date_str.startswith("D.+"):
            return pd.to_datetime(date_str[3:11], format="%Y%m%d", errors="coerce")
    except:
        return pd.NaT

df["ChildBirth"] = df["ChildBirth"].apply(parse_rm_date)
df["MotherBirth"] = df["MotherBirth"].apply(parse_rm_date)
df["MotherDeath"] = df["MotherDeath"].apply(parse_rm_date)

df["ChildBirth"] = pd.to_datetime(df["ChildBirth"], errors="coerce")
df["MotherBirth"] = pd.to_datetime(df["MotherBirth"], errors="coerce")
df["MotherDeath"] = pd.to_datetime(df["MotherDeath"], errors="coerce")

# --- Flag issues ---
df["TooYoung"] = (df["ChildBirth"] - df["MotherBirth"]).dt.days < (13 * 365)
df["PostDeath"] = df["MotherDeath"].notna() & (df["ChildBirth"] > df["MotherDeath"])

# --- Show problems only ---

df["TooOld"] = (df["ChildBirth"] - df["MotherBirth"]).dt.days > (55 * 365)
problems = df[df["TooYoung"] | df["PostDeath"] | df["TooOld"]].copy()


# --- Print results ---
print(problems[[
    "ChildGiven", "ChildSurname", "ChildBirth",
    "MotherGiven", "MotherSurname", "MotherBirth", "MotherDeath",
    "TooYoung", "PostDeath", "TooOld"
]])


# --- Father consistency check using same age logic ---
# Get child-father links via FamilyTable
father_relations = pd.read_sql_query("""
SELECT
    c.ChildID,
    f.FatherID
FROM ChildTable c
JOIN FamilyTable f ON c.FamilyID = f.FamilyID
WHERE f.FatherID IS NOT NULL
""", conn)

# Merge and analyze
df_f = father_relations \
    .merge(births.rename(columns={"PersonID": "ChildID", "BirthDate": "ChildBirth"}), on="ChildID", how="left") \
    .merge(births.rename(columns={"PersonID": "FatherID", "BirthDate": "FatherBirth"}), on="FatherID", how="left") \
    .merge(deaths.rename(columns={"PersonID": "FatherID", "DeathDate": "FatherDeath"}), on="FatherID", how="left") \
    .merge(names.rename(columns={"PersonID": "ChildID", "Given": "ChildGiven", "Surname": "ChildSurname"}), on="ChildID", how="left") \
    .merge(names.rename(columns={"PersonID": "FatherID", "Given": "FatherGiven", "Surname": "FatherSurname"}), on="FatherID", how="left")

df_f["ChildBirth"] = df_f["ChildBirth"].apply(parse_rm_date)
df_f["FatherBirth"] = df_f["FatherBirth"].apply(parse_rm_date)
df_f["FatherDeath"] = df_f["FatherDeath"].apply(parse_rm_date)

df_f["TooYoungFather"] = (df_f["ChildBirth"] - df_f["FatherBirth"]).dt.days < (13 * 365)
df_f["PostDeathFather"] = df_f["FatherDeath"].notna() & (df_f["ChildBirth"] > df_f["FatherDeath"])

problems_f = df_f[df_f["TooYoungFather"] | df_f["PostDeathFather"]].copy()

# Output father inconsistencies
print("\n\n--- Father-Child Inconsistencies ---")
print(problems_f[[
    "ChildGiven", "ChildSurname", "ChildBirth",
    "FatherGiven", "FatherSurname", "FatherBirth", "FatherDeath",
    "TooYoungFather", "PostDeathFather"
]])
