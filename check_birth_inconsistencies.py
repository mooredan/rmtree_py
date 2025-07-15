import pandas as pd
from rmutils import get_connection, run_query


# Get DB connection (with RMNOCASE)
conn = get_connection()


# --- Get FactTypeID for 'Birth' and 'Death' ---
birth_fact_id_df = run_query(
    conn, "SELECT FactTypeID FROM FactTypeTable WHERE LOWER(Name) = 'birth'"
)
birth_fact_id = birth_fact_id_df.iloc[0, 0]


death_fact_id_df = run_query(
    conn, "SELECT FactTypeID FROM FactTypeTable WHERE LOWER(Name) = 'death'"
)
death_fact_id = death_fact_id_df.iloc[0, 0]

# --- Get birth and death events ---
births = run_query(
    conn,
    f"""
SELECT OwnerID AS PersonID, Date AS BirthDate
FROM EventTable
WHERE OwnerType = 0 AND EventType = {birth_fact_id}
""",
)

deaths = run_query(
    conn,
    f"""
SELECT OwnerID AS PersonID, Date AS DeathDate
FROM EventTable
WHERE OwnerType = 0 AND EventType = {death_fact_id}
""",
)

# --- Get child-mother links via FamilyTable ---
relations = run_query(
    conn,
    """
SELECT
    c.ChildID,
    f.MotherID
FROM ChildTable c
JOIN FamilyTable f ON c.FamilyID = f.FamilyID
WHERE f.MotherID IS NOT NULL
""",
)

# --- Get primary names using OwnerID (verified against schema) ---
names = run_query(
    conn,
    """
SELECT OwnerID AS PersonID, Given, Surname
FROM NameTable
WHERE IsPrimary = 1
""",
)

# --- Merge and analyze ---
df = (
    relations.merge(
        births.rename(columns={"PersonID": "ChildID", "BirthDate": "ChildBirth"}),
        on="ChildID",
        how="left",
    )
    .merge(
        births.rename(columns={"PersonID": "MotherID", "BirthDate": "MotherBirth"}),
        on="MotherID",
        how="left",
    )
    .merge(
        deaths.rename(columns={"PersonID": "MotherID", "DeathDate": "MotherDeath"}),
        on="MotherID",
        how="left",
    )
    .merge(
        names.rename(
            columns={
                "PersonID": "ChildID",
                "Given": "ChildGiven",
                "Surname": "ChildSurname",
            }
        ),
        on="ChildID",
        how="left",
    )
    .merge(
        names.rename(
            columns={
                "PersonID": "MotherID",
                "Given": "MotherGiven",
                "Surname": "MotherSurname",
            }
        ),
        on="MotherID",
        how="left",
    )
)


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
print(
    problems[
        [
            "ChildGiven",
            "ChildSurname",
            "ChildBirth",
            "MotherGiven",
            "MotherSurname",
            "MotherBirth",
            "MotherDeath",
            "TooYoung",
            "PostDeath",
            "TooOld",
        ]
    ]
)


# --- Father consistency check using same age logic ---
# Get child-father links via FamilyTable
father_relations = run_query(
    conn,
    """
SELECT
    c.ChildID,
    f.FatherID
FROM ChildTable c
JOIN FamilyTable f ON c.FamilyID = f.FamilyID
WHERE f.FatherID IS NOT NULL
""",
)

# Merge and analyze
df_f = (
    father_relations.merge(
        births.rename(columns={"PersonID": "ChildID", "BirthDate": "ChildBirth"}),
        on="ChildID",
        how="left",
    )
    .merge(
        births.rename(columns={"PersonID": "FatherID", "BirthDate": "FatherBirth"}),
        on="FatherID",
        how="left",
    )
    .merge(
        deaths.rename(columns={"PersonID": "FatherID", "DeathDate": "FatherDeath"}),
        on="FatherID",
        how="left",
    )
    .merge(
        names.rename(
            columns={
                "PersonID": "ChildID",
                "Given": "ChildGiven",
                "Surname": "ChildSurname",
            }
        ),
        on="ChildID",
        how="left",
    )
    .merge(
        names.rename(
            columns={
                "PersonID": "FatherID",
                "Given": "FatherGiven",
                "Surname": "FatherSurname",
            }
        ),
        on="FatherID",
        how="left",
    )
)

df_f["ChildBirth"] = df_f["ChildBirth"].apply(parse_rm_date)
df_f["FatherBirth"] = df_f["FatherBirth"].apply(parse_rm_date)
df_f["FatherDeath"] = df_f["FatherDeath"].apply(parse_rm_date)

df_f["TooYoungFather"] = (df_f["ChildBirth"] - df_f["FatherBirth"]).dt.days < (13 * 365)
df_f["PostDeathFather"] = df_f["FatherDeath"].notna() & (
    df_f["ChildBirth"] > df_f["FatherDeath"]
)

problems_f = df_f[df_f["TooYoungFather"] | df_f["PostDeathFather"]].copy()

# Output father inconsistencies
print("\n\n--- Father-Child Inconsistencies ---")
print(
    problems_f[
        [
            "ChildGiven",
            "ChildSurname",
            "ChildBirth",
            "FatherGiven",
            "FatherSurname",
            "FatherBirth",
            "FatherDeath",
            "TooYoungFather",
            "PostDeathFather",
        ]
    ]
)
