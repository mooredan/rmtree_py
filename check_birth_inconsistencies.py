
import pandas as pd
from rmutils import get_connection, run_query

def parse_rm_date(date_str):
    try:
        if isinstance(date_str, str) and date_str.startswith("D.+"):
            return pd.to_datetime(date_str[3:11], format="%Y%m%d", errors="coerce")
    except:
        return pd.NaT

def main():
    conn = get_connection()

    birth_fact_id = run_query(conn, "SELECT FactTypeID FROM FactTypeTable WHERE LOWER(Name) = 'birth'").iloc[0, 0]
    death_fact_id = run_query(conn, "SELECT FactTypeID FROM FactTypeTable WHERE LOWER(Name) = 'death'").iloc[0, 0]

    births = run_query(conn, f"SELECT OwnerID AS PersonID, Date AS BirthDate FROM EventTable WHERE OwnerType = 0 AND EventType = {birth_fact_id}")
    deaths = run_query(conn, f"SELECT OwnerID AS PersonID, Date AS DeathDate FROM EventTable WHERE OwnerType = 0 AND EventType = {death_fact_id}")

    relations = run_query(conn, "SELECT c.ChildID, f.MotherID FROM ChildTable c JOIN FamilyTable f ON c.FamilyID = f.FamilyID WHERE f.MotherID IS NOT NULL")

    names = run_query(conn, "SELECT OwnerID AS PersonID, Given, Surname FROM NameTable WHERE IsPrimary = 1")

    df = (
        relations.merge(births.rename(columns={"PersonID": "ChildID", "BirthDate": "ChildBirth"}), on="ChildID", how="left")
        .merge(births.rename(columns={"PersonID": "MotherID", "BirthDate": "MotherBirth"}), on="MotherID", how="left")
        .merge(deaths.rename(columns={"PersonID": "MotherID", "DeathDate": "MotherDeath"}), on="MotherID", how="left")
        .merge(names.rename(columns={"PersonID": "ChildID", "Given": "ChildGiven", "Surname": "ChildSurname"}), on="ChildID", how="left")
        .merge(names.rename(columns={"PersonID": "MotherID", "Given": "MotherGiven", "Surname": "MotherSurname"}), on="MotherID", how="left")
    )

    df["ChildBirth"] = df["ChildBirth"].apply(parse_rm_date)
    df["MotherBirth"] = df["MotherBirth"].apply(parse_rm_date)
    df["MotherDeath"] = df["MotherDeath"].apply(parse_rm_date)

    df["TooYoung"] = (df["ChildBirth"] - df["MotherBirth"]).dt.days < (13 * 365)
    df["TooOld"] = (df["ChildBirth"] - df["MotherBirth"]).dt.days > (55 * 365)
    df["PostDeath"] = df["MotherDeath"].notna() & (df["ChildBirth"] > df["MotherDeath"])

    problems = df[df["TooYoung"] | df["TooOld"] | df["PostDeath"]].copy()

    cols = [
        "ChildGiven", "ChildSurname", "ChildBirth",
        "MotherGiven", "MotherSurname", "MotherBirth",
        "MotherDeath", "TooYoung", "TooOld", "PostDeath"
    ]

    results = problems[cols].to_dict(orient="records")

    for r in results:
        print(r)

    return results

if __name__ == "__main__":
    main()
