
import sqlite3
import signal
import argparse
from collections import defaultdict
from rmutils import get_connection, get_primary_names
from config import UNIQUE_FACT_TYPES

signal.signal(signal.SIGPIPE, signal.SIG_DFL)

def parse_rm_date(raw_date):
    import re
    match = re.search(r"\+(\d{4})(\d{2})(\d{2})", raw_date or "")
    if match:
        return f"{match.group(1)}-{match.group(2)}-{match.group(3)}"
    return None

def find_duplicate_unique_facts(conn):
    fact_type_ids = ','.join(str(fid) for fid in UNIQUE_FACT_TYPES.keys())
    query = f"""
        SELECT OwnerID, EventType, COUNT(*) as EventCount
        FROM EventTable
        WHERE OwnerType = 0
          AND EventType IN ({fact_type_ids})
        GROUP BY OwnerID, EventType
        HAVING COUNT(*) > 1
    """
    return conn.execute(query).fetchall()

def get_event_details(conn, person_ids, event_type_ids):
    ids_str = ','.join(str(pid) for pid in person_ids)
    facts_str = ','.join(str(eid) for eid in event_type_ids)
    query = f"""
        SELECT e.OwnerID, e.EventType, e.Date, e.PlaceID, p.Name AS PlaceName
        FROM EventTable e
        LEFT JOIN PlaceTable p ON e.PlaceID = p.PlaceID
        WHERE e.OwnerType = 0
          AND e.OwnerID IN ({ids_str})
          AND e.EventType IN ({facts_str})
        ORDER BY e.OwnerID, e.EventType, e.Date
    """
    return conn.execute(query).fetchall()

def run():
    conn = get_connection()
    duplicates = find_duplicate_unique_facts(conn)
    if not duplicates:
        return []

    dup_by_person = defaultdict(list)
    for pid, etype, _ in duplicates:
        dup_by_person[pid].append(etype)

    names = get_primary_names(conn, dup_by_person.keys())
    person_event_pairs = {(pid, etype) for pid, types in dup_by_person.items() for etype in types}
    details = [
        row for row in get_event_details(conn, dup_by_person.keys(), set().union(*dup_by_person.values()))
        if (row[0], row[1]) in person_event_pairs
    ]

    results = []
    for row in details:
        pid, etype, date_raw, _, place = row
        given, surname = names.get(pid, ("?", "?"))
        event = UNIQUE_FACT_TYPES.get(etype, f"EventType {etype}")
        date = parse_rm_date(date_raw)
        results.append({
            "PersonID": pid,
            "Name": f"{given} {surname}",
            "Event": event,
            "Date": date or "???",
            "Place": place or "???"
        })

    return results

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Detect duplicate unique fact types in RootsMagic database")
    parser.add_argument("--summary", action="store_true", help="Only print summary count")
    args = parser.parse_args()

    facts = run()
    if args.summary:
        print(f"[find_multiple_unique_facts] {len(facts)} duplicate facts found")
    else:
        for r in facts:
            print(f"[{r['PersonID']}] {r['Name']} — {r['Event']}: {r['Date']} at {r['Place']}")
