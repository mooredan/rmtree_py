
import sqlite3
import sys
import signal
from collections import defaultdict
from rmutils import get_connection, get_primary_names
from config import UNIQUE_FACT_TYPES

# Ignore SIGPIPE to prevent BrokenPipeError when output is piped and closed early
signal.signal(signal.SIGPIPE, signal.SIG_DFL)

def find_duplicate_unique_facts(conn):
    """
    Identify people who have more than one of a unique-type event (e.g., Birth, Death).
    Returns a list of (PersonID, EventType, Count)
    """
    fact_type_ids = ','.join(str(fid) for fid in UNIQUE_FACT_TYPES.keys())
    query = f"""
        SELECT OwnerID, EventType, COUNT(*) as EventCount
        FROM EventTable
        WHERE OwnerType = 0
          AND EventType IN ({fact_type_ids})
        GROUP BY OwnerID, EventType
        HAVING COUNT(*) > 1
    """
    cursor = conn.execute(query)
    return cursor.fetchall()

def get_event_details(conn, person_ids, event_type_ids):
    """Get full event detail for selected people and fact types."""
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

def parse_rm_date(raw_date):
    import re
    match = re.search(r"\+(\d{4})(\d{2})(\d{2})", raw_date or "")
    if match:
        return f"{match.group(1)}-{match.group(2)}-{match.group(3)}"
    return None

def main():
    conn = get_connection()

    print("Scanning for duplicate unique facts...")
    duplicates = find_duplicate_unique_facts(conn)
    if not duplicates:
        print("No duplicates found.")
        return

    # Group results
    dup_by_person = defaultdict(list)
    for pid, etype, count in duplicates:
        dup_by_person[pid].append(etype)

    # Load primary names
    names = get_primary_names(conn, dup_by_person.keys())

    # Build only the (person, event type) pairs that had duplicates
    person_event_pairs = set((pid, etype) for pid, etypes in dup_by_person.items() for etype in etypes)
    all_details = [
        row for row in get_event_details(conn, dup_by_person.keys(), set().union(*dup_by_person.values()))
        if (row[0], row[1]) in person_event_pairs
    ]
    

    # Print report
    for row in all_details:
        pid, etype, date_raw, _, place = row
        given, surname = names.get(pid, ("?", "?"))
        event = UNIQUE_FACT_TYPES.get(etype, f"EventType {etype}")
        date = parse_rm_date(date_raw)
        print(f"[{pid}] {given} {surname} — {event}: {date or '???'} at {place or '???'}")

    conn.close()

if __name__ == "__main__":
    main()
