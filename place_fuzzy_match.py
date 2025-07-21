import sqlite3
from rmutils import get_connection, get_place_details
from collections import defaultdict
from rapidfuzz import fuzz


def fetch_all_places(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT PlaceID, Name FROM PlaceTable")
    return cursor.fetchall()


def compute_similarity_scores(places, method='levenshtein', threshold=90):
    duplicates = defaultdict(list)
    seen = set()

    for i, (id1, name1) in enumerate(places):
        for j in range(i + 1, len(places)):
            id2, name2 = places[j]

            if (id1, id2) in seen or (id2, id1) in seen:
                continue

            seen.add((id1, id2))

            if method == 'levenshtein':
                score = fuzz.ratio(name1, name2)
            elif method == 'token_sort':
                score = fuzz.token_sort_ratio(name1, name2)
            else:
                raise ValueError("Unsupported similarity method")

            if score >= threshold:
                duplicates[(id1, id2)].append((score, name1, name2))

    return duplicates


def report_fuzzy_matches(duplicates):
    for (id1, id2), matches in sorted(duplicates.items(), key=lambda x: -x[1][0][0]):
        score, name1, name2 = matches[0]
        print(f"\n🔍 Similarity Score: {score}%")
        print(f"ID {id1}: {name1}")
        print(f"ID {id2}: {name2}")

        print("Details for potential duplicates:")
        print("Survivor Candidate:")
        get_place_details(conn, id1)
        print("Victim Candidate:")
        get_place_details(conn, id2)


if __name__ == '__main__':
    conn = get_connection(read_only=False)

    print("🔎 Fetching all place names...")
    places = fetch_all_places(conn)

    print("\n🧪 Running fuzzy match analysis (Levenshtein)...")
    levenshtein_matches = compute_similarity_scores(places, method='levenshtein', threshold=92)
    report_fuzzy_matches(levenshtein_matches)

    print("\n🧪 Running fuzzy match analysis (Token Sort)...")
    token_sort_matches = compute_similarity_scores(places, method='token_sort', threshold=92)
    report_fuzzy_matches(token_sort_matches)

