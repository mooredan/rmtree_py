from collections import defaultdict
from rmutils import (
    get_connection,
    normalize_place_names,
    find_duplicate_place_names,
    delete_blank_place_records,
    merge_places,
)


def devel():
    # open the connection to the database
    conn = get_connection()

    # do our best at renaming PlaceTable names
    normalize_place_names(conn, dry_run=False)

    delete_blank_place_records(conn, dry_run=True)

    # Find PlaceIDs where the place name is identical
    dupes = find_duplicate_place_names(conn)
    num_dupes = len(dupes)
    print(f"duplicates found: {num_dupes}\n")

    # let's merge those
    merge_places(conn, dupes, dry_run=False)

    conn.close()


if __name__ == "__main__":
    devel()
