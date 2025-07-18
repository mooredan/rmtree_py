from rmutils import get_connection
import csv


def dump_places(to_csv=None):
    conn = get_connection()
    cursor = conn.execute("SELECT PlaceID, Name FROM PlaceTable ORDER BY PlaceID")
    rows = cursor.fetchall()
    conn.close()

    if to_csv:
        with open(to_csv, "w", newline='', encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["PlaceID", "Name"])
            for row in rows:
                writer.writerow([row["PlaceID"], row["Name"]])
        print(f"✅ Exported {len(rows)} places to {to_csv}")
    else:
        for row in rows:
            print(f"[{row['PlaceID']}] {row['Name']}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Dump RootsMagic PlaceTable entries")
    parser.add_argument("--csv", help="Optional output file to save as CSV")
    args = parser.parse_args()

    dump_places(args.csv)
