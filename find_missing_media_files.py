
import os
from urllib.parse import urlparse, unquote
from rmutils import get_connection, run_query, get_config

def build_full_path(media_path, media_file, rmtree_dir):
    if not media_path or not media_file:
        return None
    media_path = media_path.strip()
    if media_path.startswith("*"):
        base_path = rmtree_dir
        sub_path = media_path[1:]
        local_path = os.path.join(base_path, sub_path.lstrip("/\\"))
    elif media_path.lower().startswith("file://"):
        parsed = urlparse(media_path)
        local_path = unquote(parsed.path)
    else:
        local_path = media_path
    return os.path.join(local_path, media_file.strip())

def find_missing_files():
    config = get_config()
    rmtree_dir = os.path.dirname(os.path.abspath(config["rmtree_path"]))
    conn = get_connection()

    query = "SELECT MediaID, MediaPath, MediaFile, MediaType FROM MultimediaTable"
    media_df = run_query(conn, query)

    missing_files = []
    for _, row in media_df.iterrows():
        full_path = build_full_path(row["MediaPath"], row["MediaFile"], rmtree_dir)
        if not full_path:
            continue
        if not os.path.isfile(full_path):
            missing_files.append(full_path)

    return missing_files

def main():
    missing = find_missing_files()
    if missing:
        print("Missing media files:")
        for path in missing:
            print(f" - {path}")
    else:
        print("✅ All media files found.")
    return missing

if __name__ == "__main__":
    main()
