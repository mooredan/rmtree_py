#!/usr/bin/env python3

import sqlite3

# rmfile = 'DownloadedTree.rmtree'
rmfile = '../ZebMoore_Ancestry.rmtree'

# connect to a database
conn = sqlite3.connect(rmfile);

# Set the row_factory to sqlite3.Row
conn.row_factory = sqlite3.Row

# Create a cursor object
cursor = conn.cursor()

# query
cursor.execute("SELECT MediaID,MediaPath,MediaFile FROM MultimediaTable")

# query
SqlStmt = """\
   SELECT MediaID,MediaPath,MediaFile
   FROM MultimediaTable
"""
cursor.execute(SqlStmt)

# Fetch the rows
rows = cursor.fetchall()

for row in rows:
    # print(row)
    # print(f"Column1: {row[0]}")
    print(f"MediaID: {row['MediaID']}")
    print(f"MediaPath: {row['MediaPath']}")
    print(f"MediaFile: {row['MediaFile']}")
    print()


# # Print the rows
# for row in rows:
#     print(row)

# Close the connection
conn.close()
