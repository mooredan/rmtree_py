#!/usr/bin/env python3

import sqlite3

rmfile = 'DownloadedTree.rmtree'

# connect to a database
conn = sqlite3.connect(rmfile);

# Set the row_factory to sqlite3.Row
conn.row_factory = sqlite3.Row

# Create a cursor object
cursor = conn.cursor()

# query
SqlStmt = """\
   SELECT et.OwnerID as PersonID
   FROM EventTable et
   WHERE et.OwnerType = 0 and EventType = 1 -- Birth 
   GROUP BY et.OwnerID
   HAVING COUNT(EventID) > 1
"""
cursor.execute(SqlStmt)

# Fetch the rows
rows = cursor.fetchall()

# row.fetchone()
# print(row.keys())

# Print the rows
for row in rows:
    # print(row)
    # print(f"Column1: {row[0]}")
    print(f"PersonID: {row['PersonID']}")
    SqlStmt = f"""SELECT Surname, Given FROM NameTable WHERE OwnerID = {row['PersonID']}"""
    cursor.execute(SqlStmt)
    # records = cursor.fetchall()
    record = cursor.fetchone()
    print(f"Surname: {record['Surname']}, Given: {record['Given']}")
    # for record in records:
    #     print(f"Surname: {record['Surname']}, Given: {record['Given']}")
    print()


# Close the connection
conn.close()
