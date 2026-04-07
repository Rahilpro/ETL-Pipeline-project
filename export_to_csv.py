import sqlite3
import csv
import os

db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "etl_data.db")
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

cursor.execute("SELECT * FROM github_repos")
rows = cursor.fetchall()
cols = [d[0] for d in cursor.description]

output = os.path.join(os.path.dirname(os.path.abspath(__file__)), "github_repos.csv")
with open(output, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(cols)
    writer.writerows(rows)

print(f"Exported {len(rows)} rows to {output}")
conn.close()
