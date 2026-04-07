import sqlite3
import os

# Always resolve to the folder this script lives in
db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "etl_data.db")
print(f"Connecting to: {db_path}")

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

cursor.execute("SELECT COUNT(*) FROM github_repos")
print(f"Total rows: {cursor.fetchone()[0]}")

cursor.execute("""
    SELECT name, stargazers_count, language, updated_at
    FROM github_repos
    ORDER BY stargazers_count DESC
    LIMIT 10
""")

print("\n--- Top 10 repos by stars ---")
for row in cursor.fetchall():
    print(f"  {row[0]:<35} | Stars: {row[1]:>8,} | Lang: {str(row[2]):<12} | Updated: {row[3]}")

conn.close()