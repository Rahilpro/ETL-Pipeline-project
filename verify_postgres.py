# verify_postgres.py
import psycopg2, os
from dotenv import load_dotenv
load_dotenv()

conn = psycopg2.connect(os.getenv("DB_URL"))
cursor = conn.cursor()
cursor.execute("SELECT COUNT(*) FROM github_repos")
print(f"Rows in Postgres: {cursor.fetchone()[0]}")
cursor.execute("""
    SELECT name, stargazers_count, language
    FROM github_repos
    ORDER BY stargazers_count DESC
    LIMIT 5
""")
for row in cursor.fetchall():
    print(f"  {row[0]:<40} | {row[1]:>8,} stars | {row[2]}")
conn.close()