# test_postgres.py — replace the whole file with this debug version
import os
from dotenv import load_dotenv

# Force load from the exact path
load_dotenv("/Users/rahil/Desktop/etl/.env")

db_url = os.getenv("DB_URL")
print(f"DB_URL is: {db_url}")

if not db_url:
    print("ERROR: DB_URL is empty — .env not loading correctly")
elif "localhost" in db_url or db_url.startswith("sqlite"):
    print("ERROR: DB_URL is pointing at local database, not Supabase")
elif "supabase" in db_url:
    print("DB_URL looks correct — testing connection...")
    import psycopg2
    try:
        conn = psycopg2.connect(db_url)
        cursor = conn.cursor()
        cursor.execute("SELECT version()")
        print("Connected:", cursor.fetchone()[0])
        conn.close()
    except Exception as e:
        print(f"Connection failed: {e}")
else:
    print(f"Unexpected DB_URL format: {db_url}")