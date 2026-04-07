# test_postgres.py — create this file and run it
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

try:
    conn = psycopg2.connect(os.getenv("DB_URL"))
    cursor = conn.cursor()
    cursor.execute("SELECT version()")
    print("Connected:", cursor.fetchone()[0])
    conn.close()
except Exception as e:
    print("Failed:", e)