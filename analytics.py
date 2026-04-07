# analytics.py — add this to your project now
import sqlite3, os

db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "etl_data.db")
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

queries = {

    "Language breakdown": """
        SELECT
            COALESCE(language, 'Unknown') as language,
            COUNT(*) as repo_count,
            ROUND(AVG(stargazers_count)) as avg_stars,
            MAX(stargazers_count) as top_stars
        FROM github_repos
        GROUP BY language
        ORDER BY repo_count DESC
        LIMIT 15
    """,

    "Star distribution buckets": """
        SELECT
            CASE
                WHEN stargazers_count >= 100000 THEN '100k+'
                WHEN stargazers_count >= 50000  THEN '50k-100k'
                WHEN stargazers_count >= 10000  THEN '10k-50k'
                ELSE 'under 10k'
            END as star_tier,
            COUNT(*) as repo_count
        FROM github_repos
        GROUP BY star_tier
        ORDER BY repo_count DESC
    """,

    "Most forked repos": """
        SELECT name, language, stargazers_count, forks_count,
               ROUND(CAST(forks_count AS FLOAT) / stargazers_count, 2) as fork_ratio
        FROM github_repos
        ORDER BY forks_count DESC
        LIMIT 10
    """,

    "Updated most recently": """
        SELECT name, stargazers_count, updated_at
        FROM github_repos
        ORDER BY updated_at DESC
        LIMIT 10
    """,
}

for title, sql in queries.items():
    print(f"\n{'='*55}")
    print(f"  {title}")
    print(f"{'='*55}")
    cursor.execute(sql)
    cols = [d[0] for d in cursor.description]
    print("  " + " | ".join(f"{c:<18}" for c in cols))
    print("  " + "-" * 70)
    for row in cursor.fetchall():
        print("  " + " | ".join(f"{str(v):<18}" for v in row))

conn.close()