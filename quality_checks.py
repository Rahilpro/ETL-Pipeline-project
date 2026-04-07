import psycopg2
import os
import logging
from dotenv import load_dotenv

load_dotenv("/Users/rahil/Desktop/etl/.env")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s — %(message)s"
)
logger = logging.getLogger(__name__)

class QualityChecker:

    def __init__(self):
        self.conn = psycopg2.connect(os.getenv("DB_URL"))
        self.cursor = self.conn.cursor()
        self.failures = []

    def check(self, name, sql, condition, message):
        self.cursor.execute(sql)
        result = self.cursor.fetchone()[0]
        passed = condition(result)
        status = "PASS" if passed else "FAIL"
        logger.info(f"[{status}] {name}: {result} — {message}")
        if not passed:
            self.failures.append(f"{name}: {message} (got {result})")
        return passed

    def run_all(self):
        self.check(
            name="row_count",
            sql="SELECT COUNT(*) FROM github_repos",
            condition=lambda n: n >= 100,
            message="expected at least 100 rows"
        )
        self.check(
            name="no_null_names",
            sql="SELECT COUNT(*) FROM github_repos WHERE name IS NULL",
            condition=lambda n: n == 0,
            message="name must never be null"
        )
        self.check(
            name="no_null_stars",
            sql="SELECT COUNT(*) FROM github_repos WHERE stargazers_count IS NULL",
            condition=lambda n: n == 0,
            message="stargazers_count must never be null"
        )
        self.check(
            name="no_negative_stars",
            sql="SELECT COUNT(*) FROM github_repos WHERE stargazers_count < 0",
            condition=lambda n: n == 0,
            message="star counts cannot be negative"
        )
        self.check(
            name="no_duplicate_ids",
            sql="""
                SELECT COUNT(*) FROM (
                    SELECT id
                    FROM github_repos
                    GROUP BY id
                    HAVING COUNT(*) > 1
                ) dupes
            """,
            condition=lambda n: n == 0,
            message="duplicate repo IDs found"
        )
        self.check(
            name="data_freshness",
            sql="""
                SELECT COUNT(*) FROM github_repos
                WHERE updated_at >= NOW() - INTERVAL '7 days'
            """,
            condition=lambda n: n > 0,
            message="no recently updated repos — data may be stale"
        )

        if self.failures:
            logger.error(f"FAILED: {len(self.failures)} checks failed")
            for f in self.failures:
                logger.error(f"  - {f}")
            raise ValueError(f"Quality checks failed: {self.failures}")
        else:
            logger.info("All quality checks passed")

        self.conn.close()


if __name__ == "__main__":
    QualityChecker().run_all()
