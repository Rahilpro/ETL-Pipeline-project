from sqlalchemy import create_engine, Table, Column, Integer, String, DateTime, Text, MetaData
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
import logging

logger = logging.getLogger(__name__)


def get_engine(connection_string: str):
    """
    Create a SQLAlchemy engine.
    Supports SQLite (dev) and PostgreSQL (production).

    SQLite:    sqlite:///./etl_data.db
    Postgres:  postgresql://user:pass@localhost:5432/mydb
    """
    return create_engine(connection_string, echo=False)


def get_repos_table(metadata: MetaData) -> Table:
    """Define the target table schema."""
    return Table(
        "github_repos",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String(255), nullable=False),
        Column("full_name", String(512), nullable=False),
        Column("description", Text),
        Column("stargazers_count", Integer, default=0),
        Column("forks_count", Integer, default=0),
        Column("language", String(100)),
        Column("created_at", DateTime),
        Column("updated_at", DateTime),
        Column("html_url", String(512)),
    )


class Loader:
    def __init__(self, connection_string: str, batch_size: int = 500):
        self.engine = get_engine(connection_string)
        self.batch_size = batch_size
        self.metadata = MetaData()
        self.table = get_repos_table(self.metadata)
        self.metadata.create_all(self.engine)  # creates table if it doesn't exist
        self.total_loaded = 0

    def upsert_batch(self, records: list[dict]):
        """
        Upsert: insert new rows, update existing ones by primary key.
        This makes the pipeline idempotent — running it twice is safe.
        """
        if not records:
            return

        with self.engine.begin() as conn:
            # Detect dialect for the right upsert syntax
            dialect = self.engine.dialect.name

            if dialect == "postgresql":
                stmt = pg_insert(self.table).values(records)
                stmt = stmt.on_conflict_do_update(
                    index_elements=["id"],
                    set_={col: stmt.excluded[col] for col in records[0].keys() if col != "id"}
                )
            else:
                # SQLite — use INSERT OR REPLACE
                stmt = sqlite_insert(self.table).values(records)
                stmt = stmt.on_conflict_do_update(
                    index_elements=["id"],
                    set_={col: stmt.excluded[col] for col in records[0].keys() if col != "id"}
                )

            conn.execute(stmt)
            self.total_loaded += len(records)
            logger.info(f"Upserted {len(records)} rows. Total: {self.total_loaded}")

    def load(self, record_iterator):
        """Batch records from an iterator and upsert in chunks."""
        batch = []
        for record in record_iterator:
            batch.append(record)
            if len(batch) >= self.batch_size:
                self.upsert_batch(batch)
                batch.clear()

        if batch:  # flush remaining
            self.upsert_batch(batch)