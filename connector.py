import logging
import os
from dotenv import load_dotenv

from extractor import APIExtractor
from transformer import Transformer, GitHubRepo
from loader import Loader
from state import StateStore
from quality_checks import QualityChecker          # ← add this line


load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s"
)
logger = logging.getLogger(__name__)


def run_pipeline(full_refresh: bool = False):
    logger.info("Pipeline starting")

    # --- Config ---
    state = StateStore()
    watermark_key = "github_repos_updated_at"
    last_run = None if full_refresh else state.get_watermark(watermark_key)

    if last_run:
        logger.info(f"Incremental load from: {last_run}")
    else:
        logger.info("Full load")

    # --- Extract ---
    extractor = APIExtractor(
    base_url="https://api.github.com",
    headers={
    "Authorization": f"token {os.getenv('GITHUB_TOKEN')}"
 },
)
    raw_records = extractor.extract_paginated(
        endpoint="/search/repositories",
        params={"q": "stars:>10000", "sort": "updated"},
        since=last_run,
    )

    # --- Transform ---
    transformer = Transformer(model_class=GitHubRepo)
    clean_records = transformer.transform(raw_records)

    # --- Load ---
    loader = Loader(
        connection_string=os.getenv("DB_URL", "sqlite:///./etl_data.db"),
        batch_size=200,
    )
    loader.load(clean_records)

    # --- Update state ---
    state.set_watermark(watermark_key)

    logger.info(
        f"Pipeline complete. "
        f"Loaded: {loader.total_loaded} | "
        f"Valid: {transformer.stats['valid']} | "
        f"Invalid: {transformer.stats['invalid']}"
    )
    # ── Quality checks run automatically after every load ──────────
    logger.info("Running quality checks...")
    QualityChecker().run_all()


if __name__ == "__main__":
    run_pipeline()