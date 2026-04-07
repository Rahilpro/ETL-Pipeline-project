from datetime import datetime
from typing import Optional
from pydantic import BaseModel, validator, ValidationError
import logging

logger = logging.getLogger(__name__)


class GitHubRepo(BaseModel):
    """
    Strongly-typed model for a GitHub repository record.
    Pydantic validates types and coerces where safe.
    """
    id: int
    name: str
    full_name: str
    description: Optional[str] = None
    stargazers_count: int
    forks_count: int
    language: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    html_url: str

    @validator("description", pre=True)
    def clean_description(cls, v):
        if v is None:
            return None
        return v.strip()[:500]  # truncate to column size

    @validator("name", "full_name")
    def strip_whitespace(cls, v):
        return v.strip()


class Transformer:
    def __init__(self, model_class):
        self.model_class = model_class
        self.valid = 0
        self.invalid = 0

    def transform(self, raw_records):
        """
        Yields validated, cleaned records.
        Skips invalid records and logs them — never crash the pipeline.
        """
        for raw in raw_records:
            try:
                record = self.model_class(**raw)
                self.valid += 1
                yield record.dict()  # return plain dict for SQLAlchemy
            except ValidationError as e:
                self.invalid += 1
                logger.warning(f"Skipped invalid record id={raw.get('id')}: {e}")

    @property
    def stats(self):
        return {"valid": self.valid, "invalid": self.invalid}