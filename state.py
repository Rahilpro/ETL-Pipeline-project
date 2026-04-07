import json
from pathlib import Path
from datetime import datetime, timezone


class StateStore:
    """
    Persists pipeline state (watermarks, last run time) to a JSON file.
    In production, use a database table or Redis instead.
    """

    def __init__(self, state_file: str = ".etl_state.json"):
        self.path = Path(state_file)
        self._state = self._load()

    def _load(self) -> dict:
        if self.path.exists():
            return json.loads(self.path.read_text())
        return {}

    def _save(self):
        self.path.write_text(json.dumps(self._state, indent=2, default=str))

    def get_watermark(self, key: str) -> str | None:
        """Return the last-seen timestamp for this source, or None for full load."""
        return self._state.get(key)

    def set_watermark(self, key: str, value: str = None):
        """Update the watermark to now (or a provided value)."""
        self._state[key] = value or datetime.now(timezone.utc).isoformat()
        self._save()