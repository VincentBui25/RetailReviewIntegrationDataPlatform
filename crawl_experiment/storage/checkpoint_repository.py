from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path

from crawl_experiment.core.statuses import CrawlStatus


@dataclass
class Checkpoint:
    store_id: str
    status: CrawlStatus
    reviews_seen: int = 0
    scroll_count: int = 0
    last_progress_at: float | None = None
    failure_stage: str | None = None
    started_at: str | None = None
    finished_at: str | None = None


class CheckpointRepository:
    def __init__(self, directory: str | Path):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)

    def save(self, checkpoint: Checkpoint) -> Path:
        path = self.directory / f"{checkpoint.store_id}.json"
        temporary = path.with_suffix(".tmp")
        temporary.write_text(json.dumps(asdict(checkpoint), indent=2, default=str) + "\n", encoding="utf-8")
        temporary.replace(path)
        return path

    def load(self, store_id: str) -> Checkpoint | None:
        path = self.directory / f"{store_id}.json"
        if not path.exists():
            return None
        data = json.loads(path.read_text(encoding="utf-8"))
        data["status"] = CrawlStatus(data["status"])
        return Checkpoint(**data)
