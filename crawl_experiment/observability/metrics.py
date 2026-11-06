import json
import logging
from dataclasses import asdict, dataclass


@dataclass
class MetricEvent:
    store_id: str
    stage: str
    status: str
    review_count: int = 0
    scroll_count: int = 0
    new_reviews: int = 0
    idle_count: int = 0
    parse_errors: int = 0
    elapsed_seconds: float = 0
    error: str | None = None


class CrawlMetrics:
    def __init__(self, logger: logging.Logger | None = None):
        self.logger = logger or logging.getLogger("crawl")

    def emit(self, event: MetricEvent) -> None:
        self.logger.info(json.dumps(asdict(event), sort_keys=True))
