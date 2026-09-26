from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from .statuses import CrawlStatus


@dataclass(frozen=True)
class Store:
    id: str
    name: str
    query: str
    retailer: str | None = None
    expected_address: str | None = None


@dataclass(frozen=True)
class Review:
    source: str
    source_review_id: str
    store_id: str
    author: str | None = None
    rating: float | None = None
    text: str | None = None
    displayed_date: str | None = None
    owner_response: str | None = None
    raw: dict[str, Any] = field(default_factory=dict, compare=False)

    @property
    def identity(self) -> tuple[str, str]:
        return self.source, self.source_review_id


@dataclass
class CrawlResult:
    store_id: str
    status: CrawlStatus
    reviews_seen: int = 0
    reviews_written: int = 0
    scroll_count: int = 0
    parse_errors: int = 0
    started_at: datetime | None = None
    finished_at: datetime | None = None
    failure_stage: str | None = None
    error: str | None = None
