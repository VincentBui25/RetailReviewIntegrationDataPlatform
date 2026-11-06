"""Canonical POIs, independent of review crawl models and raw source payloads."""
from dataclasses import asdict, dataclass, field, replace
from typing import Any


@dataclass(frozen=True)
class CanonicalPlace:
    place_key: str
    source: str
    source_place_id: str
    name: str | None
    lat: float
    lng: float
    address: str | None
    primary_category: str | None
    categories: list[str]
    category_hierarchy: list[str]
    product_category_group: str
    category_source: str
    confidence: float | None
    admin_area_id: str
    admin_area_name: str
    first_seen_at: str
    last_seen_at: str
    google_place_id: str | None = None
    google_match_status: str | None = None
    google_match_score: float | None = None
    google_categories: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "CanonicalPlace":
        return cls(**payload)

    def with_google_enrichment(
        self, *, place_id: str | None, match_status: str | None,
        match_score: float | None, categories: list[str],
    ) -> "CanonicalPlace":
        return replace(self, google_place_id=place_id, google_match_status=match_status,
                       google_match_score=match_score, google_categories=list(categories))
