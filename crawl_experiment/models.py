from __future__ import annotations

from dataclasses import dataclass, field
from hashlib import sha256
import json
from typing import Any


def _required(value: str, field_name: str) -> str:
    normalized = str(value).strip()
    if not normalized:
        raise ValueError(f"{field_name} must not be empty")
    return normalized


@dataclass(frozen=True)
class StoreRecord:
    retailer_code: str
    retailer_store_id: str
    name: str
    url: str
    address: str | None = None
    payload: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "retailer_code", _required(self.retailer_code, "retailer_code").lower())
        object.__setattr__(self, "retailer_store_id", _required(self.retailer_store_id, "retailer_store_id"))

    @property
    def logical_key(self) -> tuple[str, str]:
        return self.retailer_code, self.retailer_store_id


@dataclass(frozen=True)
class ReviewRecord:
    source_code: str
    source_review_id: str | None
    retailer_code: str
    retailer_store_id: str
    author: str | None = None
    published_at: str | None = None
    rating: float | None = None
    title: str | None = None
    text: str | None = None
    payload: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "source_code", _required(self.source_code, "source_code").lower())
        object.__setattr__(self, "retailer_code", _required(self.retailer_code, "retailer_code").lower())
        object.__setattr__(self, "retailer_store_id", _required(self.retailer_store_id, "retailer_store_id"))
        native_id = str(self.source_review_id).strip() if self.source_review_id is not None else ""
        object.__setattr__(self, "source_review_id", native_id or self._fallback_id())

    def _fallback_id(self) -> str:
        stable = {
            "retailer_code": self.retailer_code,
            "retailer_store_id": self.retailer_store_id,
            "author": (self.author or "").strip(),
            "published_at": (self.published_at or "").strip(),
            "rating": self.rating,
            "title": (self.title or "").strip(),
            "text": (self.text or "").strip(),
        }
        canonical = json.dumps(stable, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return "fallback:" + sha256(canonical.encode("utf-8")).hexdigest()

    @property
    def logical_key(self) -> tuple[str, str]:
        return self.source_code, str(self.source_review_id)

