from __future__ import annotations

from html import unescape
from html.parser import HTMLParser
import json
import re
from typing import Any, Iterable
from urllib.parse import urlparse

from .models import ReviewRecord, StoreRecord


STORE_ID_RE = re.compile(r"-(\d+)/?$")
CHALLENGE_MARKERS = (
    "captcha",
    "incapsula",
    "_incapsula_resource",
    "cf-chl-",
    "cloudflare",
    "access denied",
    "unusual traffic",
)


class _TextParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.parts: list[str] = []
        self.in_h1 = False
        self.h1_parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() == "h1":
            self.in_h1 = True

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() == "h1":
            self.in_h1 = False

    def handle_data(self, data: str) -> None:
        value = data.strip()
        if value:
            self.parts.append(value)
            if self.in_h1:
                self.h1_parts.append(value)


def store_id_from_url(url: str) -> str:
    path = urlparse(url).path.rstrip("/")
    match = STORE_ID_RE.search(path)
    if not match:
        raise ValueError(f"cannot derive Coles store ID from URL: {url}")
    return match.group(1)


def detect_challenge(body: bytes | str) -> bool:
    text = body.decode("utf-8", "replace") if isinstance(body, bytes) else body
    lowered = text.lower()
    return any(marker in lowered for marker in CHALLENGE_MARKERS)


def parse_store_html(url: str, body: bytes, fallback_name: str) -> StoreRecord:
    text = body.decode("utf-8", "replace")
    parser = _TextParser()
    parser.feed(text)
    name = unescape(" ".join(parser.h1_parts)).strip() or fallback_name
    return StoreRecord(
        retailer_code="coles",
        retailer_store_id=store_id_from_url(url),
        name=name,
        url=url,
        payload={"visible_text": " ".join(parser.parts)[:10000]},
    )


def parse_review_batches(
    batches: Iterable[dict[str, Any]], *, source_code: str, retailer_store_id: str
) -> list[ReviewRecord]:
    records: list[ReviewRecord] = []
    for batch in batches:
        for item in batch.get("reviews", []):
            records.append(
                ReviewRecord(
                    source_code=source_code,
                    source_review_id=item.get("id"),
                    retailer_code="coles",
                    retailer_store_id=retailer_store_id,
                    author=item.get("author"),
                    published_at=item.get("published_at"),
                    rating=item.get("rating"),
                    title=item.get("title"),
                    text=item.get("text"),
                    payload=item,
                )
            )
    return records


def load_review_batches(paths: list[str]) -> list[dict[str, Any]]:
    batches = []
    for path in paths:
        with open(path, "r", encoding="utf-8") as handle:
            batches.append(json.load(handle))
    return batches

