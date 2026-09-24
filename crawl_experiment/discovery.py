from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from html.parser import HTMLParser
import json
from pathlib import Path
import re
from typing import Any, Callable, Iterable
from urllib.parse import urlparse
from uuid import uuid4
import xml.etree.ElementTree as ET

from .http import FetchResult
from .models import StoreRecord
from .parsers import detect_challenge, store_id_from_url
from .storage import Storage


STORE_PATH_RE = re.compile(r"^/find-stores/(?:coles|coles-local)/nsw/.+-\d+/?$", re.IGNORECASE)


@dataclass
class DiscoveredStore:
    retailer_code: str
    retailer_store_id: str
    store_name: str
    store_url: str
    address: str | None = None
    suburb: str | None = None
    postcode: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    discovery_source: str = "official_store_sitemap"

    @property
    def logical_key(self) -> tuple[str, str]:
        return self.retailer_code, self.retailer_store_id

    def to_store_record(self) -> StoreRecord:
        return StoreRecord(
            retailer_code=self.retailer_code,
            retailer_store_id=self.retailer_store_id,
            name=self.store_name,
            url=self.store_url,
            address=self.address,
            payload={
                "suburb": self.suburb,
                "postcode": self.postcode,
                "latitude": self.latitude,
                "longitude": self.longitude,
                "discovery_source": self.discovery_source,
            },
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _name_from_url(url: str) -> str:
    slug = urlparse(url).path.rstrip("/").rsplit("/", 1)[-1]
    slug = re.sub(r"-\d+$", "", slug)
    return "Coles " + slug.replace("-", " ").title()


def discover_from_sitemap(
    body: bytes,
    *,
    area_path_keywords: Iterable[str],
    max_candidates: int,
) -> tuple[list[DiscoveredStore], dict[str, int]]:
    if not 1 <= max_candidates <= 20:
        raise ValueError("max_candidates must be between 1 and 20")
    keywords = tuple(value.strip().lower() for value in area_path_keywords if value.strip())
    if not keywords:
        raise ValueError("at least one area_path_keyword is required")

    root = ET.fromstring(body)
    seen: set[tuple[str, str]] = set()
    stores: list[DiscoveredStore] = []
    metrics = {"candidates_seen": 0, "duplicate_candidates": 0, "invalid_candidates": 0}
    for element in root.iter():
        if not element.tag.endswith("loc") or not element.text:
            continue
        url = element.text.strip()
        path = urlparse(url).path.lower()
        if not any(keyword in path for keyword in keywords):
            continue
        if metrics["candidates_seen"] >= max_candidates:
            break
        metrics["candidates_seen"] += 1
        parsed = urlparse(url)
        if parsed.scheme != "https" or parsed.hostname not in {"www.coles.com.au", "coles.com.au"} or not STORE_PATH_RE.match(parsed.path):
            metrics["invalid_candidates"] += 1
            continue
        try:
            store_id = store_id_from_url(url)
        except ValueError:
            metrics["invalid_candidates"] += 1
            continue
        key = ("coles", store_id)
        if key in seen:
            metrics["duplicate_candidates"] += 1
            continue
        seen.add(key)
        stores.append(
            DiscoveredStore(
                retailer_code="coles",
                retailer_store_id=store_id,
                store_name=_name_from_url(url),
                store_url=url,
            )
        )
    return stores, metrics


class _DetailParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.in_h1 = False
        self.h1: list[str] = []
        self.in_json_ld = False
        self.json_ld: list[str] = []
        self.json_documents: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attrs_dict = dict(attrs)
        if tag.lower() == "h1":
            self.in_h1 = True
        if tag.lower() == "script" and attrs_dict.get("type", "").lower() == "application/ld+json":
            self.in_json_ld = True
            self.json_ld = []

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() == "h1":
            self.in_h1 = False
        if tag.lower() == "script" and self.in_json_ld:
            self.in_json_ld = False
            self.json_documents.append("".join(self.json_ld))

    def handle_data(self, data: str) -> None:
        if self.in_h1:
            self.h1.append(data.strip())
        if self.in_json_ld:
            self.json_ld.append(data)


def _walk_json(value: Any) -> Iterable[dict[str, Any]]:
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from _walk_json(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk_json(child)


def enrich_from_store_html(store: DiscoveredStore, body: bytes) -> DiscoveredStore:
    parser = _DetailParser()
    parser.feed(body.decode("utf-8", "replace"))
    name = " ".join(part for part in parser.h1 if part).strip() or store.store_name
    address = suburb = postcode = None
    latitude = longitude = None
    for document in parser.json_documents:
        try:
            parsed = json.loads(document)
        except json.JSONDecodeError:
            continue
        for item in _walk_json(parsed):
            address_value = item.get("address")
            if isinstance(address_value, dict):
                street = address_value.get("streetAddress")
                suburb = address_value.get("addressLocality") or suburb
                postcode = str(address_value.get("postalCode")) if address_value.get("postalCode") is not None else postcode
                pieces = [street, suburb, address_value.get("addressRegion"), postcode]
                address = " ".join(str(piece).strip() for piece in pieces if piece).strip() or address
            geo = item.get("geo")
            if isinstance(geo, dict):
                try:
                    latitude = float(geo["latitude"])
                    longitude = float(geo["longitude"])
                except (KeyError, TypeError, ValueError):
                    pass
            if address or latitude is not None:
                break
        if address or latitude is not None:
            break
    return DiscoveredStore(
        retailer_code=store.retailer_code,
        retailer_store_id=store.retailer_store_id,
        store_name=name,
        store_url=store.store_url,
        address=address,
        suburb=suburb,
        postcode=postcode,
        latitude=latitude,
        longitude=longitude,
        discovery_source=store.discovery_source,
    )


def _run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "-" + uuid4().hex[:8]


class DiscoveryRunner:
    def __init__(
        self,
        *,
        storage: Storage,
        data_dir: str | Path,
        fetch: Callable[[str], FetchResult],
    ) -> None:
        self.storage = storage
        self.data_dir = Path(data_dir)
        self.fetch = fetch

    def run(self, config: dict[str, Any], max_candidates: int, enrich: bool = True) -> dict[str, Any]:
        discovery_config = config.get("discovery", {})
        sitemap_url = str(discovery_config["sitemap_url"])
        keywords = list(discovery_config.get("area_path_keywords", []))
        run_id = _run_id()
        run_dir = self.data_dir / "discovery" / run_id
        raw_dir = run_dir / "raw"
        raw_dir.mkdir(parents=True, exist_ok=False)
        requests_path = run_dir / "requests.jsonl"
        request_metrics: list[dict[str, Any]] = []

        sitemap_result = self.fetch(sitemap_url)
        (raw_dir / "sitemap-stores.xml").write_bytes(sitemap_result.body)
        self._record_request(request_metrics, requests_path, sitemap_result, "sitemap")
        blocked = detect_challenge(sitemap_result.body)
        stores: list[DiscoveredStore] = []
        parse_metrics = {"candidates_seen": 0, "duplicate_candidates": 0, "invalid_candidates": 0}
        error: str | None = None
        if blocked:
            error = "block/challenge indicator detected on official store sitemap"
        elif sitemap_result.status is None or not 200 <= sitemap_result.status < 300:
            error = sitemap_result.error or f"HTTP {sitemap_result.status}"
        else:
            try:
                stores, parse_metrics = discover_from_sitemap(
                    sitemap_result.body,
                    area_path_keywords=keywords,
                    max_candidates=max_candidates,
                )
            except (ET.ParseError, ValueError) as exc:
                error = f"invalid sitemap response: {exc}"

        if error is None and enrich:
            enriched: list[DiscoveredStore] = []
            for store in stores:
                result = self.fetch(store.store_url)
                (raw_dir / f"store-{store.retailer_store_id}.html").write_bytes(result.body)
                self._record_request(request_metrics, requests_path, result, f"store:{store.retailer_store_id}")
                if result.status is not None and 200 <= result.status < 300 and not detect_challenge(result.body):
                    store = enrich_from_store_html(store, result.body)
                enriched.append(store)
            stores = enriched

        for store in stores:
            self.storage.upsert_store(store.to_store_record())

        stores_path = run_dir / "stores.jsonl"
        stores_path.write_text(
            "".join(json.dumps(store.to_dict(), sort_keys=True) + "\n" for store in stores),
            encoding="utf-8",
        )
        compatible_config = {**config, "store_pages": [{"name": store.store_name, "url": store.store_url} for store in stores]}
        (run_dir / "crawl_config.json").write_text(
            json.dumps(compatible_config, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        latency_total = round(sum(float(item["latency_ms"]) for item in request_metrics), 3)
        summary = {
            "run_id": run_id,
            "area": config.get("area"),
            **parse_metrics,
            "unique_stores": len(stores),
            "discovery_requests": len(request_metrics),
            "http_403_count": sum(int(item["http_403_count"]) for item in request_metrics),
            "http_429_count": sum(int(item["http_429_count"]) for item in request_metrics),
            "latency_ms_total": latency_total,
            "latency_ms_average": round(latency_total / len(request_metrics), 3) if request_metrics else 0.0,
            "challenge_count": sum(int(item["challenge_detected"]) for item in request_metrics),
            "status": "failed" if error else "completed",
            "error": error,
            "stores_path": str(stores_path),
            "crawl_config_path": str(run_dir / "crawl_config.json"),
        }
        (run_dir / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        return summary

    @staticmethod
    def _record_request(
        request_metrics: list[dict[str, Any]],
        path: Path,
        result: FetchResult,
        request_type: str,
    ) -> None:
        metric = {
            "request_type": request_type,
            "url": result.url,
            "http_status": result.status,
            "latency_ms": round(result.latency_ms, 3),
            "retry_count": result.retries,
            "http_403_count": result.count_403,
            "http_429_count": result.count_429,
            "challenge_detected": detect_challenge(result.body),
            "error": result.error,
        }
        request_metrics.append(metric)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(metric, sort_keys=True) + "\n")

