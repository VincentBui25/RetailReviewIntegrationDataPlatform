"""Resolve canonical Overture places to Google identities; never discover new POIs."""
from __future__ import annotations

import math
import re
import unicodedata
from difflib import SequenceMatcher
from typing import Any
from urllib.parse import quote_plus, unquote

from crawl_experiment.core.errors import PlaceResolutionError
from crawl_experiment.core.models import ResolutionResult, Store
from crawl_experiment.core.place_catalog import CanonicalPlace
from .navigator import GoogleMapsNavigator


def normalized(value: str | None) -> str:
    text = unicodedata.normalize("NFKD", (value or "").casefold().replace("đ", "d"))
    return " ".join(re.findall(r"[a-z0-9]+", "".join(c for c in text if not unicodedata.combining(c))))


def google_identity(url: str | None) -> tuple[str | None, str | None]:
    decoded = unquote(url or "")
    match = re.search(r"!1s(0x[0-9a-f]+:0x[0-9a-f]+|ChIJ[A-Za-z0-9_-]+)(?:!|/|\?|&|$)", decoded, re.I)
    if not match:
        return None, None
    value = match[1]
    return value, "place_id" if value.startswith("ChIJ") else "data_id"


def coordinate_distance(place: CanonicalPlace, url: str | None) -> float | None:
    # !3d/!4d is the entity point. Never use the @ viewport camera location.
    match = re.search(r"!3d(-?\d+(?:\.\d+)?)!4d(-?\d+(?:\.\d+)?)", unquote(url or ""))
    if not match:
        return None
    lat, lng = map(float, match.groups())
    if not (-90 <= lat <= 90 and -180 <= lng <= 180):
        return None
    a, b = math.radians(place.lat), math.radians(lat)
    dlat, dlng = b - a, math.radians(lng - place.lng)
    hav = math.sin(dlat / 2) ** 2 + math.cos(a) * math.cos(b) * math.sin(dlng / 2) ** 2
    return 6371008.8 * 2 * math.asin(math.sqrt(min(1, max(0, hav))))


def assess_resolution(place: CanonicalPlace, diagnostics: dict[str, Any]) -> ResolutionResult:
    title = diagnostics.get("resolved_place_title")
    address = diagnostics.get("resolved_place_address")
    url = diagnostics.get("resolved_url")
    identity, kind = google_identity(url)
    distance = coordinate_distance(place, url)
    name_score = SequenceMatcher(None, normalized(place.name), normalized(title)).ratio() if title else None
    address_score = SequenceMatcher(None, normalized(place.address), normalized(address)).ratio() if place.address and address else None
    score = None
    if distance is not None and name_score is not None:
        weighted = 0.5 * name_score + 0.4 * max(0, 1 - distance / 500)
        score = (weighted + 0.1 * address_score) if address_score is not None else weighted / 0.9
    resolved = bool(diagnostics.get("place_entity_confirmed") and identity and distance is not None
                    and distance <= 100 and name_score is not None and name_score >= 0.85
                    and score is not None and score >= 0.85)
    return dict(place_key=place.place_key, overture_name=place.name, google_name=title,
                google_place_id=identity, google_id_kind=kind, coordinate_distance_m=distance,
                name_similarity=name_score, address_similarity=address_score, match_score=score,
                status="RESOLVED" if resolved else "AMBIGUOUS", resolved_url=url, google_address=address)


def resolve_place(place: CanonicalPlace, driver: Any) -> ResolutionResult:
    navigator = GoogleMapsNavigator(driver)
    query = " ".join(filter(None, [place.name, place.address, place.admin_area_name]))
    error = None
    try:
        navigator.open_store(Store(id=store_id(place), name=place.name, query=query))
    except PlaceResolutionError as exc:
        error = str(exc).splitlines()[0]
    row = assess_resolution(place, navigator.last_resolution_diagnostics)
    if row["status"] != "RESOLVED":
        body = driver.execute_script("return document.body ? document.body.innerText : '';")
        if re.search(r"no results found|không tìm thấy kết quả|không có kết quả", body or "", re.I):
            row["status"] = "NOT_FOUND"
    row["error"] = error
    return row


def store_id(place: CanonicalPlace) -> str:
    # Windows-safe filenames and durable Overture lineage for review store_id.
    import hashlib
    return "overture-" + hashlib.sha256(place.place_key.encode()).hexdigest()[:24]


class ResolvedPlaceDriver:
    """Resolved-entity bridge: replace this store's search with its verified entity URL.

    The unchanged crawler still validates name, address, entity UI and review access.
    A redirect to another Google ID fails closed, before any review extraction.
    """
    def __init__(self, driver, store, resolution):
        self.driver = driver
        self.search_url = f"https://www.google.com/maps/search/{quote_plus(store.query)}/"
        self.resolution = resolution

    def get(self, url):
        if url == self.search_url:
            self.driver.get(self.resolution["resolved_url"])
            _ = self.current_url
        else:
            self.driver.get(url)

    @property
    def current_url(self):
        url = self.driver.current_url
        if google_identity(url)[0] != self.resolution["google_place_id"]:
            raise PlaceResolutionError("resolved entity redirect changed/missing Google identity")
        return url

    def __getattr__(self, name):
        return getattr(self.driver, name)
