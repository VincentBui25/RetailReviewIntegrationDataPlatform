"""One Overture ward filtering/normalization pipeline and reusable catalog artifacts."""
from __future__ import annotations

import hashlib
import json
import math
import uuid
from copy import deepcopy
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from shapely import from_wkb, get_type_id, get_x, get_y, wkb
from shapely.geometry import Point, shape
from shapely.errors import GEOSException

from crawl_experiment.core.place_catalog import CanonicalPlace
from crawl_experiment.core.place_categories import (
    get_category_hierarchy, get_primary_category, get_product_category_group,
    is_food_place, normalize_categories,
)

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_RAW = ROOT / "experiments/poi_source_benchmark/output/poi_source_benchmark/overture_raw.parquet"
DEFAULT_WARD = ROOT / "config/admin_areas/phuong_khanh_hoi.geojson"
DEFAULT_CATALOG = ROOT / "data/place_catalog/khanh_hoi/canonical_places.parquet"
DEFAULT_SOURCE = ROOT / "experiments/overture_food_poi/output/overture_hcm_places.parquet"
DEFAULT_SOURCE_METADATA = DEFAULT_SOURCE.with_name("overture_food_summary.json")
SCHEMA_VERSION = 1


@dataclass
class PlaceCatalog:
    places: list[CanonicalPlace]
    raw_payloads: dict[str, dict[str, Any]]
    counts: dict[str, int] = field(default_factory=dict)


def administrative_polygon(admin_area):
    """Validate and parse a current WGS84 boundary once per preparation."""
    properties = admin_area.get("properties", {})
    if properties.get("is_current") is not True or properties.get("valid_to"):
        raise ValueError("admin_area must be a current ward feature")
    if not properties.get("admin_area_id") or not properties.get("admin_area_name"):
        raise ValueError("admin_area identity and name are required")
    crs = admin_area.get("crs")
    if crs and crs.get("properties", {}).get("name") not in (
        "EPSG:4326", "urn:ogc:def:crs:OGC:1.3:CRS84", "urn:ogc:def:crs:EPSG::4326",
    ):
        raise ValueError("admin_area must use WGS84 coordinates")
    polygon = shape(admin_area["geometry"])
    if polygon.geom_type not in ("Polygon", "MultiPolygon") or not polygon.is_valid or polygon.is_empty:
        raise ValueError("admin_area must contain a valid non-empty polygon")
    if any(not math.isfinite(v) for v in polygon.bounds) or not (
        -180 <= polygon.bounds[0] <= polygon.bounds[2] <= 180
        and -90 <= polygon.bounds[1] <= polygon.bounds[3] <= 90
    ):
        raise ValueError("admin_area coordinates must be WGS84 longitude/latitude")
    return polygon


def build_overture_catalog(records, admin_area, *, observed_at, existing=None) -> PlaceCatalog:
    """Compatibility API; delegates to the same pipeline used for persistence."""
    return _build_catalog(records, admin_area, administrative_polygon(admin_area),
                          observed_at=observed_at, existing=existing)


def _build_catalog(records, admin_area, polygon, *, observed_at, existing=None):
    properties = admin_area["properties"]
    # Arrow input: decode once, apply bbox before nested Python materialization.
    if isinstance(records, pa.Table):
        points = from_wkb(records["geometry"].to_pylist(), on_invalid="ignore")
        latitudes, longitudes = get_y(points), get_x(points)
        valid = (get_type_id(points) == 0) & np.isfinite(latitudes) & np.isfinite(longitudes)
        west, south, east, north = polygon.bounds
        indices = np.flatnonzero(valid & (longitudes >= west) & (longitudes <= east)
                                 & (latitudes >= south) & (latitudes <= north))
        bbox_records = records.take(pa.array(indices, type=pa.int64())).to_pylist()
        candidates = zip(bbox_records, points[indices])
    else:
        def decoded():
            for record in records:
                geometry = record.get("geometry")
                try:
                    point = wkb.loads(bytes(geometry)) if isinstance(geometry, (bytes, bytearray, memoryview)) else shape(geometry)
                except (TypeError, ValueError, AttributeError, GEOSException):
                    continue
                if not isinstance(point, Point) or point.is_empty:
                    continue
                lng, lat = point.x, point.y
                if not (math.isfinite(lat) and math.isfinite(lng) and -90 <= lat <= 90 and -180 <= lng <= 180):
                    continue
                west, south, east, north = polygon.bounds
                if west <= lng <= east and south <= lat <= north:
                    yield record, point
        candidates = decoded()

    counts = dict(raw_bbox=0, food_filtered=0, inside_polygon=0, deduplicated=0, canonical_persisted=0)
    winners = {}
    for record, point in candidates:
        counts["raw_bbox"] += 1
        if not is_food_place(record):
            continue
        counts["food_filtered"] += 1
        if not polygon.covers(point):
            continue
        counts["inside_polygon"] += 1
        source_id = record.get("id")
        if not isinstance(source_id, str) or not source_id.strip():
            continue
        # Last accepted occurrence wins; first occurrence determines row order.
        # Identical to the previous dictionary overwrite, before normalization.
        winners[f"overture:{source_id}"] = (record, point.x, point.y)

    accepted, raw_payloads = {}, {}
    for key, (record, lng, lat) in winners.items():
        source_id = record["id"]
        previous = (existing or {}).get(key)
        names = record.get("names") or {}
        addresses = record.get("addresses") or []
        address = next((a.get("freeform") for a in addresses if a.get("freeform")), None)
        place = CanonicalPlace(
            place_key=key, source="overture", source_place_id=source_id,
            name=names.get("primary"), lat=lat, lng=lng, address=address,
            primary_category=get_primary_category(record), categories=normalize_categories(record),
            category_hierarchy=get_category_hierarchy(record),
            product_category_group=get_product_category_group(record), category_source="overture",
            confidence=record.get("confidence"), admin_area_id=properties["admin_area_id"],
            admin_area_name=properties["admin_area_name"],
            first_seen_at=previous.first_seen_at if previous else observed_at, last_seen_at=observed_at,
        )
        if previous:
            place = place.with_google_enrichment(
                place_id=previous.google_place_id, match_status=previous.google_match_status,
                match_score=previous.google_match_score, categories=previous.google_categories,
            )
        accepted[key] = place
        raw_payloads[key] = deepcopy(dict(record))
    counts["deduplicated"] = len(winners)
    counts["canonical_persisted"] = len(accepted)
    return PlaceCatalog(list(accepted.values()), raw_payloads, counts)


def _schema():
    lists = {"categories", "category_hierarchy", "google_categories"}
    numbers = {"lat", "lng", "confidence", "google_match_score"}
    return pa.schema([(f.name, pa.list_(pa.string()) if f.name in lists else
                       pa.float64() if f.name in numbers else pa.string())
                      for f in fields(CanonicalPlace)])


def manifest_path(path):
    return Path(path).with_suffix(".manifest.json")


def persist_catalog(catalog, path, manifest):
    """Finalize Parquet first, manifest last; checksum rejects mixed generations."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + "." + uuid.uuid4().hex + ".tmp")
    metadata = dict(manifest, schema_version=SCHEMA_VERSION, counts=catalog.counts,
                    dedup_policy="last accepted occurrence wins; first identity occurrence orders rows")
    temporary_manifest = temporary.with_suffix(".json.tmp")
    try:
        pq.write_table(pa.Table.from_pylist([p.to_dict() for p in catalog.places], schema=_schema()),
                       temporary, compression="zstd")
        metadata["artifact_sha256"] = hashlib.sha256(temporary.read_bytes()).hexdigest()
        temporary_manifest.write_text(json.dumps(metadata, sort_keys=True, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        temporary.replace(path)
        temporary_manifest.replace(manifest_path(path))
    finally:
        temporary.unlink(missing_ok=True)
        temporary_manifest.unlink(missing_ok=True)
    return metadata


def load_catalog(path=DEFAULT_CATALOG):
    """Read typed persisted objects only: no taxonomy, polygon, raw or network."""
    path = Path(path)
    if not path.exists() or not manifest_path(path).exists():
        raise FileNotFoundError(f"Canonical catalog missing: {path}; run scripts.prepare_place_catalog first")
    manifest = json.loads(manifest_path(path).read_text(encoding="utf-8"))
    if manifest.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("Unsupported canonical catalog schema version")
    if hashlib.sha256(path.read_bytes()).hexdigest() != manifest.get("artifact_sha256"):
        raise ValueError("Canonical catalog/manifest checksum mismatch")
    table = pq.ParquetFile(path).read()
    if table.schema != _schema():
        raise ValueError("Canonical catalog fields/types are incompatible")
    places = [CanonicalPlace.from_dict(row) for row in table.to_pylist()]
    if len(places) != manifest["counts"]["canonical_persisted"] or len({p.place_key for p in places}) != len(places):
        raise ValueError("Canonical catalog row count or identities are invalid")
    return PlaceCatalog(places, {}, manifest["counts"])


def prepare_catalog(source, ward, output, lineage, *, observed_at, reader=None):
    """Cached mode reads a snapshot; explicit reader receives the polygon bbox."""
    ward = Path(ward)
    admin_area = json.loads(ward.read_text(encoding="utf-8"))
    polygon = administrative_polygon(admin_area)
    bbox = tuple(polygon.bounds)
    release = lineage.get("source_release") or lineage.get("release")
    if not release:
        raise ValueError("Actual Overture release is required in source metadata")
    if reader is None:
        table = pq.ParquetFile(source).read()
    else:
        table = reader(bbox=bbox, release=release)
    previous = {p.place_key: p for p in load_catalog(output).places} if Path(output).exists() else None
    catalog = _build_catalog(table, admin_area, polygon, observed_at=observed_at, existing=previous)
    properties = admin_area["properties"]
    metadata = dict(
        overture_release=release, overturemaps_client_version=lineage.get("client_version"),
        retrieved_at=lineage.get("retrieved_at"), generated_at=observed_at,
        source_path=str(source) if reader is None else None,
        source_url=lineage.get("source_catalog") or lineage.get("source_url"),
        attribution=lineage.get("license_attribution") or lineage.get("attribution"),
        source_mode="bbox_query" if reader else "cached_snapshot_bbox_filter",
        admin_area_id=properties["admin_area_id"], admin_area_name=properties["admin_area_name"],
        boundary_source=properties.get("source"), boundary_osm_id=properties.get("osm_id"),
        boundary_valid_from=properties.get("valid_from"),
        boundary_sha256=hashlib.sha256(json.dumps(admin_area["geometry"], sort_keys=True).encode()).hexdigest(),
        bbox=list(bbox),
    )
    return catalog, persist_catalog(catalog, output, metadata)
