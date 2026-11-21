"""Small, strict adapter for acquisition and historical Overture metadata.

Legacy metadata may omit source and row count, but never release, bbox or hash.
The caller supplies the artifact path; optional declared paths must agree with it.
"""
import hashlib
import json
import math
import re
from datetime import date
from pathlib import Path

import pyarrow.parquet as pq

BRONZE_STAGING_PREFIX = ".overture-bronze-staging-"


def release_identity(value):
    if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}\.\d+", value):
        raise ValueError("Malformed Overture release identity")
    date.fromisoformat(value.split(".")[0])
    return value


def _bbox(value):
    if isinstance(value, dict):
        if set(value) != {"west", "south", "east", "north"}:
            raise ValueError("Malformed bbox keys")
        value = [value[k] for k in ("west", "south", "east", "north")]
    if not isinstance(value, (list, tuple)) or len(value) != 4 or any(
        isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v) for v in value
    ):
        raise ValueError("Malformed bbox coordinates")
    w, s, e, n = map(float, value)
    if not (-180 <= w < e <= 180 and -90 <= s < n <= 90):
        raise ValueError("Malformed WGS84 bbox bounds")
    return [w, s, e, n]


def _hash(value):
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-fA-F]{64}", value):
        raise ValueError("Malformed snapshot checksum")
    return value.lower()


def _source(value):
    if not isinstance(value, str) or value.casefold() not in {"overture", "overture maps", "overture_maps"}:
        raise ValueError("Source identity mismatch: expected Overture")
    return "overture"


def _rows(value):
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError("Malformed snapshot row count")
    return value


def _aliases(metadata, names, validate, *, required=True):
    values = [validate(metadata[name]) for name in names if name in metadata]
    if not values:
        if required:
            raise ValueError("Missing snapshot metadata: " + "/".join(names))
        return None
    if any(value != values[0] for value in values[1:]):
        raise ValueError("Conflicting snapshot metadata aliases: " + "/".join(names))
    return values[0]


def validate_snapshot(path, metadata_path, *, expected_release=None, _allow_staging=False):
    path, metadata_path = Path(path).resolve(), Path(metadata_path).resolve()
    if not _allow_staging and any(
        parent.name.startswith(BRONZE_STAGING_PREFIX)
        for item in (path, metadata_path) for parent in item.parents
    ):
        raise ValueError("Unpublished Bronze staging snapshot cannot be consumed")
    try:
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise ValueError("Malformed snapshot metadata JSON: " + str(metadata_path)) from error
    if not isinstance(metadata, dict):
        raise ValueError("Snapshot metadata must be an object")
    release = _aliases(metadata, ("source_release", "release"), release_identity)
    digest = _aliases(metadata, ("artifact_sha256", "sha256"), _hash)
    bbox = _aliases(metadata, ("extraction_bbox", "bbox", "hcm_bbox"), _bbox)
    source = _aliases(metadata, ("source", "source_name"), _source, required=False)
    rows = _aliases(metadata, ("rows", "raw_count", "all_places_in_bbox"), _rows, required=False)
    if expected_release is not None and release != release_identity(expected_release):
        raise ValueError(f"Release mismatch: expected {expected_release}, declared {release}")
    for key in ("source_path", "snapshot_path"):
        if key in metadata:
            if not isinstance(metadata[key], str) or not metadata[key].strip():
                raise ValueError("Malformed snapshot path: " + key)
            declared = Path(metadata[key])
            if not declared.is_absolute():
                declared = metadata_path.parent / declared
            if declared.resolve() != path:
                raise ValueError("Snapshot path mismatch: " + key)
    with path.open("rb") as stream:
        if hashlib.file_digest(stream, "sha256").hexdigest() != digest:
            raise ValueError("Snapshot checksum mismatch: " + str(path))
    if rows is not None and pq.read_metadata(path).num_rows != rows:
        raise ValueError("Snapshot row count mismatch: " + str(path))
    for key in ("spatial_filter", "taxonomy_policy", "source_schema"):
        if key in metadata and (not isinstance(metadata[key], str) or not metadata[key].strip()):
            raise ValueError("Malformed extraction scope: " + key)
    return dict(metadata, source_release=release, artifact_sha256=digest,
                bbox=bbox, source=source or "overture")


def validate_pair_scope(old, new, *, required_bbox=None):
    """Exact extracts for comparison, or both cover the runner's common buffer."""
    for key in ("spatial_filter", "taxonomy_policy", "source_schema"):
        if old.get(key) != new.get(key):
            raise ValueError("Extraction scope mismatch: " + key)
    if old["source_release"] == new["source_release"]:
        raise ValueError("Two distinct source releases are required")
    old_key = (old["source_release"].split(".")[0], int(old["source_release"].split(".")[1]))
    new_key = (new["source_release"].split(".")[0], int(new["source_release"].split(".")[1]))
    if old_key >= new_key:
        raise ValueError("Release sequence mismatch: OLD must precede NEW")
    if required_bbox is None:
        if old["bbox"] != new["bbox"]:
            raise ValueError("Geographic scope mismatch: extraction bboxes differ")
    else:
        w, s, e, n = _bbox(required_bbox)
        for label, metadata in (("OLD", old), ("NEW", new)):
            a, b, c, d = metadata["bbox"]
            if not (a <= w and b <= s and c >= e and d >= n):
                raise ValueError(f"Geographic scope mismatch: {label} does not cover buffered study area")
