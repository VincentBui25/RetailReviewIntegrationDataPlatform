"""Offline, fail-closed comparison of fixed-scope Overture Places releases."""
from __future__ import annotations

import hashlib
import json
import math
import os
import tempfile
from collections import Counter
from datetime import date, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from .overture_snapshot import validate_snapshot, validate_pair_scope

STATES = ("NEW", "UPDATED", "UNCHANGED", "MISSING_FROM_RELEASE")


def selected_comparison_schema(old_schema, new_schema, fields):
    """Require exact selected field structure before projecting either snapshot.

    Nested additions/removals, type and nullability changes need an explicit
    adapter. Only unselected top-level fields and Arrow metadata are ignored.
    """
    for label, schema in (("OLD", old_schema), ("NEW", new_schema)):
        if len(schema.names) != len(set(schema.names)):
            raise ValueError(f"{label} raw schema contains duplicate field names")
        missing = set(fields) - set(schema.names)
        if missing:
            raise ValueError(f"{label} raw schema missing comparison fields: {sorted(missing)}")
    for name in fields:
        before, after = old_schema.field(name), new_schema.field(name)
        if not before.equals(after, check_metadata=False):
            raise ValueError(f"Selected raw schema mismatch: {name}: {before} -> {after}; "
                             "explicit schema adapter required")
    policy = {
        "policy": "exact-selected-fields-v1",
        "selected_fields": list(fields),
        "ignored_top_level_fields": {
            "old": sorted(set(old_schema.names) - set(fields)),
            "new": sorted(set(new_schema.names) - set(fields)),
        },
        "limitations": "Unselected top-level fields and Arrow metadata are not compared; raw snapshots remain authoritative.",
    }
    return pa.schema([old_schema.field(name) for name in fields]), policy


def _canonical(value):
    if isinstance(value, dict):
        return {k: _canonical(v) for k, v in sorted(value.items())}
    if isinstance(value, list):
        return [_canonical(v) for v in value]
    if isinstance(value, bytes):
        return {"binary_hex": value.hex()}
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
        return {"floating_value": str(value)}
    return value


def _digest(value):
    return hashlib.sha256(json.dumps(_canonical(value), sort_keys=True, ensure_ascii=False,
                                     separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def _scope(metadata):
    bbox = metadata.get("extraction_bbox", metadata.get("hcm_bbox", metadata.get("bbox")))
    if isinstance(bbox, dict):
        bbox = [bbox.get(k) for k in ("west", "south", "east", "north")]
    if not isinstance(bbox, list) or len(bbox) != 4 or not all(isinstance(v, (int, float)) for v in bbox):
        raise ValueError("Explicit extraction bounding box is required")
    return tuple(float(v) for v in bbox)


def _release(metadata):
    result = metadata.get("source_release", metadata.get("release"))
    if not isinstance(result, str) or not result.strip():
        raise ValueError("Source release is required")
    return result


def _load(path, metadata_path):
    path, metadata_path = Path(path), Path(metadata_path)
    metadata = validate_snapshot(path, metadata_path)
    parquet = pq.ParquetFile(path)
    if "id" not in parquet.schema_arrow.names:
        raise ValueError("Overture ID column missing")
    items = {}
    for batch in parquet.iter_batches(batch_size=4096):
        for row in batch.to_pylist():
            identifier = row.get("id")
            if not isinstance(identifier, str) or not identifier.strip():
                raise ValueError("Blank or malformed Overture ID")
            if identifier in items:
                raise ValueError(f"Duplicate Overture ID: {identifier}")
            items[identifier] = _digest(row)
    return metadata, parquet.schema_arrow, items


def compare_releases(old_path, old_metadata, new_path, new_metadata, *, report_path=None):
    """Compare complete, equivalent-scope extracts. Never infer business closure."""
    before, schema_before, old = _load(old_path, old_metadata)
    after, schema_after, new = _load(new_path, new_metadata)
    validate_pair_scope(before, after)
    if _scope(before) != _scope(after):
        raise ValueError("Release bbox differs: comparison is not valid")
    if schema_before != schema_after:
        raise ValueError("Release schema differs: normalize explicitly before comparing")
    if _release(before) == _release(after):
        raise ValueError("Two distinct source releases are required")
    changes = []
    for identifier in sorted(old.keys() | new.keys()):
        if identifier not in old:
            status = "NEW"
        elif identifier not in new:
            status = "MISSING_FROM_RELEASE"
        elif old[identifier] == new[identifier]:
            status = "UNCHANGED"
        else:
            status = "UPDATED"
        changes.append({"id": identifier, "status": status})
    counts = Counter(change["status"] for change in changes)
    result = {
        "old_release": _release(before), "new_release": _release(after),
        "bbox": list(_scope(before)), "old_rows": len(old), "new_rows": len(new),
        "counts": {state: counts[state] for state in STATES}, "changes": changes,
        "interpretation": "MISSING_FROM_RELEASE is absence in a comparable extract, not confirmed closure",
    }
    if report_path is not None:
        target = Path(report_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        content = json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
        temp_name = None
        try:
            with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", newline="\n",
                                             dir=target.parent, prefix=".overture-compare-",
                                             suffix=".tmp", delete=False) as handle:
                temp_name = handle.name
                handle.write(content)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp_name, target)
        finally:
            if temp_name and os.path.exists(temp_name):
                os.unlink(temp_name)
    return result
