"""Offline Google identity staging. Technical dedup only; no crawler or ER calls."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import re
import sqlite3
import unicodedata
import uuid
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import unquote, urlsplit

import pyarrow as pa
import pyarrow.parquet as pq

from ..overture_incremental import _canonical, _digest

POLICY = "google-silver-technical-v1"
IDENTIFIER = r"0x[0-9a-fA-F]+:0x[0-9a-fA-F]+|ChIJ[A-Za-z0-9_-]+"
BUSINESS_FIELDS = (
    "name_display", "name_normalized", "address_display", "address_normalized",
    "phone_display", "phone_normalized", "category_display", "category_normalized",
    "business_status", "coordinates", "rating", "review_count", "website", "menu_url",
    "price_range", "plus_code", "opening_hours_json",
)
PLACE_SCHEMA = pa.schema([
    ("google_id_kind", pa.string()), ("google_identity_value", pa.string()),
    *[(n, pa.string()) for n in ("metadata_status", "name_display", "name_normalized",
      "address_display", "address_normalized", "phone_display", "phone_normalized",
      "category_display", "category_normalized", "business_status")],
    ("latitude", pa.float64()), ("longitude", pa.float64()), ("rating", pa.float64()),
    ("review_count", pa.int64()), ("open_now", pa.bool_()),
    *[(n, pa.string()) for n in ("website", "menu_url", "price_range", "plus_code",
      "opening_hours_json", "opening_hours_display_json", "next_opening_time_json",
      "next_closing_time_json", "source_url", "current_observation_id", "observed_at",
      "source_run", "metadata_path", "metadata_sha256", "latest_business_status_raw",
      "field_provenance_json", "conflicting_values_json")],
    ("observation_count", pa.int64()), ("distinct_metadata_count", pa.int64()),
    ("source_overture_count", pa.int64()), ("has_metadata_conflict", pa.bool_()),
    ("conflict_fields", pa.list_(pa.string())), ("fallback_fields", pa.list_(pa.string())),
    ("missing_fields", pa.list_(pa.string())), ("dq_flags", pa.list_(pa.string())),
])
LINK_SCHEMA = pa.schema([
    ("overture_id", pa.string()), ("google_id_kind", pa.string()),
    ("google_identity_value", pa.string()), ("er_status", pa.string()),
    ("metadata_status", pa.string()), ("evidence_kind", pa.string()),
    ("observed_at", pa.string()), ("source_run", pa.string()),
    ("source_observation_ref", pa.string()), ("selection_policy", pa.string()),
    ("original_crawler_identity_status", pa.string()), ("collection_status", pa.string()),
    ("candidate_evidence_json", pa.string()), ("selection_evidence_json", pa.string()),
    ("opened_evidence_json", pa.string()), ("observation_count", pa.int64()),
    ("overture_google_candidate_count", pa.int64()), ("google_overture_source_count", pa.int64()),
    ("overture_has_conflicting_candidates", pa.bool_()),
    ("google_shared_by_overture_ids", pa.bool_()),
    ("source_observation_refs", pa.list_(pa.string())),
    ("selection_policies", pa.list_(pa.string())), ("dq_flags", pa.list_(pa.string())),
])


def sha(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def dump(value):
    return json.dumps(_canonical(value), ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def text(value):
    return value if isinstance(value, str) and value.strip() else None


def normalized(value):
    # Same accent/case policy as the existing resolver; no fuzzy/name dedup here.
    value = unicodedata.normalize("NFKD", (text(value) or "").casefold().replace("đ", "d"))
    return " ".join(re.findall(r"[a-z0-9]+", "".join(c for c in value if not unicodedata.combining(c)))) or None


def timestamp(value):
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if result.utcoffset() is None:
            return None
        return result.astimezone(timezone.utc).isoformat()
    except (TypeError, ValueError, AttributeError):
        return None


def typed_id(value):
    value = value.strip() if isinstance(value, str) else ""
    if re.fullmatch(r"0x[0-9a-f]+:0x[0-9a-f]+", value, flags=re.I):
        return "data_id", value.lower()
    if re.fullmatch(r"ChIJ[A-Za-z0-9_-]+", value):
        return "place_id", value
    return None


def identity(record):
    """Strict superset of the existing google_identity URL parser, without Selenium imports."""
    keys = set()
    flags = []
    for field in ("google_identifier", "google_place_id", "candidate_id"):
        value = record.get(field)
        if value is None:
            continue
        key = typed_id(value)
        if key:
            keys.add(key)
            declared = record.get("google_identifier_type") if field == "google_identifier" else None
            if declared and declared != key[0]:
                flags.append("IDENTITY_KIND_MISMATCH")
        elif field != "candidate_id":
            flags.append("INVALID_ID_VALUE")
    for field in ("maps_url", "source_url", "resolved_url", "url", "href"):
        url = record.get(field)
        if not text(url):
            continue
        parsed = urlsplit(url)
        if parsed.scheme != "https" or not re.fullmatch(r"(?:www\.)?google\.(?:com|com\.vn)", parsed.hostname or ""):
            flags.append("INVALID_GOOGLE_URL")
            continue
        for value in re.findall(r"!1s(" + IDENTIFIER + r")(?:!|/|\?|&|$)", unquote(url)):
            key = typed_id(value)
            if key:
                keys.add(key)
    if len(keys) > 1:
        return None, sorted(set(flags + ["CONTRADICTING_IDENTITIES"]))
    if not keys:
        return None, sorted(set(flags + ["MISSING_VALID_GOOGLE_ID"]))
    return next(iter(keys)), sorted(set(flags))


def discover(output):
    """Bounded layout discovery; never traverse browser profiles or review lakes."""
    output = Path(output)
    queues = sorted(set(output.glob("google*/dispatch.sqlite3")) |
                    set(output.glob("google*/batch-*/dispatch.sqlite3")))
    excluded = [str(p.parent.resolve()) for p in queues if "discarded" in p.as_posix().lower()]
    return [p.parent.resolve() for p in queues if str(p.parent.resolve()) not in excluded], excluded


class Inputs:
    def __init__(self):
        self.hashes = {}
        self.details = []

    def track(self, path):
        path = Path(path).resolve()
        digest = sha(path)
        previous = self.hashes.setdefault(str(path), digest)
        if previous != digest:
            raise ValueError("Input changed while reading: " + str(path))
        return path

    def read(self, path):
        path = self.track(path)
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(value, dict):
                raise ValueError("Expected JSON object")
            return value, path
        except (ValueError, UnicodeError) as exc:
            self.issue("INVALID_JSON", ref=str(path), detail=type(exc).__name__)
            return {}, path

    def issue(self, code, *, source=None, key=None, ref=None, field=None, detail=None):
        self.details.append(dict(issue_code=code, overture_id=source,
            google_id_kind=key[0] if key else None, google_identity_value=key[1] if key else None,
            observation_ref=ref, field=field, detail_json=dump(detail)))


def source_id(payload, fallback=None):
    value = payload.get("place_key") or fallback
    if isinstance(value, str) and value.startswith("overture:") and value[9:].strip():
        return value[9:]
    if payload.get("source") == "overture" and text(payload.get("source_id")):
        return payload["source_id"].strip()
    return None


def load(roots, overture_silver):
    data = Inputs()
    silver = data.track(overture_silver)
    ids = pq.ParquetFile(silver).read(columns=["source_id"])["source_id"].to_pylist()
    if not all(ids) or len(ids) != len(set(ids)):
        raise ValueError("Overture Silver source_id is missing/duplicated")
    silver_ids = set(ids)
    coverage = defaultdict(lambda: dict(attempts=0, states=set(), names=set()))
    metadata = []
    evidence = []
    inventory = []

    def add_evidence(source, record, ref, run, observed, kind, result=None):
        key, flags = identity(record)
        if not source:
            data.issue("MISSING_OVERTURE_ID", ref=ref)
            return
        if not key:
            data.issue("INVALID_SELECTED_ID", source=source, ref=ref, detail=flags)
            return
        for flag in flags:
            data.issue(flag, source=source, key=key, ref=ref)
        time = timestamp(observed)
        if not time:
            data.issue("MISSING_OR_INVALID_LINK_TIME", source=source, key=key, ref=ref)
        evidence.append(dict(source=source, key=key, ref=ref, run=run, time=time, kind=kind,
            policy=(result or {}).get("selection_policy_version"),
            crawler_status=(result or {}).get("identity_status"),
            collection_status=(result or {}).get("status"), record=record))

    for root in sorted(set(Path(r).resolve() for r in roots)):
        queue = root / "dispatch.sqlite3"
        data.track(queue)
        if queue.with_name(queue.name + "-wal").exists():
            data.track(queue.with_name(queue.name + "-wal"))
        summary_path = root / "summary.json"
        if not summary_path.exists():
            raise ValueError("Run has no final summary; stop it before staging: " + str(root))
        summary, _ = data.read(summary_path)
        if summary.get("status") not in {"FINISHED", "STOPPED"}:
            raise ValueError("Active/unrecognized run status: " + str(root))
        for name in ("plan.json", "manifest.json"):
            if (root / name).exists():
                data.read(root / name)
        connection = sqlite3.connect(queue.as_uri() + "?mode=ro", uri=True)
        try:
            records = connection.execute("SELECT id, place_key, payload, state, result FROM dispatch ORDER BY id").fetchall()
        finally:
            connection.close()
        by_metadata = {}
        result_count = 0
        for index, place_key, raw_payload, state, raw_result in records:
            ref = str(queue) + "#dispatch:" + str(index)
            try:
                payload = json.loads(raw_payload)
                result = json.loads(raw_result) if raw_result else None
            except ValueError:
                data.issue("INVALID_DISPATCH_JSON", ref=ref)
                continue
            source = source_id(payload, place_key)
            if not source:
                data.issue("MISSING_OVERTURE_ID", ref=ref)
                continue
            coverage[source]["states"].add(state)
            if text(payload.get("name")):
                coverage[source]["names"].add(payload["name"])
            if source not in silver_ids:
                data.issue("OVERTURE_ID_NOT_IN_SILVER", source=source, ref=ref)
            if not result:
                continue
            result_count += 1
            coverage[source]["attempts"] += 1
            observed = result.get("finished_at") or result.get("store_finished_at") or result.get("started_at")
            selected = result.get("selected_candidate")
            if isinstance(selected, dict) and selected:
                add_evidence(source, selected, ref + ":selected", str(root), observed, "SELECTED", result)
            if result.get("resolved_url") and (result.get("place_entity_confirmed") or selected):
                resolved = dict(url=result["resolved_url"], name=result.get("resolved_place_title"),
                                address=result.get("resolved_place_address"))
                add_evidence(source, resolved, ref + ":resolved", str(root), observed, "RESOLVED_PAGE", result)
            for n, attempt in enumerate(result.get("candidate_attempts") or []):
                if isinstance(attempt, dict):
                    add_evidence(source, attempt, ref + ":attempt:" + str(n), str(root), observed, "OPENED_CANDIDATE", result)
                    original = typed_id(attempt.get("selected_candidate_id"))
                    actual, _ = identity(attempt)
                    if original and actual and original != actual:
                        add_evidence(source, dict(candidate_id=original[1], identifier_conflict=True),
                                     ref + ":redirect-selected:" + str(n), str(root), observed, "REDIRECTED_SELECTION", result)
            path = result.get("google_place_details_path")
            if path:
                path = Path(path).resolve()
                if not path.is_relative_to(root):
                    data.issue("METADATA_PATH_OUTSIDE_RUN", source=source, ref=ref, detail=str(path))
                elif not path.exists():
                    data.issue("MISSING_METADATA_FILE", source=source, ref=ref, detail=str(path))
                else:
                    by_metadata[path] = (source, result)
        paths = set(root.glob("worker-*/job-*/crawl/*/metadata/*.place_details.json"))
        paths = {p.resolve() for p in paths} | set(by_metadata)
        orphan_count = 0
        for path in sorted(paths):
            m, path = data.read(path)
            source, result = by_metadata.get(path, (None, {}))
            input_path = path.parents[3] / "input.json"
            if input_path.exists():
                job, _ = data.read(input_path)
                payload = job.get("source_record") or job.get("overture") or {}
                job_source = source_id(payload)
                expected_store = (job.get("store") or {}).get("id")
                if source and job_source and source != job_source:
                    data.issue("CONFLICTING_SOURCE_ASSOCIATION", source=source, ref=str(path))
                    source = None
                elif job_source:
                    source = job_source
                if expected_store and m.get("store_id") != expected_store:
                    data.issue("METADATA_STORE_ID_MISMATCH", source=source, ref=str(path))
                    source = None
            if path not in by_metadata:
                orphan_count += 1
                data.issue("UNREFERENCED_METADATA_SNAPSHOT", source=source, ref=str(path))
            key, flags = identity(m)
            if not key:
                data.issue("INVALID_METADATA_ID", source=source, ref=str(path), detail=flags)
                continue
            for flag in flags:
                data.issue(flag, source=source, key=key, ref=str(path))
            observed = timestamp(m.get("observed_at") or m.get("collected_at"))
            if not observed:
                data.issue("MISSING_OR_INVALID_METADATA_TIME", source=source, key=key, ref=str(path))
            metadata.append(dict(key=key, raw=m, ref=str(path), run=str(root), time=observed,
                                 sha=data.hashes[str(path)], id=_digest([str(path), data.hashes[str(path)]])))
            if source:
                coverage[source]["states"].add("METADATA_OBSERVED")
                add_evidence(source, m, str(path), str(root), m.get("observed_at") or m.get("collected_at"), "METADATA", result)
        checkpoint = next(iter(sorted(root.glob("worker-*/job-*/crawl/*/checkpoints/*.json"))), None)
        checkpoint_fields = []
        if checkpoint:
            doc, _ = data.read(checkpoint)
            checkpoint_fields = sorted(doc)
        inventory.append(dict(root=str(root), run_status=summary.get("status"), queue_rows=len(records),
            result_observations=result_count, raw_metadata_files=len(paths),
            unreferenced_metadata_files=orphan_count, checkpoint_fields=checkpoint_fields))
    return data, metadata, evidence, coverage, inventory


def normalize_metadata(obs, data):
    raw = obs["raw"]
    result = {}
    flags = []
    for name in ("name", "address", "phone", "category"):
        value = text(raw.get(name))
        if raw.get(name) is not None and not isinstance(raw[name], str):
            flags.append("INVALID_" + name.upper())
        result[name + "_display"] = value
        if name == "phone":
            clean = re.sub(r"[^0-9+]", "", unicodedata.normalize("NFKC", value or ""))
            if clean.startswith("00"):
                clean = "+" + clean[2:]
            valid = bool(re.fullmatch(r"\+?[0-9]{6,15}", clean))
            result[name + "_normalized"] = clean if valid else None
            if value and not valid:
                flags.append("INVALID_PHONE")
        else:
            result[name + "_normalized"] = normalized(value)
    lat, lon = raw.get("latitude"), raw.get("longitude")
    valid_coords = all(isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v) and abs(v) <= limit
                       for v, limit in ((lat, 90), (lon, 180)))
    result["coordinates"] = [float(lat), float(lon)] if valid_coords else None
    if (lat is not None or lon is not None) and not valid_coords:
        flags.append("INVALID_COORDINATES")
    for name, low, high, integer in (("rating", 1, 5, False), ("review_count", 0, math.inf, True)):
        value = raw.get(name)
        valid = isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)
        valid = valid and low <= value <= high and (not integer or int(value) == value)
        result[name] = (int(value) if integer else float(value)) if valid else None
        if value is not None and not valid:
            flags.append("INVALID_" + name.upper())
    status = raw.get("business_status")
    result["business_status"] = status if status in {"OPERATIONAL", "CLOSED_TEMPORARILY", "CLOSED_PERMANENTLY"} else None
    if status not in (None, "UNKNOWN", "OPERATIONAL", "CLOSED_TEMPORARILY", "CLOSED_PERMANENTLY"):
        flags.append("INVALID_BUSINESS_STATUS")
    for name in ("website", "menu_url"):
        value = text(raw.get(name))
        result[name] = value if value and urlsplit(value).scheme in {"http", "https"} and urlsplit(value).netloc else None
        if value and not result[name]:
            flags.append("INVALID_" + name.upper())
    for name in ("price_range", "plus_code"):
        result[name] = text(raw.get(name))
    hours = raw.get("weekly_opening_hours")
    normalized_hours = []
    weekdays = set()
    for day in hours if isinstance(hours, list) else []:
        number = day.get("iso_weekday") if isinstance(day, dict) else None
        if type(number) is not int or not 1 <= number <= 7 or number in weekdays:
            flags.append("INVALID_OPENING_HOURS")
            normalized_hours = []
            break
        weekdays.add(number)
        state = day.get("status")
        if state not in {"OPEN", "CLOSED", "OPEN_24_HOURS", "UNKNOWN"}:
            flags.append("INVALID_OPENING_HOURS")
            normalized_hours = []
            break
        intervals = day.get("intervals")
        if state in {"OPEN", "OPEN_24_HOURS"} and not intervals:
            flags.append("INVALID_OPENING_HOURS")
            normalized_hours = []
            break
        valid = True
        for interval in intervals or []:
            if not isinstance(interval, dict) or any(not re.fullmatch(r"(?:[01]\d|2[0-3]):[0-5]\d", str(interval.get(k) or "")) for k in ("opens", "closes")) or type(interval.get("closes_day_offset")) is not int or interval["closes_day_offset"] not in {0, 1}:
                valid = False
                break
        if not valid:
            flags.append("INVALID_OPENING_HOURS")
            normalized_hours = []
            break
        normalized_hours.append(dict(iso_weekday=number, status=state,
            intervals=sorted(intervals or [], key=lambda i: (i["opens"], i["closes"])) if intervals is not None else None))
    result["opening_hours_json"] = (dump(sorted(normalized_hours, key=lambda d: d["iso_weekday"]))
        if any(d["status"] != "UNKNOWN" for d in normalized_hours) else None)
    if not hours and raw.get("opening_hours"):
        # Retain unparsed legacy display hours only in Bronze/provenance.
        flags.append("HOURS_DISPLAY_WITHOUT_NORMALIZED_WEEK")
    result["open_now"] = raw.get("open_now") if isinstance(raw.get("open_now"), bool) else None
    for flag in flags:
        data.issue(flag, key=obs["key"], ref=obs["ref"])
    result["dq_flags"] = flags
    return result


def build(loaded):
    original, observations, evidence, coverage, inventory = loaded
    data = Inputs()
    data.details = list(original.details)
    by_place, by_link = defaultdict(list), defaultdict(list)
    for obs in observations:
        obs = dict(obs, values=normalize_metadata(obs, data))
        by_place[obs["key"]].append(obs)
    raw_missing = Counter(f for group in by_place.values() for o in group
                          for f in BUSINESS_FIELDS if o["values"].get(f) is None)
    for item in evidence:
        by_link[(item["source"], *item["key"])].append(item)
    source_keys, place_sources = defaultdict(set), defaultdict(set)
    for source, kind, value in by_link:
        source_keys[source].add((kind, value))
        place_sources[(kind, value)].add(source)
    places = []
    for key in sorted(set(by_place) | set(place_sources)):
        obs = sorted(by_place.get(key, []), key=lambda o: (bool(o["time"]), o["time"] or "",
            sum(o["values"].get(f) is not None for f in BUSINESS_FIELDS), o["id"]), reverse=True)
        current = obs[0] if obs else None
        provenance, fallback, chosen, conflicts = {}, [], {}, {}
        for field in BUSINESS_FIELDS:
            validity_field = field.replace("_display", "_normalized") if field.endswith("_display") else field
            available = [o for o in obs if o["values"].get(validity_field) is not None]
            selected = available[0] if available else None
            chosen[field] = selected["values"][field] if selected else None
            if selected:
                provenance[field] = dict(observation_id=selected["id"], observed_at=selected["time"],
                    metadata_path=selected["ref"], metadata_sha256=selected["sha"], source_run=selected["run"])
                if selected is not current:
                    fallback.append(field)
            # Display spelling/punctuation changes are not substantive conflicts.
            if field.endswith("_display"):
                continue
            unique = defaultdict(list)
            for o in available:
                unique[dump(o["values"][field])].append(o["id"])
            if len(unique) > 1:
                conflicts[field] = dict(sorted(unique.items()))
                data.issue("CONFLICTING_METADATA", key=key, field=field, detail=conflicts[field])
        flags = sorted({f for o in obs for f in o["values"]["dq_flags"]})
        if fallback:
            flags.append("OLDER_VALID_FIELDS_RETAINED")
        if not current:
            flags.append("MISSING_METADATA")
            data.issue("MISSING_METADATA", key=key)
        elif current["time"] is None:
            flags.append("MISSING_OR_INVALID_METADATA_TIME")
        raw = current["raw"] if current else {}
        hours_ref = provenance.get("opening_hours_json", {}).get("observation_id")
        hours_observation = next((o for o in obs if o["id"] == hours_ref), None)
        display_hours = (hours_observation["raw"] if hours_observation else raw).get("opening_hours")
        coords = chosen.pop("coordinates")
        # Time-sensitive UI state must not be carried forward from older visits.
        open_now = current["values"]["open_now"] if current else None
        if chosen["business_status"] in {"CLOSED_PERMANENTLY", "CLOSED_TEMPORARILY"} and open_now is True:
            flags.append("BUSINESS_STATUS_OPEN_NOW_CONFLICT")
            data.issue("BUSINESS_STATUS_OPEN_NOW_CONFLICT", key=key, ref=current["ref"])
            open_now = None
        chosen["business_status"] = chosen["business_status"] or "UNKNOWN"
        missing = [f for f in ("name_normalized", "address_normalized", "phone_normalized", "category_normalized", "rating", "review_count", "opening_hours_json") if chosen.get(f) is None]
        if not coords:
            missing.append("coordinates")
        if chosen["business_status"] == "UNKNOWN":
            missing.append("business_status")
        if current and current["time"] is None:
            missing.append("observed_at")
        for field in missing:
            data.issue("MISSING_CURRENT_FIELD", key=key, field=field)
        status = "MISSING_METADATA" if not current else "INCOMPLETE_METADATA" if missing else "AVAILABLE"
        places.append(dict(google_id_kind=key[0], google_identity_value=key[1], metadata_status=status,
            **chosen, latitude=coords[0] if coords else None, longitude=coords[1] if coords else None,
            open_now=open_now, opening_hours_display_json=dump(display_hours) if display_hours else None,
            next_opening_time_json=dump(raw.get("next_opening_time")) if raw.get("next_opening_time") and chosen["business_status"] not in {"CLOSED_PERMANENTLY", "CLOSED_TEMPORARILY"} else None,
            next_closing_time_json=dump(raw.get("next_closing_time")) if raw.get("next_closing_time") and chosen["business_status"] not in {"CLOSED_PERMANENTLY", "CLOSED_TEMPORARILY"} else None,
            source_url=raw.get("source_url") or raw.get("maps_url"),
            current_observation_id=current["id"] if current else None, observed_at=current["time"] if current else None,
            source_run=current["run"] if current else None, metadata_path=current["ref"] if current else None,
            metadata_sha256=current["sha"] if current else None, latest_business_status_raw=raw.get("business_status"),
            field_provenance_json=dump(provenance), conflicting_values_json=dump(conflicts),
            observation_count=len(obs), distinct_metadata_count=len({_digest(o["values"]) for o in obs}),
            source_overture_count=len(place_sources[key]), has_metadata_conflict=bool(conflicts),
            conflict_fields=sorted(conflicts), fallback_fields=sorted(fallback), missing_fields=sorted(missing), dq_flags=sorted(set(flags))))
    states = {(p["google_id_kind"], p["google_identity_value"]): p["metadata_status"] for p in places}
    links = []
    for (source, kind, value), items in sorted(by_link.items()):
        # Metadata evidence describes a completed observation; clock + stable ref
        # decides within a time, never a final-match label from the crawler.
        items.sort(key=lambda e: (bool(e["time"]), e["time"] or "", e["kind"] == "METADATA", e["ref"]), reverse=True)
        latest = items[0]
        selected_items = [i for i in items if i["kind"] == "SELECTED"]
        opened_items = [i for i in items if i["kind"] == "OPENED_CANDIDATE"]
        flags = []
        if len(source_keys[source]) > 1:
            flags.append("OVERTURE_MULTIPLE_GOOGLE_CANDIDATES")
        if len(place_sources[(kind, value)]) > 1:
            flags.append("GOOGLE_ID_SHARED_BY_OVERTURE_IDS")
        if states[(kind, value)] == "MISSING_METADATA":
            flags.append("MISSING_METADATA")
        for item in items:
            for field in ("house_conflict", "street_conflict", "ward_conflict", "identifier_conflict", "location_conflict"):
                if item["record"].get(field):
                    flags.append(field.upper())
        if latest["kind"] in {"OPENED_CANDIDATE", "REDIRECTED_SELECTION"}:
            flags.append("OPENED_CANDIDATE_NOT_CONFIRMED_FOR_COLLECTION")
        links.append(dict(overture_id=source, google_id_kind=kind, google_identity_value=value,
            er_status="ER_PENDING", metadata_status=states[(kind, value)], evidence_kind=latest["kind"],
            observed_at=latest["time"], source_run=latest["run"], source_observation_ref=latest["ref"],
            selection_policy=latest["policy"], original_crawler_identity_status=latest["crawler_status"],
            collection_status=latest["collection_status"], candidate_evidence_json=dump(latest["record"]),
            selection_evidence_json=dump(selected_items[0]["record"]) if selected_items else None,
            opened_evidence_json=dump(opened_items[0]["record"]) if opened_items else None,
            observation_count=len(items), overture_google_candidate_count=len(source_keys[source]),
            google_overture_source_count=len(place_sources[(kind, value)]),
            overture_has_conflicting_candidates=len(source_keys[source]) > 1,
            google_shared_by_overture_ids=len(place_sources[(kind, value)]) > 1,
            source_observation_refs=sorted({i["ref"] for i in items}),
            selection_policies=sorted({i["policy"] for i in items if i["policy"]}), dq_flags=sorted(set(flags))))
        for flag in sorted(set(flags)):
            data.issue("SUSPICIOUS_LINK_" + flag, source=source, key=(kind, value), ref=latest["ref"])
    zero = []
    invalid_sources = {d["overture_id"] for d in data.details if d["issue_code"] == "INVALID_SELECTED_ID"}
    for source, item in sorted(coverage.items()):
        if not source_keys[source]:
            state = ("SELECTED_ID_INVALID" if source in invalid_sources else
                     "INTERRUPTED_UNKNOWN" if "RUNNING" in item["states"] else
                     "ZERO_SELECTED_OBSERVED" if item["attempts"] else "INPUT_ONLY_NOT_ATTEMPTED")
            zero.append(dict(overture_id=source, status=state, attempt_count=item["attempts"],
                             dispatch_states=sorted(item["states"]), source_names=sorted(item["names"])))
            data.issue(state, source=source, detail=zero[-1])
    summary = dict(policy=POLICY, raw_observation_count=sum(r["raw_metadata_files"] for r in inventory),
        valid_metadata_observation_count=len(observations), unique_overture_id_count=len(coverage),
        invalid_metadata_observation_count=sum(r["raw_metadata_files"] for r in inventory)-len(observations),
        invalid_selected_identity_observation_count=sum(d["issue_code"] == "INVALID_SELECTED_ID" for d in data.details),
        invalid_identity_declaration_count=sum(d["issue_code"] in {"INVALID_ID_VALUE", "IDENTITY_KIND_MISMATCH"} for d in data.details),
        unique_google_id_count=len(places), google_ids_with_metadata=len(by_place),
        technical_duplicate_observation_count=len(observations) - len(by_place),
        exact_repeated_normalized_metadata_count=sum(p["observation_count"] - p["distinct_metadata_count"] for p in places),
        source_link_count=len(links), raw_link_evidence_count=len(evidence),
        google_identity_collision_count=sum(len(s) > 1 for s in place_sources.values()),
        overture_ids_with_conflicting_candidates=sum(len(k) > 1 for k in source_keys.values()),
        google_ids_with_metadata_conflicts=sum(p["has_metadata_conflict"] for p in places),
        google_ids_with_normalized_observation_changes=sum(p["distinct_metadata_count"] > 1 for p in places),
        conflicting_metadata_observation_count=sum(p["observation_count"] for p in places if p["has_metadata_conflict"]),
        raw_missing_field_counts=dict(sorted(raw_missing.items())),
        missing_metadata_google_ids=sum(p["metadata_status"] == "MISSING_METADATA" for p in places),
        suspicious_source_link_count=sum(bool(l["dq_flags"]) for l in links),
        zero_candidate_overture_id_count=len(zero), zero_candidate_inventory=zero,
        issue_counts=dict(sorted(Counter(d["issue_code"] for d in data.details).items())), input_runs=inventory,
        er_policy="All collection relationships ER_PENDING; no independent ER decisions supplied.",
        current_policy="Valid typed identity; timezone-aware time before missing time; newest UTC then completeness then observation hash. Latest valid non-null per stable field, with field lineage. UNKNOWN business status is missing for fallback. Volatile open_now/next-change labels never fall back.",
        missing_metadata_policy="Identity-only rows are MISSING_METADATA, never successful business snapshots.",
        reviews_policy="No review Parquet, review text or review SQLite read, rewritten or reassigned.")
    tables = {"silver/google_places.parquet": pa.Table.from_pylist(places, schema=PLACE_SCHEMA),
              "silver/source_links.parquet": pa.Table.from_pylist(links, schema=LINK_SCHEMA)}
    validate(tables, evidence, coverage, zero)
    summary["logical_output_sha256"] = {k: _digest(v.to_pylist()) for k, v in tables.items()}
    return tables, summary, sorted(data.details, key=dump)


def validate(tables, evidence, coverage, zero):
    places = tables["silver/google_places.parquet"].to_pylist()
    links = tables["silver/source_links.parquet"].to_pylist()
    keys = [(p["google_id_kind"], p["google_identity_value"]) for p in places]
    link_keys = [(l["overture_id"], l["google_id_kind"], l["google_identity_value"]) for l in links]
    if len(set(keys)) != len(keys) or len(set(link_keys)) != len(link_keys):
        raise ValueError("Silver primary key is duplicated")
    if set(link_keys) != {(e["source"], *e["key"]) for e in evidence}:
        raise ValueError("Valid source relationship lost")
    if set(coverage) - ({l[0] for l in link_keys} | {z["overture_id"] for z in zero}):
        raise ValueError("Source coverage lost")
    for p in places:
        if typed_id(p["google_identity_value"]) != (p["google_id_kind"], p["google_identity_value"]):
            raise ValueError("Invalid current-state identity")
        for field, bound in (("latitude", 90), ("longitude", 180)):
            value = p[field]
            if value is not None and not (math.isfinite(value) and abs(value) <= bound):
                raise ValueError("Invalid current-state coordinate")
        if p["rating"] is not None and not 1 <= p["rating"] <= 5:
            raise ValueError("Invalid current rating")
    if any((l["google_id_kind"], l["google_identity_value"]) not in set(keys) or l["er_status"] != "ER_PENDING" for l in links):
        raise ValueError("Unresolved foreign key or unexpected ER promotion")


def verify_release(directory):
    directory = Path(directory)
    manifest = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
    for relative, spec in manifest["artifacts"].items():
        path = directory / relative
        if sha(path) != spec["sha256"]:
            raise ValueError("Artifact checksum mismatch: " + relative)
        if relative.endswith(".parquet"):
            table = pq.ParquetFile(path).read()
            # These are the publisher's existing contracts, also for historical
            # manifests without a schema declaration. Ignore only Arrow metadata;
            # never infer/cast types from values or trust the manifest's schema.
            expected = {"silver/google_places.parquet": PLACE_SCHEMA,
                        "silver/source_links.parquet": LINK_SCHEMA}.get(relative)
            if expected is not None and not table.schema.equals(expected, check_metadata=False):
                raise ValueError("Artifact Arrow schema mismatch: " + relative)
            if table.num_rows != spec["rows"] or _digest(table.to_pylist()) != spec["logical_sha256"]:
                raise ValueError("Artifact logical mismatch: " + relative)
    return manifest


def read_active(output):
    output = Path(output)
    if not (output / "current.json").exists():
        return None
    pointer = json.loads((output / "current.json").read_text(encoding="utf-8"))
    if not re.fullmatch(r"[0-9a-f]{24}", pointer["release_id"]):
        raise ValueError("Invalid active release ID")
    directory = output / "releases" / pointer["release_id"]
    if sha(directory / "manifest.json") != pointer["manifest_sha256"]:
        raise ValueError("Active pointer manifest checksum mismatch")
    verify_release(directory)
    return directory


def publish(roots, overture_silver, output_root, *, excluded=None, fault=lambda stage: None):
    roots = sorted(set(Path(r).resolve() for r in roots))
    output = Path(output_root).resolve()
    if not roots:
        raise ValueError("At least one finished/stopped crawl root is required")
    if any(output.is_relative_to(r) or r.is_relative_to(output) for r in roots):
        raise ValueError("Output and raw crawl roots must be disjoint")
    if Path(overture_silver).resolve().is_relative_to(output):
        raise ValueError("Output cannot contain the existing Overture input")
    loaded = load(roots, overture_silver)
    tables, summary, details = build(loaded)
    signature = dict(policy=POLICY, inputs_sha256=dict(sorted(loaded[0].hashes.items())),
                     code_sha256={str(Path(__file__).resolve()): sha(__file__),
                       str((Path(__file__).resolve().parents[1] / "overture_incremental.py").resolve()): sha((Path(__file__).resolve().parents[1] / "overture_incremental.py"))},
                     pyarrow_version=pa.__version__, excluded_runs=sorted(excluded or []))
    release_id = _digest(signature)[:24]
    summary["excluded_operator_discarded_runs"] = sorted(excluded or [])
    output.mkdir(parents=True, exist_ok=True)
    lock = output / ".publish.lock"
    with lock.open("x", encoding="utf-8") as stream:
        stream.write(str(os.getpid()))
    try:
        read_active(output)
        destination = output / "releases" / release_id
        if destination.exists():
            manifest = verify_release(destination)
            if manifest["signature"] != signature:
                raise ValueError("Release signature mismatch")
        else:
            stage = output / (".stage-" + uuid.uuid4().hex)
            (stage / "silver").mkdir(parents=True)
            (stage / "reports").mkdir()
            artifacts = {}
            for name, table in tables.items():
                pq.write_table(table, stage / name, compression="zstd")
                artifacts[name] = dict(sha256=sha(stage / name), rows=table.num_rows,
                    schema=str(table.schema), logical_sha256=_digest(table.to_pylist()))
            report = "reports/google_silver_dq_summary.json"
            (stage / report).write_text(json.dumps(summary, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
            artifacts[report] = dict(sha256=sha(stage / report))
            report = "reports/google_silver_dq_details.csv"
            with (stage / report).open("w", encoding="utf-8-sig", newline="") as stream:
                writer = csv.DictWriter(stream, fieldnames=["issue_code", "overture_id", "google_id_kind", "google_identity_value", "observation_ref", "field", "detail_json"])
                writer.writeheader()
                writer.writerows(details)
            artifacts[report] = dict(sha256=sha(stage / report))
            manifest = dict(release_id=release_id, signature=signature, artifacts=artifacts,
                            primary_keys={"google_places": ["google_id_kind", "google_identity_value"],
                                "source_links": ["overture_id", "google_id_kind", "google_identity_value"]})
            (stage / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
            verify_release(stage)
            fault("after_stage_validation")
            for path, digest in loaded[0].hashes.items():
                if sha(path) != digest:
                    raise ValueError("Raw input changed before publication: " + path)
            discovered_metadata = {str(p.resolve()) for root in roots for p in root.glob("worker-*/job-*/crawl/*/metadata/*.place_details.json")}
            if discovered_metadata - set(loaded[0].hashes):
                raise ValueError("New raw metadata appeared during staging")
            destination.parent.mkdir(exist_ok=True)
            os.replace(stage, destination)
        # A failed or interrupted publication never replaces the active bundle.
        fault("before_pointer_replace")
        pointer = output / (".current-" + uuid.uuid4().hex + ".tmp")
        pointer.write_text(json.dumps(dict(release_id=release_id,
                           manifest_sha256=sha(destination / "manifest.json")), sort_keys=True) + "\n", encoding="utf-8")
        os.replace(pointer, output / "current.json")
        return destination, summary
    finally:
        lock.unlink()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--crawl-root", type=Path, action="append", default=[])
    parser.add_argument("--discover-root", type=Path)
    parser.add_argument("--overture-silver", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    args = parser.parse_args(argv)
    roots, excluded = discover(args.discover_root) if args.discover_root else ([], [])
    directory, summary = publish(roots + args.crawl_root, args.overture_silver, args.output_root, excluded=excluded)
    print(json.dumps(dict(release=str(directory), places=summary["unique_google_id_count"],
                         links=summary["source_link_count"], zero_candidates=summary["zero_candidate_overture_id_count"]), ensure_ascii=False))


Inputs.__module__ = "fnb_platform.google_silver"

if __name__ == "__main__":
    main()
