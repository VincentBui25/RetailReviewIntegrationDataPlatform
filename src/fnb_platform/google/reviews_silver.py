"""Offline review-event staging from the exact crawl roots pinned by Places Silver."""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
from importlib.metadata import version
import json
import math
from pathlib import Path
import re
import sqlite3

import pyarrow as pa
import pyarrow.parquet as pq

from .. import google_silver as s, publication
from ..publication import check_hashes, require
from ..overture_incremental import _digest
from crawl_experiment.core.models import Review
from crawl_experiment.storage.review_repository import review_content_hash

VERSION = 'google-reviews-silver-v1'
ROOT = Path(__file__).resolve().parents[3]
KEY = ('google_id_kind', 'google_identity_value')
UTC = pa.timestamp('us', tz='UTC')


def publish_release(output, signature, writer, *, guard=lambda:None, fault=lambda stage:None):
    return publication.publish_release(output, signature, writer, guard=guard, fault=fault,
        read_active=s.read_active, verify_release=s.verify_release, digest=_digest, dump=s.dump)


def strings(fields):
    return [(f, pa.string()) for f in fields.split()]


BRIDGE_SCHEMA = pa.schema(strings('bridge_id source_run job_ref capture_ref crawler_store_id google_id_kind google_identity_value binding_status evidence_json rule_version')
    + [('dq_flags', pa.list_(pa.string()))])
OBS_SCHEMA = pa.schema(strings('observation_id source source_review_id google_id_kind google_identity_value crawler_store_id bridge_id '
    'source_run job_ref capture_ref run_id_original source_event_id observation_id_basis event_type_original '
    'reviewer_name review_text review_date_raw owner_response review_url content_hash content_hash_computed '
    'observed_at_raw published_at_raw last_modified_at_raw timestamp_status timestamp_status_original timestamp_source '
    'file_provenance_json identity_candidates_json provenance_json rule_version')
    + [('rating', pa.float64()), ('image_urls', pa.list_(pa.string())), ('published_at', UTC),
       ('last_modified_at', UTC), ('observed_at', UTC), ('current_eligible', pa.bool_()),
       ('dq_flags', pa.list_(pa.string()))])
CURRENT_SCHEMA = pa.schema(strings('logical_review_id source source_review_id google_id_kind google_identity_value '
    'current_observation_id reviewer_name review_text review_date_raw owner_response review_url content_hash '
    'timestamp_status timestamp_source current_selection_policy provenance_json rule_version')
    + [('rating', pa.float64()), ('image_urls', pa.list_(pa.string())), ('published_at', UTC),
       ('last_modified_at', UTC), ('first_seen_at', UTC), ('last_observed_at', UTC),
       ('observation_count', pa.int64()), ('distinct_content_hash_count', pa.int64()),
       ('has_multiple_content_hashes', pa.bool_()), ('dq_flags', pa.list_(pa.string()))])
INVENTORY_SCHEMA = pa.schema(strings('inventory_id collection_attempt_id inventory_entry_kind google_id_kind google_identity_value '
    'source_run job_ref capture_ref crawler_store_id bridge_id collection_status crawler_status crawler_coverage_original '
    'stop_reason business_status review_collection_status_original sort_mode sort_confirmation_status provenance_json rule_version')
    + [('advertised_review_count', pa.int64()), ('collected_review_count', pa.int64()),
       ('persisted_event_count', pa.int64()), ('max_scrolls', pa.int64()), ('max_reviews', pa.int64()),
       ('idle_scroll_limit', pa.int64()), ('actual_scroll_count', pa.int64()), ('parse_errors', pa.int64()),
       ('is_complete_google_review_coverage', pa.bool_()), ('dq_flags', pa.list_(pa.string()))])
EXCLUDED_SCHEMA = pa.schema(strings('source_run file_ref file_sha256 exclusion_reason schema_sha256 rule_version')
    + [('persisted_rows', pa.int64())])


def stable_id(value):
    return isinstance(value, str) and bool(value) and not any(c.isspace() or ord(c) < 32 for c in value)


def instant(value):
    if isinstance(value, str):
        try: value = datetime.fromisoformat(value.replace('Z', '+00:00'))
        except ValueError: return None
    if not isinstance(value, datetime) or value.utcoffset() is None: return None
    return value.astimezone(timezone.utc)


def raw_time(value):
    return value.isoformat() if isinstance(value, datetime) else str(value) if value is not None else None


class Inputs:
    """Hash before reads and recheck at publication; never open source SQLite writable."""
    def __init__(self, hashes=None):
        self.hashes = dict(hashes or {})

    def track(self, path):
        path = Path(path).resolve()
        digest = s.sha(path)
        require(self.hashes.setdefault(str(path), digest) == digest, 'Input checksum changed: ' + str(path))
        return path

    def json(self, path):
        value = json.loads(self.track(path).read_text(encoding='utf-8-sig'))
        require(isinstance(value, dict), 'Expected JSON object: ' + str(path))
        return value


def capture_files(roots):
    """Bounded known layout only. Browser profiles and unrelated outputs are excluded."""
    result = set()
    for root in roots:
        for name in ('plan.json', 'summary.json', 'dispatch.sqlite3'):
            if (root / name).exists(): result.add((root / name).resolve())
        for job in root.glob('worker-*/job-*'):
            for name in ('input.json', 'reviews.sqlite3', 'reviews.sqlite3-wal', 'reviews.sqlite3-shm'):
                p = job / name
                # SHM is volatile, not evidence. A WAL with outstanding data must be frozen first.
                if name.endswith('-shm'): continue
                if p.exists(): result.add(p.resolve())
            for pattern in ('crawl/*/summary.json', 'crawl/*/run_metadata.json', 'crawl/*/metadata/*.json',
                            'crawl/*/checkpoints/*.json', 'lake/**/*.parquet'):
                result.update(p.resolve() for p in job.glob(pattern))
    return sorted(result)


def load_inputs(google_root):
    google_root = Path(google_root).resolve()
    release = s.read_active(google_root)
    require(release is not None, 'Current Google Places Silver release required')
    manifest = s.verify_release(release)
    require(manifest['signature']['policy'] == s.POLICY, 'Expected Google Silver M1 contract')
    tracked = Inputs(manifest['signature']['inputs_sha256'])
    check_hashes(tracked.hashes)
    tracked.track(google_root / 'current.json'); tracked.track(release / 'manifest.json')
    report = tracked.json(release / 'reports/google_silver_dq_summary.json')
    roots = sorted(Path(r['root']).resolve() for r in report['input_runs'])
    require(len(roots) == len(set(roots)) and bool(roots), 'Duplicate/missing pinned crawl roots')
    for root in roots:
        for name in ('plan.json', 'summary.json', 'dispatch.sqlite3'):
            require(str(root / name) in manifest['signature']['inputs_sha256'], 'Unpinned source run: ' + str(root))
    place_path = tracked.track(release / 'silver/google_places.parquet')
    places = pq.read_table(place_path, columns=[*KEY, 'review_count']).to_pylist()
    keys = [tuple(r[k] for k in KEY) for r in places]
    require(len(keys) == len(set(keys)) and all(s.typed_id(k[1]) == k for k in keys), 'Invalid/duplicate Google Places identity')
    files = capture_files(roots)
    for path in files: tracked.track(path)
    return tracked, roots, places, release, files


def scan_legacy(roots):
    """Inventory only: never return rows to the observations loader."""
    parents = {r.parent.parent if r.name.startswith('batch-') else r.parent for r in roots}
    extra_roots = set()
    for parent in sorted(parents):
        discovered, _ = s.discover(parent)
        extra_roots.update(set(discovered) - set(roots))
    rows = []
    hashes = {}
    for root in sorted(extra_roots):
        for path in sorted(root.glob('worker-*/job-*/lake/**/*.parquet')):
            path = path.resolve(); hashes[str(path)] = s.sha(path)
            reader = pq.ParquetFile(path)
            rows.append(dict(source_run=str(root), file_ref=str(path), file_sha256=hashes[str(path)],
                exclusion_reason='NOT_IN_CURRENT_GOOGLE_PLACES_SILVER_RELEASE',
                schema_sha256=_digest(str(reader.schema_arrow)), persisted_rows=reader.metadata.num_rows, rule_version=VERSION))
    return rows, hashes


def bridge_for_capture(root, job, capture, inp, tracked):
    sid = inp.get('store', {}).get('id')
    flags, keys, refs, observed = [], set(), [], []
    if not stable_id(sid): flags.append('CRAWLER_STORE_ID_MISSING_OR_INVALID')
    for path in sorted((capture / 'metadata').glob('*.json')):
        row = tracked.json(path)
        row_sid = row.get('store_id') or row.get('id')
        if row_sid != sid:
            flags.append('METADATA_STORE_ID_MISMATCH'); continue
        if path.name.endswith('.place_details.json'):
            key, identity_flags = s.identity(row)
            flags.extend(identity_flags)
            if key: keys.add(key)
            observed.append(row)
            refs.append(dict(ref=str(path), sha256=tracked.hashes[str(path.resolve())], identity=key, kind='PLACE_DETAILS'))
        elif row.get('place_entity_confirmed') is True:
            # Only an actually resolved page URL; never the ranked/selected candidate list.
            key, identity_flags = s.identity({'resolved_url':row.get('resolved_url')})
            flags.extend(identity_flags)
            if key: keys.add(key)
            refs.append(dict(ref=str(path), sha256=tracked.hashes[str(path.resolve())], identity=key, kind='RESOLVED_PAGE'))
    if len(keys) > 1: flags.append('AMBIGUOUS_GOOGLE_BINDING')
    if not keys: flags.append('GOOGLE_BINDING_NOT_OBSERVED')
    unsafe = {'METADATA_STORE_ID_MISMATCH', 'CRAWLER_STORE_ID_MISSING_OR_INVALID', 'CONTRADICTING_IDENTITIES', 'IDENTITY_KIND_MISMATCH'}
    key = next(iter(keys)) if len(keys) == 1 and not unsafe.intersection(flags) else None
    bridge_id = 'review_bridge_' + _digest([str(root), str(job), str(capture), sid])
    row = dict(bridge_id=bridge_id, source_run=str(root), job_ref=str(job), capture_ref=str(capture), crawler_store_id=sid,
        google_id_kind=key[0] if key else None, google_identity_value=key[1] if key else None,
        binding_status='BOUND' if key else 'AMBIGUOUS' if keys else 'UNRESOLVED',
        evidence_json=s.dump(dict(identity_candidates=sorted(keys), references=refs, input_ref=str(job / 'input.json'),
            input_sha256=tracked.hashes[str((job / 'input.json').resolve())], policy='Capture + crawler store ID; observed entity only; no names/Overture ER links.')),
        rule_version=VERSION, dq_flags=sorted(set(flags)))
    return row, observed


def normalize_event(raw, bridge, path, row_index, event_id, file_sha):
    flags = []
    if 'CURRENT_STATE_NOT_ADVANCED' in (raw.get('change_diagnostics') or []):
        # Preserve the observation and its own timestamps, but do not let a
        # later arrival override the producer's stale/uncertain-order decision.
        flags.append('CURRENT_STATE_NOT_ADVANCED')
    if event_id is None: flags.append('SOURCE_EVENT_ID_MISSING_FILE_ROW_KEY_USED')
    valid_review = stable_id(raw.get('source_review_id'))
    source = raw.get('source') if isinstance(raw.get('source'), str) else None
    if source != 'google_maps': flags.append('SOURCE_MISSING_OR_UNSUPPORTED')
    if not valid_review: flags.append('STABLE_REVIEW_ID_MISSING_OR_INVALID')
    if bridge['binding_status'] != 'BOUND': flags.append('UNRESOLVED_GOOGLE_BINDING')
    if raw.get('store_id') != bridge['crawler_store_id']: flags.append('EVENT_STORE_ID_MISMATCH')
    rating = raw.get('rating')
    if rating is None: flags.append('RATING_MISSING')
    elif isinstance(rating, bool) or not isinstance(rating, (int, float)) or not math.isfinite(rating) or not 1 <= rating <= 5:
        flags.append('RATING_INVALID'); rating = None
    text = raw.get('review_text')
    if text is None or isinstance(text, str) and not text.strip(): flags.append('REVIEW_TEXT_MISSING_OR_EMPTY')
    if text is not None and not isinstance(text, str): flags.append('REVIEW_TEXT_INVALID'); text = None
    observed = instant(raw.get('observed_at'))
    if observed is None: flags.append('OBSERVED_AT_MISSING_OR_INVALID')
    status = raw.get('timestamp_status') or 'MISSING'
    if status not in {'VERIFIED', 'MISSING', 'PARSE_FAILED', 'AMBIGUOUS'}:
        flags.append('TIMESTAMP_STATUS_INVALID'); status = 'PARSE_FAILED'
    pub, modified = instant(raw.get('published_at')), instant(raw.get('last_modified_at'))
    if status == 'VERIFIED' and (pub is None or modified is None or pub > modified):
        flags.append('VERIFIED_TIMESTAMP_PAIR_INVALID'); status = 'PARSE_FAILED'
    if status != 'VERIFIED':
        flags.append('EXACT_REVIEW_TIMESTAMPS_UNAVAILABLE'); pub = modified = None
    if observed and modified and modified > observed:
        flags.append('REVIEW_TIMESTAMP_AFTER_OBSERVATION')
    digest = raw.get('content_hash')
    if not isinstance(digest, str) or not re.fullmatch(r'[0-9a-f]{64}', digest):
        flags.append('CONTENT_HASH_MISSING_OR_INVALID'); digest = None
    try:
        computed = review_content_hash(Review(source=source or '', source_review_id=raw.get('source_review_id') or '',
            store_id=raw.get('store_id') or '', author=raw.get('reviewer_name'), rating=raw.get('rating'),
            text=raw.get('review_text'), owner_response=raw.get('owner_response'), image_urls=raw.get('image_urls') or []))
    except (ValueError, TypeError, AttributeError): computed = None
    if digest and computed != digest: flags.append('CONTENT_HASH_MISMATCH')
    event_type = raw.get('change_type')
    if event_type not in {'INSERT', 'UPDATE'}: flags.append('EVENT_TYPE_UNRECOGNIZED')
    observation_id = 'review_observation_' + _digest(['google_maps', 'event', event_id] if event_id else ['file_row', file_sha, row_index])
    return dict(observation_id=observation_id, source=source,
        source_review_id=raw.get('source_review_id') if isinstance(raw.get('source_review_id'), str) else None,
        google_id_kind=bridge['google_id_kind'], google_identity_value=bridge['google_identity_value'],
        crawler_store_id=raw.get('store_id'), bridge_id=bridge['bridge_id'], source_run=bridge['source_run'],
        job_ref=bridge['job_ref'], capture_ref=bridge['capture_ref'], run_id_original=raw.get('run_id'),
        source_event_id=event_id, observation_id_basis='SOURCE_EVENT_ID' if event_id else 'FILE_SHA256_ROW_INDEX',
        event_type_original=event_type, reviewer_name=raw.get('reviewer_name'), rating=rating, review_text=text,
        review_date_raw=raw.get('review_date_raw'), owner_response=raw.get('owner_response'), review_url=raw.get('review_url'),
        image_urls=raw.get('image_urls') or [], content_hash=digest, content_hash_computed=computed,
        observed_at=observed, published_at=pub, last_modified_at=modified, observed_at_raw=raw_time(raw.get('observed_at')),
        published_at_raw=raw_time(raw.get('published_at')), last_modified_at_raw=raw_time(raw.get('last_modified_at')),
        timestamp_status=status, timestamp_status_original=raw.get('timestamp_status'), timestamp_source=raw.get('timestamp_source'),
        file_provenance_json=s.dump([dict(file_ref=str(path), file_sha256=file_sha, row_index=row_index, event_id=event_id)]),
        identity_candidates_json=s.dump([[bridge[k] for k in KEY]] if bridge['binding_status'] == 'BOUND' else []),
        provenance_json=s.dump(dict(bridge_id=bridge['bridge_id'], captured_as='PERSISTED_CHANGE_EVENT_ONLY',
            raw_source_review_id=raw.get('source_review_id'), raw_rating=raw.get('rating'),
            change_diagnostics_original=raw.get('change_diagnostics'),
            unchanged_encounters_reconstructed=False)), rule_version=VERSION,
        current_eligible=False, dq_flags=sorted(set(flags)))


QUARANTINE = {'SOURCE_MISSING_OR_UNSUPPORTED', 'STABLE_REVIEW_ID_MISSING_OR_INVALID', 'UNRESOLVED_GOOGLE_BINDING',
    'EVENT_STORE_ID_MISMATCH', 'CROSS_IDENTITY_REVIEW_CONFLICT', 'DUPLICATE_EVENT_BINDING_CONFLICT',
    'CONTENT_HASH_MISMATCH', 'CONTENT_HASH_MISSING_OR_INVALID', 'CURRENT_STATE_NOT_ADVANCED',
    'OBSERVED_AT_MISSING_OR_INVALID', 'EVENT_TYPE_UNRECOGNIZED', 'REVIEW_TIMESTAMP_AFTER_OBSERVATION'}


def resolve_events(events):
    observations = {}
    for row, raw_digest in events:
        old = observations.get(row['observation_id'])
        if old:
            require(old[1] == raw_digest, 'Conflicting payloads for one source event ID')
            merged = old[0]
            refs = json.loads(merged['file_provenance_json']) + json.loads(row['file_provenance_json'])
            merged['file_provenance_json'] = s.dump(sorted({s.dump(r):r for r in refs}.values(), key=s.dump))
            candidates = {tuple(k) for k in json.loads(merged['identity_candidates_json']) + json.loads(row['identity_candidates_json'])}
            merged['identity_candidates_json'] = s.dump(sorted(candidates))
            flags = set(merged['dq_flags']) | set(row['dq_flags'])
            if len(candidates) > 1:
                flags.add('DUPLICATE_EVENT_BINDING_CONFLICT')
                merged.update(google_id_kind=None, google_identity_value=None)
            merged['dq_flags'] = sorted(flags)
        else: observations[row['observation_id']] = (dict(row), raw_digest)
    rows = [v[0] for _,v in sorted(observations.items())]
    groups = defaultdict(list)
    for row in rows:
        if row['source'] == 'google_maps' and stable_id(row['source_review_id']):
            groups[(row['source'], row['source_review_id'])].append(row)
    current = []
    for logical_key, versions in sorted(groups.items()):
        bindings = {tuple(k) for r in versions for k in json.loads(r['identity_candidates_json'])}
        conflict = len(bindings) > 1
        hashes = {r['content_hash'] for r in versions if r['content_hash']}
        for r in versions:
            flags = set(r['dq_flags'])
            if conflict: flags.add('CROSS_IDENTITY_REVIEW_CONFLICT')
            if len(hashes) > 1: flags.add('CONTENT_VARIATION_NOT_VERIFIED_EDIT')
            r['dq_flags'] = sorted(flags)
            r['current_eligible'] = not QUARANTINE.intersection(flags)
        valid = [r for r in versions if r['current_eligible']]
        if not valid: continue
        chosen = max(valid, key=lambda r:(r['observed_at'] is not None,
            r['observed_at'] or datetime.min.replace(tzinfo=timezone.utc), r['content_hash'] or '', r['observation_id']))
        times = [r['observed_at'] for r in versions if r['observed_at']]
        flags = set(chosen['dq_flags'])
        if sum(r['observed_at'] == chosen['observed_at'] and r['content_hash'] != chosen['content_hash'] for r in valid):
            flags.add('SAME_OBSERVED_AT_CONTENT_CONFLICT')
        selected_fields = ['source', 'source_review_id', *KEY, 'reviewer_name', 'rating', 'review_text', 'review_date_raw',
            'owner_response', 'review_url', 'image_urls', 'content_hash', 'published_at', 'last_modified_at', 'timestamp_status', 'timestamp_source']
        current.append(dict({k:chosen[k] for k in selected_fields}, logical_review_id='google_review_' + _digest(list(logical_key)),
            current_observation_id=chosen['observation_id'], first_seen_at=min(times) if times else None,
            last_observed_at=max(times) if times else None, observation_count=len(versions), distinct_content_hash_count=len(hashes),
            has_multiple_content_hashes=len(hashes) > 1, current_selection_policy='VALID_BINDING_THEN_TRUSTED_OBSERVED_AT_THEN_HASH_OBSERVATION_ID;WHOLE_OBSERVATION_NO_TEXT_BACKFILL',
            provenance_json=s.dump(dict(current_observation_id=chosen['observation_id'],
                observation_ids=sorted(r['observation_id'] for r in versions), current_file_refs=json.loads(chosen['file_provenance_json']),
                first_seen_semantics='Earliest persisted observation, not necessarily the first crawl encounter.',
                missing_later_reviews_are_not_deletions=True)), rule_version=VERSION, dq_flags=sorted(flags)))
    return rows, current


def count_int(value):
    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else None


def collection_row(bridge, details, checkpoint, result, plan, run_metadata, raw_rows, advertised):
    status = result.get('status') or checkpoint.get('status') or 'UNKNOWN'
    ids = {r.get('source_review_id') for r in raw_rows if stable_id(r.get('source_review_id'))}
    errors = count_int(result.get('parse_errors'))
    if raw_rows: state = 'COLLECTED_INCOMPLETE' if status not in {'COMPLETE', 'NO_REVIEWS'} else 'COLLECTED_COMPLETENESS_UNVERIFIED'
    elif status == 'NO_REVIEWS': state = 'SOURCE_REPORTED_NO_REVIEWS'
    elif result.get('business_status') == 'CLOSED_PERMANENTLY' and status == 'COMPLETE': state = 'METADATA_ONLY_CLOSED'
    elif status in {'PARTIAL_LIMIT', 'PARTIAL_TIMEOUT'}: state = 'INCOMPLETE_NO_PERSISTED_REVIEWS'
    elif status in {'COMPLETE', 'RUNNING', 'UNKNOWN', 'ERROR'}: state = 'UNKNOWN' if status != 'ERROR' else 'FAILED'
    else: state = 'FAILED'
    flags = ['GOOGLE_REVIEW_COVERAGE_NOT_ESTABLISHED']
    if advertised and not ids: flags.append('ADVERTISED_REVIEWS_WITHOUT_COLLECTED_REVIEWS')
    if errors: flags.append('EXTRACTION_ERRORS_REPORTED')
    if status == 'NO_REVIEWS' and advertised: flags.append('NO_REVIEWS_CONTRADICTS_ADVERTISED_COUNT')
    actual = count_int(result.get('scroll_count', checkpoint.get('scroll_count')))
    if plan.get('max_reviews') is not None and len(ids) > plan['max_reviews']: flags.append('REVIEW_LIMIT_EXCEEDED')
    if plan.get('max_scrolls') is not None and actual is not None and actual > plan['max_scrolls']: flags.append('SCROLL_LIMIT_EXCEEDED')
    # The runner's requested sort is not proof of UI confirmation. Capture metadata
    # records classification after successful sort; preserve that specific evidence.
    confirmed = details.get('sort_succeeded') is True
    return dict(inventory_id='review_inventory_' + _digest([bridge['bridge_id'], *[bridge[k] for k in KEY]]),
        collection_attempt_id=bridge['bridge_id'], inventory_entry_kind='RECORDED_COLLECTION_ATTEMPT',
        **{k:bridge[k] for k in [*KEY, 'source_run', 'job_ref', 'capture_ref', 'crawler_store_id', 'bridge_id']},
        collection_status=state, crawler_status=status, crawler_coverage_original=result.get('coverage'),
        stop_reason=result.get('stop_reason') or checkpoint.get('stop_reason'), business_status=result.get('business_status'),
        review_collection_status_original=result.get('review_collection_status'), sort_mode=run_metadata.get('sort'),
        sort_confirmation_status='CONFIRMED_IN_CAPTURE' if confirmed else 'NOT_RECORDED_SEPARATELY',
        advertised_review_count=advertised, collected_review_count=len(ids), persisted_event_count=len(raw_rows),
        max_scrolls=count_int(plan.get('max_scrolls')), max_reviews=count_int(plan.get('max_reviews')),
        idle_scroll_limit=count_int(plan.get('review_idle_limit')), actual_scroll_count=actual, parse_errors=errors,
        is_complete_google_review_coverage=False, dq_flags=sorted(flags), rule_version=VERSION,
        provenance_json=s.dump(dict(bridge_id=bridge['bridge_id'], source_result=result,
            metadata_refs=json.loads(bridge['evidence_json'])['references'],
            sort_confirmation_policy='Requested newest is not inferred to be confirmed.',
            advertised_count_basis='CAPTURE_METADATA' if advertised is not None else 'MISSING_IN_CAPTURE')))


def transform(roots, places, tracked):
    place_keys = {tuple(p[k] for k in KEY) for p in places}
    bridges, events, inventory = [], [], []
    stats = Counter(); per_run = []
    for root in roots:
        plan = tracked.json(root / 'plan.json')
        summary = tracked.json(root / 'summary.json')
        by_store = defaultdict(list)
        for r in summary.get('results', []): by_store[r.get('id') or r.get('store_id')].append(r)
        run_rows = 0
        for job in sorted(root.glob('worker-*/job-*')):
            if not (job / 'input.json').exists():
                require(not list(job.glob('lake/**/*.parquet')), 'Review files without job input: ' + str(job)); continue
            inp = tracked.json(job / 'input.json')
            sid = inp.get('store', {}).get('id')
            database = job / 'reviews.sqlite3'
            state_ids = set()
            if database.exists():
                wal = database.with_name(database.name + '-wal')
                require(not wal.exists() or wal.stat().st_size == 0, 'Nonempty SQLite WAL: freeze input before staging')
                with sqlite3.connect(database.as_uri() + '?mode=ro', uri=True) as db:
                    pending = db.execute('select count(*) from pending_review_events').fetchone()[0]
                    count = db.execute('select count(*) from review_state').fetchone()[0]
                    columns = {r[1] for r in db.execute('pragma table_info(review_state)')}
                    require({'source', 'source_review_id'} <= columns,
                            'Unsupported review_state identity schema: ' + str(database))
                    state_ids = set(db.execute('select source, source_review_id from review_state'))
                require(pending == 0, 'Unflushed review outbox: ' + str(database))
                stats['sqlite_state_rows'] += count
            captures = sorted(p for p in (job / 'crawl').glob('*') if p.is_dir())
            by_capture = {}
            for capture in captures:
                bridge, observed = bridge_for_capture(root, job, capture, inp, tracked)
                if bridge['binding_status'] == 'BOUND' and tuple(bridge[k] for k in KEY) not in place_keys:
                    bridge['dq_flags'].append('GOOGLE_ID_NOT_IN_PINNED_PLACES_SILVER')
                    bridge['binding_status'] = 'UNRESOLVED'
                bridges.append(bridge); by_capture[capture.name] = (bridge, observed, [])
            for path in sorted(job.glob('lake/**/*.parquet')):
                reader = pq.ParquetFile(path)
                require({'source', 'source_review_id', 'store_id', 'observed_at'} <= set(reader.schema_arrow.names), 'Review schema missing required identity/provenance columns')
                metadata = reader.metadata.metadata or {}
                footer = json.loads(metadata[b'review_event_ids']) if b'review_event_ids' in metadata else None
                if footer is not None:
                    require(isinstance(footer, list) and len(footer) == reader.metadata.num_rows
                            and all(stable_id(e) for e in footer) and len(set(footer)) == len(footer), 'Invalid review event footer')
                stats['review_parquet_files'] += 1
                for position, raw in enumerate(reader.read().to_pylist()):
                    require(raw.get('run_id') in by_capture, 'Event run ID does not identify one capture within its job: ' + str(path))
                    bridge, _, raw_rows = by_capture[raw['run_id']]
                    raw_rows.append(raw)
                    event = normalize_event(raw, bridge, path, position, footer[position] if footer else None, tracked.hashes[str(path.resolve())])
                    events.append((event, _digest(raw))); run_rows += 1
            # SQLite is job-scoped, not capture-scoped. INSERT and all UPDATEs
            # across that job's captures provide evidence for one logical ID.
            # Match raw persisted identities before deduplication/quarantine;
            # neither pending payloads nor state can reconstruct observations.
            persisted_ids = {(raw.get('source'), raw.get('source_review_id'))
                             for _, _, rows in by_capture.values() for raw in rows}
            missing = state_ids - persisted_ids
            require(not missing,
                    'Missing persisted review identity coverage: '
                    f'{len(missing)} identities in {database}; '
                    'state-only/historical inputs without persisted evidence are unsupported '
                    '(logical identity coverage only; full event history not proven)')
            for bridge, observed, raw_rows in by_capture.values():
                if bridge['binding_status'] != 'BOUND': continue
                capture = Path(bridge['capture_ref']); details = {}
                path = capture / 'metadata' / f'{sid}.json'
                if path.exists(): details = tracked.json(path)
                cp = capture / 'checkpoints' / f'{sid}.json'
                checkpoint = tracked.json(cp) if cp.exists() else {}
                result = {}
                cap_summary = capture / 'summary.json'
                if cap_summary.exists():
                    result = next((r for r in tracked.json(cap_summary).get('stores', []) if (r.get('id') or r.get('store_id')) == sid), {})
                candidates = by_store.get(sid, [])
                scoped = [r for r in candidates if r.get('summary_path') and Path(r['summary_path']).resolve() == cap_summary]
                parent_result = scoped[0] if len(scoped)==1 else candidates[0] if len(candidates)==1 else {}
                result = {**result, **parent_result}
                advertised = count_int(observed[-1].get('review_count')) if observed else None
                run_meta_path = capture / 'run_metadata.json'
                run_metadata = tracked.json(run_meta_path) if run_meta_path.exists() else {}
                inventory.append(collection_row(bridge, details, checkpoint, result, plan, run_metadata, raw_rows, advertised))
        per_run.append(dict(source_run=str(root), persisted_event_count=run_rows, source_run_status=summary.get('status')))
    observations, current = resolve_events(events)
    covered = {tuple(r[k] for k in KEY) for r in inventory}
    for place in sorted(places, key=lambda p:tuple(p[k] for k in KEY)):
        key = tuple(place[k] for k in KEY)
        if key in covered: continue
        row = {f.name:None for f in INVENTORY_SCHEMA}
        row.update(inventory_id='review_inventory_unattempted_' + _digest(list(key)), inventory_entry_kind='NO_RECORDED_COLLECTION_ATTEMPT',
            **dict(zip(KEY,key)), collection_status='NO_RECORDED_ATTEMPT', collected_review_count=0, persisted_event_count=0,
            advertised_review_count=count_int(place.get('review_count')), is_complete_google_review_coverage=False,
            dq_flags=['NO_TRACEABLE_COLLECTION_ATTEMPT'], rule_version=VERSION,
            provenance_json=s.dump(dict(basis='Pinned Google Places identity has no bound collection attempt in the pinned runs.',
                advertised_count_basis='PLACES_SILVER_CURRENT_NOT_A_CAPTURE', inferred_no_reviews=False)))
        inventory.append(row)
    tables = {'review_identity_bridge.parquet':pa.Table.from_pylist(sorted(bridges,key=lambda r:r['bridge_id']),schema=BRIDGE_SCHEMA),
        'silver_review_observations.parquet':pa.Table.from_pylist(observations,schema=OBS_SCHEMA),
        'silver_reviews_current.parquet':pa.Table.from_pylist(current,schema=CURRENT_SCHEMA),
        'review_collection_inventory.parquet':pa.Table.from_pylist(sorted(inventory,key=lambda r:r['inventory_id']),schema=INVENTORY_SCHEMA)}
    validate(tables, place_keys, len(events))
    logical = defaultdict(list)
    for r in observations:
        if r['source'] == 'google_maps' and stable_id(r['source_review_id']):logical[(r['source'],r['source_review_id'])].append(r)
    flags = Counter(f for r in observations for f in r['dq_flags'])
    quarantine = sum(not r['current_eligible'] for r in observations)
    summary = dict(rule_version=VERSION, persisted_event_count=len(events), observation_count=len(observations),
        duplicate_persisted_events=len(events)-len(observations), unique_logical_review_count=len(current),
        unique_source_review_ids=len(logical), repeated_logical_review_ids=sum(len(v)>1 for v in logical.values()),
        extra_observations_over_unique_ids=sum(len(v)-1 for v in logical.values()),
        ids_with_multiple_content_hashes=sum(len({r['content_hash'] for r in v if r['content_hash']})>1 for v in logical.values()),
        google_identities_with_reviews=len({tuple(r[k] for k in KEY) for r in current}),
        google_identities_without_current_reviews=len(place_keys-{tuple(r[k] for k in KEY) for r in current}),
        unresolved_review_identity_bindings=sum('UNRESOLVED_GOOGLE_BINDING' in r['dq_flags'] or 'EVENT_STORE_ID_MISMATCH' in r['dq_flags'] for r in observations),
        unbound_collection_captures=sum(r['binding_status']!='BOUND' for r in bridges),
        cross_identity_review_conflicts=sum(any('CROSS_IDENTITY_REVIEW_CONFLICT' in r['dq_flags'] for r in v) for v in logical.values()),
        quarantined_observations=quarantine, missing_or_empty_text_observations=flags['REVIEW_TEXT_MISSING_OR_EMPTY'],
        rating_only_observations=sum((r['review_text'] is None or not r['review_text'].strip()) and r['rating'] is not None for r in observations),
        missing_or_empty_text_current=sum(r['review_text'] is None or not r['review_text'].strip() for r in current),
        observation_dq_flag_counts=dict(sorted(flags.items())),
        timestamp_status_counts=dict(Counter(r['timestamp_status'] for r in observations)),
        event_type_original_counts=dict(Counter(str(r['event_type_original']) for r in observations)),
        collection_status_counts=dict(Counter(r['collection_status'] for r in inventory)),
        inventory_google_identity_count=len({tuple(r[k] for k in KEY) for r in inventory}),
        collection_inventory_rows=len(inventory), source_run_count=len(roots), input_runs=per_run,
        source_review_parquet_files=stats['review_parquet_files'], sqlite_state_rows=stats['sqlite_state_rows'],
        hard_failures=0, silver_readiness='READY' if not quarantine and all(r['last_observed_at'] for r in current) else 'NOT_READY',
        gold_readiness_scope='Google-source review input only; not a canonical-store fact or proof of complete collection.',
        coverage_complete=False, historical_unchanged_observations_reconstructed=False, production_gold=False)
    return tables, summary


def validate(tables, place_keys, raw_count):
    bridges = tables['review_identity_bridge.parquet'].to_pylist()
    observations = tables['silver_review_observations.parquet'].to_pylist()
    current = tables['silver_reviews_current.parquet'].to_pylist()
    inventory = tables['review_collection_inventory.parquet'].to_pylist()
    for rows, key in [(bridges,lambda r:r['bridge_id']), (observations,lambda r:r['observation_id']),
                      (current,lambda r:(r['source'],r['source_review_id'])), (inventory,lambda r:r['inventory_id'])]:
        require(len(rows)==len({key(r) for r in rows}), 'Duplicate Reviews Silver primary key')
    require(len({(r['capture_ref'],r['crawler_store_id']) for r in bridges})==len(bridges), 'Ambiguous bridge composite key')
    by_obs = {r['observation_id']:r for r in observations}
    require(sum(len(json.loads(r['file_provenance_json'])) for r in observations)==raw_count, 'Persisted event source coverage lost')
    require({tuple(r[k] for k in KEY) for r in inventory}==place_keys, 'Google identity inventory coverage lost')
    bridge_keys = {b['bridge_id'] for b in bridges}
    require(all(r['bridge_id'] in bridge_keys for r in observations), 'Observation bridge FK missing')
    for r in current:
        o = by_obs.get(r['current_observation_id'])
        require(o is not None and o['current_eligible'] and tuple(r[k] for k in KEY) in place_keys, 'Invalid current observation binding')
        require(all(r[k]==o[k] for k in ['source','source_review_id',*KEY,'review_text','rating','content_hash']), 'Current fields were reassigned/backfilled')
    require(all(r['rating'] is None or 1<=r['rating']<=5 for r in observations), 'Invalid normalized rating')
    require(all(r['provenance_json'] and r['file_provenance_json'] for r in observations), 'Missing review provenance')
    require(all(not r['is_complete_google_review_coverage'] for r in inventory), 'Unverified complete-coverage assertion')


def verify_release(directory, places):
    """Read-only admission of published evidence; never rebuild source events."""
    directory = Path(directory)
    require((directory / 'manifest.json').is_file(), 'Missing Reviews Silver manifest')
    manifest = s.verify_release(directory)
    signature = manifest['signature']
    require(manifest.get('publication_kind') == 'GOOGLE_REVIEWS_SILVER_OFFLINE'
            and signature.get('publication_kind') == 'GOOGLE_REVIEWS_SILVER_OFFLINE'
            and signature.get('rule_version') == VERSION, 'Unsupported Reviews Silver contract')
    require(manifest.get('release_id') == directory.name == _digest(signature)[:24],
            'Reviews Silver release identity mismatch')
    schemas = {'review_identity_bridge.parquet': BRIDGE_SCHEMA,
               'silver_review_observations.parquet': OBS_SCHEMA,
               'silver_reviews_current.parquet': CURRENT_SCHEMA,
               'review_collection_inventory.parquet': INVENTORY_SCHEMA,
               'excluded_review_files_inventory.parquet': EXCLUDED_SCHEMA}
    require(set(manifest['artifacts']) == set(schemas) | {'summary.json', 'review_dq_summary.json'},
            'Incomplete/unsupported Reviews Silver artifacts')
    tables = {}
    for name, schema in schemas.items():
        table = pq.ParquetFile(directory / name).read()
        require(table.schema.equals(schema, check_metadata=False), 'Unsupported Reviews Silver schema: ' + name)
        tables[name] = table
    summary = json.loads((directory / 'review_dq_summary.json').read_text(encoding='utf-8'))
    count = summary.get('persisted_event_count')
    require(type(count) is int and count >= 0 and summary.get('hard_failures') == 0,
            'Invalid Reviews Silver integrity summary')
    require(summary.get('observation_count') == tables['silver_review_observations.parquet'].num_rows
            and summary.get('unique_logical_review_count') == tables['silver_reviews_current.parquet'].num_rows,
            'Reviews Silver summary coverage mismatch')
    validate(tables, places, count)
    require(all(not row['current_eligible'] or not QUARANTINE.intersection(row['dq_flags'])
                for row in tables['silver_review_observations.parquet'].to_pylist()),
            'Unsupported historical Reviews Silver current eligibility')
    return manifest


def publish(google_root, output_root, *, fault=lambda stage:None):
    tracked, roots, places, release, files = load_inputs(google_root)
    legacy, legacy_hashes = scan_legacy(roots)
    code_paths = [Path(__file__), ROOT / 'src/fnb_platform/google/places_silver.py', Path(publication.__file__),
        ROOT / 'src/fnb_platform/overture_incremental.py', ROOT / 'crawl_experiment/storage/review_repository.py',
        ROOT / 'crawl_experiment/core/models.py']
    signature = dict(rule_version=VERSION, publication_kind='GOOGLE_REVIEWS_SILVER_OFFLINE',
        inputs_sha256=tracked.hashes, inventory_only_inputs_sha256=legacy_hashes,
        code_sha256={str(p.resolve()):s.sha(p) for p in code_paths},
        google_places_silver_release=release.name, source_roots=list(map(str,roots)), runtime_versions={'pyarrow':version('pyarrow')})
    def guard():
        require(capture_files(roots)==files, 'Source file inventory changed during publication')
        newer, hashes = scan_legacy(roots)
        require(newer==legacy and hashes==legacy_hashes, 'Excluded-run inventory changed during publication')
    def writer(stage, rid):
        tables, summary = transform(roots,places,tracked)
        tables['excluded_review_files_inventory.parquet'] = pa.Table.from_pylist(legacy,schema=EXCLUDED_SCHEMA)
        summary.update(release_id=rid, google_places_silver_release=release.name,
            excluded_legacy_file_count=len(legacy), excluded_legacy_persisted_rows=sum(r['persisted_rows'] for r in legacy),
            excluded_legacy_run_count=len({r['source_run'] for r in legacy}),
            baseline_comparison=dict(readiness_audit_persisted_events=10929, readiness_audit_unique_review_ids=10139,
                event_delta=summary['persisted_event_count']-10929, logical_review_delta=summary['unique_logical_review_count']-10139))
        (stage / 'review_dq_summary.json').write_text(s.dump(summary),encoding='utf-8')
        return tables, summary, ['review_dq_summary.json']
    return publish_release(output_root,signature,writer,guard=guard,fault=fault)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--google-silver-root',type=Path,required=True)
    parser.add_argument('--output-root',type=Path,required=True)
    args=parser.parse_args()
    release,summary=publish(args.google_silver_root,args.output_root)
    print(s.dump(dict(release=str(release),summary=summary)))


if __name__=='__main__':main()

Inputs.__module__ = "fnb_platform.google_reviews_silver"
