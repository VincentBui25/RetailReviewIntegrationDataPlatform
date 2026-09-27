# Architecture

## Overview

The project collects Google Maps reviews through a guest browser and persists
them as an idempotent, append-only dataset for downstream analytics.

```text
CLI
  -> FullCrawlRunner
  -> CrawleeAdapter
  -> BrowserSessionManager / SeleniumBase guest browser
  -> GoogleMapsCrawler
  -> Navigator
  -> ReviewSurface
  -> Paginator
  -> Extractor
  -> ReviewRepository
  -> SQLite state + transactional outbox
  -> Parquet review changes
```

The layers deliberately separate application lifecycle, browser ownership,
source-specific DOM behavior, persistence, and run reporting.

## Entrypoint and runner

### `scripts/coles_small_set_full_crawl.py`

The full-crawl script is a thin configured entrypoint. It owns the current
`STORES` collection, parses and validates CLI arguments, selects stores, calls
`run_full_crawl()`, and returns the process exit code. It does not implement
crawler wiring, persistence wiring, artifact reporting, or summary generation.

### `crawl_experiment/runners/full_crawl_runner.py`

The full-crawl runner is the application composition root and lifecycle
manager. It:

- validates generic runner inputs;
- creates the run directory and configures logging;
- constructs `ParquetReviewWriter`, `ReviewRepository`, `GoogleMapsCrawler`,
  and `CrawleeAdapter`;
- wires the browser warm-up callback and newest-sort crawl wrapper;
- runs the crawl and closes persistence resources;
- records browser and persistence lifecycle data; and
- builds and finalizes `summary.json` and run metadata.

The runner receives stores and runtime settings explicitly. The current Coles
entrypoint retains these defaults:

```text
data/full_crawl/coles_small_set/<run_id>/
data/state/crawler_state.sqlite3
data/lake/google_maps_reviews/
```

## Run artifacts and observability

`crawl_experiment/observability/run_artifacts.py` owns run-level reporting and
artifact formats. `CrawlArtifacts` exposes callbacks used by orchestration:

- `on_attempt`
- `on_activity`
- `on_result`
- `on_terminal_failure`

This module records per-store metadata and timing, writes DLQ entries, stores
browser lifecycle and persistence statistics, and constructs the final run
summary. Operational artifacts remain compatible:

```text
summary.json
progress.log
scroll_metrics.jsonl
review_id_trace.jsonl
dlq.jsonl
checkpoints/
metadata/
run_metadata.json
```

`PARTIAL_LIMIT` is a non-error validation/safety outcome and is not written to
the DLQ. Other terminal failures enter the DLQ according to their failure
classification.

## Browser and orchestration flow

`CrawleeAdapter` coordinates requests, retries, and browser acquisition. The
browser/session layer owns SeleniumBase browser creation, warm-up, health, reuse,
retirement, and replacement.

With `concurrency=1`, one healthy browser may be reused across multiple stores.
Google is warmed when the browser/session is prepared; `GoogleMapsCrawler` does
not repeat warm-up for every store. An unhealthy or challenged session can be
retired and recreated according to orchestration and lifecycle policy.

No Google account login, personal Chrome profile, CAPTCHA solving, or proxy
rotation is part of this flow.

## Google Maps place flow

The source flow distinguishes a search preview from a confirmed place entity:

```text
/maps/search/
  -> candidate or search preview
  -> exact place resolution
  -> confirmed /maps/place/ entity
  -> Reviews
  -> access classification
  -> sort Newest
  -> pagination and extraction
```

A `/maps/search/` page or preview card is not sufficient evidence of the final
place. Review access is classified only after the navigator confirms the exact
`/maps/place/` entity.

The Google Maps source components have focused roles:

- `navigator.py`: search, candidate selection, place verification, and exact
  entity resolution;
- `review_surface.py`: Reviews UI access, access-state classification, review
  pane discovery, and Newest sorting;
- `paginator.py`: scrolling, observed review IDs, progress, deadlines, and stop
  conditions;
- `extractor.py`: conversion of a review card into the normalized `Review`
  model; and
- `crawler.py`: coordination of these source-specific components.

Browser ownership and retry decisions remain outside `GoogleMapsCrawler`.

## Persistence architecture

The persistence flow is:

```text
Review
  -> ReviewRepository
  -> durable SQLite state
  -> transactional pending_review_events outbox
  -> ParquetReviewWriter
  -> append-only Parquet INSERT / UPDATE events
```

### SQLite state

The durable database is:

```text
data/state/crawler_state.sqlite3
```

SQLite owns crawler state rather than the full long-term review payload:

- review identity: `(source, source_review_id)`;
- deterministic content hash;
- `first_seen_at` and `last_seen_at`;
- first-seen and last-seen run references;
- checkpoint metadata; and
- pending transactional outbox events.

State mutation and outbox insertion occur in the same SQLite transaction.

### Parquet payload

Normalized review payload and lineage changes are stored under:

```text
data/lake/google_maps_reviews/
  crawl_date=YYYY-MM-DD/
    run_id=<run_id>/
      part-*.parquet
```

Parquet is the primary downstream review payload and append-only change
dataset. Relative source dates such as `3 months ago` remain in
`review_date_raw`; the crawler does not invent an exact review date.

Change behavior is:

- new review ID -> `INSERT` state and Parquet event;
- known ID with the same content hash -> `UNCHANGED`, update last-seen metadata,
  and emit no Parquet row; and
- known ID with a changed content hash -> update state and emit an `UPDATE`
  Parquet event.

Parquet batches are finalized through an atomic temporary-file rename. If a
process stops after file finalization but before outbox acknowledgement, startup
replays the pending batch using a stable filename and verifies its event IDs.
This prevents duplicate Parquet emission during recovery.

DuckDB can query the recursive dataset without becoming a crawler runtime
dependency:

```sql
SELECT *
FROM read_parquet(
  'data/lake/google_maps_reviews/**/*.parquet',
  union_by_name = true
)
LIMIT 100;
```

## Incremental state

Incremental decisions use durable review identities rather than relative date
labels. Repository batch lookup APIs expose known IDs for future overlap logic.
The crawler may use known-ID overlap, known streaks, no-new-scroll counts, and
new-review counts without treating `review_date_raw` as a cursor.

## Status semantics

| Status | Meaning |
|---|---|
| `COMPLETE` | Natural end of the review list reached |
| `NO_REVIEWS` | Confirmed place has no reviews |
| `PARTIAL_TIMEOUT` | Deadline reached while pagination was still progressing |
| `PARTIAL_LIMIT` | Configured safety or validation limit reached; non-error and no DLQ |
| `PAGINATION_STALLED` | Unexpected repeated no-growth condition |

Other terminal failures may enter the DLQ according to failure classification.

## Current boundaries

- Google Maps DOM and source logic stays under `sources/google_maps/`.
- Retry and request orchestration stays under `orchestration/`.
- Browser and session lifecycle stays under `browser/`.
- Durable SQLite state and Parquet persistence stays under `storage/`.
- Run metadata, DLQ, timing, and summaries stay under `observability/`.
- Application lifecycle composition stays under `runners/`.
- Scripts remain thin configured entrypoints.

These boundaries keep source behavior testable while allowing the persistence
dataset and browser lifecycle to evolve independently.
