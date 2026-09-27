# Architecture

## Overview

The project collects Google Maps reviews through a guest browser and persists them as an idempotent, append-only dataset for downstream analytics.

```text
CLI
  -> FullCrawlRunner
  -> CrawleeAdapter
  -> BrowserSessionManager
  -> SeleniumBase guest browser
  -> GoogleMapsCrawler
  -> Navigator
  -> ReviewSurface
  -> Paginator
  -> Extractor
  -> ReviewRepository
  -> SQLite state + transactional outbox
  -> Parquet review changes
```

The architecture deliberately separates application lifecycle, browser ownership, source-specific DOM behavior, persistence, and run-level reporting.

## Entrypoint and Runner

### `scripts/coles_small_set_full_crawl.py`

The full-crawl script is a thin configured entrypoint. It owns the current `STORES` collection, parses and validates CLI arguments, selects stores, calls `run_full_crawl()`, and returns the process exit code.

It does not implement crawler wiring, persistence wiring, browser lifecycle, artifact reporting, or summary generation.

### `crawl_experiment/runners/full_crawl_runner.py`

The full-crawl runner is the application composition root and lifecycle manager. It:

- validates generic runner inputs;
- creates the run directory and configures logging;
- constructs `ParquetReviewWriter`, `ReviewRepository`, `GoogleMapsCrawler`, and `CrawleeAdapter`;
- wires browser warm-up and the newest-sort crawl wrapper;
- executes the crawl and closes persistence resources;
- records browser and persistence lifecycle statistics; and
- builds and finalizes `summary.json` and run metadata.

The runner receives stores and runtime settings explicitly. The current configured entrypoint uses these default paths:

```text
data/full_crawl/coles_small_set/<run_id>/
data/state/crawler_state.sqlite3
data/lake/google_maps_reviews/
```

## Run Artifacts and Observability

`crawl_experiment/observability/run_artifacts.py` owns run-level reporting and artifact formats.

`CrawlArtifacts` exposes callbacks used by orchestration:

- `on_attempt`
- `on_activity`
- `on_result`
- `on_terminal_failure`

This module records per-store metadata and timing, writes DLQ entries, stores browser lifecycle and persistence statistics, and constructs the final run summary.

Operational artifacts include:

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

`PARTIAL_LIMIT` is treated as a non-error validation or safety outcome and is not written to the DLQ. Other terminal failures may enter the DLQ according to failure classification.

## Browser and Orchestration Flow

`CrawleeAdapter` coordinates requests, retries, and browser acquisition.

The browser/session layer owns SeleniumBase browser creation, warm-up, health checks, reuse, retirement, and replacement. The runner supplies the browser factory and warm-up callback, while the adapter owns the `BrowserSessionManager` for the run.

With `concurrency=1`, a healthy browser may be reused across multiple stores.

```text
Create browser
  -> warm up once
  -> crawl store A
  -> crawl store B
  -> crawl store C
```

`GoogleMapsCrawler` does not repeat browser warm-up for every store. An unhealthy, invalid, or challenged session can be retired and recreated according to orchestration and lifecycle policy.

The current flow does not automate Google account login, use a personal Chrome profile, solve CAPTCHAs, or rotate proxies.

## Google Maps Place Flow

The source flow distinguishes a search preview from a confirmed place entity:

```text
/maps/search/
  -> candidate or search preview
  -> exact place resolution
  -> confirmed /maps/place/ entity
  -> Reviews
  -> access classification
  -> sort Newest
  -> pagination
  -> extraction
```

A `/maps/search/` result or preview card is not sufficient evidence of the final place. Review access is classified only after the navigator confirms the intended `/maps/place/` entity.

The Google Maps source components have focused responsibilities:

- `navigator.py` — search, candidate selection, place verification, and exact entity resolution;
- `review_surface.py` — Reviews UI access, access-state classification, review pane discovery, and Newest sorting;
- `paginator.py` — scrolling, observed review IDs, progress tracking, deadlines, and stop conditions;
- `extractor.py` — conversion of a review card into the normalized `Review` model; and
- `crawler.py` — coordination of these source-specific components.

Browser ownership and retry decisions remain outside `GoogleMapsCrawler`.

## Persistence Architecture

The persistence flow is:

```text
Review
  -> ReviewRepository
  -> durable SQLite state
  -> transactional pending_review_events outbox
  -> ParquetReviewWriter
  -> append-only Parquet INSERT / UPDATE events
```

### SQLite State

The durable state database is:

```text
data/state/crawler_state.sqlite3
```

SQLite owns crawler state and review identity rather than the long-term analytical review payload.

It stores:

- review identity: `(source, source_review_id)`;
- deterministic content hash;
- `first_seen_at` and `last_seen_at`;
- first-seen and last-seen run references;
- checkpoint metadata; and
- pending transactional outbox events.

State mutation and outbox insertion occur in the same SQLite transaction.

### Parquet Payload

Normalized review payloads and change events are stored under:

```text
data/lake/google_maps_reviews/
  crawl_date=YYYY-MM-DD/
    run_id=<run_id>/
      part-*.parquet
```

Parquet is the primary downstream review payload and append-only analytical history.

Relative source dates such as `3 months ago` remain in `review_date_raw`; the crawler does not invent an exact review timestamp.

Change semantics are:

```text
new review ID
  -> INSERT state
  -> INSERT Parquet event

known ID + same content hash
  -> update last-seen metadata
  -> UNCHANGED
  -> no Parquet event

known ID + changed content hash
  -> update state
  -> UPDATE Parquet event
```

Parquet batches are finalized through an atomic temporary-file rename.

If the process stops after file finalization but before outbox acknowledgement, startup recovery replays the pending batch using a stable filename and verifies the stored event IDs before acknowledging the outbox entries.

This prevents duplicate Parquet emission during recovery.

DuckDB can query the recursive Parquet dataset without becoming a crawler runtime dependency:

```sql
SELECT *
FROM read_parquet(
  'data/lake/google_maps_reviews/**/*.parquet',
  union_by_name = true
)
LIMIT 100;
```

## Incremental State

Incremental decisions should rely on durable review identities rather than relative date labels.

Repository batch lookup APIs expose known review IDs for future overlap-based crawl stopping. A future incremental strategy can combine signals such as:

- known-ID overlap;
- consecutive known-review streaks;
- no-new-review scroll counts; and
- newly discovered review counts.

`review_date_raw` is not used as a reliable cursor because relative labels change over time.

## Status Semantics

| Status | Meaning |
|---|---|
| `COMPLETE` | Natural end of the review list reached |
| `NO_REVIEWS` | Confirmed place has no reviews |
| `PARTIAL_TIMEOUT` | Deadline reached while pagination was still progressing |
| `PARTIAL_LIMIT` | Configured safety or validation limit reached; non-error and no DLQ |
| `PAGINATION_STALLED` | Unexpected repeated no-growth condition |

Other terminal failures may enter the DLQ according to failure classification.

## Module Boundaries

- `browser/` owns browser and session lifecycle.
- `orchestration/` owns request scheduling and retry orchestration.
- `sources/google_maps/` owns Google Maps-specific DOM and source behavior.
- `storage/` owns durable SQLite state and Parquet persistence.
- `observability/` owns run metadata, timing, DLQ, metrics, and summaries.
- `runners/` owns application lifecycle composition and dependency wiring.
- `scripts/` remain thin configured entrypoints.

These boundaries keep source behavior testable while allowing persistence, orchestration, and browser lifecycle to evolve independently.
