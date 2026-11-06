# Retail Review Integration Data Platform

An end-to-end data engineering prototype for discovering food businesses,
resolving them to Google Maps, and producing durable review data for analytics
and recommendation workloads.

## What it does

The current pipeline is Overture-first: Overture Maps supplies canonical food
POIs, OSM/Nominatim supplies administrative boundaries, and Google Maps is used
for entity resolution, enrichment, and review collection. Google keyword/grid
discovery remains an experiment or fallback; it is not the canonical discovery
source. Foursquare is deferred.

```text
OSM/Nominatim boundary + Overture Places
        -> canonical place catalog
        -> Google Maps entity resolution
        -> review crawler
        -> SQLite state + Parquet change events
```

Core engineering features include stable review-ID idempotency, deterministic
content hashes, transactional outbox delivery, append-only Parquet events,
incremental known-review stopping, reconciliation windows with a conservative
coverage gate, and FULL/LIMITED/UNKNOWN access states with deferred retry and
backoff. The production review path is DOM-based; network extraction remains
experimental.

## Run locally

Prepare the catalog from the configured source cache:

```powershell
python -m scripts.prepare_place_catalog
```

Run the configured full crawler:

```powershell
python -m scripts.coles_small_set_full_crawl --store-limit 1 --validation-mode
```

Run tests with `pytest`. Review browser and output settings before any live
execution.

## Current status

The MVP covers persisted place discovery, browser-backed Google resolution,
full/incremental/reconciliation review modes, durable SQLite state, and
Parquet review mutations. Google-only businesses absent from Overture remain an
accepted coverage limitation. Scaling beyond single-browser workloads is future
work.

See the [architecture guide](docs/architecture.md) for the design, modes,
failure boundaries, and next scaling step.
