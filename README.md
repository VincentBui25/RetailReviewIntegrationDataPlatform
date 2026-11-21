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


## DuckDB warehouse and Superset analytics

M5 adds a separate, on-demand BI path; the shared crawler implementation and its instructions above are retained:

`Published Gold Parquet -> DuckDB + dbt -> immutable read-only snapshot -> Superset`

The public Demo generates invented non-PII records; it does not ship Google data or require a live crawl. Prerequisites: Git, PowerShell 7.2+, Docker Desktop with Linux containers and Docker Compose v2. Python/dbt/browser checks run in Docker.

```powershell
pwsh -File .\scripts\Invoke-DuckDbPilot.ps1
```

Setup creates `.env` from `.env.example` if absent, checks ports, builds the required images, creates per-run secrets, runs focused tests and dbt, imports the native 20-chart/3-tab dashboard, verifies reads before/during/after refresh, and opens dbt Docs. Only PostgreSQL metadata, Superset and dbt Docs stay running. PostgreSQL does not serve analytics tables. Crawler and Airflow are not started by this BI setup.

- Superset: http://127.0.0.1:18101/superset/dashboard/hcmc-fnb-market-overview/
- dbt Docs/Lineage: http://127.0.0.1:18102/
- Username: `portfolio_admin`. Copy the generated password locally without printing it:

```powershell
(Get-Content -Raw .\.local\duckdb-final\mentor-demo\secrets\superset_admin_password).Trim() | Set-Clipboard
pwsh -File .\scripts\Invoke-DuckDbPilot.ps1 -Docs business_analytics
pwsh -File .\scripts\Invoke-DuckDbPilot.ps1 -Action Stop
pwsh -File .\scripts\Invoke-DuckDbPilot.ps1 -Action Restart
```

Stop keeps volumes, secrets and warehouse files. Rerun the same command to resume; changed source/options require a new `-RunName`. Configure distinct ports in `.env` for multiple deployments. Never reset existing volumes to recover a failed setup.

For authorized existing publications, configure the mounted input roots plus six explicit release paths/manifest SHA256 values in `.env`, then use `-Mode Real -RunName my-real-01`. No private publications are included, and no implicit latest selection or Demo fallback is allowed. Seven dimensional models preserve store/review/mapping grain; the three business marts and shared BI views own metric definitions.

[Dimensional Modeling](docs/DIMENSIONAL_MODELING.md), [business requirements](docs/01_BRD.md), and [Monitoring handoff](docs/MONITORING_HANDOFF.md) explain the models and evidence. Default Demo has two invented stores and four reviews; it is not the 725-record real pilot. Accepted ER mappings are machine decisions, not independently verified ground truth. Unmatched reviews retain their source identities.

This shared snapshot packages BI and its publication-validation dependencies. Optional `-WithRuntime`, the personal branch's newer Airflow/native-crawler integration and its pending crawler edits are not ported here; keep using the shared crawler entrypoints above. The existing shared SQLite/outbox implementation is preserved. BI source is synchronized by reviewed file snapshots, not by importing personal Git ancestry. Windows M5 Real/fresh-source evidence belongs to the personal source release; shared baseline compatibility checks are reported separately. Linux host and shared live-crawler/full-pipeline E2E are NOT_VERIFIED. Street tiles require network/provider access; localhost is not a public deployment.
