# Monitoring handoff

Architecture: crawler -> Bronze/Silver Parquet -> canonical integration/Gold publications -> DuckDB/dbt -> immutable read-only serving snapshot -> Superset. This document describes the M5 BI snapshot on shared history; no monitoring stack is deployed.

## Persistence actually present on this shared branch

- `crawl_experiment/storage/review_repository.py` / `ReviewRepository`: SQLite review state, logical identity/content hashes, durable `pending_review_events` outbox and checkpoint/reconciliation metadata.
- `crawl_experiment/storage/place_access_repository.py` / `PlaceAccessRepository`: SQLite access outcomes, cooldown and retry eligibility.
- `crawl_experiment/storage/checkpoint_repository.py` and `crawl_experiment/observability/run_artifacts.py`: file checkpoints, summaries, failed jobs, timings and diagnostic logs.
- `crawl_experiment/storage/parquet_review_writer.py`: append-only review change evidence and atomic persisted batches. Event count is not unique logical review count.
- `compose.analytics.yaml` PostgreSQL: Superset metadata only (`fnb_superset`), including users/datasets/charts/dashboard definitions. Analytics lives in DuckDB, not PostgreSQL.
- DuckDB: four dimensions, two facts, source bridge, three marts and shared `bi` views; Superset queries an independent serving snapshot read-only.

The shared crawler's existing implementation is preserved. The personal branch's optional PostgreSQL crawl-state hook, Windows dispatch runner and newer Airflow integration are not included in this BI snapshot. Do not claim PostgreSQL is the shared crawler's state backend, or report a scheduled Airflow DagRun from a successful BI runner. There is no Airflow UI/status/log service in the default BI setup.

## Evidence and metrics

| Source | Metrics and interpretation |
|---|---|
| Crawler SQLite state/outbox/access tables | Pending outbox count/age, checkpoint age, access-state distribution, failed/deferred jobs and next eligibility. Query read-only; retain durable recovery evidence |
| Crawler checkpoint/summary/run files | Completion, partial/limited/error counts, failure stage, retries and capture/runtime timings. Use configured paths from the existing shared crawler entrypoint; raw logs may contain source identities |
| Publication `<root>/current.json`, `releases/<id>/manifest.json` | Exact release, artifact/manifest SHA256, row counts and pinned lineage. Directory existence or exit 0 alone is insufficient |
| dbt `dbt-target/run_results.json`, `manifest.json` | Model/test success/error/skip, duration and actual lineage; reject stale or mismatched build evidence |
| DuckDB `dimensional.release_audit`, `bi.v_data_quality` | Store/mapping/review conservation, source-only/unresolved counts, taxonomy status and missing metadata within one pinned release |
| `.local/duckdb-final/<run>/warehouse/` | Bundle manifest, `current.json`, `serving-current.json`, `serving/<id>/snapshot.json`, `query-timings.json`: immutable snapshot identity/hash and measured queries |
| `.local/duckdb-final/<run>/` | `checkpoint.json`, `verification.json`, stage logs, browser verification and `resources.jsonl`: task outcomes, tile responses and sampled container RAM |

Initial monitoring candidates: failed/stuck tasks, old checkpoints, growing pending outbox, missing/corrupt publications, dbt failures, mapping fanout, review-conservation failures, unavailable UI and provider tile errors. Set thresholds against an explicitly agreed refresh schedule; this manual pilot implies no live SLO.

The root runner selects dbt profiles and database variables automatically. Four dbt projects remain separate; Demo builds the dimensional/business projects. Gold/Area Docs need their own existing valid build artifacts. Snapshot refresh reconnects Superset to an exact read-only URI and retains old files for existing readers.

## Verification boundaries

Personal M5 Real and isolated Windows fresh-source verification passed on the reviewed source release: 725 real canonical records, 302 accepted links, 423 source-only and 10,139 logical reviews (6,942 attached + 3,197 unmatched); seven models/41 dbt tests and 20 charts/3 tabs. These are historical personal-run results, not a new execution on this shared branch. Public Demo uses invented records instead.

Shared-snapshot focused/configuration checks must be reported separately from full E2E. A passing native runner is not a scheduled Airflow DagRun. Shared live crawling, personal crawler-state integration and Linux-host execution are NOT_VERIFIED. No credentials, cookies, source review text, database files or raw logs belong in Git.
