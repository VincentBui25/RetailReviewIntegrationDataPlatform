# Crawler benchmark

The benchmark measures review throughput, browser/process resources and effective
worker utilization for VM sizing. It is not POI discovery or a coverage benchmark.
Samples are deterministic, high-confidence canonical Overture places; the saved
sample/fingerprint freezes membership across comparable runs.
New samples read the persisted canonical Parquet via `--catalog`. Existing sample
JSON remains unchanged/self-contained and can load even without the source catalog.
Prepare the catalog explicitly with `python -m scripts.prepare_place_catalog`;
no benchmark path re-queries Overture or re-runs taxonomy/PIP filtering.

Configured concurrency N is the ceiling of independent worker processes, not an
internal Crawlee concurrency. All workers claim from one runner-owned FIFO; each
owns one sequential browser/session manager and fresh Crawlee queue per place.
Retries keep the job until terminal success/failure; the next job is claimed only
after queue cleanup. Healthy browsers reuse within workers, never across them.

## Metrics

- `active_worker_count`: sampled claimed workers, including resolution/retry/persistence.
- `avg_active_workers`: time-weighted claim/release intervals; peak is maximum overlap.
- `worker_utilization`: active_worker_seconds / (configured_concurrency * total_run_seconds).
- `pending_place_count`: unclaimed jobs; final heavy-place tails can legitimately leave workers idle.
- Reviews written, throughput, place outcomes, retry/LIMITED/challenge rates and per-scroll timing.
- Descendant-tree CPU, crawler/browser RSS, Chrome process counts, browser restarts and unexpected crashes.

RSS may double-count shared pages; detached/short-lived processes and denied reads
limit measurement. Sampler errors are explicit. PARTIAL_LIMIT/PARTIAL_TIMEOUT are
partial successes, not COMPLETE. The benchmark uses fresh per-run/worker state to
avoid measuring unchanged-only reruns; production state/lake are untouched.

## Fail-fast and artifacts

Three missing/invalid request-file warnings for the same request abort a worker.
The parent also aborts after `--no-progress-timeout-seconds` (default
max(180, crawl timeout +120)) without progress, including blocked synchronous calls.
Only benchmark-owned worker descendants are terminated; completed persistence is
kept. An interrupted job is not silently requeued. Other workers can consume the
remaining FIFO jobs. Structured lifecycle/corruption events include worker/store
identity, timestamp and queue_remaining.

Artifacts: `output/crawler_benchmark/runs/<run_id>/` contains selected_sample,
events.jsonl, place/scroll/resource CSVs, run_metrics.json, benchmark_summary.md
and worker metadata/checkpoints/DLQ/progress logs. Data is separate under
`data/crawler_benchmark/state/<run_id>/<worker>.sqlite3` and
`data/crawler_benchmark/reviews/crawl_date=.../run_id=<worker_run_id>/`.
No VM sizing recommendation follows from an all-blocked or single run.

## Manual commands (examples only)

Run from the repository root. Prepare-only opens no browser:

```powershell
python -m scripts.benchmark_crawler --sample-size 10 --concurrency 2 --prepare-only
python -m scripts.benchmark_crawler --sample-size 10 --concurrency 2 --max-scrolls 10 --timeout-seconds 90
python -m scripts.benchmark_crawler --sample-size 6 --concurrency 2 --max-scrolls 50 --timeout-seconds 300
```

Long live runs are manual. Compare only identical fingerprints, limits, headless
settings, queue strategy and watchdog settings. Offline tests cover skewed work,
early failure, two-worker isolation, real local filesystem queues and watchdogs.
