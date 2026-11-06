"""Self-contained benchmark orchestration; independent sequential browser workers."""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import multiprocessing
import os
import queue
import re
import time
import uuid
from dataclasses import asdict
from pathlib import Path

from crawl_experiment.browser.browser_factory import BrowserFactory
from crawl_experiment.core.errors import LimitedReviewViewError
from crawl_experiment.core.models import CrawlResult, Store
from crawl_experiment.core.place_catalog import CanonicalPlace
from crawl_experiment.core.statuses import CrawlStatus
from crawl_experiment.observability.crawler_benchmark import BenchmarkMetrics, ResourceSampler, now
from crawl_experiment.observability.metrics import CrawlMetrics
from crawl_experiment.observability.run_artifacts import CrawlArtifacts, read_json, write_json
from crawl_experiment.orchestration.crawlee_adapter import CrawleeAdapter
from crawl_experiment.orchestration.retry_policy import SmokeRetryPolicy
from crawl_experiment.runners.full_crawl_runner import configure_logging
from crawl_experiment.sources.google_maps.crawler import GoogleMapsCrawler
from crawl_experiment.sources.google_maps.navigator import GoogleMapsNavigator
from crawl_experiment.sources.google_maps.place_resolution import ResolvedPlaceDriver, normalized, resolve_place, store_id
from crawl_experiment.storage.checkpoint_repository import CheckpointRepository
from crawl_experiment.storage.parquet_review_writer import ParquetReviewWriter
from crawl_experiment.storage.place_catalog import DEFAULT_CATALOG, DEFAULT_WARD, load_catalog
from crawl_experiment.storage.review_repository import ReviewRepository

ROOT = Path(__file__).resolve().parents[2]


def sample_places(places, count):
    """Deterministic round-robin high-confidence groups, with duplicate-name guard."""
    eligible = sorted((p for p in places if p.name and p.confidence is not None and p.confidence >= 0.8),
                      key=lambda p: (-p.confidence, p.source_place_id))
    groups = ("restaurant", "cafe_coffee", "bakery_dessert", "beverage", "fast_food", "bar", "other_food")
    selected, names = [], set()
    while len(selected) < count:
        before = len(selected)
        for group in groups:
            candidate = next((p for p in eligible if p.product_category_group == group and normalized(p.name) not in names), None)
            if candidate:
                selected.append(candidate)
                names.add(normalized(candidate.name))
            if len(selected) == count:
                break
        if before == len(selected):
            raise ValueError(f"Only {len(selected)} distinct high-confidence places available")
    return selected


def load_sample(path, count, catalog_path, ward_path=None):
    if path.exists():
        payload = json.loads(path.read_text(encoding="utf-8"))
        places = [CanonicalPlace.from_dict(p) for p in payload["places"]]
        if len(places) != count:
            raise ValueError("Persisted sample size differs; use the matching --sample-size or another --sample-file")
        if len({p.place_key for p in places}) != count:
            raise ValueError("Persisted sample contains duplicate identities")
    else:
        catalog = load_catalog(catalog_path)
        places = sample_places(catalog.places, count)
        path.parent.mkdir(parents=True, exist_ok=True)
        write_json(path, dict(places=[p.to_dict() for p in places], canonical_catalog=str(catalog_path),
                              ward=str(ward_path), catalog_count=len(catalog.places), created_at=now()))
    fingerprint = hashlib.sha256(json.dumps([p.to_dict() for p in places], sort_keys=True,
                                           ensure_ascii=False).encode()).hexdigest()
    return places, fingerprint


class BenchmarkReviewRepository(ReviewRepository):
    def __init__(self, *args, observer, **kwargs):
        self.observer = observer
        super().__init__(*args, **kwargs)

    def upsert_review_state(self, review, observed_at=None):
        change = super().upsert_review_state(review, observed_at)
        try:
            self.observer(dict(event="review_observed", store_id=review.store_id,
                               source_review_id=review.source_review_id, change_type=change.value))
        except Exception:
            logging.getLogger(__name__).warning("Benchmark payload metrics observer failed", exc_info=True)
        return change


def worker_entry(worker_id, places_payload, directory, config, event_queue, place_queue):
    """Top-level spawn target; all telemetry sent to the parent, never shared CSVs."""
    def emit(event):
        try:
            try:
                remaining = max(0, place_queue.qsize() - config["configured_concurrency"])
            except (NotImplementedError, AttributeError):
                remaining = None
            event_queue.put(dict(event, worker_id=worker_id, timestamp=now(),
                                 store_id=event.get("store_id"), queue_remaining=remaining,
                                 monotonic_seconds=time.monotonic()))
        except Exception:
            logging.getLogger(__name__).warning("Benchmark telemetry queue unavailable", exc_info=True)

    corruption = RequestQueueCorruptionHandler(emit)
    logging.getLogger("crawlee").addHandler(corruption)
    try:
        asyncio.run(worker_run(worker_id, places_payload, Path(directory), config, emit, place_queue=place_queue))
    except BaseException as exc:
        emit(dict(event="worker_error", error_type=type(exc).__name__, error_message=str(exc).splitlines()[0]))
    finally:
        logging.getLogger("crawlee").removeHandler(corruption)
        emit(dict(event="worker_finished"))


class RequestQueueCorruptionHandler(logging.Handler):
    """Observe Crawlee corruption warnings without altering or deleting storage."""
    def __init__(self, emit, threshold=3):
        super().__init__(logging.WARNING)
        self.emit_event, self.threshold, self.counts = emit, threshold, {}

    def emit(self, record):
        message = record.getMessage()
        if "Request file" not in message or not any(term in message for term in ("not found", "missing or invalid")):
            return
        match = re.search(r'Request file(?: for)? ["\']([^"\']+)["\']', message)
        identity = match.group(1) if match else "unknown-request"
        count = self.counts[identity] = self.counts.get(identity, 0) + 1
        self.emit_event(dict(event="request_queue_corruption", request_identity=identity,
                             occurrences=count, fatal=count >= self.threshold))


def terminate_worker(process):
    """End only this benchmark worker and its descendants, including its browser."""
    import psutil
    try:
        children = psutil.Process(process.pid).children(recursive=True)
    except psutil.Error:
        children = []
    process.terminate()
    process.join(timeout=5)
    if process.is_alive():
        process.kill()
        process.join(timeout=5)
    for child in reversed(children):
        try:
            child.terminate()
        except psutil.Error:
            pass
    _, alive = psutil.wait_procs(children, timeout=2)
    for child in alive:
        try:
            child.kill()
        except psutil.Error:
            pass


async def worker_run(worker_id, places_payload, directory, config, emit, *, place_queue):
    worker_run_id = directory.name + "-" + worker_id
    worker_dir = directory / "workers" / worker_run_id
    for folder in (worker_dir, worker_dir / "checkpoints", worker_dir / "metadata"):
        folder.mkdir(parents=True, exist_ok=True)
    logger = configure_logging(worker_dir / "progress.log")
    os.environ["CRAWLEE_STORAGE_DIR"] = str(worker_dir / "crawlee_storage")
    places = [CanonicalPlace.from_dict(p) for p in places_payload]
    by_id = {store_id(p): p for p in places}
    assigned_stores = []
    # Fresh benchmark-only state per run/worker makes write throughput comparable.
    # It is not the production incremental state and survives restarts for audit.
    state = ROOT / "data/crawler_benchmark/state" / directory.name / f"{worker_id}.sqlite3"
    lake = ROOT / "data/crawler_benchmark/reviews"
    writer = ParquetReviewWriter(lake, worker_run_id, config["benchmark_started_at"][:10], batch_size=50)
    artifacts = CrawlArtifacts(worker_dir, config["benchmark_started_at"], stores=(),
                               state_database=state, parquet_root=lake,
                               runner_name="scripts.benchmark_crawler", quarantine_max_scrolls=False)
    repository = BenchmarkReviewRepository(state, run_id=worker_run_id, parquet_writer=writer, observer=emit)
    attempt_context = {}
    resolved_ids = {}
    active_crawl_seconds = {}
    runner_error = None
    adapter = None
    current_store = None

    def claim_next():
        nonlocal current_store
        if current_store is not None:
            emit(dict(event="place_released", store_id=current_store.id))
            emit(dict(event="worker_released_job", store_id=current_store.id))
            current_store = None
        payload = place_queue.get()  # FIFO tasks followed by one sentinel per worker.
        if payload is None:
            emit(dict(event="worker_idle", store_id=None))
            return None
        place = CanonicalPlace.from_dict(payload)
        store = Store(store_id(place), place.name, place.name)
        current_store = store
        assigned_stores.append(store)
        artifacts.stores = tuple(assigned_stores)
        emit(dict(event="place_claimed", store_id=store.id, attempt=0))
        emit(dict(event="worker_claimed_job", store_id=store.id))
        write_json(worker_dir / "metadata" / f"{store.id}.json", {
            **asdict(store), "status": "QUEUED", "attempts_started": 0,
            "queued_at": config["benchmark_started_at"]})
        metadata_path = worker_dir / "run_metadata.json"
        write_json(metadata_path, {**read_json(metadata_path), "store_count": len(assigned_stores),
                                   "store_ids": [s.id for s in assigned_stores]})
        return store

    def activity(name, details):
        artifacts.on_activity(name, details)
        emit(dict(event="job_progress", store_id=details.get("store_id"), activity=name))
        if name == "review_surface_classified":
            emit(dict(event="review_surface_limited" if details.get("access_state") == "LIMITED" else name,
                      **details, attempt=attempt_context.get(details.get("store_id"))))

    crawler = GoogleMapsCrawler(repository, CheckpointRepository(worker_dir / "checkpoints"), CrawlMetrics(logger),
                                max_scrolls=config["configured_max_scrolls"], timeout_seconds=config["configured_timeout_seconds"],
                                run_id=worker_run_id, scroll_metrics_path=worker_dir / "scroll_metrics.jsonl",
                                review_id_trace_path=worker_dir / "review_id_trace.jsonl", activity_observer=activity,
                                scroll_observer=lambda details: emit(dict(details, event="scroll_metric",
                                                                        attempt=attempt_context.get(details.get("store_id")))))

    def attempt(store, number):
        artifacts.on_attempt(store, number)
        attempt_context[store.id] = number
        emit(dict(event="attempt_started", store_id=store.id, attempt=number))

    def crawl(store, driver):
        try:
            resolution = resolve_place(by_id[store.id], driver)
        except Exception as exc:
            emit(dict(event="resolution", store_id=store.id, status="ERROR",
                      error_type=type(exc).__name__, error_message=str(exc).splitlines()[0]))
            raise
        identity = resolution.get("google_place_id")
        if resolution["status"] == "RESOLVED" and identity in resolved_ids and resolved_ids[identity] != store.id:
            resolution.update(status="AMBIGUOUS", error="Google identity matched another selected POI")
        if resolution["status"] == "RESOLVED":
            resolved_ids[identity] = store.id
        emit(dict(resolution, event="resolution", store_id=store.id, attempt=attempt_context[store.id]))
        if resolution["status"] in {"AMBIGUOUS", "NOT_FOUND"}:
            return CrawlResult(store.id, CrawlStatus.PLACE_RESOLUTION_FAILED, stop_reason=resolution["status"])
        target = Store(store.id, resolution["google_name"], store.query, expected_address=resolution.get("google_address"))
        started = time.monotonic()
        try:
            return crawler.crawl(target, ResolvedPlaceDriver(driver, target, resolution), sort_newest=True,
                                 warm_up=False, elapsed_offset_seconds=artifacts.elapsed_for_store(store.id))
        except LimitedReviewViewError as exc:
            exc.smoke_store_id = store.id
            raise
        finally:
            active_crawl_seconds[store.id] = active_crawl_seconds.get(store.id, 0) + time.monotonic() - started

    def result(store, value, number):
        status = value.stop_reason if value.stop_reason in {"AMBIGUOUS", "NOT_FOUND"} else str(value.status)
        if status not in {"AMBIGUOUS", "NOT_FOUND"}:
            artifacts.on_result(store, value, number)
        else:
            path = worker_dir / "metadata" / f"{store.id}.json"
            write_json(path, {**read_json(path), "status": status, "final_status": status,
                              "terminal": True, "dlq": False, "review_crawling_skipped": True,
                              "store_finished_at": now(), "store_elapsed_seconds": artifacts.elapsed_for_store(store.id),
                              "reviews_seen": 0, "reviews_written": 0})
        emit(dict(event="place_finished", store_id=store.id, attempt=number, status=status,
                  active_crawl_seconds=active_crawl_seconds.get(store.id, 0)))
        repository.flush()

    def failed(store, decision, number, exc):
        artifacts.on_terminal_failure(store, decision, number, exc)
        emit(dict(event="place_failed", store_id=store.id, attempt=number, status=str(decision.status),
                  error_type=type(exc).__name__, error_message=str(exc).splitlines()[0]))

    def warm_up(driver):
        driver.set_page_load_timeout(30)
        GoogleMapsNavigator(driver).warm_up()
        crawler.mark_browser_warm_up_completed()

    try:
        adapter = CrawleeAdapter(BrowserFactory(headless=config["headless"]), crawl, retry_policy=SmokeRetryPolicy(),
                                 max_concurrency=1, on_attempt=attempt, on_result=result, on_terminal_failure=failed,
                                 browser_warm_up=warm_up, event_observer=emit, work_provider=claim_next,
                                 worker_id=worker_id)
        await adapter.run(())
    except BaseException as exc:
        runner_error = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        if adapter:
            artifacts.record_browser_lifecycle(adapter.lifecycle_summary)
        repository.close()
        artifacts.record_persistence(repository.stats())
        finished = now()
        artifacts.finalize(finished_at=finished, runner_error=runner_error)
        # Canonical outcomes are not CrawlStatus enum values. Keep this worker's
        # benchmark report separate from the production review-only summary.
        write_json(worker_dir / "summary.json", dict(
            run_id=worker_run_id, worker_id=worker_id, started_at=config["benchmark_started_at"],
            finished_at=finished, runner_error=runner_error, persistence=repository.stats(),
            browser_lifecycle=adapter.lifecycle_summary if adapter else {},
            places=[read_json(worker_dir / "metadata" / f"{store.id}.json") for store in assigned_stores]))
        emit(dict(event="persistence_finalized", **repository.stats()))


def add_comparison(directory, metrics):
    comparable = []
    for path in directory.parent.glob("*/run_metrics.json"):
        row = json.loads(path.read_text(encoding="utf-8"))
        if all(row.get(k) == metrics.get(k) for k in (
            "sample_fingerprint", "configured_max_scrolls", "configured_timeout_seconds", "headless", "work_queue_mode",
            "request_queue_strategy", "no_progress_timeout_seconds")):
            comparable.append(row)
    if len(comparable) < 2:
        return
    text = "\n## Comparable runs (same frozen sample and limits)\n\n"
    text += "Concurrency | Avg active | Peak active | Utilization | Reviews/sec | Places/hour | Avg CPU | P95 CPU | Peak RAM MB | Retry % | LIMITED % | Challenge % | Crashes\n"
    text += "---|---|---|---|---|---|---|---|---|---|---|---|---\n"
    for row in sorted(comparable, key=lambda r: (r["configured_concurrency"], r["started_at"])):
        text += (f"{row['configured_concurrency']} | {row['avg_active_workers']:.3f} | {row['peak_active_workers']} | "
                 f"{row['worker_utilization']:.2%} | {row['reviews_per_second']:.3f} | {row['places_per_hour']:.2f} | "
                 f"{row['avg_cpu_percent']} | {row['p95_cpu_percent']} | {row['peak_ram_mb']} | "
                 f"{row['retry_rate']:.2%} | {row['limited_rate']:.2%} | {row['challenge_rate']:.2%} | {row['browser_crash_count']}\n")
    with (directory / "benchmark_summary.md").open("a", encoding="utf-8") as stream:
        stream.write(text)


def run_benchmark(places, directory, config):
    watchdog_seconds = config.get("no_progress_timeout_seconds", max(180, config["configured_timeout_seconds"] + 120))
    if watchdog_seconds <= 0:
        raise ValueError("no_progress_timeout_seconds must be positive")
    config = dict(config, work_queue_mode="shared_dynamic", request_queue_strategy="fresh_per_job",
                  no_progress_timeout_seconds=watchdog_seconds)
    metrics = BenchmarkMetrics(directory, directory.name, [(store_id(p), p.name) for p in places], config)
    context = multiprocessing.get_context("spawn")
    events = context.Queue()
    work_queue = context.Queue()
    for place in places:
        work_queue.put(place.to_dict())
    for _ in range(config["configured_concurrency"]):
        work_queue.put(None)
    sampler = ResourceSampler(metrics, interval=config["sampling_interval_seconds"])
    processes = []
    final = None
    try:
        sampler.start()
        for index in range(config["configured_concurrency"]):
            process = context.Process(target=worker_entry,
                                      args=(f"worker-{index + 1}", [p.to_dict() for p in places], str(directory), config, events, work_queue))
            process.start()
            processes.append(process)
        finished = set()
        last_progress = {f"worker-{i + 1}": time.monotonic() for i in range(len(processes))}
        current_jobs = {}

        def abort_worker(worker, reason):
            event = dict(worker_id=worker, store_id=current_jobs.get(worker), timestamp=now(),
                         queue_remaining=metrics.pending_count(), error_type="BenchmarkWorkerStalled", error_message=reason)
            metrics.event(dict(event, event="worker_stalled"))
            metrics.event(dict(event, event="worker_error"))
            terminate_worker(processes[int(worker.split("-")[-1]) - 1])
            finished.add(worker)
            metrics.event(dict(event, event="worker_finished"))

        while len(finished) < len(processes):
            for worker, last in list(last_progress.items()):
                if worker not in finished and time.monotonic() - last > watchdog_seconds:
                    abort_worker(worker, f"No job progress for {watchdog_seconds:g}s")
            try:
                event = events.get(timeout=0.5)
            except queue.Empty:
                for index, process in enumerate(processes):
                    worker = f"worker-{index + 1}"
                    if not process.is_alive() and worker not in finished:
                        finished.add(worker)
                        metrics.event(dict(event="worker_finished", worker_id=worker))
                        if process.exitcode:
                            metrics.event(dict(event="worker_error", worker_id=worker,
                                               error_type="WorkerProcessExit", error_message=f"exitcode={process.exitcode}"))
                continue
            # Parent-owned pending identity set is authoritative; qsize includes
            # sentinels and is not reliable on every multiprocessing platform.
            event["queue_remaining"] = metrics.pending_count() - int(
                event["event"] == "place_claimed" and event.get("store_id") in metrics.pending_places)
            metrics.event(event)
            worker = event["worker_id"]
            if event["event"] == "request_queue_corruption" and event.get("fatal") and worker not in finished:
                abort_worker(worker, "Repeated missing/invalid Crawlee request file: " + event["request_identity"])
            elif event["event"] != "request_queue_corruption":
                last_progress[worker] = time.monotonic()
            if event["event"] in {"place_claimed", "worker_claimed_job"}:
                current_jobs[worker] = event.get("store_id")
            elif event["event"] in {"place_released", "worker_released_job", "worker_idle"}:
                current_jobs[worker] = None
            if event["event"] == "worker_finished":
                finished.add(event["worker_id"])
        for process in processes:
            process.join()
        while True:
            try:
                metrics.event(events.get_nowait())
            except queue.Empty:
                break
    finally:
        # Interrupt/error path: give workers a chance to unwind, then end only
        # processes started by this benchmark. Never kill unrelated browsers.
        for process in processes:
            process.join(timeout=5)
            if process.is_alive():
                metrics.event(dict(event="worker_error", error_type="BenchmarkInterrupted",
                                   error_message="worker terminated during parent cleanup"))
                terminate_worker(process)
        sampler.stop()
        try:
            final = metrics.finish()
        finally:
            metrics.close()
            events.close()
            events.join_thread()
            work_queue.cancel_join_thread()
            work_queue.close()
        add_comparison(directory, final)
    return final


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sample-size", type=int, default=10)
    parser.add_argument("--concurrency", type=int, choices=(1, 2, 3, 4), default=1)
    parser.add_argument("--max-scrolls", type=int, default=10)
    parser.add_argument("--timeout-seconds", type=float, default=90)
    parser.add_argument("--no-progress-timeout-seconds", type=float,
                        help="Worker watchdog; default max(180, timeout-seconds + 120)")
    parser.add_argument("--sample-interval", type=int, choices=(2, 5), default=2)
    parser.add_argument("--sample-file", type=Path)
    parser.add_argument("--catalog", type=Path, default=DEFAULT_CATALOG)
    parser.add_argument("--raw-catalog", type=Path, help="Deprecated: prepare a canonical catalog explicitly first")
    parser.add_argument("--ward", type=Path, default=DEFAULT_WARD)
    parser.add_argument("--output-root", type=Path, default=ROOT / "output/crawler_benchmark")
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--prepare-only", action="store_true")
    args = parser.parse_args(argv)
    if args.raw_catalog:
        parser.error("Use scripts.prepare_place_catalog first, then --catalog; frozen sample files still load unchanged")
    if not 1 <= args.sample_size <= 50 or args.sample_size < args.concurrency:
        parser.error("sample-size must be 1..50 and at least concurrency")
    if args.max_scrolls <= 0 or args.timeout_seconds <= 0:
        parser.error("max-scrolls and timeout-seconds must be positive")
    if args.no_progress_timeout_seconds is not None and args.no_progress_timeout_seconds <= 0:
        parser.error("no-progress-timeout-seconds must be positive")
    sample_path = args.sample_file or args.output_root / "samples" / f"sample-{args.sample_size}.json"
    places, fingerprint = load_sample(sample_path, args.sample_size, args.catalog, args.ward)
    print(json.dumps(dict(sample_file=str(sample_path), sample_fingerprint=fingerprint,
                          selected=[dict(name=p.name, overture_id=p.source_place_id,
                                         group=p.product_category_group, confidence=p.confidence) for p in places]),
                     ensure_ascii=False, indent=2), flush=True)
    if args.prepare_only:
        print("PREPARED ONLY: no browsers or live requests started")
        return 0
    run_id = now().replace(":", "").replace("-", "").replace("+0000", "Z") + "-" + uuid.uuid4().hex[:8]
    directory = args.output_root / "runs" / run_id
    directory.mkdir(parents=True)
    write_json(directory / "selected_sample.json", dict(places=[p.to_dict() for p in places], sample_fingerprint=fingerprint))
    config = dict(configured_concurrency=args.concurrency, configured_max_scrolls=args.max_scrolls,
                  configured_timeout_seconds=args.timeout_seconds, sampling_interval_seconds=args.sample_interval,
                  sample_fingerprint=fingerprint, sample_size=len(places), headless=args.headless, benchmark_started_at=now(),
                  state_strategy="fresh benchmark-only state per run/worker; production state untouched")
    config["work_queue_mode"] = "shared_dynamic"
    if args.no_progress_timeout_seconds is not None:
        config["no_progress_timeout_seconds"] = args.no_progress_timeout_seconds
    metrics = run_benchmark(places, directory, config)
    print(json.dumps(metrics, indent=2), flush=True)
    return 1 if metrics["worker_errors"] else 0
