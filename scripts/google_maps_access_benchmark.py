"""Controlled, manual-only benchmark for guest access-state degradation."""

from __future__ import annotations

import argparse
import logging
import time
from typing import Any

from crawl_experiment.browser.browser_factory import BrowserFactory
from crawl_experiment.browser.session_manager import BrowserSessionManager
from crawl_experiment.core.errors import BrowserSessionError, ChallengeError
from crawl_experiment.observability.access_benchmark import AccessBenchmark
from crawl_experiment.observability.metrics import CrawlMetrics
from crawl_experiment.orchestration.retry_policy import RetryPolicy
from crawl_experiment.sources.google_maps.crawler import GoogleMapsCrawler
from crawl_experiment.sources.google_maps.navigator import GoogleMapsNavigator
from crawl_experiment.storage.checkpoint_repository import CheckpointRepository
from crawl_experiment.storage.review_repository import ReviewRepository
from crawl_experiment.storage.parquet_review_writer import ParquetReviewWriter
from crawl_experiment.observability.run_artifacts import (
    CrawlArtifacts, build_summary, utc_now, write_json,
)
from crawl_experiment.runners.full_crawl_runner import (
    DEFAULT_PARQUET_ROOT as PARQUET_ROOT,
    DEFAULT_STATE_DATABASE as STATE_DATABASE,
    configure_logging,
    create_run_directory,
)
from scripts.coles_small_set_full_crawl import STORES


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Controlled Google Maps guest access benchmark")
    parser.add_argument("--store-limit", type=int, default=3)
    parser.add_argument("--timeout-seconds", type=float, default=180)
    parser.add_argument("--max-scrolls-per-store", type=int, default=10)
    parser.add_argument("--stop-on-access-degradation", action="store_true")
    return parser


def run_benchmark(args: argparse.Namespace) -> tuple[Any, dict]:
    stores = STORES[: args.store_limit]
    run_directory = create_run_directory()
    logger = configure_logging(run_directory / "progress.log")
    started_at = utc_now()
    started = time.monotonic()
    artifacts = CrawlArtifacts(
        run_directory,
        started_at,
        stores=stores,
        state_database=STATE_DATABASE,
        parquet_root=PARQUET_ROOT,
        quarantine_max_scrolls=False,
    )
    benchmark = AccessBenchmark(
        run_directory / "access_state_timeline.jsonl", run_directory.name
    )
    reviews = ReviewRepository(
        STATE_DATABASE,
        run_id=run_directory.name,
        parquet_writer=ParquetReviewWriter(PARQUET_ROOT, run_directory.name, started_at[:10]),
    )
    manager = BrowserSessionManager(
        BrowserFactory(headless=False),
        warm_up=lambda driver: GoogleMapsNavigator(driver).warm_up(),
        logger=logging.getLogger("crawl_experiment.browser"),
    )
    crawler = GoogleMapsCrawler(
        reviews,
        CheckpointRepository(run_directory / "checkpoints"),
        CrawlMetrics(logger),
        max_scrolls=args.max_scrolls_per_store,
        timeout_seconds=args.timeout_seconds,
        run_id=run_directory.name,
        scroll_metrics_path=run_directory / "scroll_metrics.jsonl",
        review_id_trace_path=run_directory / "review_id_trace.jsonl",
        activity_observer=benchmark.observe,
    )
    runner_error = None
    retry_policy = RetryPolicy()
    try:
        for index, store in enumerate(stores, 1):
            try:
                managed = manager.acquire(store.id, 1)
                if benchmark.browser_instance_id != managed.instance_id:
                    benchmark.browser_created(managed.instance_id)
                    benchmark.warm_up_completed()
                benchmark.begin_store(store.id, index)
                artifacts.on_attempt(store, 1)
                result = crawler.crawl(store, managed.driver, warm_up=False)
                artifacts.on_result(store, result, 1)
                benchmark.complete_store()
            except ChallengeError as exc:
                benchmark.challenge_encountered = True
                benchmark.stop("CHALLENGE_DETECTED")
                runner_error = f"{type(exc).__name__}: {exc}"
                artifacts.on_terminal_failure(store, retry_policy.decide(exc, 0), 1, exc)
                break
            except BrowserSessionError as exc:
                benchmark.browser_lost("BROWSER_SESSION_LOST")
                runner_error = f"{type(exc).__name__}: {exc}"
                artifacts.on_terminal_failure(store, retry_policy.decide(exc, 0), 1, exc)
                break
            except Exception as exc:  # classified degradation is expected termination
                runner_error = f"{type(exc).__name__}: {exc}"
                artifacts.on_terminal_failure(store, retry_policy.decide(exc, 0), 1, exc)
                if benchmark.should_stop and args.stop_on_access_degradation:
                    benchmark.stop("ACCESS_DEGRADATION_DETECTED")
                    break
                logger.exception("Benchmark store failed")
                break
            if benchmark.should_stop and args.stop_on_access_degradation:
                benchmark.stop("ACCESS_DEGRADATION_DETECTED")
                break
        else:
            benchmark.stop("STORE_LIMIT_REACHED")
    finally:
        manager.close()
        reviews.close()
        artifacts.record_persistence(reviews.stats())
        lifecycle = manager.summary()
        artifacts.record_browser_lifecycle(lifecycle)
        finished_at = utc_now()
        summary = build_summary(
            run_directory,
            started_at=started_at,
            finished_at=finished_at,
            runner_error=runner_error,
            total_elapsed_seconds=time.monotonic() - started,
            stores=stores,
            timeout_seconds=args.timeout_seconds,
            max_scrolls=args.max_scrolls_per_store,
            concurrency=1,
        )
        summary["benchmark"] = benchmark.summary()
        summary["artifacts"]["access_state_timeline"] = str(
            run_directory / "access_state_timeline.jsonl"
        )
        write_json(run_directory / "summary.json", summary)
        artifacts.finalize(finished_at=finished_at, runner_error=runner_error)
    return run_directory, summary


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if not 1 <= args.store_limit <= len(STORES):
        raise SystemExit(f"store-limit must be between 1 and {len(STORES)}")
    if args.timeout_seconds <= 0 or args.max_scrolls_per_store <= 0:
        raise SystemExit("timeout and max scrolls must be positive")
    run_directory, _ = run_benchmark(args)
    print(f"Benchmark artifacts: {run_directory}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
