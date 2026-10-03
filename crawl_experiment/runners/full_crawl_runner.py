from __future__ import annotations

import asyncio
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from crawl_experiment.browser.browser_factory import BrowserFactory
from crawl_experiment.core.models import Store
from crawl_experiment.observability.metrics import CrawlMetrics
from crawl_experiment.observability.run_artifacts import (
    CrawlArtifacts,
    build_summary,
    utc_now,
    write_json,
)
from crawl_experiment.orchestration.crawlee_adapter import CrawleeAdapter
from crawl_experiment.sources.google_maps.crawler import GoogleMapsCrawler
from crawl_experiment.sources.google_maps.navigator import GoogleMapsNavigator
from crawl_experiment.storage.checkpoint_repository import CheckpointRepository
from crawl_experiment.storage.parquet_review_writer import ParquetReviewWriter
from crawl_experiment.storage.review_repository import ReviewRepository
from crawl_experiment.storage.place_access_repository import PlaceAccessRepository
from crawl_experiment.orchestration.access_policy import AccessPolicy, AccessPolicyConfig
from crawl_experiment.orchestration.review_incremental import IncrementalConfig
from crawl_experiment.orchestration.review_reconciliation import ReconciliationConfig


DEFAULT_OUTPUT_ROOT = Path("data/full_crawl/coles_small_set")
DEFAULT_TIMEOUT_SECONDS = 7_200
DEFAULT_MAX_SCROLLS = 5_000
DEFAULT_CONCURRENCY = 1
DEFAULT_STATE_DATABASE = Path("data/state/crawler_state.sqlite3")
DEFAULT_PARQUET_ROOT = Path("data/lake/google_maps_reviews")


def create_run_directory(output_root: Path = DEFAULT_OUTPUT_ROOT) -> Path:
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    run_directory = output_root / run_id
    run_directory.mkdir(parents=True, exist_ok=False)
    (run_directory / "checkpoints").mkdir()
    (run_directory / "metadata").mkdir()
    return run_directory


def configure_logging(progress_log: Path) -> logging.Logger:
    logger = logging.getLogger("full_crawl")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
    file_handler = logging.FileHandler(progress_log, encoding="utf-8")
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    logger.propagate = False

    for name in ("crawl", "crawl_experiment", "crawlee"):
        child = logging.getLogger(name)
        child.setLevel(logging.INFO)
        child.handlers.clear()
        child.addHandler(file_handler)
        child.addHandler(stream_handler)
        child.propagate = False
    return logger


async def _run_crawl(
    run_directory: Path,
    logger: logging.Logger,
    artifacts: CrawlArtifacts,
    *,
    started_at: str,
    stores: tuple[Store, ...],
    state_database: Path,
    parquet_root: Path,
    timeout_seconds: float,
    max_scrolls: int,
    concurrency: int,
    access_policy_config: AccessPolicyConfig | None = None,
    crawl_mode: str = 'REVIEW_FULL',
    incremental_config: IncrementalConfig | None = None,
    reconciliation_config: ReconciliationConfig | None = None,
) -> None:
    os.environ["CRAWLEE_STORAGE_DIR"] = str(run_directory / "crawlee_storage")
    writer = ParquetReviewWriter(parquet_root, run_directory.name, started_at[:10])
    reviews = ReviewRepository(state_database, run_id=run_directory.name, parquet_writer=writer)
    access_repository=PlaceAccessRepository(state_database)
    access_policy=AccessPolicy(access_repository,access_policy_config)
    incremental_results={}
    try:
        def activity(event,details):
            artifacts.on_activity(event,details)
            if event=='incremental_finished':
                incremental_results[details['store_id']]={k:v for k,v in details.items() if k!='store_id'}
            if event=='review_surface_classified' and details.get('access_state')=='FULL':
                access_policy.full(details['store_id'])
        crawler = GoogleMapsCrawler(
            reviews,
            CheckpointRepository(run_directory / "checkpoints"),
            CrawlMetrics(logger),
            max_scrolls=max_scrolls,
            timeout_seconds=timeout_seconds,
            run_id=run_directory.name,
            scroll_metrics_path=run_directory / "scroll_metrics.jsonl",
            review_id_trace_path=run_directory / "review_id_trace.jsonl",
            activity_observer=activity,
        )

        def crawl_newest(store: Store, driver: Any):
            result=crawler.crawl(
                store,
                driver,
                sort_newest=True,
                elapsed_offset_seconds=artifacts.elapsed_for_store(store.id),
                warm_up=False,
                **({'incremental_config':incremental_config or IncrementalConfig()} if crawl_mode=='REVIEW_INCREMENTAL' else {}),
                **({'reconciliation_config':reconciliation_config or ReconciliationConfig()} if crawl_mode=='REVIEW_RECONCILIATION' else {}),
            )
            if crawl_mode=='REVIEW_INCREMENTAL':
                incremental_results[store.id]=result.incremental_metrics
            if crawl_mode=='REVIEW_RECONCILIATION':
                incremental_results[store.id]=result.reconciliation_metrics
                write_json(run_directory / f"reconciliation_decisions_{store.id}.json",
                           {"store_id": store.id, "decisions": result.reconciliation_metrics.get("decisions", [])})
            return result

        def deferred(store,metrics):
            artifacts.on_access_deferred(store,metrics)
            if crawl_mode in ('REVIEW_INCREMENTAL','REVIEW_RECONCILIATION'):
                incremental_results[store.id]=dict(new_reviews=0,updated_reviews=0,known_unchanged_reviews=0,
                    reviews_examined=0,streak_at_stop=0,stop_reason='ACCESS_DEFERRED',scroll_count=0,elapsed_seconds=0)

        def warm_up_browser(driver: Any) -> None:
            GoogleMapsNavigator(driver).warm_up()
            crawler.mark_browser_warm_up_completed()

        adapter = CrawleeAdapter(
            BrowserFactory(headless=False),
            crawl_newest,
            max_concurrency=concurrency,
            on_attempt=artifacts.on_attempt,
            on_result=artifacts.on_result,
            on_terminal_failure=artifacts.on_terminal_failure,
            browser_warm_up=warm_up_browser,
            access_policy=access_policy,
            on_deferred=deferred,
        )
        logger.info(
            "Starting full crawl for %d stores with concurrency=%d",
            len(stores),
            concurrency,
        )
        try:
            await adapter.run(stores)
            logger.info("Crawlee finished all queued stores")
        finally:
            artifacts.record_browser_lifecycle(adapter.lifecycle_summary)
            write_json(run_directory/'access_job_metrics.json',getattr(adapter,'access_job_metrics',[]))
            if crawl_mode in ('REVIEW_INCREMENTAL','REVIEW_RECONCILIATION'):
                write_json(run_directory/'incremental_metrics.json',incremental_results)
    finally:
        access_repository.close()
        reviews.close()
        artifacts.record_persistence(reviews.stats())


def run_full_crawl(
    stores: Iterable[Store],
    *,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    state_database: Path = DEFAULT_STATE_DATABASE,
    parquet_root: Path = DEFAULT_PARQUET_ROOT,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
    max_scrolls: int = DEFAULT_MAX_SCROLLS,
    concurrency: int = DEFAULT_CONCURRENCY,
    validation_mode: bool = False,
    runner_name: str = "scripts.coles_small_set_full_crawl",
    access_policy_config: AccessPolicyConfig | None = None,
    crawl_mode: str = 'REVIEW_FULL',
    incremental_config: IncrementalConfig | None = None,
    reconciliation_config: ReconciliationConfig | None = None,
) -> int:
    stores = tuple(stores)
    if crawl_mode not in ('REVIEW_FULL','REVIEW_INCREMENTAL','REVIEW_RECONCILIATION'):
        raise ValueError('invalid crawl_mode')
    if not stores:
        raise ValueError("stores must not be empty")
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")
    if max_scrolls <= 0:
        raise ValueError("max_scrolls must be positive")
    if concurrency <= 0:
        raise ValueError("concurrency must be positive")
    run_directory = create_run_directory(output_root)
    logger = configure_logging(run_directory / "progress.log")
    started_at = utc_now()
    run_started_monotonic = time.monotonic()
    artifacts = CrawlArtifacts(
        run_directory,
        started_at,
        stores=stores,
        state_database=state_database,
        parquet_root=parquet_root,
        concurrency=concurrency,
        runner_name=runner_name,
        quarantine_max_scrolls=not validation_mode,
    )
    runner_error = None
    exit_code = 0
    try:
        asyncio.run(
            _run_crawl(
                run_directory,
                logger,
                artifacts,
                started_at=started_at,
                stores=stores,
                state_database=state_database,
                parquet_root=parquet_root,
                timeout_seconds=timeout_seconds,
                max_scrolls=max_scrolls,
                concurrency=concurrency,
                access_policy_config=access_policy_config,
                crawl_mode=crawl_mode,
                incremental_config=incremental_config,
                reconciliation_config=reconciliation_config,
            )
        )
    except Exception as exc:  # noqa: BLE001 - final artifact must survive runner failure
        runner_error = f"{type(exc).__name__}: {exc}"
        logger.exception("Full-crawl runner failed")
        exit_code = 1
    finally:
        finished_at = utc_now()
        summary = build_summary(
            run_directory,
            started_at=started_at,
            finished_at=finished_at,
            runner_error=runner_error,
            total_elapsed_seconds=time.monotonic() - run_started_monotonic,
            stores=stores,
            timeout_seconds=timeout_seconds,
            max_scrolls=max_scrolls,
            concurrency=concurrency,
        )
        write_json(run_directory / "summary.json", summary)
        artifacts.finalize(finished_at=finished_at, runner_error=runner_error)
        logger.info("Summary: %s", run_directory / "summary.json")
        logger.info("State database: %s", state_database)
        logger.info("Parquet output: %s", summary["parquet_output_path"])
    return exit_code
