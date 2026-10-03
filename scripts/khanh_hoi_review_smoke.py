"""Bounded canonical Overture -> Google resolution -> existing review crawler.

Run with --prepare-only to inspect the deterministic sample without a browser.
Live execution writes progress and partial reports; no discovery grid is used.
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import os
import time
from collections import Counter
from pathlib import Path

from crawl_experiment.browser.browser_factory import BrowserFactory
from crawl_experiment.core.errors import LimitedReviewViewError
from crawl_experiment.core.models import CrawlResult, Store
from crawl_experiment.core.place_catalog import CanonicalPlace
from crawl_experiment.observability.metrics import CrawlMetrics
from crawl_experiment.observability.run_artifacts import CrawlArtifacts, read_json, utc_now, write_json
from crawl_experiment.orchestration.crawlee_adapter import CrawleeAdapter
from crawl_experiment.orchestration.retry_policy import SmokeRetryPolicy
from crawl_experiment.core.statuses import CrawlStatus
from crawl_experiment.runners.full_crawl_runner import configure_logging, create_run_directory
from crawl_experiment.sources.google_maps.crawler import GoogleMapsCrawler
from crawl_experiment.sources.google_maps.navigator import GoogleMapsNavigator
from crawl_experiment.sources.google_maps.place_resolution import (
    ResolvedPlaceDriver, assess_resolution, coordinate_distance, google_identity,
    normalized, resolve_place, store_id,
)
from crawl_experiment.storage.checkpoint_repository import CheckpointRepository
from crawl_experiment.storage.parquet_review_writer import ParquetReviewWriter
from crawl_experiment.storage.place_catalog import DEFAULT_CATALOG, DEFAULT_WARD, load_catalog
from crawl_experiment.storage.review_repository import ReviewRepository, review_content_hash

ROOT = Path(__file__).resolve().parents[1]
SELECTED_FIELDS = ["place_key", "name", "source_place_id", "lat", "lng", "address",
                   "primary_category", "product_category_group", "confidence"]
RESOLUTION_FIELDS = ["place_key", "overture_name", "google_name", "google_place_id",
                     "google_id_kind", "coordinate_distance_m", "name_similarity",
                     "address_similarity", "match_score", "status", "resolved_url",
                     "google_address", "elapsed_seconds", "error"]


def select_places(places, limit=5, minimum_confidence=0.8):
    if not 1 <= limit <= 5:
        raise ValueError("limit must be between 1 and 5")
    eligible = sorted(
        (p for p in places if p.name and p.confidence is not None and p.confidence >= minimum_confidence),
        key=lambda p: (-p.confidence, p.source_place_id),
    )
    chosen, names, identities = [], set(), set()

    def choose(candidates):
        for p in candidates:
            # Same normalized name is a conservative chain/duplicate-name guard.
            if normalized(p.name) not in names and p.place_key not in identities:
                chosen.append(p)
                names.add(normalized(p.name))
                identities.add(p.place_key)
                return

    for group in ("restaurant", "cafe_coffee", "bakery_dessert", "beverage")[:limit]:
        choose(p for p in eligible if p.product_category_group == group)
    if len(chosen) < limit:
        used_categories = {p.primary_category for p in chosen}
        choose(p for p in eligible if p.product_category_group in ("restaurant", "other_food", "fast_food")
               and p.primary_category not in used_categories)
    while len(chosen) < limit:
        previous = len(chosen)
        choose(eligible)
        if len(chosen) == previous:
            break
    return chosen[:limit]




def write_csv(path, rows, fields):
    temporary = path.with_suffix(".csv.tmp")
    with temporary.open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.DictWriter(stream, fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: json.dumps(value, ensure_ascii=False) if isinstance(value, list) else value
                             for key, value in row.items()})
    temporary.replace(path)


class InspectionReviewRepository(ReviewRepository):
    """Delegate every write unchanged; retain <=20 observed payloads per store.

    Inspection includes UNCHANGED reviews too; the Parquet sink still emits only
    INSERT/UPDATE, using the original repository implementation.
    """
    def __init__(self, *args, **kwargs):
        self.samples = {}
        super().__init__(*args, **kwargs)

    def upsert_review_state(self, review, observed_at=None):
        observed_at = observed_at or self.clock()
        change = super().upsert_review_state(review, observed_at)
        sample = self.samples.setdefault(review.store_id, {})
        if review.source_review_id in sample or len(sample) < 20:
            sample[review.source_review_id] = dict(
                run_id=self.run_id, source=review.source, source_review_id=review.source_review_id,
                store_id=review.store_id, reviewer_name=review.author, rating=review.rating,
                review_text=review.text, review_date_raw=review.displayed_date,
                owner_response=review.owner_response, review_url=review.review_url,
                image_urls=list(dict.fromkeys(url.strip() for url in review.image_urls if url.strip())),
                observed_at=observed_at, content_hash=review_content_hash(review), change_type=change.value,
            )
        return change

    def inspection_rows(self):
        return [row for sample in self.samples.values() for row in sample.values()]


async def execute(selected, output, available, timeout_seconds, max_scrolls):
    run_dir = create_run_directory(output / "runs")
    logger = configure_logging(run_dir / "progress.log")
    previous_storage_dir = os.environ.get("CRAWLEE_STORAGE_DIR")
    os.environ["CRAWLEE_STORAGE_DIR"] = str(run_dir / "crawlee_storage")
    started = utc_now()
    state_path = ROOT / "data/state/crawler_state.sqlite3"
    parquet_root = ROOT / "data/lake/google_maps_reviews"
    writer = ParquetReviewWriter(parquet_root, run_dir.name, started[:10], batch_size=50)
    stores = tuple(Store(id=store_id(p), name=p.name, query=p.name) for p in selected)
    places = {store_id(p): p for p in selected}
    artifacts = CrawlArtifacts(run_dir, started, stores=stores, state_database=state_path,
                               parquet_root=parquet_root, quarantine_max_scrolls=False,
                               runner_name="scripts.khanh_hoi_review_smoke")
    resolutions, outcomes = {}, {}
    repository = None
    adapter = None
    runner_error = None

    def report(error=None, finished=False):
        counts = Counter(r["status"] for r in resolutions.values())
        stats = repository.stats() if repository else {}
        summary = dict(mode="live", finished=finished, overture_pois_available=available,
                       selected=len(selected), resolved=counts["RESOLVED"], ambiguous=counts["AMBIGUOUS"],
                       not_found=counts["NOT_FOUND"], resolution_errors=counts["ERROR"],
                       crawled_successfully=sum(r["status"] in ("COMPLETE", "NO_REVIEWS", "PARTIAL_LIMIT", "PARTIAL_TIMEOUT")
                                                for r in outcomes.values()),
                       total_reviews_written=stats.get("new_reviews_written", 0) + stats.get("changed_reviews_written", 0),
                       persistence=stats, places=list(outcomes.values()),
                       elapsed_time_per_place={s.id: read_json(run_dir / "metadata" / f"{s.id}.json").get("store_elapsed_seconds",
                                               artifacts.elapsed_for_store(s.id) if s.id in resolutions else None) for s in stores},
                       limits={"timeout_seconds": timeout_seconds, "max_scrolls": max_scrolls, "inspection_reviews_per_place": 20},
                       run_directory=str(run_dir), runner_error=error)
        write_csv(output / "google_resolution.csv", resolutions.values(), RESOLUTION_FIELDS)
        write_csv(output / "reviews_sample.csv", repository.inspection_rows() if repository else [], list(writer.COLUMNS))
        write_json(output / "summary.json", summary)
        return summary

    try:
        repository = InspectionReviewRepository(state_path, run_id=run_dir.name, parquet_writer=writer)
        crawler = GoogleMapsCrawler(repository, CheckpointRepository(run_dir / "checkpoints"), CrawlMetrics(logger),
                                    max_scrolls=max_scrolls, timeout_seconds=timeout_seconds, run_id=run_dir.name,
                                    scroll_metrics_path=run_dir / "scroll_metrics.jsonl",
                                    review_id_trace_path=run_dir / "review_id_trace.jsonl", activity_observer=artifacts.on_activity)

        def crawl_one(store, driver):
            start = time.monotonic()
            try:
                resolution = resolve_place(places[store.id], driver)
            except Exception as exc:
                resolutions[store.id] = dict(place_key=places[store.id].place_key, overture_name=store.name,
                                              status="ERROR", error=f"{type(exc).__name__}: {str(exc).splitlines()[0]}")
                report()
                raise  # Existing adapter owns retries/session retirement/DLQ.
            resolution["elapsed_seconds"] = time.monotonic() - start
            if resolution["status"] == "RESOLVED" and any(
                other_id != store.id and row.get("google_place_id") == resolution["google_place_id"]
                and row["status"] == "RESOLVED" for other_id, row in resolutions.items()
            ):
                resolution.update(status="AMBIGUOUS", error="Google identity already matched to another selected POI")
            resolutions[store.id] = resolution
            report()
            if resolution["status"] in {"AMBIGUOUS", "NOT_FOUND"}:
                logger.info("Canonical resolution terminal store_id=%s status=%s; skipping reviews",
                            store.id, resolution["status"])
                # Return normally so Crawlee completes the request without
                # consulting retry policy. This is not a review crawl failure.
                return CrawlResult(store.id, CrawlStatus.PLACE_RESOLUTION_FAILED,
                                   stop_reason=resolution["status"])
            resolved_store = Store(id=store.id, name=resolution["google_name"], query=store.query,
                                   expected_address=resolution["google_address"])
            try:
                return crawler.crawl(resolved_store, ResolvedPlaceDriver(driver, resolved_store, resolution),
                                     sort_newest=True, warm_up=False,
                                     elapsed_offset_seconds=artifacts.elapsed_for_store(store.id))
            except LimitedReviewViewError as exc:
                # Tag only this runner's review-surface failure. The adapter
                # retires the old browser and invokes resolution again on retry.
                exc.smoke_store_id = store.id
                raise

        def warm_up(driver):
            driver.set_page_load_timeout(30)
            GoogleMapsNavigator(driver).warm_up()
            crawler.mark_browser_warm_up_completed()

        def on_result(store, result, attempt):
            if result.stop_reason in {"AMBIGUOUS", "NOT_FOUND"}:
                status = result.stop_reason
                path = run_dir / "metadata" / f"{store.id}.json"
                write_json(path, {**read_json(path), "status": status, "final_status": status,
                                  "terminal": True, "dlq": False, "review_crawling_skipped": True,
                                  "stop_reason": status, "attempts": attempt, "attempts_started": attempt,
                                  "store_finished_at": utc_now(),
                                  "store_elapsed_seconds": round(artifacts.elapsed_for_store(store.id), 3),
                                  "reviews_seen": 0, "reviews_written": 0})
                outcomes[store.id] = dict(store_id=store.id, name=store.name, status=status,
                                         review_crawling_skipped=True, reviews_seen=0, reviews_written=0)
                report()
                return
            artifacts.on_result(store, result, attempt)
            outcomes[store.id] = dict(store_id=store.id, name=store.name, status=str(result.status),
                                     reviews_seen=result.reviews_seen, reviews_written=result.reviews_written)
            repository.flush()
            report()

        def on_failure(store, decision, attempt, exc):
            artifacts.on_terminal_failure(store, decision, attempt, exc)
            outcomes[store.id] = dict(store_id=store.id, name=store.name, status=str(decision.status), error=str(exc).splitlines()[0])
            report()

        adapter = CrawleeAdapter(BrowserFactory(headless=False), crawl_one,
                                 retry_policy=SmokeRetryPolicy(),
                                 browser_warm_up=warm_up, on_attempt=artifacts.on_attempt,
                                 on_result=on_result, on_terminal_failure=on_failure, max_concurrency=1)
        report()
        await adapter.run(stores)
    except BaseException as exc:
        runner_error = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        if adapter:
            artifacts.record_browser_lifecycle(adapter.lifecycle_summary)
        if repository:
            repository.close()
            artifacts.record_persistence(repository.stats())
        artifacts.finalize(finished_at=utc_now(), runner_error=runner_error)
        summary = report(error=runner_error, finished=True)
        write_json(run_dir / "summary.json", summary)
        if previous_storage_dir is None:
            os.environ.pop("CRAWLEE_STORAGE_DIR", None)
        else:
            os.environ["CRAWLEE_STORAGE_DIR"] = previous_storage_dir
    return report(finished=True)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", type=Path, default=DEFAULT_CATALOG)
    parser.add_argument("--raw-catalog", type=Path, help="Deprecated: prepare a canonical catalog explicitly first")
    parser.add_argument("--ward", type=Path, default=DEFAULT_WARD)
    parser.add_argument("--output", type=Path, default=ROOT / "output/smoke_test")
    parser.add_argument("--prepare-only", action="store_true")
    args = parser.parse_args(argv)
    if args.raw_catalog:
        parser.error("Use scripts.prepare_place_catalog --source <raw> first, then --catalog <canonical>")
    catalog = load_catalog(args.catalog)
    selected = select_places(catalog.places)
    if len(selected) != 5:
        parser.error("expected five distinct high-confidence places; no live crawl started")
    args.output.mkdir(parents=True, exist_ok=True)
    rows = [p.to_dict() for p in selected]
    write_csv(args.output / "selected_places.csv", rows, SELECTED_FIELDS)
    print(json.dumps([{field: r[field] for field in SELECTED_FIELDS} for r in rows], ensure_ascii=False, indent=2), flush=True)
    if args.prepare_only:
        write_json(args.output / "selection_metadata.json", dict(
            mode="prepare_only", overture_pois_available=len(catalog.places), selected=len(selected),
            canonical_catalog=str(args.catalog), ward=str(args.ward), observed_at=utc_now()))
        print(f"PREPARED ONLY: {len(catalog.places)} available, {len(selected)} selected. No browser started.")
        return 0
    summary = asyncio.run(execute(selected, args.output, len(catalog.places), timeout_seconds=90, max_scrolls=5))
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 1 if summary["runner_error"] or summary["resolution_errors"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
