from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from crawl_experiment.core.failure_taxonomy import classify_exception, classify_status
from crawl_experiment.core.models import Store
from crawl_experiment.core.statuses import CrawlStatus
from crawl_experiment.orchestration.retry_policy import RetryAction, RetryDecision


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def write_json(path: Path, value: dict[str, Any]) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, default=str) + "\n", encoding="utf-8")
    temporary.replace(path)


def read_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def read_checkpoint(run_directory: Path, store_id: str) -> dict[str, Any]:
    return read_json(run_directory / "checkpoints" / f"{store_id}.json")


def append_json_line(path: Path, value: dict[str, Any]) -> None:
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(value, default=str, sort_keys=True) + "\n")
        stream.flush()


class CrawlArtifacts:
    def __init__(
        self,
        run_directory: Path,
        started_at: str,
        *,
        stores: Iterable[Store] = (),
        state_database: Path = Path("data/state/crawler_state.sqlite3"),
        parquet_root: Path = Path("data/lake/google_maps_reviews"),
        concurrency: int = 1,
        runner_name: str = "scripts.coles_small_set_full_crawl",
        clock=time.monotonic,
        wall_clock=utc_now,
        quarantine_max_scrolls: bool = True,
    ):
        self.run_directory = run_directory
        self.started_at = started_at
        self.state_database = state_database
        self.parquet_root = parquet_root
        self.clock = clock
        self.wall_clock = wall_clock
        self.stores = tuple(stores)
        self.quarantine_max_scrolls = quarantine_max_scrolls
        self._store_starts: dict[str, float] = {}
        self._dlq_keys: set[tuple[str, str, str, str]] = set()
        self.dlq_path = run_directory / "dlq.jsonl"
        self.dlq_path.touch(exist_ok=True)
        (run_directory / "scroll_metrics.jsonl").touch(exist_ok=True)
        (run_directory / "review_id_trace.jsonl").touch(exist_ok=True)
        write_json(
            run_directory / "run_metadata.json",
            {
                "run_id": run_directory.name,
                "started_at": started_at,
                "source": "google_maps",
                "runner": runner_name,
                "store_count": len(self.stores),
                "store_ids": [store.id for store in self.stores],
                "concurrency": concurrency,
                "sort": "newest",
                "state_db_path": str(state_database),
                "parquet_output_path": str(
                    parquet_root / f"crawl_date={started_at[:10]}" / f"run_id={run_directory.name}"
                ),
                "checkpoints": str(run_directory / "checkpoints"),
                "dlq": str(self.dlq_path),
                "scroll_metrics": str(run_directory / "scroll_metrics.jsonl"),
                "review_id_trace": str(run_directory / "review_id_trace.jsonl"),
            },
        )
        for store in self.stores:
            self._write_store(store, status="QUEUED", attempts_started=0, queued_at=started_at)

    def _path(self, store: Store) -> Path:
        return self.run_directory / "metadata" / f"{store.id}.json"

    def _write_store(self, store: Store, **updates: Any) -> None:
        path = self._path(store)
        value = {**asdict(store), **read_json(path), **updates}
        value["metadata_path"] = str(path)
        value["checkpoint_path"] = str(
            self.run_directory / "checkpoints" / f"{store.id}.json"
        )
        write_json(path, value)

    def on_attempt(self, store: Store, attempt_number: int) -> None:
        self._store_starts.setdefault(store.id, self.clock())
        current = read_json(self._path(store))
        store_started_at = current.get("store_started_at") or self.wall_clock()
        self._write_store(
            store,
            status="RUNNING",
            attempts=attempt_number,
            attempts_started=attempt_number,
            store_started_at=store_started_at,
            last_attempt_started_at=self.wall_clock(),
        )

    def elapsed_for_store(self, store_id: str) -> float:
        now = self.clock()
        return max(0.0, now - self._store_starts.get(store_id, now))

    def on_activity(self, event: str, details: dict[str, Any]) -> None:
        if event not in {
            "search_phase_timing", "place_resolution", "review_access_after_entity_open"
        }:
            return
        store_id = details.get("store_id")
        store = next((item for item in self.stores if item.id == store_id), None)
        if store is not None:
            self._write_store(store, **details)

    def on_result(self, store: Store, result: Any, attempt_number: int) -> None:
        timing = self._finish_timing(store)
        classification = classify_status(result.status)
        validation_limit_reached = result.status is CrawlStatus.PARTIAL_LIMIT
        self._write_store(
            store,
            status=str(result.status), final_status=str(result.status),
            failure_stage=result.failure_stage,
            failure_category=classification.category if classification else None,
            failure_reason_code=classification.reason_code if classification else None,
            attempts_started=attempt_number, attempts=attempt_number, terminal=True,
            dlq=classification is not None and not validation_limit_reached,
            validation_limit_reached=validation_limit_reached,
            reviews_seen=result.reviews_seen, reviews_written=result.reviews_written,
            scroll_count=result.scroll_count, stop_reason=result.stop_reason,
            configured_max_scrolls=result.configured_max_scrolls,
            parse_errors=result.parse_errors, **timing,
        )
        if classification and not validation_limit_reached:
            self._emit_dlq(
                store, status=str(result.status), failure_stage=result.failure_stage,
                classification=classification, error=result.error, attempts=attempt_number,
                reviews_seen=result.reviews_seen,
                elapsed_seconds=timing["store_elapsed_seconds"],
                started_at=timing["store_started_at"],
                finished_at=timing["store_finished_at"],
                retryable=classification.retryable,
            )

    def on_access_deferred(self, store: Store, metrics: dict) -> None:
        """Deferred access is scheduler state, not a terminal-error DLQ entry."""
        timing=self._finish_timing(store)
        self._write_store(store,status=metrics['job_status'],final_status=metrics['job_status'],
            terminal=True,dlq=False,**metrics,**timing)

    def on_terminal_failure(
        self, store: Store, decision: RetryDecision, attempt_number: int, error: Exception
    ) -> None:
        timing = self._finish_timing(store)
        classification = classify_exception(error)
        concise_error = str(error).splitlines()[0]
        checkpoint = read_checkpoint(self.run_directory, store.id)
        self._emit_dlq(
            store, status=str(decision.status), failure_stage=checkpoint.get("failure_stage"),
            classification=classification,
            error=f"{type(error).__name__}: {concise_error}", attempts=attempt_number,
            reviews_seen=checkpoint.get("reviews_seen", 0),
            elapsed_seconds=timing["store_elapsed_seconds"],
            started_at=timing["store_started_at"],
            finished_at=timing["store_finished_at"],
            retryable=classification.retryable and decision.action is not RetryAction.STOP,
        )
        self._write_store(
            store, status=str(decision.status), final_status=str(decision.status),
            attempts_started=attempt_number, attempts=attempt_number, terminal=True, dlq=True,
            retry_action=str(decision.action), failure_stage=checkpoint.get("failure_stage"),
            failure_category=classification.category,
            failure_reason_code=classification.reason_code,
            error_type=type(error).__name__, error=concise_error,
            reviews_seen=checkpoint.get("reviews_seen", 0),
            scroll_count=checkpoint.get("scroll_count", 0), **timing,
        )

    def _finish_timing(self, store: Store) -> dict[str, Any]:
        metadata = read_json(self._path(store))
        now = self.clock()
        started = self._store_starts.get(store.id, now)
        elapsed = max(0.0, now - started)
        return {
            "store_started_at": metadata.get("store_started_at") or self.started_at,
            "store_finished_at": self.wall_clock(),
            "store_elapsed_seconds": round(elapsed, 3),
            "store_elapsed_minutes": round(elapsed / 60, 3),
        }

    def _persisted_for_store(self, store_id: str) -> int:
        database = Path(
            read_json(self.run_directory / "run_metadata.json").get(
                "state_db_path", self.state_database
            )
        )
        if not database.exists():
            return 0
        with sqlite3.connect(database) as connection:
            row = connection.execute(
                "SELECT COUNT(*) FROM review_state WHERE source=? AND store_id=?",
                ("google_maps", store_id),
            ).fetchone()
        return int(row[0])

    def _emit_dlq(
        self, store: Store, *, status: str, failure_stage: str | None,
        classification: Any, error: str | None, attempts: int, reviews_seen: int,
        elapsed_seconds: float, started_at: str, finished_at: str, retryable: bool,
    ) -> None:
        key = (self.run_directory.name, store.id, status, classification.reason_code)
        if key in self._dlq_keys:
            return
        self._dlq_keys.add(key)
        append_json_line(
            self.dlq_path,
            {
                "run_id": self.run_directory.name, "store_id": store.id,
                "store_name": store.name, "query": store.query, "retailer": store.retailer,
                "status": status, "final_status": status, "failure_stage": failure_stage,
                "failure_category": classification.category,
                "failure_reason_code": classification.reason_code, "error": error,
                "attempts": attempts, "retryable": retryable,
                "reviews_seen": reviews_seen,
                "reviews_persisted": self._persisted_for_store(store.id),
                "checkpoint_path": str(
                    self.run_directory / "checkpoints" / f"{store.id}.json"
                ),
                "started_at": started_at, "finished_at": finished_at,
                "elapsed_seconds": elapsed_seconds,
            },
        )

    def finalize(self, *, finished_at: str, runner_error: str | None) -> None:
        path = self.run_directory / "run_metadata.json"
        write_json(
            path,
            {**read_json(path), "finished_at": finished_at, "runner_error": runner_error,
             "summary": str(self.run_directory / "summary.json")},
        )

    def record_browser_lifecycle(self, summary: dict[str, Any]) -> None:
        path = self.run_directory / "run_metadata.json"
        write_json(path, {**read_json(path), **summary})

    def record_persistence(self, summary: dict[str, Any]) -> None:
        path = self.run_directory / "run_metadata.json"
        write_json(path, {**read_json(path), **summary})


def persisted_counts(database: Path, stores: Iterable[Store]) -> tuple[int, dict[str, int]]:
    stores = tuple(stores)
    counts = {store.id: 0 for store in stores}
    if not database.exists():
        return 0, counts
    with sqlite3.connect(database) as connection:
        rows = connection.execute(
            """SELECT store_id, COUNT(*) FROM review_state
               WHERE source = 'google_maps' GROUP BY store_id"""
        ).fetchall()
    for store_id, count in rows:
        if store_id in counts:
            counts[store_id] = int(count)
    return sum(counts.values()), counts


def build_summary(
    run_directory: Path, *, started_at: str, finished_at: str,
    stores: Iterable[Store], timeout_seconds: float, max_scrolls: int,
    concurrency: int = 1, runner_error: str | None = None,
    total_elapsed_seconds: float = 0.0,
) -> dict[str, Any]:
    lifecycle = read_json(run_directory / "run_metadata.json")
    database = Path(lifecycle.get("state_db_path", run_directory / "reviews.sqlite3"))
    stores = tuple(stores)
    total_reviews, counts = persisted_counts(database, stores)
    store_summaries = []
    for store in stores:
        checkpoint = read_checkpoint(run_directory, store.id)
        metadata = read_json(run_directory / "metadata" / f"{store.id}.json")
        status = checkpoint.get("status", metadata.get("final_status", CrawlStatus.ERROR))
        classification = classify_status(status)
        elapsed = float(
            metadata.get("store_elapsed_seconds", checkpoint.get("elapsed_seconds", 0.0))
        )
        persisted = counts[store.id]
        store_summaries.append(
            {
                **asdict(store), "status": status,
                "failure_stage": checkpoint.get("failure_stage"),
                "failure_category": metadata.get(
                    "failure_category", classification.category if classification else None
                ),
                "failure_reason_code": metadata.get(
                    "failure_reason_code", classification.reason_code if classification else None
                ),
                "reviews_seen": checkpoint.get("reviews_seen", 0),
                "reviews_persisted": persisted, "stop_reason": metadata.get("stop_reason"),
                "configured_max_scrolls": metadata.get("configured_max_scrolls", max_scrolls),
                "scroll_count": checkpoint.get("scroll_count", 0),
                "started_at": metadata.get("store_started_at", checkpoint.get("started_at")),
                "finished_at": metadata.get("store_finished_at", checkpoint.get("finished_at")),
                "store_started_at": metadata.get("store_started_at", checkpoint.get("started_at")),
                "store_finished_at": metadata.get("store_finished_at", checkpoint.get("finished_at")),
                "store_elapsed_seconds": round(elapsed, 3),
                "store_elapsed_minutes": round(elapsed / 60, 3),
                "reviews_per_minute": round(persisted / (elapsed / 60), 3) if elapsed else 0.0,
                "avg_seconds_per_review": round(elapsed / persisted, 3) if persisted else 0.0,
                "attempts": metadata.get("attempts", metadata.get("attempts_started", 0)),
                "warmup_to_search_ms": metadata.get("warmup_to_search_ms"),
                "search_to_resolved_ms": metadata.get("search_to_resolved_ms"),
                "resolved_to_reviews_ms": metadata.get("resolved_to_reviews_ms"),
                "reviews_to_classification_ms": metadata.get("reviews_to_classification_ms"),
                "final_access_state": metadata.get("final_access_state"),
                "initial_url": metadata.get("initial_url"),
                "navigation_state": metadata.get("navigation_state"),
                "search_candidate_count": metadata.get("search_candidate_count"),
                "selected_candidate": metadata.get("selected_candidate"),
                "resolved_place_title": metadata.get("resolved_place_title"),
                "resolved_place_address": metadata.get("resolved_place_address"),
                "resolved_url": metadata.get("resolved_url"),
                "place_entity_confirmed": metadata.get("place_entity_confirmed", False),
                "access_state_after_place_resolution": metadata.get(
                    "access_state_after_place_resolution"
                ),
                "checkpoint": str(run_directory / "checkpoints" / f"{store.id}.json"),
            }
        )
    success_statuses = {CrawlStatus.COMPLETE, CrawlStatus.NO_REVIEWS, CrawlStatus.SUCCESS_DOM,
                        CrawlStatus.SUCCESS_NETWORK, CrawlStatus.SUCCESS_HYBRID}
    deferred_statuses = {CrawlStatus.DEFERRED_LIMITED, CrawlStatus.DEFERRED_UNKNOWN}
    complete_count = sum(item["status"] in success_statuses for item in store_summaries)
    partial_count = sum(item["status"] == CrawlStatus.PARTIAL_TIMEOUT for item in store_summaries)
    partial_limit_count = sum(item["status"] == CrawlStatus.PARTIAL_LIMIT for item in store_summaries)
    failed_ids = [
        item["id"] for item in store_summaries
        if item["status"] not in success_statuses | deferred_statuses |
           {CrawlStatus.PARTIAL_TIMEOUT, CrawlStatus.PARTIAL_LIMIT}
    ]
    dlq_path = run_directory / "dlq.jsonl"
    dlq_count = sum(1 for line in dlq_path.read_text(encoding="utf-8").splitlines() if line.strip()) if dlq_path.exists() else 0
    total_elapsed_seconds = max(0.0, total_elapsed_seconds)
    reconciliation_metrics = {}
    metrics_path = run_directory / "incremental_metrics.json"
    if metrics_path.exists():
        raw_metrics = read_json(metrics_path)
        for value in raw_metrics.values():
            if isinstance(value, dict) and "reconciliation_reviews_examined" in value:
                for key in (
                    "reconciliation_reviews_examined", "reconciliation_window_size",
                    "expected_reviews_in_window", "observed_reviews_in_window",
                    "missed_new_reviews", "updated_reviews", "verified_unchanged_reviews",
                    "missing_reviews", "possibly_deleted_reviews", "reactivated_reviews",
                    "miss_count_incremented", "parquet_rows_written",
                    "durable_review_count_total", "stop_reason",
                ):
                    if key in value:
                        reconciliation_metrics[key] = reconciliation_metrics.get(key, 0) + value[key] if isinstance(value[key], (int, float)) else value[key]
                reconciliation_metrics["reconciliation_decisions"] = reconciliation_metrics.get("reconciliation_decisions", 0) + len(value.get("decisions", []))
    return {
        "run_id": run_directory.name, "started_at": started_at, "finished_at": finished_at,
        "runner_error": runner_error,
        "total_elapsed_seconds": round(total_elapsed_seconds, 3),
        "total_elapsed_minutes": round(total_elapsed_seconds / 60, 3),
        "configuration": {
            "store_count": len(stores), "estimated_reviews": None,
            "concurrency": concurrency, "sort": "newest",
            "crawler_timeout_seconds_per_store": timeout_seconds,
            "max_scrolls_per_store": max_scrolls,
            "deduplication_key": ["source", "source_review_id"],
            "network": "direct", "captcha_solving": False,
        },
        "total_reviews_persisted": total_reviews, "state_db_path": str(database),
        "parquet_output_path": lifecycle.get("parquet_output_path"),
        "parquet_files_written": lifecycle.get("parquet_files_written", 0),
        "parquet_rows_written": lifecycle.get("parquet_rows_written", 0),
        "new_reviews_written": lifecycle.get("new_reviews_written", 0),
        "changed_reviews_written": lifecycle.get("changed_reviews_written", 0),
        "unchanged_reviews_seen": lifecycle.get("unchanged_reviews_seen", 0),
        **reconciliation_metrics,
        "total_reviews_per_minute": round(total_reviews / (total_elapsed_seconds / 60), 3) if total_elapsed_seconds else 0.0,
        "complete_store_count": complete_count, "partial_timeout_count": partial_count,
        "partial_limit_count": partial_limit_count, "failed_store_count": len(failed_ids),
        "dlq_count": dlq_count, "failed_store_ids": failed_ids,
        "browsers_created": lifecycle.get("browsers_created", 0),
        "browsers_retired": lifecycle.get("browsers_retired", 0),
        "browser_restarts": lifecycle.get("browser_restarts", 0),
        "warm_up_count": lifecycle.get("warm_up_count", 0),
        "stores_per_browser": lifecycle.get("stores_per_browser", {}),
        "stores": store_summaries,
        "artifacts": {
            "state_database": str(database), "parquet_output": lifecycle.get("parquet_output_path"),
            "progress_log": str(run_directory / "progress.log"),
            "summary": str(run_directory / "summary.json"),
            "checkpoints": str(run_directory / "checkpoints"),
            "metadata": str(run_directory / "metadata"),
            "run_metadata": str(run_directory / "run_metadata.json"),
            "dlq": str(run_directory / "dlq.jsonl"),
            "scroll_metrics": str(run_directory / "scroll_metrics.jsonl"),
            "review_id_trace": str(run_directory / "review_id_trace.jsonl"),
            "reconciliation_metrics": str(run_directory / "incremental_metrics.json"),
            "reconciliation_decisions": str(run_directory / "reconciliation_decisions_*.json"),
        },
    }
