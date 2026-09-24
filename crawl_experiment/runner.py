from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from hashlib import sha256
import json
from pathlib import Path
import threading
from typing import Any, Callable
from uuid import uuid4

from .http import FetchResult, HttpClient
from .parsers import detect_challenge, parse_store_html, store_id_from_url
from .storage import Storage


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def new_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "-" + uuid4().hex[:8]


class CrawlRunner:
    def __init__(
        self,
        *,
        storage: Storage,
        data_dir: str | Path,
        concurrency: int,
        fetch: Callable[[str], FetchResult],
    ) -> None:
        self.storage = storage
        self.data_dir = Path(data_dir)
        self.concurrency = max(1, concurrency)
        self.fetch = fetch
        self._metric_lock = threading.Lock()

    def run(self, tasks: list[dict[str, str]], resume_run_id: str | None = None) -> dict[str, Any]:
        run_id = new_run_id()
        raw_dir = self.data_dir / "raw" / run_id
        metric_dir = self.data_dir / "metrics"
        raw_dir.mkdir(parents=True, exist_ok=False)
        metric_dir.mkdir(parents=True, exist_ok=True)
        metric_path = metric_dir / f"{run_id}.jsonl"
        self.storage.start_run(run_id, utc_now(), resume_run_id)
        for task in tasks:
            self.storage.add_task(run_id, task["task_key"], task["url"], task["name"])

        results: list[dict[str, Any]] = []
        with ThreadPoolExecutor(max_workers=self.concurrency) as executor:
            futures = {
                executor.submit(self._crawl_one, run_id, task, raw_dir, metric_path): task
                for task in tasks
            }
            for future in as_completed(futures):
                try:
                    results.append(future.result())
                except Exception as exc:  # defensive isolation around each worker
                    task = futures[future]
                    self.storage.finish_task(run_id, task["task_key"], "failed", str(exc))
                    results.append({"status": "failed", "error": str(exc), "task_key": task["task_key"]})

        summary = {
            "run_id": run_id,
            "resumed_from_run_id": resume_run_id,
            "tasks_total": len(tasks),
            "tasks_succeeded": sum(item["status"] == "succeeded" for item in results),
            "tasks_failed": sum(item["status"] == "failed" for item in results),
            "http_403_count": sum(int(item.get("http_403_count", 0)) for item in results),
            "http_429_count": sum(int(item.get("http_429_count", 0)) for item in results),
            "challenge_count": sum(bool(item.get("challenge_detected")) for item in results),
            "records_extracted": sum(int(item.get("records_extracted", 0)) for item in results),
            "stores_total_in_database": self.storage.count("stores"),
            "reviews_total_in_database": self.storage.count("reviews"),
            "finished_at": utc_now(),
        }
        status = "completed_with_failures" if summary["tasks_failed"] else "completed"
        self.storage.finish_run(run_id, summary["finished_at"], status, summary)
        (metric_dir / f"{run_id}.summary.json").write_text(
            json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        return summary

    def _crawl_one(
        self, run_id: str, task: dict[str, str], raw_dir: Path, metric_path: Path
    ) -> dict[str, Any]:
        result = self.fetch(task["url"])
        digest = sha256(task["url"].encode("utf-8")).hexdigest()[:12]
        raw_path = raw_dir / f"store_{task['task_key']}_{digest}.html"
        raw_path.write_bytes(result.body)
        challenge = detect_challenge(result.body)
        records_extracted = 0
        error = result.error
        status = "failed"
        if challenge:
            store = parse_store_html(task["url"], result.body, task["name"])
            self.storage.upsert_store(store)
            records_extracted = 1
            error = "block/challenge indicator detected"
        elif result.status is not None and 200 <= result.status < 300:
            store = parse_store_html(task["url"], result.body, task["name"])
            self.storage.upsert_store(store)
            records_extracted = 1
            status = "succeeded"
        elif result.status is not None:
            error = error or f"HTTP {result.status}"
        else:
            error = error or "request failed without an HTTP response"

        metric = {
            "run_id": run_id,
            "task_key": task["task_key"],
            "url": task["url"],
            "http_status": result.status,
            "latency_ms": round(result.latency_ms, 3),
            "retry_count": result.retries,
            "is_403": result.status == 403,
            "is_429": result.status == 429,
            "http_403_count": result.count_403,
            "http_429_count": result.count_429,
            "challenge_detected": challenge,
            "error": error,
            "records_extracted": records_extracted,
            "raw_path": str(raw_path),
        }
        self.storage.record_metric(metric)
        with self._metric_lock:
            with metric_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(metric, sort_keys=True) + "\n")
        self.storage.finish_task(run_id, task["task_key"], status, error)
        return {**metric, "status": status}


def build_tasks(config: dict[str, Any], max_stores: int) -> list[dict[str, str]]:
    if not 1 <= max_stores <= 10:
        raise ValueError("max_stores must be between 1 and 10")
    seen: set[tuple[str, str]] = set()
    tasks = []
    for item in config.get("store_pages", []):
        store_id = store_id_from_url(item["url"])
        key = (str(config.get("retailer_code", "coles")).lower(), store_id)
        if key in seen:
            continue
        seen.add(key)
        tasks.append({"task_key": store_id, "url": item["url"], "name": item.get("name", f"Coles {store_id}")})
        if len(tasks) >= max_stores:
            break
    return tasks


def client_from_config(config: dict[str, Any], interval: float, retries: int) -> HttpClient:
    return HttpClient(
        interval_seconds=interval,
        timeout_seconds=float(config.get("timeout_seconds", 20.0)),
        max_retries=retries,
    )
