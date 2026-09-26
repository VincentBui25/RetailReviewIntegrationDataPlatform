"""Bounded live smoke test for the modular Google Maps crawler.

Run this script manually. The supervisor enforces a five-minute wall-clock limit;
the worker still uses the production Crawlee/SeleniumBase crawler path.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sqlite3
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from crawl_experiment.browser.browser_factory import BrowserFactory
from crawl_experiment.core.models import Store
from crawl_experiment.core.statuses import CrawlStatus
from crawl_experiment.observability.metrics import CrawlMetrics
from crawl_experiment.orchestration.crawlee_adapter import CrawleeAdapter
from crawl_experiment.sources.google_maps.crawler import GoogleMapsCrawler
from crawl_experiment.sources.google_maps.health import (
    HealthState,
    PageEvidence,
    classify_page,
)
from crawl_experiment.storage.checkpoint_repository import (
    Checkpoint,
    CheckpointRepository,
)
from crawl_experiment.storage.review_repository import ReviewRepository

STORE = Store(
    id="coles-710",
    name="Coles World Square",
    query="Coles World Square",
    retailer="coles",
    expected_address="650 George St, Sydney NSW 2000, Australia",
)
HARD_TIMEOUT_SECONDS = 300
CRAWL_TIMEOUT_SECONDS = 270
OUTPUT_ROOT = Path("data/smoke/google_maps_world_square")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_json(path: Path) -> dict[str, Any] | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def write_json(path: Path, value: dict[str, Any]) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, default=str) + "\n", encoding="utf-8")
    temporary.replace(path)


def review_summary(database: Path) -> dict[str, Any]:
    if not database.exists():
        return {"persisted_count": 0, "stable_review_ids": []}

    with sqlite3.connect(database) as connection:
        rows = connection.execute(
            """SELECT source_review_id, author, rating, text, displayed_date
            FROM reviews
            WHERE source = ? AND store_id = ?
            ORDER BY source_review_id""",
            ("google_maps", STORE.id),
        ).fetchall()

    normalized_count = sum(
        bool(review_id) and any(value is not None for value in row[1:])
        for row in rows
        for review_id in [row[0]]
    )
    return {
        "persisted_count": len(rows),
        "normalized_count": normalized_count,
        "stable_review_ids": [row[0] for row in rows if row[0]],
    }


def health_classifier_checks() -> dict[str, bool]:
    return {
        "healthy": classify_page("https://google.com/maps", "", has_place=True)
        is HealthState.HEALTHY,
        "limited": classify_page(
            "https://google.com/maps",
            "limited view",
            evidence=PageEvidence(),
        )
        is HealthState.LIMITED,
        "rate_limited": classify_page("https://google.com/sorry/", "")
        is HealthState.RATE_LIMITED,
        "challenge": classify_page("https://google.com/maps", "not a robot")
        is HealthState.CHALLENGE,
    }


def reached_stages(progress_log: Path) -> set[str]:
    try:
        lines = progress_log.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        return set()

    stages = set()
    for line in lines:
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if event.get("store_id") == STORE.id and event.get("stage"):
            stages.add(event["stage"])
    return stages


def pagination_progressed(progress_log: Path) -> bool:
    try:
        lines = progress_log.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        return False
    for line in lines:
        marker = "pagination scroll: "
        if marker not in line:
            continue
        try:
            diagnostics = json.loads(line.split(marker, 1)[1])
        except json.JSONDecodeError:
            continue
        if (
            diagnostics.get("scroll_top_changed")
            or diagnostics.get("cards_found", 0) > 0
            or diagnostics.get("new_review_ids", 0) > 0
        ):
            return True
    return False


def build_summary(
    run_directory: Path,
    *,
    started_at: str,
    finished_at: str,
    hard_stopped: bool,
    worker_exit_code: int | None,
) -> dict[str, Any]:
    checkpoint_path = run_directory / "checkpoints" / f"{STORE.id}.json"
    checkpoint = read_json(checkpoint_path) or {}
    reviews = review_summary(run_directory / "reviews.sqlite3")
    stages = reached_stages(run_directory / "progress.log")
    review_ids = reviews["stable_review_ids"]
    semantic_health_statuses = {
        CrawlStatus.LIMITED,
        CrawlStatus.RATE_LIMITED,
        CrawlStatus.CHALLENGE,
    }
    health_was_classified = (
        "store_resolved" in stages
        or checkpoint.get("status") in semantic_health_statuses
    )

    return {
        "store": {"id": STORE.id, "name": STORE.name, "query": STORE.query},
        "limits": {
            "stores": 1,
            "hard_timeout_seconds": HARD_TIMEOUT_SECONDS,
            "crawler_timeout_seconds": CRAWL_TIMEOUT_SECONDS,
            "network": "direct",
            "proxy": False,
            "captcha_solving": False,
        },
        "started_at": started_at,
        "finished_at": finished_at,
        "hard_stopped": hard_stopped,
        "worker_exit_code": worker_exit_code,
        "status": checkpoint.get("status", CrawlStatus.ERROR),
        "failure_stage": checkpoint.get("failure_stage"),
        "reached_stages": sorted(stages),
        "checkpoint": checkpoint,
        "checks": {
            "browser_started": "browser_started" in stages,
            "warm_up_completed": "warm_up_completed" in stages,
            "store_resolved": "store_resolved" in stages,
            "reviews_surface_opened": "reviews_surface_opened" in stages,
            "sort_newest_applied": "sort_newest_applied" in stages,
            "review_pane_found": "review_pane_found" in stages,
            "pagination_progress": pagination_progressed(
                run_directory / "progress.log"
            ),
            "stable_review_ids_extracted": bool(review_ids),
            "reviews_normalized": reviews.get("normalized_count", 0) > 0,
            "reviews_persisted": reviews.get("persisted_count", 0) > 0,
            "checkpoint_written": bool(checkpoint),
            "health_status_classified": health_was_classified,
        },
        "health_classifier_self_check": health_classifier_checks(),
        "reviews": reviews,
        "artifacts": {
            "progress_log": str(run_directory / "progress.log"),
            "summary": str(run_directory / "summary.json"),
            "checkpoint": str(checkpoint_path),
            "database": str(run_directory / "reviews.sqlite3"),
        },
    }


async def run_worker(run_directory: Path) -> None:
    os.environ["CRAWLEE_STORAGE_DIR"] = str(run_directory / "crawlee_storage")
    logger = logging.getLogger("smoke")
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler(sys.stdout))

    reviews = ReviewRepository(run_directory / "reviews.sqlite3")
    try:
        crawler = GoogleMapsCrawler(
            reviews,
            CheckpointRepository(run_directory / "checkpoints"),
            CrawlMetrics(logger),
            timeout_seconds=CRAWL_TIMEOUT_SECONDS,
        )
        adapter = CrawleeAdapter(
            BrowserFactory(headless=False),
            crawler.crawl,
            max_concurrency=1,
        )
        logger.info("Starting one-store smoke test: %s", STORE.name)
        await adapter.run([STORE])
        logger.info("Crawlee run finished")
    finally:
        reviews.close()


def worker_main(run_directory: Path) -> int:
    started_at = utc_now()
    exit_code = 0
    try:
        asyncio.run(run_worker(run_directory))
    except Exception:
        logging.getLogger("smoke").exception("Smoke-test worker failed")
        exit_code = 1
    finally:
        summary = build_summary(
            run_directory,
            started_at=started_at,
            finished_at=utc_now(),
            hard_stopped=False,
            worker_exit_code=exit_code,
        )
        write_json(run_directory / "summary.json", summary)
    return exit_code


def mark_hard_timeout(run_directory: Path, started_at: str) -> None:
    repository = CheckpointRepository(run_directory / "checkpoints")
    existing = repository.load(STORE.id)
    made_progress = bool(
        existing and (existing.reviews_seen > 0 or existing.scroll_count > 0)
    )
    status = CrawlStatus.PARTIAL_TIMEOUT if made_progress else CrawlStatus.ERROR
    repository.save(
        Checkpoint(
            store_id=STORE.id,
            status=status,
            reviews_seen=existing.reviews_seen if existing else 0,
            scroll_count=existing.scroll_count if existing else 0,
            last_progress_at=existing.last_progress_at if existing else None,
            failure_stage="timeout",
            started_at=existing.started_at if existing else started_at,
            finished_at=utc_now(),
        )
    )


def terminate_process_tree(process: subprocess.Popen[Any]) -> None:
    if os.name == "nt":
        subprocess.run(
            ["taskkill", "/PID", str(process.pid), "/T", "/F"],
            check=False,
            capture_output=True,
            text=True,
        )
    else:
        process.kill()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()


def supervisor_main() -> int:
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    run_directory = OUTPUT_ROOT / run_id
    run_directory.mkdir(parents=True)
    started_at = utc_now()
    command = [
        sys.executable,
        "-m",
        "scripts.google_maps_smoke_test",
        "--worker",
        str(run_directory),
    ]

    progress_path = run_directory / "progress.log"
    with progress_path.open("w", encoding="utf-8") as progress:
        progress.write(f"{started_at} supervisor starting worker\n")
        progress.flush()
        process = subprocess.Popen(
            command,
            cwd=Path.cwd(),
            stdout=progress,
            stderr=subprocess.STDOUT,
            creationflags=(
                subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
            ),
        )
        try:
            worker_exit_code = process.wait(timeout=HARD_TIMEOUT_SECONDS)
            hard_stopped = False
        except subprocess.TimeoutExpired:
            terminate_process_tree(process)
            worker_exit_code = process.returncode
            hard_stopped = True
            progress.write(f"{utc_now()} hard timeout reached; worker tree stopped\n")
            progress.flush()

    if hard_stopped:
        mark_hard_timeout(run_directory, started_at)
        write_json(
            run_directory / "summary.json",
            build_summary(
                run_directory,
                started_at=started_at,
                finished_at=utc_now(),
                hard_stopped=True,
                worker_exit_code=worker_exit_code,
            ),
        )

    summary_path = run_directory / "summary.json"
    print(f"Smoke test finished. Summary: {summary_path}")
    print(f"Progress log: {progress_path}")
    summary = read_json(summary_path)
    return 0 if summary and summary.get("status") != CrawlStatus.ERROR else 1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker", type=Path, help=argparse.SUPPRESS)
    return parser.parse_args()


if __name__ == "__main__":
    arguments = parse_args()
    raise SystemExit(
        worker_main(arguments.worker) if arguments.worker else supervisor_main()
    )
