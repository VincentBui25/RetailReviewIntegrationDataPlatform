from __future__ import annotations

import json
from pathlib import Path
import sqlite3
import threading
from typing import Any, Iterable

from .models import ReviewRecord, StoreRecord


SCHEMA = """
CREATE TABLE IF NOT EXISTS stores (
  retailer_code TEXT NOT NULL,
  retailer_store_id TEXT NOT NULL,
  name TEXT NOT NULL,
  url TEXT NOT NULL,
  address TEXT,
  payload_json TEXT NOT NULL,
  first_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (retailer_code, retailer_store_id)
);
CREATE TABLE IF NOT EXISTS reviews (
  source_code TEXT NOT NULL,
  source_review_id TEXT NOT NULL,
  retailer_code TEXT NOT NULL,
  retailer_store_id TEXT NOT NULL,
  author TEXT,
  published_at TEXT,
  rating REAL,
  title TEXT,
  text TEXT,
  payload_json TEXT NOT NULL,
  first_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (source_code, source_review_id)
);
CREATE TABLE IF NOT EXISTS crawl_runs (
  run_id TEXT PRIMARY KEY,
  resumed_from_run_id TEXT,
  started_at TEXT NOT NULL,
  finished_at TEXT,
  status TEXT NOT NULL,
  summary_json TEXT
);
CREATE TABLE IF NOT EXISTS crawl_tasks (
  run_id TEXT NOT NULL,
  task_key TEXT NOT NULL,
  url TEXT NOT NULL,
  name TEXT NOT NULL,
  status TEXT NOT NULL,
  error TEXT,
  PRIMARY KEY (run_id, task_key)
);
CREATE TABLE IF NOT EXISTS request_metrics (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT NOT NULL,
  task_key TEXT NOT NULL,
  url TEXT NOT NULL,
  http_status INTEGER,
  latency_ms REAL NOT NULL,
  retry_count INTEGER NOT NULL,
  is_403 INTEGER NOT NULL,
  is_429 INTEGER NOT NULL,
  http_403_count INTEGER NOT NULL,
  http_429_count INTEGER NOT NULL,
  challenge_detected INTEGER NOT NULL,
  error TEXT,
  records_extracted INTEGER NOT NULL,
  recorded_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"""


class Storage:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(self.path, check_same_thread=False)
        self._conn.executescript(SCHEMA)
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    def start_run(self, run_id: str, started_at: str, resumed_from: str | None = None) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO crawl_runs(run_id,resumed_from_run_id,started_at,status) VALUES(?,?,?,'running')",
                (run_id, resumed_from, started_at),
            )

    def finish_run(self, run_id: str, finished_at: str, status: str, summary: dict[str, Any]) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE crawl_runs SET finished_at=?,status=?,summary_json=? WHERE run_id=?",
                (finished_at, status, json.dumps(summary, sort_keys=True), run_id),
            )

    def add_task(self, run_id: str, task_key: str, url: str, name: str) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT OR IGNORE INTO crawl_tasks(run_id,task_key,url,name,status) VALUES(?,?,?,?,'pending')",
                (run_id, task_key, url, name),
            )

    def finish_task(self, run_id: str, task_key: str, status: str, error: str | None = None) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE crawl_tasks SET status=?,error=? WHERE run_id=? AND task_key=?",
                (status, error, run_id, task_key),
            )

    def failed_tasks(self, run_id: str) -> list[dict[str, str]]:
        rows = self._conn.execute(
            "SELECT task_key,url,name FROM crawl_tasks WHERE run_id=? AND status='failed' ORDER BY task_key",
            (run_id,),
        ).fetchall()
        return [{"task_key": row[0], "url": row[1], "name": row[2]} for row in rows]

    def upsert_store(self, store: StoreRecord) -> bool:
        with self._lock, self._conn:
            exists = self._conn.execute(
                "SELECT 1 FROM stores WHERE retailer_code=? AND retailer_store_id=?",
                store.logical_key,
            ).fetchone()
            self._conn.execute(
                """INSERT INTO stores(retailer_code,retailer_store_id,name,url,address,payload_json)
                   VALUES(?,?,?,?,?,?)
                   ON CONFLICT(retailer_code,retailer_store_id) DO UPDATE SET
                     name=excluded.name,url=excluded.url,address=excluded.address,
                     payload_json=excluded.payload_json,updated_at=CURRENT_TIMESTAMP""",
                (*store.logical_key, store.name, store.url, store.address, json.dumps(store.payload, sort_keys=True)),
            )
            return exists is None

    def upsert_review(self, review: ReviewRecord) -> bool:
        with self._lock, self._conn:
            exists = self._conn.execute(
                "SELECT 1 FROM reviews WHERE source_code=? AND source_review_id=?",
                review.logical_key,
            ).fetchone()
            self._conn.execute(
                """INSERT INTO reviews(source_code,source_review_id,retailer_code,retailer_store_id,
                   author,published_at,rating,title,text,payload_json) VALUES(?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(source_code,source_review_id) DO UPDATE SET
                     retailer_code=excluded.retailer_code,retailer_store_id=excluded.retailer_store_id,
                     author=excluded.author,published_at=excluded.published_at,rating=excluded.rating,
                     title=excluded.title,text=excluded.text,payload_json=excluded.payload_json,
                     updated_at=CURRENT_TIMESTAMP""",
                (
                    *review.logical_key,
                    review.retailer_code,
                    review.retailer_store_id,
                    review.author,
                    review.published_at,
                    review.rating,
                    review.title,
                    review.text,
                    json.dumps(review.payload, sort_keys=True),
                ),
            )
            return exists is None

    def upsert_reviews(self, reviews: Iterable[ReviewRecord]) -> int:
        return sum(1 for review in reviews if self.upsert_review(review))

    def record_metric(self, metric: dict[str, Any]) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                """INSERT INTO request_metrics(run_id,task_key,url,http_status,latency_ms,retry_count,
                   is_403,is_429,http_403_count,http_429_count,challenge_detected,error,records_extracted)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    metric["run_id"], metric["task_key"], metric["url"], metric.get("http_status"),
                    metric["latency_ms"], metric["retry_count"], int(metric["is_403"]),
                    int(metric["is_429"]), metric["http_403_count"], metric["http_429_count"],
                    int(metric["challenge_detected"]), metric.get("error"),
                    metric["records_extracted"],
                ),
            )

    def count(self, table: str) -> int:
        if table not in {"stores", "reviews", "request_metrics"}:
            raise ValueError("unsupported table")
        return int(self._conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
