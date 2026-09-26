from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterable
from pathlib import Path
from threading import Lock

from crawl_experiment.core.models import Review


class ReviewRepository:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        if str(path) != ":memory:":
            self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = Lock()
        self._db = sqlite3.connect(str(path), check_same_thread=False)
        self._db.execute(
            """CREATE TABLE IF NOT EXISTS reviews(
                source TEXT NOT NULL,
                source_review_id TEXT NOT NULL,
                store_id TEXT NOT NULL,
                author TEXT,
                rating REAL,
                text TEXT,
                displayed_date TEXT,
                owner_response TEXT,
                raw_json TEXT NOT NULL,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY(source, source_review_id)
            )"""
        )
        self._db.commit()

    def exists(self, source: str, source_review_id: str) -> bool:
        row = self._db.execute(
            "SELECT 1 FROM reviews WHERE source=? AND source_review_id=?",
            (source, source_review_id),
        ).fetchone()
        return row is not None

    def upsert(self, review: Review) -> bool:
        with self._lock, self._db:
            inserted = not self.exists(*review.identity)
            self._db.execute(
                """INSERT INTO reviews VALUES(?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)
                ON CONFLICT(source,source_review_id) DO UPDATE SET
                    store_id=excluded.store_id,
                    author=excluded.author,
                    rating=excluded.rating,
                    text=excluded.text,
                    displayed_date=excluded.displayed_date,
                    owner_response=excluded.owner_response,
                    raw_json=excluded.raw_json,
                    updated_at=CURRENT_TIMESTAMP""",
                (
                    review.source,
                    review.source_review_id,
                    review.store_id,
                    review.author,
                    review.rating,
                    review.text,
                    review.displayed_date,
                    review.owner_response,
                    json.dumps(review.raw, sort_keys=True),
                ),
            )
            return inserted

    def upsert_many(self, reviews: Iterable[Review]) -> int:
        return sum(self.upsert(review) for review in reviews)

    def count(self) -> int:
        return int(self._db.execute("SELECT count(*) FROM reviews").fetchone()[0])

    def close(self) -> None:
        self._db.close()
