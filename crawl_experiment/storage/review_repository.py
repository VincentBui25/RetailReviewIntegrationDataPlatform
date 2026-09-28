from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Iterable
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from threading import Lock
from typing import Any

from crawl_experiment.core.models import Review

from .parquet_review_writer import ParquetReviewWriter


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalized_image_urls(review: Review) -> list[str]:
    return list(dict.fromkeys(url.strip() for url in review.image_urls if url.strip()))


def review_content_hash(review: Review) -> str:
    """Hash mutable content; review URLs are lineage metadata, not content."""
    payload = {
        "image_urls": sorted(_normalized_image_urls(review)),
        "owner_response": review.owner_response,
        "rating": review.rating,
        "review_text": review.text,
        "reviewer_name": review.author,
    }
    canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class ReviewChange(str, Enum):
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    UNCHANGED = "UNCHANGED"

    @property
    def emitted(self) -> bool:
        return self is not ReviewChange.UNCHANGED


class ReviewRepository:
    """Durable identity/state repository with an optional Parquet outbox sink."""

    def __init__(
        self,
        path: str | Path,
        *,
        run_id: str = "unspecified",
        parquet_writer: ParquetReviewWriter | None = None,
        clock=utc_now,
    ):
        self.path = Path(path)
        if str(path) != ":memory:":
            self.path.parent.mkdir(parents=True, exist_ok=True)
        self.run_id = run_id
        self.parquet_writer = parquet_writer
        self.clock = clock
        self._lock = Lock()
        self._flush_lock = Lock()
        self._db = sqlite3.connect(str(path), check_same_thread=False)
        self._db.row_factory = sqlite3.Row
        self._db.executescript(
            """CREATE TABLE IF NOT EXISTS review_state(
                source TEXT NOT NULL,
                source_review_id TEXT NOT NULL,
                store_id TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                first_seen_run_id TEXT NOT NULL,
                last_seen_run_id TEXT NOT NULL,
                UNIQUE(source, source_review_id)
            );
            CREATE TABLE IF NOT EXISTS pending_review_events(
                event_id TEXT PRIMARY KEY,
                source TEXT NOT NULL,
                source_review_id TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(source, source_review_id, content_hash, created_at)
            );
            CREATE TABLE IF NOT EXISTS checkpoint_metadata(
                run_id TEXT NOT NULL,
                store_id TEXT NOT NULL,
                metadata_json TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY(run_id, store_id)
            );
            CREATE VIEW IF NOT EXISTS reviews AS
            SELECT source, source_review_id, store_id,
                   NULL AS author, NULL AS rating, NULL AS text,
                   NULL AS displayed_date, NULL AS owner_response,
                   '{}' AS raw_json, last_seen_at AS updated_at
            FROM review_state;
            """
        )
        self._db.commit()
        self.last_change_type: str | None = None
        self.new_reviews_written = 0
        self.changed_reviews_written = 0
        self.unchanged_reviews_seen = 0
        self.parquet_files_written = 0
        self.parquet_rows_written = 0
        if self.parquet_writer is not None:
            self.flush()

    def exists(self, source: str, source_review_id: str) -> bool:
        return self.get_review_state(source, source_review_id) is not None

    def get_review_state(self, source: str, review_id: str) -> dict[str, Any] | None:
        row = self._db.execute(
            "SELECT * FROM review_state WHERE source=? AND source_review_id=?",
            (source, review_id),
        ).fetchone()
        return dict(row) if row is not None else None

    def batch_lookup_known_ids(self, source: str, ids: Iterable[str]) -> set[str]:
        values = sorted(set(ids))
        known: set[str] = set()
        for offset in range(0, len(values), 900):
            chunk = values[offset : offset + 900]
            if not chunk:
                continue
            placeholders = ",".join("?" for _ in chunk)
            rows = self._db.execute(
                f"SELECT source_review_id FROM review_state WHERE source=? AND source_review_id IN ({placeholders})",
                [source, *chunk],
            ).fetchall()
            known.update(row[0] for row in rows)
        return known

    def get_known_review_ids(self, ids: Iterable[str], source: str = "google_maps") -> set[str]:
        return self.batch_lookup_known_ids(source, ids)

    def upsert_review_state(self, review: Review, observed_at: str | None = None) -> ReviewChange:
        digest = review_content_hash(review)
        observed_at = observed_at or self.clock()
        with self._lock, self._db:
            current = self._db.execute(
                "SELECT content_hash FROM review_state WHERE source=? AND source_review_id=?",
                review.identity,
            ).fetchone()
            change = (
                ReviewChange.INSERT
                if current is None
                else ReviewChange.UPDATE
                if current[0] != digest
                else ReviewChange.UNCHANGED
            )
            if current is None:
                self._db.execute(
                    "INSERT INTO review_state VALUES(?,?,?,?,?,?,?,?)",
                    (*review.identity, review.store_id, digest, observed_at, observed_at, self.run_id, self.run_id),
                )
            else:
                self._db.execute(
                    """UPDATE review_state SET store_id=?, content_hash=?, last_seen_at=?,
                       last_seen_run_id=? WHERE source=? AND source_review_id=?""",
                    (review.store_id, digest, observed_at, self.run_id, *review.identity),
                )
            if change.emitted and self.parquet_writer is not None:
                payload = {
                    "run_id": self.run_id, "source": review.source,
                    "source_review_id": review.source_review_id, "store_id": review.store_id,
                    "reviewer_name": review.author, "rating": review.rating,
                    "review_text": review.text, "review_date_raw": review.displayed_date,
                    "owner_response": review.owner_response, "review_url": review.review_url,
                    "image_urls": _normalized_image_urls(review),
                    "observed_at": observed_at,
                    "content_hash": digest, "change_type": change.value,
                }
                event_id = hashlib.sha256(
                    f"{self.run_id}\0{review.source}\0{review.source_review_id}\0{digest}\0{observed_at}".encode()
                ).hexdigest()
                self._db.execute(
                    "INSERT OR IGNORE INTO pending_review_events VALUES(?,?,?,?,?,?)",
                    (event_id, review.source, review.source_review_id, digest,
                     json.dumps(payload, ensure_ascii=False, sort_keys=True), observed_at),
                )
            self.last_change_type = None if change is ReviewChange.UNCHANGED else change.value
            if change is ReviewChange.INSERT:
                self.new_reviews_written += 1
            elif change is ReviewChange.UPDATE:
                self.changed_reviews_written += 1
            else:
                self.unchanged_reviews_seen += 1
        if self.parquet_writer is not None and self.pending_count() >= self.parquet_writer.batch_size:
            self.flush()
        return change

    def upsert(self, review: Review) -> bool:
        """Compatibility API: return whether this is a newly discovered identity."""
        return self.upsert_review_state(review) is ReviewChange.INSERT

    def upsert_many(self, reviews: Iterable[Review]) -> int:
        return sum(self.upsert(review) for review in reviews)

    def pending_count(self) -> int:
        return int(self._db.execute("SELECT count(*) FROM pending_review_events").fetchone()[0])

    def save_checkpoint_metadata(self, store_id: str, metadata: dict[str, Any]) -> None:
        observed_at = self.clock()
        with self._lock, self._db:
            self._db.execute(
                """INSERT INTO checkpoint_metadata VALUES(?,?,?,?)
                   ON CONFLICT(run_id, store_id) DO UPDATE SET
                   metadata_json=excluded.metadata_json, updated_at=excluded.updated_at""",
                (self.run_id, store_id, json.dumps(metadata, sort_keys=True), observed_at),
            )

    def get_checkpoint_metadata(self, run_id: str, store_id: str) -> dict[str, Any] | None:
        row = self._db.execute(
            "SELECT metadata_json FROM checkpoint_metadata WHERE run_id=? AND store_id=?",
            (run_id, store_id),
        ).fetchone()
        return json.loads(row[0]) if row else None

    def flush(self) -> None:
        if self.parquet_writer is None:
            return
        with self._flush_lock:
            while True:
                with self._lock:
                    rows = self._db.execute(
                        "SELECT event_id, payload_json FROM pending_review_events ORDER BY created_at, event_id LIMIT ?",
                        (self.parquet_writer.batch_size,),
                    ).fetchall()
                if not rows:
                    return
                decoded = [(row["event_id"], json.loads(row["payload_json"])) for row in rows]
                first_run_id = decoded[0][1]["run_id"]
                decoded = [item for item in decoded if item[1]["run_id"] == first_run_id]
                writer = self.parquet_writer
                if first_run_id != writer.run_id:
                    writer = ParquetReviewWriter(
                        writer.root,
                        first_run_id,
                        decoded[0][1]["observed_at"][:10],
                        batch_size=writer.batch_size,
                    )
                before_files = writer.files_written
                before_rows = writer.rows_written
                _, event_ids = writer.write_batch(decoded)
                with self._lock, self._db:
                    self._db.executemany(
                        "DELETE FROM pending_review_events WHERE event_id=?",
                        [(event_id,) for event_id in event_ids],
                    )
                self.parquet_files_written += writer.files_written - before_files
                self.parquet_rows_written += writer.rows_written - before_rows

    def count(self) -> int:
        return int(self._db.execute("SELECT count(*) FROM review_state").fetchone()[0])

    def stats(self) -> dict[str, Any]:
        return {
            "state_db_path": str(self.path),
            "parquet_output_path": str(self.parquet_writer.output_path) if self.parquet_writer else None,
            "parquet_files_written": self.parquet_files_written,
            "parquet_rows_written": self.parquet_rows_written,
            "new_reviews_written": self.new_reviews_written,
            "changed_reviews_written": self.changed_reviews_written,
            "unchanged_reviews_seen": self.unchanged_reviews_seen,
        }

    def close(self) -> None:
        self.flush()
        self._db.close()
