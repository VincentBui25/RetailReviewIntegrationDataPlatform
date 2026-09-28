from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any, Iterable


class ParquetReviewWriter:
    """Atomic, idempotent writer for append-only review change batches."""

    COLUMNS = (
        "run_id", "source", "source_review_id", "store_id", "reviewer_name",
        "rating", "review_text", "review_date_raw", "owner_response",
        "review_url", "image_urls", "observed_at", "content_hash", "change_type",
    )

    def __init__(self, root: str | Path, run_id: str, crawl_date: str, *, batch_size: int = 500):
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        self.root = Path(root)
        self.crawl_date = crawl_date
        self.output_path = self.root / f"crawl_date={crawl_date}" / f"run_id={run_id}"
        self.run_id = run_id
        self.batch_size = batch_size
        self.files_written = 0
        self.rows_written = 0

    @staticmethod
    def schema():
        import pyarrow as pa

        return pa.schema([
            ("run_id", pa.string()), ("source", pa.string()),
            ("source_review_id", pa.string()), ("store_id", pa.string()),
            ("reviewer_name", pa.string()), ("rating", pa.float64()),
            ("review_text", pa.string()), ("review_date_raw", pa.string()),
            ("owner_response", pa.string()), ("review_url", pa.string()),
            ("image_urls", pa.list_(pa.string())), ("observed_at", pa.string()),
            ("content_hash", pa.string()), ("change_type", pa.string()),
        ])

    @staticmethod
    def _fingerprint(event_ids: list[str]) -> str:
        # Keep paths usable on Windows; footer verification below makes a rare
        # truncated-digest collision fail closed instead of acknowledging it.
        return hashlib.sha256("\n".join(event_ids).encode()).hexdigest()[:20]

    def write_batch(self, events: Iterable[tuple[str, dict[str, Any]]]) -> tuple[Path | None, list[str]]:
        items = list(events)
        if not items:
            return None, []
        event_ids = [event_id for event_id, _ in items]
        fingerprint = self._fingerprint(event_ids)
        target = self.output_path / f"part-{fingerprint}.parquet"
        if target.exists():
            import pyarrow.parquet as pq

            metadata = pq.ParquetFile(target).metadata.metadata or {}
            persisted_ids = json.loads(metadata.get(b"review_event_ids", b"[]"))
            if persisted_ids != event_ids:
                raise RuntimeError(f"Parquet batch filename collision: {target}")
            return target, event_ids

        import pyarrow as pa
        import pyarrow.parquet as pq

        self.output_path.mkdir(parents=True, exist_ok=True)
        rows = [{column: payload.get(column) for column in self.COLUMNS} for _, payload in items]
        table = pa.Table.from_pylist(rows, schema=self.schema())
        table = table.replace_schema_metadata(
            {b"review_event_ids": json.dumps(event_ids, separators=(",", ":")).encode()}
        )
        temporary = self.output_path / f".{fingerprint[:8]}-{os.getpid()}.tmp"
        try:
            pq.write_table(table, temporary, compression="snappy")
            temporary.replace(target)
        finally:
            temporary.unlink(missing_ok=True)
        self.files_written += 1
        self.rows_written += len(rows)
        return target, event_ids
