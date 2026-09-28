"""Bounded synthetic SQLite -> outbox -> Parquet -> DuckDB validation."""

from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
from typing import Any

import duckdb
import pyarrow.parquet as pq

from crawl_experiment.core.models import Review
from crawl_experiment.storage.parquet_review_writer import ParquetReviewWriter
from crawl_experiment.storage.review_repository import ReviewChange, ReviewRepository


RUN_ID = "synthetic-persistence-e2e"
CRAWL_DATE = "2026-09-27"


def run_validation(output_root: Path) -> dict[str, Any]:
    data_root = output_root / "data"
    state_path = data_root / "state" / "crawler_state.sqlite3"
    lake_root = data_root / "lake" / "google_maps_reviews"
    writer = ParquetReviewWriter(lake_root, RUN_ID, CRAWL_DATE, batch_size=100)
    moments = iter(
        [
            "2026-09-27T01:00:00+00:00",
            "2026-09-27T01:01:00+00:00",
            "2026-09-27T01:02:00+00:00",
        ]
    )
    repository = ReviewRepository(
        state_path,
        run_id=RUN_ID,
        parquet_writer=writer,
        clock=lambda: next(moments),
    )
    original = Review(
        "google_maps", "synthetic-review-1", "synthetic-store-1",
        author="Synthetic Reviewer", rating=5.0, text="Original text",
        displayed_date="3 months ago",
        review_url="https://www.google.com/maps/reviews/data=synthetic-review-1",
        image_urls=["https://images.example/review-1.jpg"],
    )
    updated = Review(
        "google_maps", "synthetic-review-1", "synthetic-store-1",
        author="Synthetic Reviewer", rating=4.0, text="Updated text",
        displayed_date="3 months ago",
        review_url="https://www.google.com/maps/reviews/data=synthetic-review-1",
        image_urls=["https://images.example/review-1.jpg", "https://images.example/review-2.jpg"],
    )

    assert repository.upsert_review_state(original) is ReviewChange.INSERT
    first_state = repository.get_review_state("google_maps", original.source_review_id)
    assert repository.upsert_review_state(original) is ReviewChange.UNCHANGED
    unchanged_state = repository.get_review_state("google_maps", original.source_review_id)
    assert unchanged_state["first_seen_at"] == first_state["first_seen_at"]
    assert unchanged_state["last_seen_at"] != first_state["last_seen_at"]
    assert repository.pending_count() == 1

    assert repository.upsert_review_state(updated) is ReviewChange.UPDATE
    updated_state = repository.get_review_state("google_maps", original.source_review_id)
    assert updated_state["content_hash"] != first_state["content_hash"]
    assert repository.pending_count() == 2

    # Simulate a crash after atomic Parquet rename but before outbox acknowledgement.
    pending = repository._db.execute(
        "SELECT event_id, payload_json FROM pending_review_events ORDER BY created_at, event_id"
    ).fetchall()
    writer.write_batch(
        [(row["event_id"], json.loads(row["payload_json"])) for row in pending]
    )
    repository._db.close()

    recovered = ReviewRepository(
        state_path,
        run_id=RUN_ID,
        parquet_writer=ParquetReviewWriter(lake_root, RUN_ID, CRAWL_DATE, batch_size=100),
    )
    assert recovered.pending_count() == 0
    recovered.close()
    replayed_again = ReviewRepository(
        state_path,
        run_id=RUN_ID,
        parquet_writer=ParquetReviewWriter(lake_root, RUN_ID, CRAWL_DATE, batch_size=100),
    )
    replayed_again.close()

    parquet_files = sorted(lake_root.rglob("*.parquet"))
    assert len(parquet_files) == 1
    footer_ids = json.loads(
        (pq.ParquetFile(parquet_files[0]).metadata.metadata or {})[b"review_event_ids"]
    )
    assert len(footer_ids) == len(set(footer_ids)) == 2

    parquet_glob = (lake_root / "**" / "*.parquet").as_posix()
    connection = duckdb.connect(":memory:")
    try:
        rows = connection.execute(
            "SELECT * FROM read_parquet(?, union_by_name = true)", [parquet_glob]
        ).fetchdf()
        schema = connection.execute(
            "DESCRIBE SELECT * FROM read_parquet(?, union_by_name = true)",
            [parquet_glob],
        ).fetchall()
    finally:
        connection.close()

    assert len(rows) == 2
    assert rows["change_type"].tolist() == ["INSERT", "UPDATE"]
    assert rows["review_url"].tolist() == [original.review_url, updated.review_url]
    assert [list(urls) for urls in rows["image_urls"]] == [
        original.image_urls,
        updated.image_urls,
    ]
    event_identity_columns = [
        "run_id", "source", "source_review_id", "content_hash",
        "observed_at", "change_type",
    ]
    assert len(rows[event_identity_columns].drop_duplicates()) == 2
    expected_columns = list(ParquetReviewWriter.COLUMNS)
    assert list(rows.columns[: len(expected_columns)]) == expected_columns
    partition_columns = list(rows.columns[len(expected_columns) :])
    assert partition_columns == ["crawl_date"]
    assert rows["crawl_date"].astype(str).unique().tolist() == [CRAWL_DATE]

    with sqlite3.connect(state_path) as connection:
        connection.row_factory = sqlite3.Row
        state_rows = [dict(row) for row in connection.execute("SELECT * FROM review_state")]
        pending_count = connection.execute(
            "SELECT COUNT(*) FROM pending_review_events"
        ).fetchone()[0]

    result = {
        "output_root": str(output_root.resolve()),
        "state_db_path": str(state_path.resolve()),
        "parquet_glob": parquet_glob,
        "parquet_files": [str(path.resolve()) for path in parquet_files],
        "sqlite_state_rows": state_rows,
        "pending_outbox_rows": pending_count,
        "parquet_row_count": len(rows),
        "change_types": rows["change_type"].tolist(),
        "unique_event_identities": len(rows[event_identity_columns].drop_duplicates()),
        "duckdb_columns": [item[0] for item in schema],
        "duckdb_partition_columns": partition_columns,
    }
    output_root.mkdir(parents=True, exist_ok=True)
    (output_root / "validation_summary.json").write_text(
        json.dumps(result, indent=2) + "\n", encoding="utf-8"
    )
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-root", type=Path, required=True)
    args = parser.parse_args(argv)
    print(json.dumps(run_validation(args.output_root), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
