import sqlite3

import pytest

pa = pytest.importorskip("pyarrow")
import pyarrow.parquet as pq

from crawl_experiment.core.models import Review
from crawl_experiment.storage.parquet_review_writer import ParquetReviewWriter
from crawl_experiment.storage.review_repository import (
    ReviewChange,
    ReviewRepository,
    review_content_hash,
)


def repository(tmp_path, run_id="run-1", batch_size=2, clock=None):
    writer = ParquetReviewWriter(tmp_path / "lake", run_id, "2026-09-27", batch_size=batch_size)
    kwargs = {"clock": clock} if clock else {}
    return ReviewRepository(
        tmp_path / "state" / "crawler_state.sqlite3",
        run_id=run_id,
        parquet_writer=writer,
        **kwargs,
    )


def rows(tmp_path):
    files = list((tmp_path / "lake").rglob("*.parquet"))
    if not files:
        return [], files
    return pa.concat_tables([pq.ParquetFile(path).read() for path in files]).to_pylist(), files


def test_new_same_and_changed_review_are_append_only_changes(tmp_path):
    moments = iter(["first", "second", "third"])
    repo = repository(tmp_path, batch_size=10, clock=lambda: next(moments))
    original = Review("google_maps", "r1", "s1", author="A", rating=5, text="great", displayed_date="3 months ago")
    assert repo.upsert(original) is True
    first = repo.get_review_state("google_maps", "r1")
    assert repo.upsert(original) is False
    assert repo.last_change_type is None
    changed = Review("google_maps", "r1", "s1", author="A", rating=4, text="edited", displayed_date="3 months ago")
    assert repo.upsert(changed) is False
    assert repo.last_change_type == "UPDATE"
    final = repo.get_review_state("google_maps", "r1")
    repo.close()

    payload, files = rows(tmp_path)
    assert [row["change_type"] for row in payload] == ["INSERT", "UPDATE"]
    assert len(files) == 1
    assert first["first_seen_at"] == final["first_seen_at"] == "first"
    assert final["last_seen_at"] == "third"
    assert payload[0]["review_date_raw"] == "3 months ago"
    assert payload[0]["content_hash"] == review_content_hash(original)


def test_explicit_change_result_disambiguates_legacy_boolean_contract(tmp_path):
    repo = repository(tmp_path, batch_size=10)
    original = Review("google_maps", "r1", "s1", text="one")
    changed = Review("google_maps", "r1", "s1", text="two")
    assert repo.upsert_review_state(original) is ReviewChange.INSERT
    assert repo.upsert_review_state(changed) is ReviewChange.UPDATE
    assert repo.upsert_review_state(changed) is ReviewChange.UNCHANGED
    repo.close()

    legacy = repository(tmp_path / "legacy", batch_size=10)
    assert legacy.upsert(original) is True
    assert legacy.upsert(changed) is False
    assert legacy.last_change_type == "UPDATE"
    legacy.close()


def test_state_unique_key_and_batch_lookup_persist_across_runs(tmp_path):
    repo = repository(tmp_path, "run-1")
    repo.upsert(Review("google_maps", "r1", "s1"))
    repo.close()
    second = repository(tmp_path, "run-2")
    assert second.get_known_review_ids(["r1", "missing"]) == {"r1"}
    assert second.upsert(Review("google_maps", "r1", "s1")) is False
    second.close()
    assert len(rows(tmp_path)[0]) == 1
    with sqlite3.connect(tmp_path / "state" / "crawler_state.sqlite3") as connection:
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                    "INSERT INTO review_state (source,source_review_id,store_id,content_hash,first_seen_at,last_seen_at,first_seen_run_id,last_seen_run_id) VALUES(?,?,?,?,?,?,?,?)",
                ("google_maps", "r1", "other", "hash", "a", "b", "x", "y"),
            )


def test_schema_partial_flush_and_empty_output(tmp_path):
    empty = repository(tmp_path / "empty", batch_size=10)
    empty.close()
    assert not list((tmp_path / "empty" / "lake").rglob("*.parquet"))

    repo = repository(tmp_path, batch_size=10)
    repo.upsert(Review("google_maps", "r1", "s1", text="one"))
    repo.close()
    payload, files = rows(tmp_path)
    assert len(payload) == 1 and len(files) == 1
    assert pq.read_schema(files[0]).names == list(ParquetReviewWriter.COLUMNS)


def test_repeated_batch_write_uses_stable_file(tmp_path):
    writer = ParquetReviewWriter(tmp_path, "run-1", "2026-09-27")
    payload = {column: None for column in writer.COLUMNS}
    payload.update({"run_id": "run-1", "source": "google_maps", "source_review_id": "r1", "change_type": "INSERT"})
    first, _ = writer.write_batch([("event-1", payload)])
    second, _ = writer.write_batch([("event-1", payload)])
    assert first == second
    assert len(list(tmp_path.rglob("*.parquet"))) == 1
    assert pq.ParquetFile(first).read().num_rows == 1


def test_state_and_outbox_rollback_together(tmp_path):
    repo = repository(tmp_path, batch_size=10)
    repo._db.execute(
        """CREATE TRIGGER reject_outbox BEFORE INSERT ON pending_review_events
           BEGIN SELECT RAISE(ABORT, 'outbox rejected'); END"""
    )
    with pytest.raises(sqlite3.IntegrityError, match="outbox rejected"):
        repo.upsert_review_state(Review("google_maps", "r1", "s1", text="one"))
    assert repo.get_review_state("google_maps", "r1") is None
    assert repo.pending_count() == 0
    repo._db.close()


def test_startup_replay_recognizes_existing_batch_without_duplication(tmp_path):
    repo = repository(tmp_path, batch_size=10)
    repo.upsert_review_state(Review("google_maps", "r1", "s1", text="one"))
    pending = repo._db.execute(
        "SELECT event_id, payload_json FROM pending_review_events"
    ).fetchone()
    import json

    repo.parquet_writer.write_batch([(pending["event_id"], json.loads(pending["payload_json"]))])
    repo._db.close()  # simulate crash after rename and before outbox acknowledgement

    replay = repository(tmp_path, batch_size=10)
    assert replay.pending_count() == 0
    replay.close()
    again = repository(tmp_path, batch_size=10)
    again.close()
    assert len(list((tmp_path / "lake").rglob("*.parquet"))) == 1


def test_unrelated_event_sets_have_distinct_verified_filenames(tmp_path):
    writer = ParquetReviewWriter(tmp_path, "run-1", "2026-09-27")
    payload = {column: None for column in writer.COLUMNS}
    first, _ = writer.write_batch([("a" * 64, payload)])
    second, _ = writer.write_batch([("b" * 64, payload)])
    assert first != second
    assert len(first.stem.removeprefix("part-")) == 20
    assert len(second.stem.removeprefix("part-")) == 20


def test_filename_collision_is_detected_instead_of_acknowledging_wrong_batch(tmp_path, monkeypatch):
    writer = ParquetReviewWriter(tmp_path, "run-1", "2026-09-27")
    payload = {column: None for column in writer.COLUMNS}
    monkeypatch.setattr(writer, "_fingerprint", lambda _: "f" * 20)
    writer.write_batch([("event-a", payload)])
    with pytest.raises(RuntimeError, match="filename collision"):
        writer.write_batch([("event-b", payload)])


def test_review_media_schema_hash_policy_and_outbox_replay(tmp_path):
    repo = repository(tmp_path, batch_size=10)
    original = Review(
        "google_maps", "r1", "s1", text="one",
        review_url="https://www.google.com/maps/reviews/data=first",
        image_urls=[
            "https://images.example/two.jpg",
            "https://images.example/one.jpg",
            "https://images.example/two.jpg",
        ],
    )
    url_only_change = Review(
        "google_maps", "r1", "s1", text="one",
        review_url="https://www.google.com/maps/reviews/data=second",
        image_urls=[
            "https://images.example/one.jpg",
            "https://images.example/two.jpg",
        ],
    )
    image_change = Review(
        "google_maps", "r1", "s1", text="one",
        review_url=url_only_change.review_url,
        image_urls=[
            "https://images.example/one.jpg",
            "https://images.example/two.jpg",
            "https://images.example/three.jpg",
        ],
    )

    assert repo.upsert_review_state(original) is ReviewChange.INSERT
    assert review_content_hash(original) == review_content_hash(url_only_change)
    assert repo.upsert_review_state(url_only_change) is ReviewChange.UNCHANGED
    assert review_content_hash(image_change) != review_content_hash(original)
    assert repo.upsert_review_state(image_change) is ReviewChange.UPDATE
    repo.close()

    payload, files = rows(tmp_path)
    assert pq.read_schema(files[0]).field("review_url").type == pa.string()
    assert pq.read_schema(files[0]).field("image_urls").type == pa.list_(pa.string())
    assert [row["change_type"] for row in payload] == ["INSERT", "UPDATE"]
    assert payload[0]["review_url"] == original.review_url
    assert payload[0]["image_urls"] == [
        "https://images.example/two.jpg",
        "https://images.example/one.jpg",
    ]
    assert payload[1]["image_urls"] == image_change.image_urls


def test_image_removal_or_replacement_emits_update_but_reordering_does_not(tmp_path):
    repo = repository(tmp_path, batch_size=10)
    original = Review(
        "google_maps", "r1", "s1",
        image_urls=["https://images.example/one.jpg", "https://images.example/two.jpg"],
    )
    reordered = Review(
        "google_maps", "r1", "s1",
        image_urls=["https://images.example/two.jpg", "https://images.example/one.jpg"],
    )
    removed = Review(
        "google_maps", "r1", "s1",
        image_urls=["https://images.example/one.jpg"],
    )
    replaced = Review(
        "google_maps", "r1", "s1",
        image_urls=["https://images.example/three.jpg"],
    )

    assert repo.upsert_review_state(original) is ReviewChange.INSERT
    assert repo.upsert_review_state(reordered) is ReviewChange.UNCHANGED
    assert repo.upsert_review_state(removed) is ReviewChange.UPDATE
    assert repo.upsert_review_state(replaced) is ReviewChange.UPDATE
    repo.close()

    payload, _ = rows(tmp_path)
    assert [row["change_type"] for row in payload] == ["INSERT", "UPDATE", "UPDATE"]
    assert payload[0]["image_urls"] == original.image_urls
    assert payload[1]["image_urls"] == removed.image_urls
    assert payload[2]["image_urls"] == replaced.image_urls


def test_current_run_change_counters_are_mutually_exclusive(tmp_path):
    first_run = repository(tmp_path, run_id="run-1", batch_size=1000)
    reviews = [
        Review("google_maps", f"r{index}", "s1", text=f"review {index}")
        for index in range(60)
    ]
    for review in reviews:
        first_run.upsert_review_state(review)
    assert first_run.stats()["new_reviews_written"] == 60
    assert first_run.stats()["changed_reviews_written"] == 0
    assert first_run.stats()["unchanged_reviews_seen"] == 0
    first_run.close()

    second_run = repository(tmp_path, run_id="run-2", batch_size=1000)
    for review in reviews:
        second_run.upsert_review_state(review)
    assert second_run.stats()["new_reviews_written"] == 0
    assert second_run.stats()["changed_reviews_written"] == 0
    assert second_run.stats()["unchanged_reviews_seen"] == 60
    second_run.close()

    mixed_run = repository(tmp_path, run_id="run-3", batch_size=1000)
    for review in reviews[:20]:
        mixed_run.upsert_review_state(review)
    for review in reviews[20:30]:
        mixed_run.upsert_review_state(
            Review(review.source, review.source_review_id, review.store_id, text=f"{review.text} edited")
        )
    for index in range(60, 65):
        mixed_run.upsert_review_state(
            Review("google_maps", f"r{index}", "s1", text=f"review {index}")
        )
    stats = mixed_run.stats()
    assert stats["new_reviews_written"] == 5
    assert stats["changed_reviews_written"] == 10
    assert stats["unchanged_reviews_seen"] == 20
    assert stats["parquet_rows_written"] == 0
    mixed_run.close()


def test_old_and_new_parquet_schema_union_by_name(tmp_path):
    duckdb = pytest.importorskip("duckdb")
    old_path = tmp_path / "crawl_date=2026-09-26" / "run_id=old" / "part-old.parquet"
    old_path.parent.mkdir(parents=True)
    old_schema = pa.schema([
        field for field in ParquetReviewWriter.schema()
        if field.name not in {"review_url", "image_urls"}
    ])
    pq.write_table(pa.Table.from_pylist([{
        field.name: None for field in old_schema
    }], schema=old_schema), old_path)

    writer = ParquetReviewWriter(tmp_path, "new", "2026-09-27")
    payload = {column: None for column in writer.COLUMNS}
    payload.update({
        "run_id": "new", "source": "google_maps", "source_review_id": "r1",
        "image_urls": ["https://images.example/one.jpg"], "change_type": "INSERT",
    })
    writer.write_batch([("event-new", payload)])

    connection = duckdb.connect(":memory:")
    try:
        result = connection.execute(
            "SELECT review_url, image_urls FROM read_parquet(?, union_by_name = true) ORDER BY run_id",
            [(tmp_path / "**" / "*.parquet").as_posix()],
        ).fetchall()
    finally:
        connection.close()
    assert result == [(None, ["https://images.example/one.jpg"]), (None, None)]
