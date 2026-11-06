import json
import sqlite3

import pytest

from crawl_experiment.core.models import Review
from crawl_experiment.storage.review_repository import ReviewRepository
from crawl_experiment.observability.run_artifacts import CrawlArtifacts, build_summary
from crawl_experiment.runners.full_crawl_runner import create_run_directory, run_full_crawl
from scripts.coles_small_set_full_crawl import CONCURRENCY, STORES, build_parser


EXPECTED_NAMES = [
    "No.1 Malatang - World Square",
    "Central Ma La Town",
    "Zhang Liang Malatang Haymarket",
    "Coles Local Newport",
    "Coles Chullora",
    "Coles Local Sydney CBD - York Street",
    "Coles Berowra",
]


def test_runner_has_exact_requested_stores_in_order():
    assert [store.name for store in STORES] == EXPECTED_NAMES
    assert len({store.id for store in STORES}) == 7
    assert all(store.query == store.name for store in STORES)
    assert [store.retailer for store in STORES[:3]] == ["restaurant"] * 3
    assert all(store.retailer == "coles" for store in STORES[3:])
    assert CONCURRENCY == 1


def test_runner_can_select_chullora_for_bounded_validation():
    args = build_parser().parse_args(
        [
            "--store-id", "coles-chullora", "--max-scrolls", "10",
            "--timeout-seconds", "180", "--validation-mode",
        ]
    )
    assert args.store_id == ["coles-chullora"]
    assert args.max_scrolls == 10
    assert args.timeout_seconds == 180
    assert args.validation_mode is True


def test_runner_accepts_two_explicit_stores_in_order():
    args = build_parser().parse_args(
        [
            "--store-id", "coles-berowra",
            "--store-id", "coles-chullora",
        ]
    )
    assert args.store_id == ["coles-berowra", "coles-chullora"]


def test_thin_cli_delegates_selected_stores_and_options(monkeypatch):
    from scripts import coles_small_set_full_crawl as module

    captured = {}

    def fake_run(stores, **options):
        captured["stores"] = tuple(stores)
        captured["options"] = options
        return 7

    monkeypatch.setattr(module, "run_full_crawl", fake_run)
    result = module.main(
        [
            "--store-id", "coles-berowra",
            "--timeout-seconds", "180",
            "--max-scrolls", "10",
            "--validation-mode",
        ]
    )

    assert result == 7
    assert [store.id for store in captured["stores"]] == ["coles-berowra"]
    assert captured["options"]["timeout_seconds"] == 180
    assert captured["options"]["max_scrolls"] == 10
    assert captured["options"]["concurrency"] == 1
    assert captured["options"]["validation_mode"] is True


@pytest.mark.parametrize(
    ("stores", "options", "message"),
    [
        ((), {}, "stores must not be empty"),
        ((STORES[0],), {"timeout_seconds": 0}, "timeout_seconds must be positive"),
        ((STORES[0],), {"max_scrolls": 0}, "max_scrolls must be positive"),
        ((STORES[0],), {"concurrency": 0}, "concurrency must be positive"),
    ],
)
def test_reusable_runner_validates_generic_invariants_before_creating_artifacts(
    tmp_path, stores, options, message
):
    output_root = tmp_path / "runs"
    with pytest.raises(ValueError, match=message):
        run_full_crawl(stores, output_root=output_root, **options)
    assert not output_root.exists()


def test_run_directory_has_required_artifact_layout(tmp_path):
    run_directory = create_run_directory(tmp_path)

    assert run_directory.parent == tmp_path
    assert (run_directory / "checkpoints").is_dir()
    assert (run_directory / "metadata").is_dir()


def test_summary_uses_checkpoints_and_one_shared_database(tmp_path):
    run_directory = create_run_directory(tmp_path)
    first, second = STORES[:2]
    (run_directory / "checkpoints" / f"{first.id}.json").write_text(
        json.dumps(
            {
                "status": "COMPLETE",
                "reviews_seen": 2,
                "scroll_count": 3,
                "failure_stage": None,
            }
        ),
        encoding="utf-8",
    )
    repository = ReviewRepository(run_directory / "reviews.sqlite3")
    try:
        repository.upsert(Review("google_maps", "stable-1", first.id))
        repository.upsert(Review("google_maps", "stable-2", second.id))
        assert repository.upsert(
            Review("google_maps", "stable-1", first.id, text="updated")
        ) is False
    finally:
        repository.close()
    (run_directory / "metadata" / f"{first.id}.json").write_text(
        json.dumps(
            {
                "store_started_at": "start",
                "store_finished_at": "finish",
                "store_elapsed_seconds": 120.0,
                "attempts": 1,
            }
        ),
        encoding="utf-8",
    )
    (run_directory / "dlq.jsonl").write_text("", encoding="utf-8")

    summary = build_summary(
        run_directory,
        started_at="start",
        finished_at="finish",
        total_elapsed_seconds=120.0,
        stores=STORES,
        timeout_seconds=7_200,
        max_scrolls=5_000,
    )

    assert summary["configuration"]["sort"] == "newest"
    assert summary["configuration"]["concurrency"] == 1
    assert summary["total_reviews_persisted"] == 2
    assert summary["stores"][0]["status"] == "COMPLETE"
    assert summary["stores"][0]["reviews_persisted"] == 1
    assert summary["stores"][1]["reviews_persisted"] == 1
    assert summary["stores"][0]["store_elapsed_seconds"] == 120.0
    assert summary["stores"][0]["reviews_per_minute"] == 0.5
    assert summary["stores"][0]["attempts"] == 1
    assert summary["total_elapsed_seconds"] == 120.0
    assert summary["dlq_count"] == 0
    assert first.id not in summary["failed_store_ids"]
    with sqlite3.connect(run_directory / "reviews.sqlite3") as connection:
        assert connection.execute("SELECT COUNT(*) FROM reviews").fetchone()[0] == 2


def test_summary_exposes_non_error_max_scroll_terminal_reason(tmp_path):
    run_directory = create_run_directory(tmp_path)
    store = STORES[0]
    (run_directory / "checkpoints" / f"{store.id}.json").write_text(
        json.dumps(
            {
                "status": "PARTIAL_LIMIT",
                "reviews_seen": 110,
                "scroll_count": 10,
                "failure_stage": None,
            }
        ),
        encoding="utf-8",
    )
    (run_directory / "metadata" / f"{store.id}.json").write_text(
        json.dumps(
            {
                "final_status": "PARTIAL_LIMIT",
                "stop_reason": "max_scrolls",
                "configured_max_scrolls": 10,
            }
        ),
        encoding="utf-8",
    )
    (run_directory / "dlq.jsonl").write_text("", encoding="utf-8")

    summary = build_summary(
        run_directory,
        started_at="start",
        finished_at="finish",
        stores=(store,),
        timeout_seconds=7_200,
        max_scrolls=10,
    )

    item = summary["stores"][0]
    assert item["status"] == "PARTIAL_LIMIT"
    assert item["stop_reason"] == "max_scrolls"
    assert item["configured_max_scrolls"] == 10
    assert item["scroll_count"] == 10
    assert summary["partial_limit_count"] == 1
    assert summary["failed_store_count"] == 0
    assert summary["dlq_count"] == 0


def test_metadata_and_dlq_record_terminal_store_failure(tmp_path):
    from crawl_experiment.core.errors import ChallengeError
    from crawl_experiment.orchestration.retry_policy import RetryPolicy

    run_directory = create_run_directory(tmp_path)
    artifacts = CrawlArtifacts(run_directory, "start", stores=STORES)
    store = STORES[0]
    error = ChallengeError("challenge page")
    decision = RetryPolicy().decide(error, attempt=0)

    artifacts.on_attempt(store, 1)
    artifacts.on_terminal_failure(store, decision, 1, error)

    metadata = json.loads(
        (run_directory / "metadata" / f"{store.id}.json").read_text(
            encoding="utf-8"
        )
    )
    dlq = [
        json.loads(line)
        for line in (run_directory / "dlq.jsonl").read_text(
            encoding="utf-8"
        ).splitlines()
    ]
    assert metadata["attempts_started"] == 1
    assert metadata["status"] == "CHALLENGE"
    assert metadata["dlq"] is True
    assert dlq[0]["store_id"] == store.id
    assert dlq[0]["failure_reason_code"] == "CHALLENGE_DETECTED"

    summary = build_summary(
        run_directory,
        started_at="start",
        finished_at="finish",
        total_elapsed_seconds=10,
        stores=STORES,
        timeout_seconds=7_200,
        max_scrolls=5_000,
    )
    assert summary["dlq_count"] == 1
    assert store.id in summary["failed_store_ids"]


def test_search_phase_metrics_and_place_resolution_appear_in_metadata(tmp_path):
    run_directory = create_run_directory(tmp_path)
    artifacts = CrawlArtifacts(
        run_directory,
        "start",
        stores=(STORES[0],),
    )
    artifacts.on_activity(
        "search_phase_timing",
        {
            "store_id": STORES[0].id,
            "warmup_to_search_ms": 10.0,
            "search_to_resolved_ms": 20.0,
            "resolved_to_reviews_ms": 30.0,
            "reviews_to_classification_ms": 40.0,
            "final_access_state": "FULL",
        },
    )
    artifacts.on_activity(
        "place_resolution",
        {
            "store_id": STORES[0].id,
            "initial_url": "https://www.google.com/maps/search/example/",
            "navigation_state": "PLACE_ENTITY",
            "search_candidate_count": 2,
            "selected_candidate": {
                "text": STORES[0].name,
                "address": "World Square, Sydney",
                "url": "https://www.google.com/maps/place/example/",
            },
            "resolved_place_title": STORES[0].name,
            "resolved_place_address": "World Square, Sydney",
            "resolved_url": "https://www.google.com/maps/place/example/",
            "place_entity_confirmed": True,
        },
    )
    artifacts.on_activity(
        "review_access_after_entity_open",
        {
            "store_id": STORES[0].id,
            "access_state_after_place_resolution": "FULL",
        },
    )

    summary = build_summary(
        run_directory,
        started_at="start",
        finished_at="finish",
        stores=(STORES[0],),
        timeout_seconds=7_200,
        max_scrolls=5_000,
    )

    assert summary["stores"][0]["warmup_to_search_ms"] == 10.0
    assert summary["stores"][0]["search_to_resolved_ms"] == 20.0
    assert summary["stores"][0]["resolved_to_reviews_ms"] == 30.0
    assert summary["stores"][0]["reviews_to_classification_ms"] == 40.0
    assert summary["stores"][0]["final_access_state"] == "FULL"
    assert summary["stores"][0]["navigation_state"] == "PLACE_ENTITY"
    assert summary["stores"][0]["search_candidate_count"] == 2
    assert summary["stores"][0]["selected_candidate"]["text"] == STORES[0].name
    assert summary["stores"][0]["resolved_place_title"] == STORES[0].name
    assert summary["stores"][0]["resolved_place_address"] == "World Square, Sydney"
    assert summary["stores"][0]["place_entity_confirmed"] is True
    assert summary["stores"][0]["access_state_after_place_resolution"] == "FULL"
