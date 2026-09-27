import json
from datetime import datetime, timezone
from types import SimpleNamespace

from crawl_experiment.core.errors import ReviewSurfaceError, SortError
from crawl_experiment.core.failure_taxonomy import classify_exception, classify_status
from crawl_experiment.core.models import CrawlResult, Store
from crawl_experiment.core.statuses import CrawlStatus
from crawl_experiment.orchestration.retry_policy import RetryPolicy
from crawl_experiment.sources.google_maps.crawler import GoogleMapsCrawler
from crawl_experiment.storage.review_repository import ReviewRepository
from crawl_experiment.observability.run_artifacts import CrawlArtifacts
from crawl_experiment.runners.full_crawl_runner import create_run_directory
from tests.test_extractor import Card


class TickingClock:
    def __init__(self, step=0.1):
        self.value = 0.0
        self.step = step

    def __call__(self):
        value = self.value
        self.value += self.step
        return value


def test_scroll_metrics_and_progress_elapsed_are_real(monkeypatch, tmp_path):
    from crawl_experiment.sources.google_maps import crawler as module

    class Navigator:
        def __init__(self, driver): pass
        def warm_up(self): pass
        def open_store(self, store): return "resolved"

    class Surface:
        def __init__(self, driver): pass
        def prepare_and_classify(self):
            from crawl_experiment.sources.google_maps.review_surface import ReviewSurfaceState
            return ReviewSurfaceState.FULL, object(), {}
        def sort_newest(self): pass
        def resolve_review_pane(self): return object()

    class Paginator:
        def __init__(self, *args, **kwargs):
            self.calls = 0
            self.state = SimpleNamespace(
                seen_ids=set(), scroll_count=0, last_progress_at=0.0,
                last_new_ids=set(), idle_count=0,
            )
        def cards(self):
            return [Card("r-1")] if self.calls == 0 else [Card("r-1"), Card("r-2")]
        def observe(self, cards):
            ids = {card.get_attribute("data-review-id") for card in cards}
            self.state.last_new_ids = ids - self.state.seen_ids
            self.state.seen_ids.update(ids)
            return len(self.state.last_new_ids)
        def stop_reason(self): return "natural_end" if self.calls else None
        def scroll(self):
            self.calls += 1
            self.state.scroll_count += 1
            return {
                "client_height": 400, "scroll_height": 900,
                "scroll_top_before": 0, "scroll_top_after": 500,
                "scroll_top_changed": True, "wait_for_growth_ms": 12.5,
            }

    class Checkpoints:
        def save(self, value): pass

    class Metrics:
        def __init__(self): self.events = []
        def emit(self, value): self.events.append(value)

    monkeypatch.setattr(module, "GoogleMapsNavigator", Navigator)
    monkeypatch.setattr(module, "ReviewSurface", Surface)
    monkeypatch.setattr(module, "ReviewPaginator", Paginator)
    clock = TickingClock()
    metrics = Metrics()
    path = tmp_path / "scroll_metrics.jsonl"
    trace = tmp_path / "review_id_trace.jsonl"
    repo = ReviewRepository(":memory:")
    try:
        result = GoogleMapsCrawler(
            repo, Checkpoints(), metrics, clock=clock,
            wall_clock=lambda: datetime.now(timezone.utc), run_id="run-1",
            scroll_metrics_path=path, review_id_trace_path=trace,
        ).crawl(
            Store("store-1", "Store", "Store"),
            object(),
            elapsed_offset_seconds=5.0,
        )
    finally:
        repo.close()

    line = json.loads(path.read_text(encoding="utf-8").splitlines()[0])
    required = {
        "run_id", "store_id", "scroll_count", "scroll_started_at",
        "elapsed_since_store_start_seconds", "iteration_elapsed_ms",
        "reviews_before", "new_review_ids", "reviews_after", "cards_found",
        "client_height", "scroll_height", "scroll_top_before",
        "scroll_top_after", "scroll_top_changed", "idle_count",
        "locate_cards_ms", "dedupe_ms", "extract_new_reviews_ms",
        "persist_ms", "wait_for_growth_ms",
    }
    assert required <= line.keys()
    assert line["new_review_ids"] == 1
    elapsed = [event.elapsed_seconds for event in metrics.events]
    assert elapsed[0] == 5.0
    assert elapsed[-1] > elapsed[0]
    assert result.elapsed_seconds > 0
    traces = [json.loads(item) for item in trace.read_text(encoding="utf-8").splitlines()]
    assert {(item["review_id"], item["discovered_scroll"]) for item in traces} == {
        ("r-1", 0), ("r-2", 1)
    }


def test_failure_reason_taxonomy_for_intercepted_review_tab():
    classification = classify_exception(
        ReviewSurfaceError("element click intercepted by overlay")
    )
    assert classification.category == "UI_INTERACTION"
    assert classification.reason_code == "REVIEW_TAB_CLICK_INTERCEPTED"


def test_review_access_failure_taxonomy_and_no_reviews_success():
    limited = classify_status(CrawlStatus.LIMITED)
    auth = classify_status(CrawlStatus.AUTH_REQUIRED)

    assert (limited.category, limited.reason_code) == (
        "ACCESS", "LIMITED_REVIEW_VIEW"
    )
    assert (auth.category, auth.reason_code) == ("AUTH", "SIGN_IN_REQUIRED")
    assert classify_status(CrawlStatus.NO_REVIEWS) is None


def test_sort_failure_is_dead_lettered_once_and_complete_is_not(tmp_path):
    run_directory = create_run_directory(tmp_path)
    clock = TickingClock(step=2.0)
    artifacts = CrawlArtifacts(
        run_directory, "start", clock=clock, wall_clock=lambda: "wall"
    )
    failed = Store("failed", "Failed", "Failed", "retailer")
    complete = Store("complete", "Complete", "Complete", "retailer")
    for store in (failed, complete):
        artifacts._write_store(store, status="QUEUED", attempts=0)

    error = SortError("sort menu failed to open")
    decision = RetryPolicy(max_surface_retries=1).decide(error, attempt=1)
    artifacts.on_attempt(failed, 2)
    artifacts.on_terminal_failure(failed, decision, 2, error)
    artifacts.on_terminal_failure(failed, decision, 2, error)
    artifacts.on_attempt(complete, 1)
    artifacts.on_result(
        complete, CrawlResult("complete", CrawlStatus.COMPLETE), 1
    )

    rows = [json.loads(line) for line in artifacts.dlq_path.read_text(encoding="utf-8").splitlines()]
    assert len(rows) == 1
    assert rows[0]["failure_reason_code"] == "SORT_MENU_NOT_OPENED"
    assert rows[0]["attempts"] == 2
    assert rows[0]["elapsed_seconds"] > 0
    assert rows[0]["retryable"] is False


def test_validation_max_scroll_limit_is_not_dead_lettered(tmp_path):
    run_directory = create_run_directory(tmp_path)
    store = Store("validation", "Validation", "Validation", "retailer")
    artifacts = CrawlArtifacts(
        run_directory,
        "start",
        stores=(store,),
        quarantine_max_scrolls=False,
    )
    artifacts.on_attempt(store, 1)
    artifacts.on_result(
        store,
        CrawlResult(
            store.id,
            CrawlStatus.PARTIAL_LIMIT,
            reviews_seen=10,
            scroll_count=10,
            stop_reason="max_scrolls",
            configured_max_scrolls=10,
        ),
        1,
    )

    assert artifacts.dlq_path.read_text(encoding="utf-8") == ""
    metadata = json.loads(
        (run_directory / "metadata" / f"{store.id}.json").read_text(
            encoding="utf-8"
        )
    )
    assert metadata["validation_limit_reached"] is True
    assert metadata["stop_reason"] == "max_scrolls"
    assert metadata["configured_max_scrolls"] == 10
