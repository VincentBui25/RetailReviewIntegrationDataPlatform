import asyncio
import sys
from types import SimpleNamespace

from crawl_experiment.core.errors import (
    AuthRequiredError,
    BrowserSessionError,
    ChallengeError,
    PaginationStalledError,
    ReviewParseError,
    LimitedReviewViewError,
    SortError,
)
from crawl_experiment.core.models import CrawlResult, Review, Store
from crawl_experiment.core.statuses import CrawlStatus
from crawl_experiment.orchestration.crawlee_adapter import CrawleeAdapter
from crawl_experiment.orchestration.retry_policy import RetryAction, RetryPolicy
from crawl_experiment.sources.google_maps.crawler import GoogleMapsCrawler
from crawl_experiment.sources.google_maps.health import (
    HealthState,
    PageEvidence,
    classify_page,
)
from crawl_experiment.sources.google_maps.paginator import ReviewPaginator
from crawl_experiment.storage.review_repository import ReviewRepository


class Card:
    def __init__(self, review_id): self.review_id = review_id
    def get_attribute(self, name): return self.review_id if name == "data-review-id" else None


class Driver:
    def find_elements(self, *_): return []
    def execute_script(self, *_): pass


def test_crawlee_concurrency_desired_does_not_exceed_maximum(monkeypatch):
    captured = {}

    class ConcurrencySettings:
        def __init__(self, *, max_concurrency, desired_concurrency):
            assert desired_concurrency <= max_concurrency
            captured["maximum"] = max_concurrency
            captured["desired"] = desired_concurrency

    class RequestQueue:
        @classmethod
        async def open(cls):
            return cls()

        async def add_requests(self, requests):
            assert requests == []

    class BasicCrawler:
        def __init__(self, **kwargs):
            captured["settings"] = kwargs["concurrency_settings"]

        async def run(self, **_):
            return None

    monkeypatch.setitem(
        sys.modules,
        "crawlee",
        SimpleNamespace(ConcurrencySettings=ConcurrencySettings, Request=object),
    )
    monkeypatch.setitem(
        sys.modules,
        "crawlee.crawlers",
        SimpleNamespace(BasicCrawler=BasicCrawler),
    )
    monkeypatch.setitem(
        sys.modules,
        "crawlee.storages",
        SimpleNamespace(RequestQueue=RequestQueue),
    )

    adapter = CrawleeAdapter(SimpleNamespace(), lambda *_: None, max_concurrency=1)
    asyncio.run(adapter.run([]))

    assert captured["maximum"] == 1
    assert captured["desired"] == 1


def test_health_classification_is_semantic():
    assert classify_page("https://google.com/sorry/", "") is HealthState.RATE_LIMITED
    assert classify_page("https://google.com/maps", "verify you are not a robot") is HealthState.CHALLENGE
    assert classify_page(
        "https://google.com/maps",
        "limited view",
        evidence=PageEvidence(),
    ) is HealthState.LIMITED


def test_repository_dedupes_stable_source_identity():
    repo = ReviewRepository(":memory:")
    try:
        review = Review("google_maps", "stable-1", "store-1", author="A")
        assert repo.upsert(review) is True
        assert repo.upsert(review) is False
        assert repo.count() == 1
    finally: repo.close()


def test_paginator_requires_repeated_idle_iterations():
    paginator = ReviewPaginator(Driver(), object(), idle_limit=3)
    assert paginator.observe([Card("a")]) == 1
    assert paginator.observe([Card("a")]) == 0
    assert paginator.stop_reason() is None
    paginator.observe([Card("a")]); paginator.observe([Card("a")])
    assert paginator.stop_reason() == "stalled"


def test_paginator_timeout_is_distinct_from_stall():
    now = [10.0]
    paginator = ReviewPaginator(Driver(), object(), deadline=11, clock=lambda: now[0])
    paginator.observe([Card("a")]); now[0] = 12
    assert paginator.stop_reason() == "timeout"
    assert paginator.state.seen_ids == {"a"}


def test_retry_policy_does_not_restart_for_parse_or_stall():
    policy = RetryPolicy()
    assert policy.decide(ReviewParseError()).action is RetryAction.SKIP
    assert policy.decide(PaginationStalledError()).action is RetryAction.PRESERVE_PARTIAL
    assert policy.decide(BrowserSessionError()).action is RetryAction.RETRY_FRESH_SESSION
    assert policy.decide(ChallengeError()).action is RetryAction.STOP
    assert policy.decide(ReviewParseError()).status is CrawlStatus.EXTRACTION_DEGRADED
    assert policy.decide(LimitedReviewViewError()).status is CrawlStatus.LIMITED
    assert policy.decide(LimitedReviewViewError()).action is RetryAction.STOP
    assert policy.decide(AuthRequiredError()).status is CrawlStatus.AUTH_REQUIRED
    assert policy.decide(AuthRequiredError()).action is RetryAction.STOP


def test_sort_failure_gets_one_bounded_fresh_session_retry():
    policy = RetryPolicy(max_surface_retries=1)
    first = policy.decide(SortError("menu failed"), attempt=0)
    exhausted = policy.decide(SortError("menu failed"), attempt=1)

    assert first.action is RetryAction.RETRY_FRESH_SESSION
    assert first.reason == "sort_failed"
    assert exhausted.action is RetryAction.STOP
    assert exhausted.status is CrawlStatus.SORT_FAILED


def test_adapter_retries_sort_failure_with_fresh_session(monkeypatch):
    calls = []
    retired = []

    class ConcurrencySettings:
        def __init__(self, **kwargs): pass

    class Request:
        @classmethod
        def from_url(cls, url, unique_key, user_data):
            return SimpleNamespace(user_data=user_data)

    class RequestQueue:
        @classmethod
        async def open(cls): return cls()
        async def add_requests(self, requests): self.requests = requests

    class Session:
        def __init__(self, attempt):
            self.id, self.attempt, self.driver = str(attempt), attempt, object()
        def __enter__(self): return self
        def __exit__(self, *_): pass

    class BrowserFactory:
        def create(self, session_id):
            return Session(len(calls))

    class BasicCrawler:
        def __init__(self, **kwargs): self.handler = kwargs["request_handler"]
        async def run(self, **_):
            for attempt in (0, 1):
                session = SimpleNamespace(
                    id=str(attempt),
                    retire=lambda attempt=attempt: retired.append(attempt),
                )
                context = SimpleNamespace(
                    request=SimpleNamespace(
                        retry_count=attempt,
                        user_data={"store_id": "store-1"},
                    ),
                    session=session,
                )
                try:
                    await self.handler(context)
                    break
                except SortError:
                    continue

    monkeypatch.setitem(
        sys.modules,
        "crawlee",
        SimpleNamespace(
            ConcurrencySettings=ConcurrencySettings,
            Request=Request,
        ),
    )
    monkeypatch.setitem(sys.modules, "crawlee.crawlers", SimpleNamespace(BasicCrawler=BasicCrawler))
    monkeypatch.setitem(sys.modules, "crawlee.storages", SimpleNamespace(RequestQueue=RequestQueue))

    def crawl_one(store, driver):
        calls.append(store.id)
        if len(calls) == 1:
            raise SortError("sort menu failed")
        return CrawlResult(store.id, CrawlStatus.COMPLETE)

    adapter = CrawleeAdapter(BrowserFactory(), crawl_one, max_concurrency=1)
    asyncio.run(adapter.run([Store("store-1", "Store", "Store")]))

    assert calls == ["store-1", "store-1"]
    assert retired == [0]


def test_partial_timeout_preserves_extracted_review(monkeypatch):
    from crawl_experiment.sources.google_maps import crawler as module
    from tests.test_extractor import Card as ExtractableCard

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
            self.state = type("State", (), {"seen_ids": {"r-1"}, "scroll_count": 2, "last_progress_at": 1.0})()
        def cards(self): return [ExtractableCard("r-1")]
        def observe(self, cards): return 1
        def stop_reason(self): return "timeout"

    class Checkpoints:
        def __init__(self): self.saved = []
        def save(self, value): self.saved.append(value)

    class Metrics:
        def __init__(self): self.events = []
        def emit(self, value): self.events.append(value)

    monkeypatch.setattr(module, "GoogleMapsNavigator", Navigator)
    monkeypatch.setattr(module, "ReviewSurface", Surface)
    monkeypatch.setattr(module, "ReviewPaginator", Paginator)
    repo, checkpoints, metrics = ReviewRepository(":memory:"), Checkpoints(), Metrics()
    try:
        result = GoogleMapsCrawler(repo, checkpoints, metrics).crawl(Store("s", "Store", "Store"), object())
        assert result.status is CrawlStatus.PARTIAL_TIMEOUT
        assert result.reviews_written == 1 and repo.count() == 1
        assert checkpoints.saved[-1].status is CrawlStatus.PARTIAL_TIMEOUT
        assert [event.stage for event in metrics.events[:6]] == [
            "browser_started",
            "warm_up_completed",
            "store_resolved",
            "reviews_surface_opened",
            "review_pane_found",
            "sort_newest_applied",
        ]
        assert all(event.status == "PROGRESS" for event in metrics.events[:-1])
        assert metrics.events[-1].status is CrawlStatus.PARTIAL_TIMEOUT
    finally:
        repo.close()


def test_timeout_without_new_ids_is_not_reported_as_partial_growth():
    assert GoogleMapsCrawler._completion_status(
        "timeout", 0, timeout_had_growth=False
    ) is CrawlStatus.PAGINATION_STALLED


def test_timeout_with_new_ids_is_partial_timeout():
    assert GoogleMapsCrawler._completion_status(
        "timeout", 0, timeout_had_growth=True
    ) is CrawlStatus.PARTIAL_TIMEOUT


def test_max_scrolls_reached_is_partial_limit():
    assert GoogleMapsCrawler._completion_status(
        "max_scrolls", 0
    ) is CrawlStatus.PARTIAL_LIMIT


def test_no_growth_stall_is_pagination_stalled():
    assert GoogleMapsCrawler._completion_status(
        "stalled", 0
    ) is CrawlStatus.PAGINATION_STALLED


def test_natural_end_is_complete():
    assert GoogleMapsCrawler._completion_status(
        "natural_end", 0
    ) is CrawlStatus.COMPLETE


def test_no_reviews_finishes_without_sort_or_retry(monkeypatch):
    from crawl_experiment.sources.google_maps import crawler as module
    from crawl_experiment.sources.google_maps.review_surface import ReviewSurfaceState

    class Navigator:
        def __init__(self, driver): pass
        def warm_up(self): pass
        def open_store(self, store): return "resolved"

    class Surface:
        def __init__(self, driver): pass
        def prepare_and_classify(self):
            return ReviewSurfaceState.NO_REVIEWS, None, {"no_reviews_evidence": ["no reviews"]}
        def sort_newest(self):
            raise AssertionError("sort must not run")

    class Checkpoints:
        def save(self, value): pass

    class Metrics:
        def emit(self, value): pass

    monkeypatch.setattr(module, "GoogleMapsNavigator", Navigator)
    monkeypatch.setattr(module, "ReviewSurface", Surface)
    repository = ReviewRepository(":memory:")
    try:
        result = GoogleMapsCrawler(
            repository, Checkpoints(), Metrics()
        ).crawl(Store("empty", "Empty", "Empty"), object())
    finally:
        repository.close()

    assert result.status is CrawlStatus.NO_REVIEWS


def test_review_access_errors_are_not_sort_failed():
    assert GoogleMapsCrawler._failure_status(LimitedReviewViewError()) == (
        CrawlStatus.LIMITED,
        "review_surface",
    )
    assert GoogleMapsCrawler._failure_status(AuthRequiredError()) == (
        CrawlStatus.AUTH_REQUIRED,
        "review_surface",
    )
