from __future__ import annotations

import time
from datetime import datetime, timezone

from crawl_experiment.core.errors import (
    BrowserSessionError,
    ChallengeError,
    LimitedViewError,
    NavigationError,
    PlaceResolutionError,
    RateLimitedError,
    ReviewParseError,
    ReviewSurfaceError,
    SortError,
)
from crawl_experiment.core.models import CrawlResult, Store
from crawl_experiment.core.statuses import CrawlStatus
from crawl_experiment.observability.metrics import CrawlMetrics, MetricEvent
from crawl_experiment.storage.checkpoint_repository import (
    Checkpoint,
    CheckpointRepository,
)
from crawl_experiment.storage.review_repository import ReviewRepository

from . import selectors
from .extractor import ReviewExtractor
from .navigator import GoogleMapsNavigator
from .paginator import ReviewPaginator
from .review_surface import ReviewSurface


class GoogleMapsCrawler:
    """Thin source coordinator; recovery and browser lifecycle live elsewhere."""

    def __init__(
        self,
        reviews: ReviewRepository,
        checkpoints: CheckpointRepository,
        metrics: CrawlMetrics | None = None,
        *,
        max_scrolls: int = 1000,
        idle_limit: int = 8,
        timeout_seconds: float = 900,
        clock=time.monotonic,
    ):
        self.reviews = reviews
        self.checkpoints = checkpoints
        self.metrics = metrics or CrawlMetrics()
        self.max_scrolls = max_scrolls
        self.idle_limit = idle_limit
        self.timeout_seconds = timeout_seconds
        self.clock = clock

    def crawl(self, store: Store, driver, *, sort_newest=True) -> CrawlResult:
        result = CrawlResult(
            store.id,
            CrawlStatus.ERROR,
            started_at=datetime.now(timezone.utc),
        )
        self._checkpoint(result, "browser_started")
        try:
            navigator = GoogleMapsNavigator(driver)
            navigator.warm_up()
            self._checkpoint(result, "warm_up_completed")
            navigator.open_store(store)
            self._checkpoint(result, "store_resolved")

            surface = ReviewSurface(driver)
            review_pane = surface.open_reviews()
            self._checkpoint(result, "reviews_surface_opened")
            self._checkpoint(result, "review_pane_found")
            if sort_newest:
                surface.sort_newest()
                review_pane = surface.resolve_review_pane()
                self._checkpoint(result, "sort_newest_applied")

            paginator = ReviewPaginator(
                driver,
                review_pane,
                max_scrolls=self.max_scrolls,
                idle_limit=self.idle_limit,
                deadline=self.clock() + self.timeout_seconds,
                clock=self.clock,
            )
            extractor = ReviewExtractor()
            while True:
                cards = paginator.cards()
                paginator.observe(cards)
                self._extract_new_reviews(cards, store.id, extractor, result)
                result.reviews_seen = len(paginator.state.seen_ids)
                result.scroll_count = paginator.state.scroll_count
                self._checkpoint(result, "pagination", paginator.state.last_progress_at)
                reason = paginator.stop_reason()
                if reason:
                    break
                paginator.scroll()

            result.status = self._completion_status(reason, result.parse_errors)
            return result
        except Exception as exc:
            result.status, result.failure_stage = self._failure_status(exc)
            result.error = f"{type(exc).__name__}: {exc}"
            raise
        finally:
            result.finished_at = datetime.now(timezone.utc)
            self._checkpoint(result, result.failure_stage)

    def _extract_new_reviews(
        self,
        cards,
        store_id: str,
        extractor: ReviewExtractor,
        result: CrawlResult,
    ) -> None:
        for card in cards:
            review_id = card.get_attribute(selectors.REVIEW_ID_ATTRIBUTE)
            if not review_id or self.reviews.exists("google_maps", review_id):
                continue
            try:
                result.reviews_written += int(
                    self.reviews.upsert(extractor.extract(card, store_id))
                )
            except ReviewParseError:
                result.parse_errors += 1

    @staticmethod
    def _completion_status(reason: str | None, parse_errors: int) -> CrawlStatus:
        if reason == "timeout":
            return CrawlStatus.PARTIAL_TIMEOUT
        if reason in {"stalled", "max_scrolls"}:
            return CrawlStatus.PAGINATION_STALLED
        if parse_errors:
            return CrawlStatus.EXTRACTION_DEGRADED
        return CrawlStatus.COMPLETE

    def _checkpoint(
        self,
        result: CrawlResult,
        stage: str | None,
        last_progress_at: float | None = None,
    ) -> None:
        is_progress_event = result.finished_at is None and result.status is CrawlStatus.ERROR
        metric_status = "PROGRESS" if is_progress_event else result.status
        self.checkpoints.save(
            Checkpoint(
                result.store_id,
                result.status,
                result.reviews_seen,
                result.scroll_count,
                last_progress_at,
                stage,
                result.started_at.isoformat() if result.started_at else None,
                result.finished_at.isoformat() if result.finished_at else None,
            )
        )
        self.metrics.emit(
            MetricEvent(
                result.store_id,
                stage or "complete",
                metric_status,
                result.reviews_seen,
                result.scroll_count,
                parse_errors=result.parse_errors,
                error=result.error,
            )
        )

    @staticmethod
    def _failure_status(error: Exception) -> tuple[CrawlStatus, str]:
        if isinstance(error, BrowserSessionError):
            return CrawlStatus.SESSION_FAILED, "browser"
        if isinstance(error, ChallengeError):
            return CrawlStatus.CHALLENGE, "health"
        if isinstance(error, RateLimitedError):
            return CrawlStatus.RATE_LIMITED, "health"
        if isinstance(error, LimitedViewError):
            return CrawlStatus.LIMITED, "navigation"
        if isinstance(error, PlaceResolutionError):
            return CrawlStatus.PLACE_RESOLUTION_FAILED, "navigation"
        if isinstance(error, SortError):
            return CrawlStatus.SORT_FAILED, "review_surface"
        if isinstance(error, ReviewSurfaceError):
            return CrawlStatus.REVIEW_SURFACE_UNAVAILABLE, "review_surface"
        if isinstance(error, NavigationError):
            return CrawlStatus.NAVIGATION_FAILED, "navigation"
        return CrawlStatus.ERROR, "crawl"
