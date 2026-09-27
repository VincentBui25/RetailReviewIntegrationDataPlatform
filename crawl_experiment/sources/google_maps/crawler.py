from __future__ import annotations

import time
import json
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from collections.abc import Callable

from crawl_experiment.core.errors import (
    BrowserSessionError,
    AuthRequiredError,
    ChallengeError,
    LimitedViewError,
    NavigationError,
    PlaceResolutionError,
    RateLimitedError,
    ReviewParseError,
    ReviewSurfaceError,
    LimitedReviewViewError,
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
from .review_surface import ReviewSurface, ReviewSurfaceState


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
        wall_clock=lambda: datetime.now(timezone.utc),
        run_id: str | None = None,
        scroll_metrics_path: str | Path | None = None,
        review_id_trace_path: str | Path | None = None,
        activity_observer: Callable[[str, dict], None] | None = None,
    ):
        self.reviews = reviews
        self.checkpoints = checkpoints
        self.metrics = metrics or CrawlMetrics()
        self.max_scrolls = max_scrolls
        self.idle_limit = idle_limit
        self.timeout_seconds = timeout_seconds
        self.clock = clock
        self.wall_clock = wall_clock
        self.run_id = run_id
        self.scroll_metrics_path = Path(scroll_metrics_path) if scroll_metrics_path else None
        self.review_id_trace_path = Path(review_id_trace_path) if review_id_trace_path else None
        self.activity_observer = activity_observer
        self._browser_warm_up_completed_at: float | None = None
        self._artifact_lock = Lock()

    def mark_browser_warm_up_completed(self) -> None:
        self._browser_warm_up_completed_at = self.clock()

    def crawl(
        self,
        store: Store,
        driver,
        *,
        sort_newest=True,
        elapsed_offset_seconds: float = 0.0,
        warm_up: bool = True,
    ) -> CrawlResult:
        started_monotonic = self.clock()
        elapsed_for_store = lambda: elapsed_offset_seconds + (
            self.clock() - started_monotonic
        )
        result = CrawlResult(
            store.id,
            CrawlStatus.ERROR,
            started_at=self.wall_clock(),
            configured_max_scrolls=self.max_scrolls,
        )
        self._checkpoint(
            result, "browser_started", elapsed_seconds=elapsed_offset_seconds
        )
        try:
            navigator = GoogleMapsNavigator(driver)
            if warm_up:
                navigator.warm_up()
                self.mark_browser_warm_up_completed()
                self._checkpoint(
                    result,
                    "warm_up_completed",
                    elapsed_seconds=elapsed_for_store(),
                )
            self._activity("navigation", store_id=store.id)
            search_started = self.clock()
            phase_times: dict[str, float | None] = {
                "warm_up_completed": self._browser_warm_up_completed_at,
                "search_started": search_started,
                "search_navigated": None,
                "resolved": None,
                "reviews_opened": None,
                "classified": None,
            }

            def after_search_navigation() -> None:
                phase_times["search_navigated"] = self.clock()

            navigator.after_search_navigation = after_search_navigation
            try:
                navigator.open_store(store)
            except Exception:
                self._activity(
                    "place_resolution",
                    store_id=store.id,
                    **getattr(navigator, "last_resolution_diagnostics", {}),
                )
                raise
            phase_times["resolved"] = self.clock()
            self._activity(
                "place_resolution",
                store_id=store.id,
                **getattr(navigator, "last_resolution_diagnostics", {}),
            )
            self._checkpoint(result, "store_resolved", elapsed_seconds=elapsed_for_store())

            surface = ReviewSurface(driver)

            def before_access_classification() -> None:
                phase_times["reviews_opened"] = self.clock()

            surface.before_access_classification = before_access_classification
            surface_state, review_pane, surface_evidence = (
                surface.prepare_and_classify()
            )
            phase_times["classified"] = self.clock()
            self._record_search_phase(store.id, phase_times, str(surface_state))
            self._activity(
                "review_access_after_entity_open",
                store_id=store.id,
                access_state_after_place_resolution=str(surface_state),
            )
            self._activity(
                "review_surface_classified",
                store_id=store.id,
                access_state=str(surface_state),
                evidence=surface_evidence,
                opened=review_pane is not None,
            )
            if surface_state is ReviewSurfaceState.NO_REVIEWS:
                result.status = CrawlStatus.NO_REVIEWS
                result.failure_stage = None
                return result
            if surface_state is ReviewSurfaceState.LIMITED:
                raise LimitedReviewViewError(
                    f"limited review surface: {surface_evidence}"
                )
            if surface_state is ReviewSurfaceState.AUTH_REQUIRED:
                raise AuthRequiredError(
                    f"review sign-in required: {surface_evidence}"
                )
            if review_pane is None:
                raise ReviewSurfaceError(
                    f"review surface state UNKNOWN: {surface_evidence}"
                )
            self._checkpoint(result, "reviews_surface_opened", elapsed_seconds=elapsed_for_store())
            self._checkpoint(result, "review_pane_found", elapsed_seconds=elapsed_for_store())
            if sort_newest:
                self._activity("sort_attempted", store_id=store.id)
                try:
                    surface.sort_newest()
                except Exception as exc:
                    degraded_state = (
                        ReviewSurfaceState.AUTH_REQUIRED
                        if isinstance(exc, AuthRequiredError)
                        else ReviewSurfaceState.LIMITED
                        if isinstance(exc, LimitedReviewViewError)
                        else None
                    )
                    if degraded_state is not None:
                        self._activity(
                            "review_surface_classified",
                            store_id=store.id,
                            access_state=str(degraded_state),
                            evidence={"sort_error": str(exc).splitlines()[0]},
                            opened=False,
                        )
                    self._activity(
                        "sort_failed",
                        store_id=store.id,
                        error=f"{type(exc).__name__}: {exc}",
                    )
                    raise
                review_pane = surface.resolve_review_pane()
                self._activity("sort_succeeded", store_id=store.id)
                self._checkpoint(result, "sort_newest_applied", elapsed_seconds=elapsed_for_store())

            paginator = ReviewPaginator(
                driver,
                review_pane,
                max_scrolls=self.max_scrolls,
                idle_limit=self.idle_limit,
                deadline=self.clock() + self.timeout_seconds,
                clock=self.clock,
            )
            extractor = ReviewExtractor()
            new_ids_in_last_observation = 0
            pending_scroll = None
            while True:
                locate_started = self.clock()
                cards = paginator.cards()
                locate_cards_ms = (self.clock() - locate_started) * 1000
                reviews_before = len(paginator.state.seen_ids)
                dedupe_started = self.clock()
                new_ids_in_last_observation = paginator.observe(cards)
                self._activity(
                    "reviews_observed",
                    store_id=store.id,
                    seen=new_ids_in_last_observation,
                    persisted=0,
                )
                dedupe_ms = (self.clock() - dedupe_started) * 1000
                new_ids = set(getattr(paginator.state, "last_new_ids", set()))
                if new_ids_in_last_observation and not new_ids:
                    new_ids = {
                        card.get_attribute(selectors.REVIEW_ID_ATTRIBUTE)
                        for card in cards
                    }
                    new_ids.discard(None)
                    new_ids.discard("")
                self._trace_review_ids(store.id, new_ids, paginator.state.scroll_count)
                extract_ms, persist_ms = self._extract_new_reviews(
                    cards, store.id, extractor, result, new_ids
                )
                result.reviews_seen = len(paginator.state.seen_ids)
                result.scroll_count = paginator.state.scroll_count
                elapsed = elapsed_for_store()
                idle_count = getattr(paginator.state, "idle_count", 0)
                if pending_scroll is not None:
                    self._write_scroll_metric(
                        {
                            **pending_scroll,
                            "iteration_elapsed_ms": round(
                                (self.clock() - pending_scroll.pop("_started")) * 1000,
                                3,
                            ),
                            "reviews_before": reviews_before,
                            "new_review_ids": new_ids_in_last_observation,
                            "reviews_after": result.reviews_seen,
                            "cards_found": len(cards),
                            "idle_count": idle_count,
                            "locate_cards_ms": round(locate_cards_ms, 3),
                            "dedupe_ms": round(dedupe_ms, 3),
                            "extract_new_reviews_ms": round(extract_ms, 3),
                            "persist_ms": round(persist_ms, 3),
                        }
                    )
                self._checkpoint(
                    result,
                    "pagination",
                    paginator.state.last_progress_at,
                    elapsed_seconds=elapsed,
                    new_reviews=new_ids_in_last_observation,
                    idle_count=idle_count,
                )
                reason = paginator.stop_reason()
                if reason:
                    result.stop_reason = reason
                    break
                scroll_started = self.clock()
                scroll_started_at = self.wall_clock().isoformat()
                diagnostics = paginator.scroll()
                self._activity("scroll", store_id=store.id)
                pending_scroll = {
                    "run_id": self.run_id,
                    "store_id": store.id,
                    "scroll_count": paginator.state.scroll_count,
                    "scroll_started_at": scroll_started_at,
                    "elapsed_since_store_start_seconds": round(
                        scroll_started - started_monotonic, 3
                    ),
                    "client_height": diagnostics.get("client_height"),
                    "scroll_height": diagnostics.get("scroll_height"),
                    "scroll_top_before": diagnostics.get("scroll_top_before"),
                    "scroll_top_after": diagnostics.get("scroll_top_after"),
                    "scroll_top_changed": diagnostics.get("scroll_top_changed"),
                    "wait_for_growth_ms": diagnostics.get("wait_for_growth_ms", 0.0),
                    "_started": scroll_started,
                }

            result.status = self._completion_status(
                reason,
                result.parse_errors,
                timeout_had_growth=new_ids_in_last_observation > 0,
            )
            return result
        except Exception as exc:
            result.status, result.failure_stage = self._failure_status(exc)
            result.error = f"{type(exc).__name__}: {exc}"
            raise
        finally:
            result.finished_at = self.wall_clock()
            result.elapsed_seconds = max(0.0, elapsed_for_store())
            self._checkpoint(
                result,
                result.failure_stage,
                elapsed_seconds=result.elapsed_seconds,
            )

    def _extract_new_reviews(
        self,
        cards,
        store_id: str,
        extractor: ReviewExtractor,
        result: CrawlResult,
        new_ids: set[str],
    ) -> tuple[float, float]:
        extract_seconds = 0.0
        persist_seconds = 0.0
        for card in cards:
            review_id = card.get_attribute(selectors.REVIEW_ID_ATTRIBUTE)
            if not review_id or review_id not in new_ids:
                continue
            try:
                extract_started = self.clock()
                review = extractor.extract(card, store_id)
                extract_seconds += self.clock() - extract_started
                persist_started = self.clock()
                change = self.reviews.upsert_review_state(review)
                emitted = change.emitted
                result.reviews_written += int(emitted)
                self._activity(
                    "reviews_observed",
                    store_id=store_id,
                    seen=0,
                    persisted=int(emitted),
                )
                persist_seconds += self.clock() - persist_started
            except ReviewParseError:
                result.parse_errors += 1
        return extract_seconds * 1000, persist_seconds * 1000

    def _activity(self, event: str, **details) -> None:
        if self.activity_observer is not None:
            self.activity_observer(event, details)

    def _record_search_phase(
        self,
        store_id: str,
        phases: dict[str, float | None],
        access_state: str,
    ) -> None:
        def elapsed_ms(start: str, end: str) -> float | None:
            left, right = phases.get(start), phases.get(end)
            if left is None or right is None:
                return None
            return round(max(0.0, right - left) * 1000, 3)

        self._activity(
            "search_phase_timing",
            store_id=store_id,
            warmup_to_search_ms=elapsed_ms("warm_up_completed", "search_started"),
            search_to_resolved_ms=elapsed_ms("search_navigated", "resolved"),
            resolved_to_reviews_ms=elapsed_ms("resolved", "reviews_opened"),
            reviews_to_classification_ms=elapsed_ms("reviews_opened", "classified"),
            final_access_state=access_state,
        )

    @staticmethod
    def _completion_status(
        reason: str | None,
        parse_errors: int,
        *,
        timeout_had_growth: bool = True,
    ) -> CrawlStatus:
        if reason == "timeout":
            return (
                CrawlStatus.PARTIAL_TIMEOUT
                if timeout_had_growth
                else CrawlStatus.PAGINATION_STALLED
            )
        if reason == "max_scrolls":
            return CrawlStatus.PARTIAL_LIMIT
        if reason == "stalled":
            return CrawlStatus.PAGINATION_STALLED
        if reason in {"natural_end", "advertised_count"}:
            return CrawlStatus.EXTRACTION_DEGRADED if parse_errors else CrawlStatus.COMPLETE
        if parse_errors:
            return CrawlStatus.EXTRACTION_DEGRADED
        return CrawlStatus.COMPLETE

    def _checkpoint(
        self,
        result: CrawlResult,
        stage: str | None,
        last_progress_at: float | None = None,
        *,
        elapsed_seconds: float | None = None,
        new_reviews: int = 0,
        idle_count: int = 0,
    ) -> None:
        if elapsed_seconds is not None:
            result.elapsed_seconds = max(0.0, elapsed_seconds)
        is_progress_event = result.finished_at is None and result.status is CrawlStatus.ERROR
        metric_status = "PROGRESS" if is_progress_event else result.status
        checkpoint = Checkpoint(
            result.store_id,
            result.status,
            result.reviews_seen,
            result.scroll_count,
            last_progress_at,
            stage,
            result.started_at.isoformat() if result.started_at else None,
            result.finished_at.isoformat() if result.finished_at else None,
            result.elapsed_seconds,
        )
        self.checkpoints.save(checkpoint)
        self.reviews.save_checkpoint_metadata(
            result.store_id,
            {
                "status": str(checkpoint.status),
                "reviews_seen": checkpoint.reviews_seen,
                "scroll_count": checkpoint.scroll_count,
                "last_progress_at": checkpoint.last_progress_at,
                "failure_stage": checkpoint.failure_stage,
                "started_at": checkpoint.started_at,
                "finished_at": checkpoint.finished_at,
                "elapsed_seconds": checkpoint.elapsed_seconds,
            },
        )
        self.metrics.emit(
            MetricEvent(
                result.store_id,
                stage or "complete",
                metric_status,
                result.reviews_seen,
                result.scroll_count,
                parse_errors=result.parse_errors,
                elapsed_seconds=result.elapsed_seconds,
                new_reviews=new_reviews,
                idle_count=idle_count,
                error=result.error,
            )
        )

    def _append_json_line(self, path: Path | None, value: dict) -> None:
        if path is None:
            return
        with self._artifact_lock, path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(value, default=str, sort_keys=True) + "\n")
            stream.flush()

    def _write_scroll_metric(self, value: dict) -> None:
        value.pop("_started", None)
        self._append_json_line(self.scroll_metrics_path, value)

    def _trace_review_ids(
        self, store_id: str, review_ids: set[str], discovered_scroll: int
    ) -> None:
        for review_id in sorted(review_ids):
            self._append_json_line(
                self.review_id_trace_path,
                {
                    "run_id": self.run_id,
                    "store_id": store_id,
                    "review_id": review_id,
                    "discovered_scroll": discovered_scroll,
                },
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
        if isinstance(error, LimitedReviewViewError):
            return CrawlStatus.LIMITED, "review_surface"
        if isinstance(error, AuthRequiredError):
            return CrawlStatus.AUTH_REQUIRED, "review_surface"
        if isinstance(error, PlaceResolutionError):
            return CrawlStatus.PLACE_RESOLUTION_FAILED, "navigation"
        if isinstance(error, SortError):
            return CrawlStatus.SORT_FAILED, "review_surface"
        if isinstance(error, ReviewSurfaceError):
            return CrawlStatus.REVIEW_SURFACE_UNAVAILABLE, "review_surface"
        if isinstance(error, NavigationError):
            return CrawlStatus.NAVIGATION_FAILED, "navigation"
        return CrawlStatus.ERROR, "crawl"
