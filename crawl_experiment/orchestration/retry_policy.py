from dataclasses import dataclass
from enum import StrEnum

from crawl_experiment.core.errors import (
    BrowserSessionError,
    AuthRequiredError,
    ChallengeError,
    LimitedViewError,
    NavigationError,
    PaginationStalledError,
    RateLimitedError,
    ReviewParseError,
    LimitedReviewViewError,
    SortError,
)
from crawl_experiment.core.statuses import CrawlStatus


class RetryAction(StrEnum):
    RETRY_FRESH_SESSION = "retry_fresh_session"
    RETRY_NAVIGATION = "retry_navigation"
    COOLDOWN = "cooldown"
    STOP = "stop"
    PRESERVE_PARTIAL = "preserve_partial"
    SKIP = "skip"


@dataclass(frozen=True)
class RetryDecision:
    action: RetryAction
    status: CrawlStatus
    delay_seconds: float = 0
    reason: str | None = None


class RetryPolicy:
    def __init__(
        self,
        *,
        max_session_retries: int = 2,
        max_navigation_retries: int = 2,
        max_surface_retries: int = 1,
        rate_limit_cooldown: float = 60,
    ):
        self.max_session_retries = max_session_retries
        self.max_navigation_retries = max_navigation_retries
        self.max_surface_retries = max_surface_retries
        self.rate_limit_cooldown = rate_limit_cooldown

    def decide(self, error: Exception, attempt: int = 0) -> RetryDecision:
        if isinstance(error, AuthRequiredError):
            return RetryDecision(RetryAction.STOP, CrawlStatus.AUTH_REQUIRED)
        if isinstance(error, LimitedReviewViewError):
            return RetryDecision(RetryAction.STOP, CrawlStatus.LIMITED)
        if isinstance(error, ReviewParseError):
            return RetryDecision(RetryAction.SKIP, CrawlStatus.EXTRACTION_DEGRADED)
        if isinstance(error, PaginationStalledError):
            return RetryDecision(RetryAction.PRESERVE_PARTIAL, CrawlStatus.PAGINATION_STALLED)
        if isinstance(error, SortError):
            action = (
                RetryAction.RETRY_FRESH_SESSION
                if attempt < self.max_surface_retries
                else RetryAction.STOP
            )
            return RetryDecision(action, CrawlStatus.SORT_FAILED, reason="sort_failed")
        if isinstance(error, ChallengeError):
            return RetryDecision(RetryAction.STOP, CrawlStatus.CHALLENGE)
        if isinstance(error, RateLimitedError):
            return RetryDecision(
                RetryAction.COOLDOWN,
                CrawlStatus.RATE_LIMITED,
                self.rate_limit_cooldown,
            )
        if isinstance(error, LimitedViewError):
            action = (
                RetryAction.RETRY_FRESH_SESSION
                if attempt < self.max_session_retries
                else RetryAction.STOP
            )
            return RetryDecision(action, CrawlStatus.LIMITED)
        if isinstance(error, BrowserSessionError):
            action = (
                RetryAction.RETRY_FRESH_SESSION
                if attempt < self.max_session_retries
                else RetryAction.STOP
            )
            return RetryDecision(action, CrawlStatus.SESSION_FAILED)
        if isinstance(error, NavigationError):
            action = (
                RetryAction.RETRY_NAVIGATION
                if attempt < self.max_navigation_retries
                else RetryAction.STOP
            )
            return RetryDecision(action, CrawlStatus.NAVIGATION_FAILED)
        return RetryDecision(RetryAction.STOP, CrawlStatus.ERROR)


class SmokeRetryPolicy(RetryPolicy):
    """Smoke/benchmark policy: one fresh-browser attempt per tagged LIMITED place."""
    def __init__(self):
        self.default_policy = RetryPolicy()
        # Crawlee's request ceiling needs room for one additional LIMITED retry
        # after ordinary navigation retries. All other decisions delegate to
        # the unchanged default policy, so their budgets do not increase.
        super().__init__(max_session_retries=self.default_policy.max_session_retries + 1)
        self.limited_retried_stores = set()

    def decide(self, error, attempt=0):
        store_id = getattr(error, "smoke_store_id", None)
        if isinstance(error, LimitedReviewViewError) and store_id is not None:
            if store_id not in self.limited_retried_stores:
                self.limited_retried_stores.add(store_id)
                return RetryDecision(RetryAction.RETRY_FRESH_SESSION, CrawlStatus.LIMITED,
                                     reason="smoke_limited_review_retry")
        return self.default_policy.decide(error, attempt)
