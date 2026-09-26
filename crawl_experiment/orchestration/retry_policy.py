from dataclasses import dataclass
from enum import StrEnum

from crawl_experiment.core.errors import (
    BrowserSessionError,
    ChallengeError,
    LimitedViewError,
    NavigationError,
    PaginationStalledError,
    RateLimitedError,
    ReviewParseError,
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
