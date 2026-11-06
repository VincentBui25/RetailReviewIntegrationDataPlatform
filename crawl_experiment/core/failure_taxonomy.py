from __future__ import annotations

from dataclasses import dataclass

from .errors import (
    BrowserSessionError,
    AuthRequiredError,
    ChallengeError,
    NavigationError,
    PlaceResolutionError,
    RateLimitedError,
    ReviewParseError,
    ReviewSurfaceError,
    LimitedReviewViewError,
    SortError,
)
from .statuses import CrawlStatus


@dataclass(frozen=True)
class FailureClassification:
    category: str
    reason_code: str
    retryable: bool


STATUS_FAILURES = {
    CrawlStatus.DEFERRED_LIMITED: FailureClassification("ACCESS_POLICY", "DEFERRED_LIMITED", True),
    CrawlStatus.DEFERRED_UNKNOWN: FailureClassification("ACCESS_POLICY", "DEFERRED_UNKNOWN", True),
    CrawlStatus.FAILED_BROWSER: FailureClassification("BROWSER", "FAILED_BROWSER", True),
    CrawlStatus.FAILED_RUNTIME: FailureClassification("RUNTIME", "FAILED_RUNTIME", True),
    CrawlStatus.FAILED_PARSE: FailureClassification("EXTRACTION", "FAILED_PARSE", False),
    CrawlStatus.AUTH_REQUIRED: FailureClassification(
        "AUTH", "SIGN_IN_REQUIRED", False
    ),
    CrawlStatus.PARTIAL_TIMEOUT: FailureClassification(
        "PAGINATION", "PAGINATION_TIMEOUT", True
    ),
    CrawlStatus.PAGINATION_STALLED: FailureClassification(
        "PAGINATION", "PAGINATION_NO_GROWTH", True
    ),
    CrawlStatus.EXTRACTION_DEGRADED: FailureClassification(
        "EXTRACTION", "EXTRACTION_FAILED", False
    ),
    CrawlStatus.SESSION_FAILED: FailureClassification(
        "SESSION", "SESSION_LOST", True
    ),
    CrawlStatus.NAVIGATION_FAILED: FailureClassification(
        "NAVIGATION", "NAVIGATION_FAILED", True
    ),
    CrawlStatus.PLACE_RESOLUTION_FAILED: FailureClassification(
        "NAVIGATION", "PLACE_NOT_RESOLVED", True
    ),
    CrawlStatus.LIMITED: FailureClassification(
        "ACCESS", "LIMITED_REVIEW_VIEW", False
    ),
    CrawlStatus.RATE_LIMITED: FailureClassification(
        "RATE_LIMIT", "RATE_LIMITED", True
    ),
    CrawlStatus.CHALLENGE: FailureClassification(
        "CHALLENGE", "CHALLENGE_DETECTED", False
    ),
    CrawlStatus.REVIEW_SURFACE_UNAVAILABLE: FailureClassification(
        "UI_INTERACTION", "REVIEW_SURFACE_NOT_FOUND", True
    ),
    CrawlStatus.SORT_FAILED: FailureClassification(
        "UI_INTERACTION", "SORT_MENU_NOT_OPENED", True
    ),
    CrawlStatus.ERROR: FailureClassification(
        "UNKNOWN", "UNKNOWN_ERROR", False
    ),
}


def classify_status(status: CrawlStatus | str) -> FailureClassification | None:
    status = CrawlStatus(status)
    if status in {
        CrawlStatus.COMPLETE,
        CrawlStatus.NO_REVIEWS,
        CrawlStatus.PARTIAL_LIMIT,
        CrawlStatus.SUCCESS_DOM,
        CrawlStatus.SUCCESS_NETWORK,
        CrawlStatus.SUCCESS_HYBRID,
    }:
        return None
    return STATUS_FAILURES.get(status)


def classify_exception(error: Exception) -> FailureClassification:
    if isinstance(error, AuthRequiredError):
        return STATUS_FAILURES[CrawlStatus.AUTH_REQUIRED]
    if isinstance(error, LimitedReviewViewError):
        return STATUS_FAILURES[CrawlStatus.LIMITED]
    if isinstance(error, SortError):
        return STATUS_FAILURES[CrawlStatus.SORT_FAILED]
    if isinstance(error, ReviewSurfaceError):
        reason = (
            "REVIEW_TAB_CLICK_INTERCEPTED"
            if "click intercepted" in str(error).casefold()
            else "REVIEW_SURFACE_NOT_FOUND"
        )
        return FailureClassification("UI_INTERACTION", reason, True)
    if isinstance(error, PlaceResolutionError):
        return STATUS_FAILURES[CrawlStatus.PLACE_RESOLUTION_FAILED]
    if isinstance(error, BrowserSessionError):
        return STATUS_FAILURES[CrawlStatus.SESSION_FAILED]
    if isinstance(error, RateLimitedError):
        return STATUS_FAILURES[CrawlStatus.RATE_LIMITED]
    if isinstance(error, ChallengeError):
        return STATUS_FAILURES[CrawlStatus.CHALLENGE]
    if isinstance(error, NavigationError):
        return STATUS_FAILURES[CrawlStatus.NAVIGATION_FAILED]
    if isinstance(error, ReviewParseError):
        return STATUS_FAILURES[CrawlStatus.EXTRACTION_DEGRADED]
    return STATUS_FAILURES[CrawlStatus.ERROR]
