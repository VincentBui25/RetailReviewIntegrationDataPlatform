from __future__ import annotations

import logging
import time
from urllib.parse import quote_plus

from crawl_experiment.core.errors import (
    ChallengeError,
    LimitedViewError,
    NavigationError,
    PlaceResolutionError,
    RateLimitedError,
)
from crawl_experiment.core.models import Store

from . import selectors
from .health import HealthState, PageEvidence, classify_page, limited_signals

logger = logging.getLogger(__name__)


class GoogleMapsNavigator:
    def __init__(
        self,
        driver,
        *,
        base_url: str = "https://www.google.com",
        resolve_timeout_seconds: float = 8,
        poll_interval: float = 0.25,
        clock=time.monotonic,
        sleeper=time.sleep,
    ):
        self.driver = driver
        self.base_url = base_url.rstrip("/")
        self.resolve_timeout_seconds = resolve_timeout_seconds
        self.poll_interval = poll_interval
        self.clock = clock
        self.sleeper = sleeper

    def warm_up(self) -> None:
        try:
            self.driver.get(self.base_url)
        except Exception as exc:
            raise NavigationError(f"google.com warm-up failed: {exc}") from exc

    def open_store(self, store: Store) -> str:
        try:
            self.driver.get(f"{self.base_url}/maps/search/{quote_plus(store.query)}/")
            deadline = self.clock() + self.resolve_timeout_seconds
            while True:
                current_url = self.driver.current_url
                body = self.driver.find_element("tag name", "body").text
                evidence = self._page_evidence()
                state = classify_page(current_url, body, evidence=evidence)
                if state is not HealthState.DEGRADED or self.clock() >= deadline:
                    break
                self.sleeper(self.poll_interval)
        except (LimitedViewError, RateLimitedError, ChallengeError):
            raise
        except Exception as exc:
            raise NavigationError(f"Maps navigation failed: {exc}") from exc
        if state is HealthState.LIMITED:
            signals = limited_signals(body, evidence)
            diagnostics = {
                "url": current_url,
                "signals": signals,
                "place_tabs": evidence.has_place_tabs,
                "review_ui": evidence.has_review_ui,
                "place_shell": evidence.has_place_shell,
                "sign_in_prompt": evidence.has_sign_in_prompt,
            }
            logger.warning("Google Maps LIMITED evidence: %s", diagnostics)
            raise LimitedViewError(f"Google Maps limited view: {diagnostics}")
        if state is HealthState.RATE_LIMITED:
            raise RateLimitedError("Google rate limit page")
        if state is HealthState.CHALLENGE:
            raise ChallengeError("Google challenge page")
        if state is HealthState.DEGRADED:
            raise PlaceResolutionError(
                f"place did not resolve after {self.resolve_timeout_seconds}s; "
                f"url={current_url!r}"
            )
        haystack = f"{getattr(self.driver, 'title', '')} {body}".lower()
        tokens = [x for x in store.name.lower().split() if len(x) > 2]
        if tokens and not any(x in haystack for x in tokens):
            raise PlaceResolutionError(f"resolved page does not plausibly match {store.name}")
        return current_url

    def _page_evidence(self) -> PageEvidence:
        return PageEvidence(
            has_place_tabs=bool(
                self.driver.find_elements("css selector", selectors.PLACE_TABS)
            ),
            has_review_ui=bool(
                self.driver.find_elements("css selector", selectors.REVIEWS_TAB_CSS)
                or self.driver.find_elements("css selector", selectors.REVIEW_CARD)
            ),
            has_place_shell=bool(
                self.driver.find_elements("css selector", selectors.PLACE_SHELL)
            ),
            has_sign_in_prompt=bool(
                self.driver.find_elements("css selector", selectors.SIGN_IN_PROMPT)
            ),
        )
