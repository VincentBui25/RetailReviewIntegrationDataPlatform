from __future__ import annotations

import logging
import re
import time
from enum import StrEnum
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
from .health import HealthState, PageEvidence, classify_page

logger = logging.getLogger(__name__)


class NavigationState(StrEnum):
    SEARCH_RESULTS = "SEARCH_RESULTS"
    SEARCH_PREVIEW = "SEARCH_PREVIEW"
    PLACE_ENTITY = "PLACE_ENTITY"


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
        self.last_resolution_diagnostics: dict[str, object] = {}

    def warm_up(self) -> None:
        try:
            self.driver.get(self.base_url)
        except Exception as exc:
            raise NavigationError(f"google.com warm-up failed: {exc}") from exc

    def open_store(self, store: Store) -> str:
        candidate_clicked = False
        initial_url = None
        try:
            self.driver.get(f"{self.base_url}/maps/search/{quote_plus(store.query)}/")
            initial_url = self.driver.current_url
            after_navigation = getattr(self, "after_search_navigation", None)
            if after_navigation is not None:
                after_navigation()
            deadline = self.clock() + self.resolve_timeout_seconds
            while True:
                current_url = self.driver.current_url
                body = self.driver.find_element("tag name", "body").text
                evidence = self._page_evidence()
                state = classify_page(current_url, body, evidence=evidence)
                if state is HealthState.RATE_LIMITED:
                    raise RateLimitedError("Google rate limit page")
                if state is HealthState.CHALLENGE:
                    raise ChallengeError("Google challenge page")

                candidates = self._candidate_details()
                resolved = self._resolved_place(store, current_url, evidence, candidates)
                if resolved is not None:
                    self.last_resolution_diagnostics = {
                        "initial_url": initial_url,
                        "navigation_state": str(NavigationState.PLACE_ENTITY),
                        "search_candidate_count": (
                            self.last_resolution_diagnostics.get("candidate_count", 0)
                            if candidate_clicked
                            else len(candidates)
                        ),
                        "selected_candidate": self.last_resolution_diagnostics.get(
                            "selected_candidate"
                        ),
                        **resolved,
                    }
                    return current_url

                if not candidate_clicked:
                    selected = self._best_exact_candidate(store, candidates)
                    if selected is not None:
                        self.last_resolution_diagnostics = {
                            "initial_url": initial_url,
                            "navigation_state": str(NavigationState.SEARCH_RESULTS),
                            "candidate_count": len(candidates),
                            "selected_candidate": {
                                "text": selected["text"],
                                "address": selected["address"],
                                "url": selected["href"],
                            },
                            "resolved_place_title": None,
                            "resolved_place_address": None,
                            "resolved_url": None,
                            "place_entity_confirmed": False,
                        }
                        selected["element"].click()
                        candidate_clicked = True

                if self.clock() >= deadline:
                    break
                self.sleeper(self.poll_interval)
        except (LimitedViewError, RateLimitedError, ChallengeError):
            raise
        except Exception as exc:
            raise NavigationError(f"Maps navigation failed: {exc}") from exc
        navigation_state = (
            NavigationState.SEARCH_RESULTS if candidates else NavigationState.SEARCH_PREVIEW
        )
        self.last_resolution_diagnostics = {
            **self.last_resolution_diagnostics,
            "initial_url": initial_url,
            "navigation_state": str(navigation_state),
            "search_candidate_count": len(candidates),
            "selected_candidate": self.last_resolution_diagnostics.get(
                "selected_candidate"
            ),
            "resolved_place_title": self._place_title(),
            "resolved_place_address": self._place_address(),
            "resolved_url": current_url,
            "place_entity_confirmed": False,
        }
        raise PlaceResolutionError(
            f"exact place entity did not resolve after {self.resolve_timeout_seconds}s; "
            f"diagnostics={self.last_resolution_diagnostics}"
        )

    @staticmethod
    def _normalized(value: str | None) -> str:
        return " ".join(re.findall(r"[a-z0-9]+", (value or "").casefold()))

    def _place_title(self) -> str | None:
        elements = self.driver.find_elements("css selector", selectors.PLACE_TITLE)
        for element in elements:
            text = (getattr(element, "text", "") or element.get_attribute("aria-label") or "").strip()
            if text:
                return text
        return None

    def _place_address(self) -> str | None:
        elements = self.driver.find_elements("css selector", selectors.PLACE_ADDRESS)
        for element in elements:
            text = (getattr(element, "text", "") or element.get_attribute("aria-label") or "").strip()
            if text:
                return text
        return None

    def _candidate_details(self) -> list[dict[str, object]]:
        candidates = []
        seen = set()
        for element in self.driver.find_elements(
            "css selector", selectors.SEARCH_RESULT_CANDIDATES
        ):
            href = element.get_attribute("href") or ""
            text = (
                element.get_attribute("aria-label")
                or getattr(element, "text", "")
                or ""
            ).strip()
            address = (element.get_attribute("data-address") or "").strip() or None
            key = (href, text, address)
            if key in seen:
                continue
            seen.add(key)
            candidates.append(
                {"element": element, "href": href, "text": text, "address": address}
            )
        return candidates

    def _best_exact_candidate(
        self, store: Store, candidates: list[dict[str, object]]
    ) -> dict[str, object] | None:
        target = self._normalized(store.name)
        matches = [
            candidate
            for candidate in candidates
            if self._normalized(str(candidate["text"])) == target
        ]
        if store.expected_address:
            addressed = [
                candidate
                for candidate in matches
                if self._address_matches(
                    store.expected_address, str(candidate.get("address") or "")
                )
            ]
            if addressed:
                return addressed[0]
            if any(candidate.get("address") for candidate in matches):
                return None
        return matches[0] if matches else None

    def _address_matches(self, expected: str, actual: str) -> bool:
        expected_normalized = self._normalized(expected)
        actual_normalized = self._normalized(actual)
        return bool(
            expected_normalized
            and actual_normalized
            and (
                expected_normalized == actual_normalized
                or expected_normalized in actual_normalized
                or actual_normalized in expected_normalized
            )
        )

    def _resolved_place(
        self,
        store: Store,
        current_url: str,
        evidence: PageEvidence,
        candidates: list[dict[str, object]],
    ) -> dict[str, object] | None:
        title = self._place_title()
        address = self._place_address()
        title_matches = self._normalized(title) == self._normalized(store.name)
        address_matches = (
            not store.expected_address
            or self._address_matches(store.expected_address, address or "")
        )
        canonical_place_url = "/maps/place/" in (current_url or "").casefold()
        detail_pane = bool(
            self.driver.find_elements("css selector", selectors.PLACE_DETAIL_PANE)
        )
        entity_marker = bool(
            self.driver.find_elements("css selector", selectors.PLACE_ENTITY_MARKER)
        )
        entity_ui = bool(title and evidence.has_place_shell and detail_pane and entity_marker)
        if not (title_matches and address_matches and entity_ui):
            return None
        if not canonical_place_url:
            return None
        return {
            "resolved_place_title": title,
            "resolved_place_address": address,
            "resolved_url": current_url,
            "place_entity_confirmed": True,
        }

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
