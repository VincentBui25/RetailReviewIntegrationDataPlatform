from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field

from crawl_experiment.core.errors import PaginationStalledError

from . import selectors

logger = logging.getLogger(__name__)


@dataclass
class PaginationState:
    seen_ids: set[str] = field(default_factory=set)
    scroll_count: int = 0
    idle_count: int = 0
    last_progress_at: float = field(default_factory=time.monotonic)
    timed_out: bool = False
    non_scrollable: bool = False
    last_scroll_diagnostics: dict[str, object] = field(default_factory=dict)
    observation_started: bool = False


class ReviewPaginator:
    def __init__(
        self,
        driver,
        pane,
        *,
        max_scrolls: int = 1000,
        idle_limit: int = 8,
        deadline: float | None = None,
        advertised_count: int | None = None,
        clock=time.monotonic,
        wait_seconds: float = 1.0,
        wait_interval: float = 0.1,
        sleeper=time.sleep,
    ):
        self.driver = driver
        self.pane = pane
        self.max_scrolls = max_scrolls
        self.idle_limit = idle_limit
        self.deadline = deadline
        self.advertised_count = advertised_count
        self.clock = clock
        self.wait_seconds = wait_seconds
        self.wait_interval = wait_interval
        self.sleeper = sleeper
        self.state = PaginationState(last_progress_at=clock())

    def cards(self):
        pane_find_elements = getattr(self.pane, "find_elements", None)
        if pane_find_elements is not None:
            return pane_find_elements("css selector", selectors.REVIEW_CARD)
        return self.driver.find_elements("css selector", selectors.REVIEW_CARD)

    def observe(self, cards) -> int:
        ids = {c.get_attribute(selectors.REVIEW_ID_ATTRIBUTE) for c in cards}
        ids.discard(None)
        ids.discard("")
        new_count = len(ids - self.state.seen_ids)
        self.state.seen_ids.update(ids)
        if new_count:
            self.state.idle_count = 0
            self.state.last_progress_at = self.clock()
        elif self.state.observation_started:
            self.state.idle_count += 1
        self.state.observation_started = True
        return new_count

    def stop_reason(self) -> str | None:
        if self.deadline is not None and self.clock() >= self.deadline:
            self.state.timed_out = True
            return "timeout"
        if self.advertised_count and len(self.state.seen_ids) >= self.advertised_count:
            return "advertised_count"
        if self.state.scroll_count >= self.max_scrolls:
            return "max_scrolls"
        if self.state.non_scrollable:
            return "stalled"
        if self.state.idle_count >= self.idle_limit:
            return "stalled"
        return None

    def scroll(self) -> dict[str, object]:
        before_cards = self.cards()
        before_ids = self._review_ids(before_cards)
        before_metrics = self._pane_metrics()
        result = self.driver.execute_script(selectors.SCROLL_TO_END_SCRIPT, self.pane)
        self.state.scroll_count += 1
        after_metrics = self._metrics_from_script_result(result) or self._pane_metrics()
        after_cards = self._wait_for_dom_growth(before_ids)
        after_ids = self._review_ids(after_cards)
        new_ids = sorted(after_ids - self.state.seen_ids)
        moved = self._scroll_top_changed(before_metrics, after_metrics)
        if not moved and not new_ids:
            self.state.non_scrollable = True
        diagnostics = {
            "scroll_count": self.state.scroll_count,
            "scroll_top_before": before_metrics.get("scrollTop"),
            "scroll_top_after": after_metrics.get("scrollTop"),
            "scroll_height": after_metrics.get("scrollHeight"),
            "client_height": after_metrics.get("clientHeight"),
            "scroll_top_changed": moved,
            "cards_found": len(after_cards),
            "unique_review_ids": len(after_ids),
            "new_review_ids": len(new_ids),
            "idle_count": self.state.idle_count,
        }
        self.state.last_scroll_diagnostics = diagnostics
        logger.info("pagination scroll: %s", json.dumps(diagnostics, sort_keys=True))
        return diagnostics

    def require_progress(self) -> None:
        if self.stop_reason() == "stalled":
            raise PaginationStalledError("review pagination stopped progressing")

    def _wait_for_dom_growth(self, before_ids: set[str]):
        cards = self.cards()
        max_polls = max(1, int(self.wait_seconds / self.wait_interval))
        for _ in range(max_polls):
            if self._review_ids(cards) - before_ids:
                return cards
            self.sleeper(self.wait_interval)
            cards = self.cards()
        return cards

    def _pane_metrics(self) -> dict[str, float | None]:
        result = self.driver.execute_script(
            """
            const pane = arguments[0];
            return {
                scrollTop: pane.scrollTop,
                scrollHeight: pane.scrollHeight,
                clientHeight: pane.clientHeight,
            };
            """,
            self.pane,
        )
        return self._metrics_from_script_result(result)

    @staticmethod
    def _metrics_from_script_result(result) -> dict[str, float | None]:
        if isinstance(result, dict) and isinstance(result.get("after"), dict):
            result = result["after"]
        if not isinstance(result, dict):
            return {"scrollTop": None, "scrollHeight": None, "clientHeight": None}
        return {
            "scrollTop": result.get("scrollTop"),
            "scrollHeight": result.get("scrollHeight"),
            "clientHeight": result.get("clientHeight"),
        }

    @staticmethod
    def _scroll_top_changed(before, after) -> bool:
        return (
            before.get("scrollTop") is not None
            and after.get("scrollTop") is not None
            and before.get("scrollTop") != after.get("scrollTop")
        )

    @staticmethod
    def _review_ids(cards) -> set[str]:
        ids = {card.get_attribute(selectors.REVIEW_ID_ATTRIBUTE) for card in cards}
        ids.discard(None)
        ids.discard("")
        return ids
