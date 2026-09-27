from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ACCESS_STATES = {"FULL", "LIMITED", "AUTH_REQUIRED", "NO_REVIEWS", "UNKNOWN"}
DEGRADED_STATES = {"LIMITED", "AUTH_REQUIRED"}


class AccessBenchmark:
    """Run-scoped guest access timeline and logical activity counters."""

    def __init__(self, path: Path, run_id: str, *, clock=time.monotonic):
        self.path = path
        self.path.touch(exist_ok=True)
        self.run_id = run_id
        self.clock = clock
        self.run_started = clock()
        self.browser_started = self.run_started
        self.browser_instance_id: str | None = None
        self.store_id: str | None = None
        self.store_index: int | None = None
        self.attempt = 0
        self.initial_access_state: str | None = None
        self.access_state = "UNKNOWN"
        self.transition: dict[str, Any] | None = None
        self.stores_started = 0
        self.stores_completed = 0
        self.navigation_count = 0
        self.review_surface_open_count = 0
        self.sort_attempt_count = 0
        self.scroll_count = 0
        self.reviews_seen_count = 0
        self.reviews_persisted_count = 0
        self.browser_count = 0
        self.warm_up_count = 0
        self.retry_count = 0
        self.browser_restarted = False
        self.session_lost = False
        self.challenge_encountered = False
        self.network_identity_changed = False

    @property
    def should_stop(self) -> bool:
        return self.transition is not None

    def browser_created(self, instance_id: str) -> None:
        if self.browser_count:
            self.browser_restarted = True
        self.browser_count += 1
        self.browser_instance_id = instance_id
        self.browser_started = self.clock()

    def warm_up_completed(self) -> None:
        self.warm_up_count += 1

    def browser_lost(self, reason: str) -> None:
        self.session_lost = True
        self.record("run_stopped", reason=reason)

    def begin_store(self, store_id: str, store_index: int, attempt: int = 1) -> None:
        self.store_id, self.store_index, self.attempt = store_id, store_index, attempt
        self.stores_started += 1
        self.record("store_began", reason="STORE_BEGAN")

    def complete_store(self) -> None:
        self.stores_completed += 1

    def retry_navigation(self) -> None:
        self.retry_count += 1
        self.attempt += 1
        self.record("retry_navigation", reason="RETRY_NAVIGATION")

    def observe(self, event: str, details: dict[str, Any]) -> None:
        if event == "navigation":
            self.navigation_count += 1
        elif event == "review_surface_classified":
            if details.get("opened"):
                self.review_surface_open_count += 1
            self.classify(details.get("access_state", "UNKNOWN"), details.get("evidence"))
            return
        elif event == "sort_attempted":
            self.sort_attempt_count += 1
            self.record("sort_attempted", reason="SORT_ATTEMPTED")
        elif event == "sort_succeeded":
            self.record("sort_succeeded", reason="SORT_SUCCEEDED")
        elif event == "sort_failed":
            self.record("sort_failed", reason="SORT_FAILED", evidence=details)
        elif event == "scroll":
            self.scroll_count += 1
        elif event == "reviews_observed":
            self.reviews_seen_count += int(details.get("seen", 0))
            self.reviews_persisted_count += int(details.get("persisted", 0))

    def classify(self, state: str, evidence: Any = None) -> None:
        state = state if state in ACCESS_STATES else "UNKNOWN"
        previous = self.access_state
        if self.initial_access_state is None:
            self.initial_access_state = state
        self.access_state = state
        transition = f"{previous}_TO_{state}" if previous != state else None
        self.record(
            "review_surface_classified",
            previous_access_state=previous,
            transition=transition,
            reason=f"{state}_REVIEW_VIEW",
            evidence=evidence,
        )
        if previous == "FULL" and state in DEGRADED_STATES and self.transition is None:
            self.transition = self._snapshot(transition=transition)
            self.record(
                "access_degradation_first_detected",
                previous_access_state=previous,
                transition=transition,
                reason=f"FIRST_{state}_DETECTED",
                evidence=evidence,
            )

    def stop(self, reason: str) -> None:
        self.record("run_stopped", reason=reason)

    def record(self, event: str, **values: Any) -> None:
        value = self._snapshot(**values)
        value["event"] = event
        with self.path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(value, default=str, sort_keys=True) + "\n")
            stream.flush()

    def _snapshot(self, **values: Any) -> dict[str, Any]:
        now = self.clock()
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "run_id": self.run_id,
            "browser_instance_id": self.browser_instance_id,
            "store_id": self.store_id,
            "store_index": self.store_index,
            "attempt": self.attempt,
            "access_state": self.access_state,
            "previous_access_state": values.pop("previous_access_state", self.access_state),
            "transition": values.pop("transition", None),
            "elapsed_run_seconds": round(now - self.run_started, 3),
            "elapsed_browser_seconds": round(now - self.browser_started, 3),
            "stores_started": self.stores_started,
            "stores_completed": self.stores_completed,
            "cumulative_navigations": self.navigation_count,
            "cumulative_review_surface_opens": self.review_surface_open_count,
            "cumulative_sort_attempts": self.sort_attempt_count,
            "cumulative_scrolls": self.scroll_count,
            "cumulative_reviews_seen": self.reviews_seen_count,
            "cumulative_reviews_persisted": self.reviews_persisted_count,
            **values,
        }

    def summary(self) -> dict[str, Any]:
        transition = self.transition or {}
        return {
            "initial_access_state": self.initial_access_state or "UNKNOWN",
            "final_access_state": self.access_state,
            "access_transition_detected": self.transition is not None,
            "transition_type": transition.get("transition"),
            "transition_store_id": transition.get("store_id"),
            "transition_store_index": transition.get("store_index"),
            "transition_elapsed_seconds": transition.get("elapsed_run_seconds"),
            "stores_completed_before_transition": transition.get("stores_completed"),
            "navigations_before_transition": transition.get("cumulative_navigations"),
            "review_surface_opens_before_transition": transition.get("cumulative_review_surface_opens"),
            "sort_attempts_before_transition": transition.get("cumulative_sort_attempts"),
            "scrolls_before_transition": transition.get("cumulative_scrolls"),
            "reviews_seen_before_transition": transition.get("cumulative_reviews_seen"),
            "reviews_persisted_before_transition": transition.get("cumulative_reviews_persisted"),
            "browser_instance_id": self.browser_instance_id,
            "browser_count": self.browser_count,
            "warm_up_count": self.warm_up_count,
            "retry_count": self.retry_count,
            "browser_reused": self.browser_count == 1,
            "browser_restarted": self.browser_restarted,
            "network_identity_changed": self.network_identity_changed,
            "session_lost": self.session_lost,
            "challenge_encountered": self.challenge_encountered,
            "unexpected_retry_count": self.retry_count,
            "benchmark_valid": not self.browser_restarted and not self.session_lost,
        }
