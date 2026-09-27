from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from .browser_factory import BrowserFactory


@dataclass
class ManagedBrowser:
    instance_id: str
    session: Any
    driver: Any
    stores: set[str] = field(default_factory=set)


class BrowserSessionManager:
    """Owns one sequential guest browser for one crawler run."""

    def __init__(
        self,
        browser_factory: BrowserFactory,
        warm_up: Callable[[Any], None] | None = None,
        logger: logging.Logger | None = None,
    ):
        self.browser_factory = browser_factory
        self.warm_up = warm_up
        self.logger = logger or logging.getLogger(__name__)
        self.current: ManagedBrowser | None = None
        self._sequence = 0
        self._closed = False
        self.browsers_created = 0
        self.browsers_retired = 0
        self.browser_restarts = 0
        self.warm_up_count = 0
        self.stores_per_browser: dict[str, set[str]] = {}

    def acquire(self, store_id: str, attempt: int) -> ManagedBrowser:
        self._closed = False
        if self.current is not None and not self.is_alive():
            self.retire("session_lost", store_id=store_id, attempt=attempt)
        if self.current is None:
            self._create(store_id, attempt)
        else:
            self._record_store(self.current, store_id)
        self._event(
            "browser_reused",
            self.current,
            store_id=store_id,
            attempt=attempt,
        )
        return self.current

    def is_alive(self) -> bool:
        if self.current is None:
            return False
        checker = getattr(self.current.session, "is_alive", None)
        if checker is None:
            return self.current.driver is not None
        try:
            return bool(checker())
        except Exception:  # noqa: BLE001 - health checks must not escape
            return False

    def retire(
        self,
        reason: str,
        *,
        store_id: str | None = None,
        attempt: int | None = None,
    ) -> None:
        managed, self.current = self.current, None
        if managed is None:
            return
        self._event(
            "browser_restart_reason",
            managed,
            store_id=store_id,
            attempt=attempt,
            reason=reason,
        )
        self._close_session(managed.session)
        self.browsers_retired += 1
        if reason != "run_shutdown":
            self.browser_restarts += 1
        self._event(
            "browser_retired",
            managed,
            store_id=store_id,
            attempt=attempt,
            reason=reason,
        )

    def close(self) -> None:
        if self._closed:
            return
        self.retire("run_shutdown")
        self._closed = True

    def summary(self) -> dict[str, Any]:
        return {
            "browsers_created": self.browsers_created,
            "browsers_retired": self.browsers_retired,
            "browser_restarts": self.browser_restarts,
            "warm_up_count": self.warm_up_count,
            "stores_per_browser": {
                key: sorted(value) for key, value in self.stores_per_browser.items()
            },
        }

    def _create(self, store_id: str, attempt: int) -> None:
        self._sequence += 1
        instance_id = f"browser-{self._sequence:03d}"
        session = self.browser_factory.create(instance_id)
        try:
            starter = getattr(session, "start", None)
            driver = starter() if starter else session.__enter__().driver
            managed = ManagedBrowser(instance_id, session, driver)
            self.current = managed
            self.browsers_created += 1
            self.stores_per_browser[instance_id] = set()
            self._record_store(managed, store_id)
            self._event(
                "browser_created", managed, store_id=store_id, attempt=attempt
            )
            if self.warm_up:
                self._event(
                    "warm_up_started", managed, store_id=store_id, attempt=attempt
                )
                self.warm_up(driver)
                self.warm_up_count += 1
                self._event(
                    "warm_up_completed", managed, store_id=store_id, attempt=attempt
                )
        except Exception:
            if self.current is None:
                self.current = ManagedBrowser(instance_id, session, None)
            self.retire("warm_up_failed", store_id=store_id, attempt=attempt)
            raise

    def _record_store(self, managed: ManagedBrowser, store_id: str) -> None:
        managed.stores.add(store_id)
        self.stores_per_browser.setdefault(managed.instance_id, set()).add(store_id)

    @staticmethod
    def _close_session(session: Any) -> None:
        closer = getattr(session, "close", None)
        if closer:
            closer()
            return
        exit_method = getattr(session, "__exit__", None)
        if exit_method:
            exit_method(None, None, None)

    def _event(
        self,
        event: str,
        managed: ManagedBrowser,
        *,
        store_id: str | None = None,
        attempt: int | None = None,
        reason: str | None = None,
    ) -> None:
        self.logger.info(
            "event=%s browser_instance_id=%s store_id=%s attempt=%s reason=%s",
            event,
            managed.instance_id,
            store_id or "-",
            attempt if attempt is not None else "-",
            reason or "-",
        )
