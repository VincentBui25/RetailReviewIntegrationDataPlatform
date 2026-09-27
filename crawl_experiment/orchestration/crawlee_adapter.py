from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from typing import Any

from crawl_experiment.browser.browser_factory import BrowserFactory
from crawl_experiment.browser.session_manager import BrowserSessionManager
from crawl_experiment.core.errors import BrowserSessionError, ChallengeError
from crawl_experiment.core.models import CrawlResult, Store

from .retry_policy import RetryAction, RetryPolicy

logger = logging.getLogger(__name__)


class CrawleeAdapter:
    """Crawlee 1.x request lifecycle around project-owned SeleniumBase sessions.

    Crawlee never sees Google Maps selectors or WebDriver internals. Automatic
    request retries are disabled because RetryPolicy makes semantic decisions.
    """
    def __init__(
        self,
        browser_factory: BrowserFactory,
        crawl_one: Callable[[Store, Any], CrawlResult],
        retry_policy: RetryPolicy | None = None,
        *,
        max_concurrency: int = 1,
        on_attempt: Callable[[Store, int], None] | None = None,
        on_result: Callable[[Store, CrawlResult, int], None] | None = None,
        on_terminal_failure: Callable[[Store, Any, int, Exception], None] | None = None,
        browser_warm_up: Callable[[Any], None] | None = None,
    ):
        self.browser_factory = browser_factory
        self.crawl_one = crawl_one
        self.retry_policy = retry_policy or RetryPolicy()
        self.max_concurrency = max_concurrency
        self.on_attempt = on_attempt
        self.on_result = on_result
        self.on_terminal_failure = on_terminal_failure
        self.browser_warm_up = browser_warm_up
        self.results: list[CrawlResult] = []
        self.lifecycle_summary: dict[str, Any] = {}

    async def run(self, stores: Sequence[Store]) -> Any:
        if self.max_concurrency != 1:
            raise RuntimeError(
                "browser reuse currently requires max_concurrency=1"
            )
        try:
            from crawlee import ConcurrencySettings, Request
            from crawlee.crawlers import BasicCrawler
            from crawlee.storages import RequestQueue
        except ImportError as exc:
            raise RuntimeError("Crawlee is required for orchestration; install requirements.txt") from exc
        stores_by_id = {store.id: store for store in stores}
        browsers = BrowserSessionManager(
            self.browser_factory,
            warm_up=self.browser_warm_up,
            logger=logger,
        )
        queue = await RequestQueue.open()
        await queue.add_requests(
            [
                Request.from_url(
                    f"https://google.local/store/{store.id}",
                    unique_key=store.id,
                    user_data={"store_id": store.id},
                )
                for store in stores
            ]
        )

        async def handle(context: Any) -> None:
            store = stores_by_id[context.request.user_data["store_id"]]
            attempt = int(context.request.retry_count or 0)
            attempt_number = attempt + 1
            if self.on_attempt:
                self.on_attempt(store, attempt_number)
            try:
                managed = browsers.acquire(store.id, attempt_number)
                result = self.crawl_one(store, managed.driver)
                self.results.append(result)
                if self.on_result:
                    self.on_result(store, result, attempt_number)
            except Exception as exc:
                decision = self.retry_policy.decide(exc, attempt)
                logger.info(
                    "request attempt=%d error=%s action=%s status=%s reason=%s",
                    attempt + 1,
                    type(exc).__name__,
                    decision.action,
                    decision.status,
                    decision.reason or "unspecified",
                )
                browser_alive = browsers.is_alive()
                if isinstance(exc, ChallengeError):
                    browsers.retire(
                        "challenge", store_id=store.id, attempt=attempt_number
                    )
                    browser_alive = False
                elif isinstance(exc, BrowserSessionError) or not browser_alive:
                    browsers.retire(
                        "session_lost", store_id=store.id, attempt=attempt_number
                    )
                    browser_alive = False
                if decision.action is RetryAction.RETRY_FRESH_SESSION:
                    browsers.retire(
                        decision.reason or "fresh_session_retry",
                        store_id=store.id,
                        attempt=attempt_number,
                    )
                    if context.session:
                        context.session.retire()
                    raise
                if decision.action is RetryAction.RETRY_NAVIGATION:
                    raise
                if not browser_alive and context.session:
                    context.session.retire()
                if self.on_terminal_failure:
                    self.on_terminal_failure(store, decision, attempt_number, exc)

        crawler = BasicCrawler(
            request_manager=queue,
            request_handler=handle,
            max_request_retries=max(
                self.retry_policy.max_session_retries,
                self.retry_policy.max_navigation_retries,
                self.retry_policy.max_surface_retries,
            ),
            use_session_pool=True,
            retry_on_blocked=False,
            concurrency_settings=ConcurrencySettings(
                max_concurrency=self.max_concurrency,
                desired_concurrency=self.max_concurrency,
            ),
        )
        try:
            return await crawler.run(purge_request_queue=False)
        finally:
            browsers.close()
            self.lifecycle_summary = browsers.summary()
