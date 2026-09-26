from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from typing import Any

from crawl_experiment.browser.browser_factory import BrowserFactory
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
    ):
        self.browser_factory = browser_factory
        self.crawl_one = crawl_one
        self.retry_policy = retry_policy or RetryPolicy()
        self.max_concurrency = max_concurrency
        self.results: list[CrawlResult] = []

    async def run(self, stores: Sequence[Store]) -> Any:
        try:
            from crawlee import ConcurrencySettings, Request
            from crawlee.crawlers import BasicCrawler
            from crawlee.storages import RequestQueue
        except ImportError as exc:
            raise RuntimeError("Crawlee is required for orchestration; install requirements.txt") from exc
        stores_by_id = {store.id: store for store in stores}
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
            session = self.browser_factory.create(getattr(context.session, "id", store.id))
            try:
                with session:
                    self.results.append(self.crawl_one(store, session.driver))
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
                if decision.action in {RetryAction.RETRY_FRESH_SESSION, RetryAction.RETRY_NAVIGATION}:
                    if context.session:
                        context.session.retire()
                    raise
                if decision.action in {RetryAction.COOLDOWN, RetryAction.STOP} and context.session:
                    context.session.retire()

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
        return await crawler.run(purge_request_queue=False)
