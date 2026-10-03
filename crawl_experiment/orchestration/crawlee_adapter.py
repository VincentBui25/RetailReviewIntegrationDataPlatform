from __future__ import annotations

import logging
import uuid
from collections.abc import Callable, Sequence
from typing import Any

from crawl_experiment.browser.browser_factory import BrowserFactory
from crawl_experiment.browser.session_manager import BrowserSessionManager
from crawl_experiment.core.errors import BrowserSessionError, ChallengeError, LimitedReviewViewError, LimitedViewError, ReviewSurfaceError, ReviewParseError, NavigationError, PlaceResolutionError, CrawlError
from crawl_experiment.core.statuses import CrawlStatus
from crawl_experiment.core.models import CrawlResult, Store

from .retry_policy import RetryAction, RetryPolicy
from .access_policy import AccessDecision

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
        event_observer: Callable[[dict[str, Any]], None] | None = None,
        work_provider: Callable[[], Store | None] | None = None,
        worker_id: str = "worker-1",
        access_policy=None,
        on_deferred=None,
    ):
        self.browser_factory = browser_factory
        self.crawl_one = crawl_one
        self.retry_policy = retry_policy or RetryPolicy()
        self.max_concurrency = max_concurrency
        self.on_attempt = on_attempt
        self.on_result = on_result
        self.on_terminal_failure = on_terminal_failure
        self.browser_warm_up = browser_warm_up
        self.event_observer = event_observer
        self.work_provider = work_provider
        self.worker_id = worker_id
        self.access_policy=access_policy
        self.on_deferred=on_deferred
        self.access_job_metrics=[]
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
        if self.work_provider and stores:
            raise ValueError("Use either static stores or a dynamic work_provider, not both")
        browsers = BrowserSessionManager(
            self.browser_factory,
            warm_up=self.browser_warm_up,
            logger=logger,
            event_observer=self.event_observer,
            worker_id=self.worker_id,
        )
        job_terminal = False
        failure_counts={}
        async def handle(context: Any) -> None:
            nonlocal job_terminal
            store = stores_by_id[context.request.user_data["store_id"]]
            attempt = int(context.request.retry_count or 0)
            attempt_number = attempt + 1
            policy=self.access_policy
            counts=failure_counts.setdefault(store.id,{})
            if policy and not policy.eligible(store.id):
                row=policy.repository.get(store.id)
                status='DEFERRED_UNKNOWN' if row['last_access_state']=='UNKNOWN' else 'DEFERRED_LIMITED'
                decision=AccessDecision(row['last_access_state'],status,
                    backoff_seconds=max(0,(__import__('datetime').datetime.fromisoformat(row['next_eligible_at'])-policy.clock()).total_seconds()))
                metrics=policy.metrics(store.id,decision,0)
                self.access_job_metrics.append(metrics)
                if self.on_deferred:
                    self.on_deferred(store,metrics)
                job_terminal=True
                return
            if policy:
                policy.attempt(store.id)
            if self.on_attempt:
                self.on_attempt(store, attempt_number)
            try:
                managed = browsers.acquire(store.id, attempt_number)
                result = self.crawl_one(store, managed.driver)
                if policy:
                    policy.full(store.id,successful=not result.parse_errors)
                    network=getattr(result,'network_review_count',None)
                    fallback=getattr(result,'dom_fallback_count',None)
                    status='SUCCESS_DOM' if network is None else 'SUCCESS_HYBRID' if fallback else 'SUCCESS_NETWORK'
                    if result.parse_errors:
                        status='FAILED_PARSE'
                    metrics=policy.metrics(store.id,AccessDecision('FULL',status),attempt_number,network,fallback)
                    metrics['runtime_error_count']=counts.get('RUNTIME_ERROR',0)
                    metrics['browser_error_count']=counts.get('BROWSER_ERROR',0)
                    self.access_job_metrics.append(metrics)
                self.results.append(result)
                if self.on_result:
                    self.on_result(store, result, attempt_number)
                job_terminal = True
            except Exception as exc:
                decision = self.retry_policy.decide(exc, attempt)
                if policy:
                    state=None
                    if isinstance(exc,(LimitedReviewViewError,LimitedViewError)):
                        state='LIMITED'
                    elif isinstance(exc,ReviewSurfaceError) and 'state UNKNOWN:' in str(exc):
                        state='UNKNOWN'
                    elif isinstance(exc,BrowserSessionError) or (isinstance(exc,NavigationError) and not isinstance(exc,PlaceResolutionError)) or type(exc).__module__.startswith('selenium.common.exceptions'):
                        state='BROWSER_ERROR'
                    elif isinstance(exc,RuntimeError) and not isinstance(exc,CrawlError):
                        state='RUNTIME_ERROR'
                    if state:
                        access=policy.failure(store.id,state,counts)
                        counts[state]=counts.get(state,0)+1
                        metrics=policy.metrics(store.id,access,attempt_number)
                        metrics['runtime_error_count']=counts.get('RUNTIME_ERROR',0)
                        metrics['browser_error_count']=counts.get('BROWSER_ERROR',0)
                        self.access_job_metrics.append(metrics)
                        self._observe(dict(event='access_policy_decision',**metrics))
                        if access.retry_fresh:
                            from .retry_policy import RetryDecision
                            decision=RetryDecision(RetryAction.RETRY_FRESH_SESSION,decision.status,reason=state.lower())
                        elif access.job_status.startswith('DEFERRED_'):
                            browsers.retire('access_deferred',store_id=store.id,attempt=attempt_number)
                            if self.on_deferred:
                                self.on_deferred(store,metrics)
                            job_terminal=True
                            return
                        else:
                            from .retry_policy import RetryDecision
                            decision=RetryDecision(RetryAction.STOP,decision.status,reason=access.job_status)
                    elif isinstance(exc,ReviewParseError):
                        self.access_job_metrics.append(policy.metrics(store.id,AccessDecision('FULL','FAILED_PARSE'),attempt_number))
                self._observe(dict(event="retry_decision", store_id=store.id, attempt=attempt_number,
                                   browser_instance_id=browsers.current.instance_id if browsers.current else None,
                                   error_type=type(exc).__name__, error_message=str(exc).splitlines()[0],
                                   retry_reason=decision.reason, retry_action=str(decision.action),
                                   status=str(decision.status)))
                if isinstance(exc, ChallengeError):
                    self._observe(dict(event="google_challenge", store_id=store.id, attempt=attempt_number,
                                       browser_instance_id=browsers.current.instance_id if browsers.current else None,
                                       evidence={"error_type": type(exc).__name__, "message": str(exc).splitlines()[0]}))
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
                job_terminal = True

        def make_crawler(queue):
            return BasicCrawler(
                request_manager=queue,
                request_handler=handle,
                max_request_retries=max(
                    (1+self.access_policy.config.max_browser_retries+self.access_policy.config.max_runtime_retries) if self.access_policy else 0,
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

        async def run_queue(job_stores, *, isolated):
            # A dynamic job never opens the default queue or inherits its head.
            # Drop only after Crawlee.run has unwound, through the storage API.
            queue = await RequestQueue.open(name="benchmark-job-" + uuid.uuid4().hex) if isolated else await RequestQueue.open()
            try:
                await queue.add_requests([
                    Request.from_url(f"https://google.local/store/{store.id}", unique_key=store.id,
                                     user_data={"store_id": store.id}) for store in job_stores
                ])
                return await make_crawler(queue).run(purge_request_queue=False)
            finally:
                if isolated:
                    await queue.drop()

        try:
            if not self.work_provider:
                return await run_queue(stores, isolated=False)
            statistics = None
            while (store := self.work_provider()) is not None:
                stores_by_id = {store.id: store}
                job_terminal = False
                statistics = await run_queue([store], isolated=True)
                if not job_terminal:
                    raise RuntimeError(f"Crawlee job {store.id} ended without a terminal outcome")
                self._observe(dict(event="worker_finished_job", store_id=store.id))
            return statistics
        finally:
            browsers.close()
            self.lifecycle_summary = browsers.summary()

    def _observe(self, event: dict[str, Any]) -> None:
        if self.event_observer:
            try:
                self.event_observer(event)
            except Exception:  # Metrics hooks do not affect retry decisions.
                logger.warning("Crawler metrics observer failed", exc_info=True)
