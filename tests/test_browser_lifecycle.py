import asyncio
import sys
from types import SimpleNamespace

import pytest

from crawl_experiment.browser.session_manager import BrowserSessionManager
from crawl_experiment.core.errors import (
    BrowserSessionError,
    ChallengeError,
    NavigationError,
    SortError,
)
from crawl_experiment.core.models import CrawlResult, Store
from crawl_experiment.core.statuses import CrawlStatus
from crawl_experiment.orchestration.crawlee_adapter import CrawleeAdapter
from crawl_experiment.orchestration.retry_policy import RetryPolicy


class FakeDriver:
    def __init__(self, number):
        self.number = number


class FakeSession:
    def __init__(self, number):
        self.driver = FakeDriver(number)
        self.alive = True
        self.close_count = 0

    def start(self):
        return self.driver

    def is_alive(self):
        return self.alive

    def close(self):
        self.close_count += 1
        self.alive = False


class FakeFactory:
    def __init__(self):
        self.sessions = []

    def create(self, _session_id):
        session = FakeSession(len(self.sessions) + 1)
        self.sessions.append(session)
        return session


def install_fake_crawlee(monkeypatch, *, crash_after_handlers=False):
    class ConcurrencySettings:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

    class Request:
        @classmethod
        def from_url(cls, url, unique_key, user_data):
            return SimpleNamespace(url=url, unique_key=unique_key, user_data=user_data)

    class RequestQueue:
        requests = []

        @classmethod
        async def open(cls):
            cls.requests = []
            return cls()

        async def add_requests(self, requests):
            type(self).requests = list(requests)

    class BasicCrawler:
        def __init__(self, **kwargs):
            self.handler = kwargs["request_handler"]
            self.max_retries = kwargs["max_request_retries"]

        async def run(self, **_):
            for request in RequestQueue.requests:
                attempt = 0
                while True:
                    context_session = SimpleNamespace(
                        id=f"crawlee-{attempt}", retire=lambda: None
                    )
                    context = SimpleNamespace(
                        request=SimpleNamespace(
                            user_data=request.user_data,
                            retry_count=attempt,
                        ),
                        session=context_session,
                    )
                    try:
                        await self.handler(context)
                        break
                    except Exception:
                        if attempt >= self.max_retries:
                            break
                        attempt += 1
            if crash_after_handlers:
                raise RuntimeError("crawler crashed")

    monkeypatch.setitem(
        sys.modules,
        "crawlee",
        SimpleNamespace(ConcurrencySettings=ConcurrencySettings, Request=Request),
    )
    monkeypatch.setitem(
        sys.modules, "crawlee.crawlers", SimpleNamespace(BasicCrawler=BasicCrawler)
    )
    monkeypatch.setitem(
        sys.modules,
        "crawlee.storages",
        SimpleNamespace(RequestQueue=RequestQueue),
    )


def stores():
    return [
        Store("store-a", "Store A", "Store A"),
        Store("store-b", "Store B", "Store B"),
    ]


def test_two_successful_stores_share_browser_and_warm_up_once(monkeypatch):
    install_fake_crawlee(monkeypatch)
    factory = FakeFactory()
    used = []
    warmups = []

    def crawl(store, driver):
        used.append((store.id, driver.number))
        return CrawlResult(store.id, CrawlStatus.COMPLETE)

    adapter = CrawleeAdapter(
        factory,
        crawl,
        browser_warm_up=lambda driver: warmups.append(driver.number),
    )
    asyncio.run(adapter.run(stores()))

    assert used == [("store-a", 1), ("store-b", 1)]
    assert warmups == [1]
    assert len(factory.sessions) == 1
    assert factory.sessions[0].close_count == 1
    assert adapter.lifecycle_summary["stores_per_browser"] == {
        "browser-001": ["store-a", "store-b"]
    }


def test_retry_navigation_reuses_healthy_browser(monkeypatch):
    install_fake_crawlee(monkeypatch)
    factory = FakeFactory()
    drivers = []

    def crawl(store, driver):
        drivers.append(driver.number)
        if len(drivers) == 1:
            raise NavigationError("retry navigation")
        return CrawlResult(store.id, CrawlStatus.COMPLETE)

    adapter = CrawleeAdapter(factory, crawl)
    asyncio.run(adapter.run(stores()[:1]))

    assert drivers == [1, 1]
    assert adapter.lifecycle_summary["browsers_created"] == 1
    assert adapter.lifecycle_summary["browser_restarts"] == 0


def test_retry_fresh_session_replaces_browser(monkeypatch):
    install_fake_crawlee(monkeypatch)
    factory = FakeFactory()
    drivers = []

    def crawl(store, driver):
        drivers.append(driver.number)
        if len(drivers) == 1:
            raise SortError("fresh browser required")
        return CrawlResult(store.id, CrawlStatus.COMPLETE)

    warmups = []
    adapter = CrawleeAdapter(
        factory,
        crawl,
        browser_warm_up=lambda driver: warmups.append(driver.number),
    )
    asyncio.run(adapter.run(stores()[:1]))

    assert drivers == [1, 2]
    assert factory.sessions[0].close_count == 1
    assert adapter.lifecycle_summary["browsers_created"] == 2
    assert adapter.lifecycle_summary["browser_restarts"] == 1
    assert warmups == [1, 2]


def test_challenge_retires_browser_before_next_store(monkeypatch):
    install_fake_crawlee(monkeypatch)
    factory = FakeFactory()
    used = []

    def crawl(store, driver):
        used.append((store.id, driver.number))
        if store.id == "store-a":
            raise ChallengeError("challenge")
        return CrawlResult(store.id, CrawlStatus.COMPLETE)

    adapter = CrawleeAdapter(factory, crawl)
    asyncio.run(adapter.run(stores()))

    assert used == [("store-a", 1), ("store-b", 2)]
    assert adapter.lifecycle_summary["browser_restarts"] == 1


def test_dead_driver_is_retired_before_retry(monkeypatch):
    install_fake_crawlee(monkeypatch)
    factory = FakeFactory()
    drivers = []

    def crawl(store, driver):
        drivers.append(driver.number)
        if len(drivers) == 1:
            factory.sessions[0].alive = False
            raise BrowserSessionError("invalid session")
        return CrawlResult(store.id, CrawlStatus.COMPLETE)

    adapter = CrawleeAdapter(factory, crawl)
    asyncio.run(adapter.run(stores()[:1]))

    assert drivers == [1, 2]
    assert adapter.lifecycle_summary["browser_restarts"] == 1


def test_stop_keeps_healthy_browser_for_next_store(monkeypatch):
    install_fake_crawlee(monkeypatch)
    factory = FakeFactory()
    used = []

    def crawl(store, driver):
        used.append((store.id, driver.number))
        if store.id == "store-a":
            raise SortError("terminal sort failure")
        return CrawlResult(store.id, CrawlStatus.COMPLETE)

    adapter = CrawleeAdapter(
        factory,
        crawl,
        retry_policy=RetryPolicy(max_surface_retries=0),
    )
    asyncio.run(adapter.run(stores()))

    assert used == [("store-a", 1), ("store-b", 1)]
    assert adapter.lifecycle_summary["browsers_created"] == 1


def test_exception_cleanup_closes_retained_browser(monkeypatch):
    install_fake_crawlee(monkeypatch, crash_after_handlers=True)
    factory = FakeFactory()
    adapter = CrawleeAdapter(
        factory,
        lambda store, driver: CrawlResult(store.id, CrawlStatus.COMPLETE),
    )

    with pytest.raises(RuntimeError, match="crawler crashed"):
        asyncio.run(adapter.run(stores()[:1]))

    assert factory.sessions[0].close_count == 1


def test_manager_cleanup_is_idempotent():
    factory = FakeFactory()
    manager = BrowserSessionManager(factory)
    manager.acquire("store-a", 1)

    manager.close()
    manager.close()

    assert factory.sessions[0].close_count == 1
