import asyncio
import csv
import json
import multiprocessing
import queue
import threading
import time
from collections import deque
from types import SimpleNamespace

import pyarrow.parquet as pq
import pytest

from crawl_experiment.browser.session_manager import BrowserSessionManager
from crawl_experiment.core.models import CrawlResult, Review
from crawl_experiment.core.statuses import CrawlStatus
from crawl_experiment.observability.crawler_benchmark import BenchmarkMetrics, ResourceSampler, percentile
from crawl_experiment.runners import crawler_benchmark_runner as runner
from crawl_experiment.storage.parquet_review_writer import ParquetReviewWriter
from tests.test_khanh_hoi_review_smoke import place, resolution


def config():
    return dict(configured_concurrency=1, configured_max_scrolls=10, configured_timeout_seconds=90,
                benchmark_started_at="2026-10-01T00:00:00Z", sampling_interval_seconds=2, headless=True,
                sample_fingerprint="synthetic")


def test_metrics_denominators_scroll_rows_and_dedup(tmp_path):
    clock = [0]
    collector = BenchmarkMetrics(tmp_path, "run", [("a", "A"), ("b", "B")], config(), clock=lambda: clock[0])
    for attempt in (1, 2):
        collector.event(dict(event="attempt_started", store_id="a", attempt=attempt))
        collector.event(dict(event="review_surface_limited", store_id="a", attempt=attempt, evidence={"limited": True}))
        collector.event(dict(event="retry_decision", store_id="a", attempt=attempt, status="LIMITED",
                             retry_action="retry_fresh_session" if attempt == 1 else "stop", error_type="LimitedReviewViewError"))
    collector.event(dict(event="attempt_started", store_id="b", attempt=1))
    collector.event(dict(event="google_challenge", store_id="b", attempt=1))
    collector.event(dict(event="retry_decision", store_id="b", attempt=1, status="CHALLENGE", retry_action="stop"))
    collector.event(dict(event="browser_retired", store_id="a", reason="run_shutdown"))
    collector.event(dict(event="browser_restarted", store_id="a"))
    for index, new in ((1, 2), (2, 0)):
        event = dict(event="scroll_metric", store_id="a", attempt=2, scroll_count=index,
                     new_review_ids=new, reviews_after=2, iteration_elapsed_ms=100, at_end=False)
        collector.event(event)
        collector.event(event)  # duplicate telemetry does not duplicate a scroll row
    collector.event(dict(event="review_observed", store_id="a", source_review_id="r", change_type="INSERT"))
    collector.event(dict(event="review_observed", store_id="a", source_review_id="r", change_type="UNCHANGED"))
    clock[0] = 10
    collector.event(dict(event="place_finished", store_id="a", status="PARTIAL_LIMIT", active_crawl_seconds=5))
    collector.event(dict(event="place_failed", store_id="b", status="CHALLENGE"))
    final = collector.finish()
    collector.close()
    assert final["places_attempted"] == 2 and final["total_place_attempts"] == 3
    assert final["retry_count"] == 1 and final["retry_rate"] == pytest.approx(1 / 3)
    assert final["limited_place_count"] == 1 and final["limited_attempt_count"] == 2
    assert final["limited_rate"] == final["challenge_rate"] == 0.5
    assert final["challenge_attempt_count"] == 1
    assert final["browser_crash_count"] == 0 and final["browser_restart_count"] == 1
    assert final["reviews_seen"] == final["reviews_written"] == 1
    assert final["reviews_per_second"] == 0.1
    assert final["crawler_active_reviews_per_second"] == 0.2
    scrolls = list(csv.DictReader((tmp_path / "scroll_metrics.csv").open()))
    assert len(scrolls) == 2
    assert float(scrolls[0]["ms_per_new_review"]) == 50
    assert scrolls[1]["ms_per_new_review"] == ""
    assert percentile([1, 2, 3]) == pytest.approx(2.9)


def test_session_crash_does_not_include_intentional_retire():
    events = []
    class Session:
        driver = object()
        alive = True
        def start(self):
            return self.driver
        def is_alive(self):
            return self.alive
        def close(self):
            self.alive = False
    class Factory:
        def create(self, identity):
            return Session()
    manager = BrowserSessionManager(Factory(), event_observer=events.append)
    manager.acquire("a", 1)
    manager.retire("smoke_limited_review_retry", store_id="a", attempt=1)
    manager.acquire("a", 2)
    manager.close()
    assert not any(e["event"] == "browser_crash" for e in events)
    assert sum(e["event"] == "browser_restarted" for e in events) == 1
    manager.acquire("b", 1)
    manager.current.session.alive = False
    manager.acquire("b", 2)
    assert sum(e["event"] == "browser_crash" for e in events) == 1
    manager.close()


def test_sampler_stops_and_writes_real_local_resources(tmp_path):
    collector = BenchmarkMetrics(tmp_path, "run", [], config())
    sampler = ResourceSampler(collector)
    ready = threading.Event()
    original = collector.resource
    def resource(row):
        original(row)
        ready.set()
    collector.resource = resource
    sampler.start()
    assert ready.wait(5)
    sampler.stop()
    assert not sampler.thread.is_alive()
    final = collector.finish()
    collector.close()
    assert final["resource_sample_count"] >= 1
    assert final["peak_ram_mb"] > 0
    assert final["reviews_per_second"] == 0
    assert (tmp_path / "resource_metrics.csv").read_text().count("\n") >= 2


def test_frozen_sample_reloads_without_source_access(tmp_path):
    places = [place(str(i), group=("restaurant", "cafe_coffee")[i % 2]) for i in range(12)]
    sample = runner.sample_places(places, 10)
    assert sample == runner.sample_places(list(reversed(places)), 10)
    file = tmp_path / "sample.json"
    file.write_text(json.dumps(dict(places=[p.to_dict() for p in sample])))
    restored, fingerprint = runner.load_sample(file, 10, tmp_path / "absent", tmp_path / "absent")
    assert restored == sample
    assert fingerprint == runner.load_sample(file, 10, None, None)[1]
    with pytest.raises(ValueError, match="size differs"):
        runner.load_sample(file, 9, None, None)


def test_benchmark_repository_still_persists_payload(tmp_path):
    events = []
    writer = ParquetReviewWriter(tmp_path / "lake", "run", "2026-10-01")
    repo = runner.BenchmarkReviewRepository(tmp_path / "state.sqlite3", run_id="run", parquet_writer=writer, observer=events.append)
    review = Review("google_maps", "id", "store", text="Synthetic payload")
    repo.upsert_review_state(review)
    repo.upsert_review_state(review)
    repo.close()
    rows = [row for path in writer.output_path.glob("*.parquet") for row in pq.ParquetFile(path).read().to_pylist()]
    assert len(rows) == 1 and rows[0]["review_text"] == "Synthetic payload"
    assert [e["change_type"] for e in events] == ["INSERT", "UNCHANGED"]
    assert "system_cpu_percent" not in rows[0]


def test_offline_worker_terminal_resolution_and_real_persistence(monkeypatch, tmp_path):
    monkeypatch.setattr(runner, "ROOT", tmp_path)
    events = []
    places = [place("one"), place("two")]
    crawled = []
    class Crawler:
        def __init__(self, repo, *args, **kwargs):
            self.repo = repo
        def crawl(self, store, driver, **kwargs):
            crawled.append(store.id)
            self.repo.upsert_review_state(Review("google_maps", "r", store.id, text="Synthetic"))
            return CrawlResult(store.id, CrawlStatus.PARTIAL_LIMIT, reviews_seen=1, reviews_written=1)
    class Adapter:
        lifecycle_summary = {}
        def __init__(self, factory, callback, **kwargs):
            self.callback, self.options = callback, kwargs
        async def run(self, stores):
            assert not stores
            while (store := self.options["work_provider"]()) is not None:
                self.options["on_attempt"](store, 1)
                value = self.callback(store, object())
                self.options["on_result"](store, value, 1)
    def resolve(p, driver):
        return dict(status="AMBIGUOUS" if p.source_place_id == "two" else "RESOLVED",
                    google_place_id="id", google_name=p.name, google_address=p.address,
                    resolved_url=resolution(p)["resolved_url"])
    monkeypatch.setattr(runner, "GoogleMapsCrawler", Crawler)
    monkeypatch.setattr(runner, "CrawleeAdapter", Adapter)
    monkeypatch.setattr(runner, "resolve_place", resolve)
    directory = tmp_path / "benchmark"
    directory.mkdir()
    work = queue.Queue()
    for p in places:
        work.put(p.to_dict())
    work.put(None)
    asyncio.run(runner.worker_run("worker-1", [p.to_dict() for p in places], directory, config(), events.append, place_queue=work))
    assert crawled == [runner.store_id(places[0])]
    assert [e["status"] for e in events if e["event"] == "place_finished"] == ["PARTIAL_LIMIT", "AMBIGUOUS"]
    payload_files = list((tmp_path / "data/crawler_benchmark/reviews").rglob("*.parquet"))
    assert len(payload_files) == 1
    assert pq.ParquetFile(payload_files[0]).metadata.num_rows == 1


def test_metrics_hook_failure_does_not_change_persistence_or_browser_lifecycle(tmp_path):
    def failing(event):
        raise RuntimeError("synthetic observer failure")
    writer = ParquetReviewWriter(tmp_path / "lake", "run", "2026-10-01")
    repo = runner.BenchmarkReviewRepository(tmp_path / "state.sqlite3", run_id="run", parquet_writer=writer, observer=failing)
    assert repo.upsert_review_state(Review("google_maps", "id", "store", text="Preserved")).value == "INSERT"
    repo.close()
    assert sum(pq.ParquetFile(p).metadata.num_rows for p in writer.output_path.glob("*.parquet")) == 1
    adapter = runner.CrawleeAdapter(object(), lambda *args: None, event_observer=failing)
    adapter._observe(dict(event="retry_decision"))  # observer exceptions must not escape


def test_scroll_hook_preserves_existing_jsonl_and_observer_is_best_effort(tmp_path):
    from crawl_experiment.sources.google_maps.crawler import GoogleMapsCrawler
    from crawl_experiment.storage.checkpoint_repository import CheckpointRepository
    events = []
    path = tmp_path / "scroll.jsonl"
    crawler = GoogleMapsCrawler(object(), CheckpointRepository(tmp_path / "checkpoints"),
                                scroll_metrics_path=path, scroll_observer=events.append)
    payload = dict(store_id="a", scroll_count=1, new_review_ids=0)
    crawler._write_scroll_metric(dict(payload))
    assert json.loads(path.read_text()) == payload
    assert events == [payload]
    crawler.scroll_observer = lambda event: (_ for _ in ()).throw(RuntimeError("synthetic"))
    crawler._write_scroll_metric(dict(payload))
    assert len(path.read_text().splitlines()) == 2


def synthetic_worker(worker_id, payloads, directory, configuration, events, place_queue):
    """Spawn-safe offline worker; no browser and no network access."""
    barrier = configuration.get("synthetic_start_barrier")
    if barrier:
        barrier.wait(timeout=10)
    def emit(event):
        events.put(dict(event, worker_id=worker_id, monotonic_seconds=time.monotonic()))
    while (payload := place_queue.get()) is not None:
        identity = runner.store_id(runner.CanonicalPlace.from_dict(payload))
        emit(dict(event="place_claimed", store_id=identity))
        skew = configuration.get("synthetic_skew")
        early_failure = skew and payload["source_place_id"] == "1"
        duration = 0.6 if skew and payload["source_place_id"] == "0" else 0.04 if skew else 0
        for event in (
            dict(event="attempt_started", attempt=1),
            dict(event="resolution", status="RESOLVED", google_place_id="synthetic"),
        ):
            emit(dict(event, store_id=identity))
        time.sleep(duration)
        if early_failure:
            emit(dict(event="place_failed", store_id=identity, status="ERROR"))
        else:
            emit(dict(event="review_observed", store_id=identity, source_review_id=identity, change_type="INSERT"))
            emit(dict(event="place_finished", store_id=identity, status="PARTIAL_LIMIT", active_crawl_seconds=duration))
        emit(dict(event="place_released", store_id=identity))
    emit(dict(event="worker_finished"))


def test_spawned_benchmark_workers_and_sampler_leave_no_monitor_process(monkeypatch, tmp_path):
    previous = {p.pid for p in multiprocessing.active_children()}
    monkeypatch.setattr(runner, "worker_entry", synthetic_worker)
    configuration = {**config(), "configured_concurrency": 2}
    directory = tmp_path / "runs" / "offline-spawn"
    result = runner.run_benchmark([place(str(i)) for i in range(4)], directory, configuration)
    assert result["places_attempted"] == result["places_crawled_successfully"] == result["reviews_written"] == 4
    assert not result["worker_errors"]
    assert {p.pid for p in multiprocessing.active_children()} <= previous
    assert not any(t.name == "crawler-resource-sampler" for t in threading.enumerate())
    assert all((directory / name).exists() for name in (
        "run_metrics.json", "place_metrics.csv", "scroll_metrics.csv", "resource_metrics.csv", "events.jsonl", "benchmark_summary.md"))


def test_dynamic_queue_fast_worker_claims_more_after_early_failure(monkeypatch, tmp_path):
    monkeypatch.setattr(runner, "worker_entry", synthetic_worker)
    configuration = {**config(), "configured_concurrency": 2, "synthetic_skew": True,
                     "synthetic_start_barrier": multiprocessing.get_context("spawn").Barrier(2)}
    selected = [place(str(i)) for i in range(6)]
    directory = tmp_path / "runs" / "skewed-workload"
    final = runner.run_benchmark(selected, directory, configuration)
    events = [json.loads(line) for line in (directory / "events.jsonl").read_text().splitlines()]
    claims = [e for e in events if e["event"] == "place_claimed"]
    assert len(claims) == len({e["store_id"] for e in claims}) == 6
    heavy = next(e for e in claims if e["store_id"] == runner.store_id(selected[0]))
    failure = next(e for e in events if e["event"] == "place_failed")
    assert failure["worker_id"] != heavy["worker_id"]
    fast_claims = [e for e in claims if e["worker_id"] == failure["worker_id"]]
    assert len(fast_claims) > 1
    heavy_release = next(e for e in events if e["event"] == "place_released" and e["store_id"] == heavy["store_id"])
    assert fast_claims[1]["monotonic_seconds"] < heavy_release["monotonic_seconds"]
    assert final["peak_active_workers"] == 2
    assert final["unclaimed_place_count"] == 0
    assert final["places_attempted"] == 6 and final["places_crawled_successfully"] == 5
    assert 0 < final["worker_utilization"] <= 1
    assert final["avg_active_workers"] == pytest.approx(2 * final["worker_utilization"])
    assert "active_worker_count" in (directory / "resource_metrics.csv").read_text().splitlines()[0]


def test_worker_utilization_integrates_claim_release_including_retries(tmp_path):
    clock = [0]
    collector = BenchmarkMetrics(tmp_path, "run", [("a", "A"), ("b", "B")],
                                 {**config(), "configured_concurrency": 2, "work_queue_mode": "shared_dynamic"},
                                 clock=lambda: clock[0])
    collector.event(dict(event="place_claimed", worker_id="one", store_id="a"))
    clock[0] = 2
    collector.event(dict(event="place_claimed", worker_id="two", store_id="b"))
    clock[0] = 3
    collector.event(dict(event="retry_decision", worker_id="one", store_id="a", attempt=1,
                         status="LIMITED", retry_action="retry_fresh_session"))
    assert collector.active_workers() == 2  # the retrying worker still owns work
    clock[0] = 5
    collector.event(dict(event="place_released", worker_id="two", store_id="b"))
    clock[0] = 8
    collector.event(dict(event="place_released", worker_id="one", store_id="a"))
    clock[0] = 10
    result = collector.finish()
    collector.close()
    assert result["active_worker_seconds"] == 11
    assert result["avg_active_workers"] == 1.1
    assert result["peak_active_workers"] == 2
    assert result["worker_utilization"] == 0.55


def test_dynamic_adapter_keeps_browser_and_claims_next_only_after_terminal(monkeypatch):
    from crawlee.storages import RequestQueue
    from crawl_experiment.core.errors import AuthRequiredError, LimitedReviewViewError
    from crawl_experiment.core.models import Store
    from crawl_experiment.orchestration.retry_policy import SmokeRetryPolicy
    pending = deque()
    queue_names, dropped = [], []
    class FakeQueue:
        async def add_requests(self, requests):
            pending.extend(requests)
        async def drop(self):
            assert not pending
            dropped.append(True)
    async def open_queue(*args, **kwargs):
        assert kwargs.get("name", "").startswith("benchmark-job-")
        assert not pending
        queue_names.append(kwargs["name"])
        return FakeQueue()
    class FakeCrawler:
        def __init__(self, **kwargs):
            self.options = kwargs
            assert kwargs["concurrency_settings"].max_concurrency == 1
        async def run(self, **kwargs):
            while pending:
                request = pending.popleft()
                context = SimpleNamespace(request=request, session=SimpleNamespace(retire=lambda: None))
                try:
                    await self.options["request_handler"](context)
                except LimitedReviewViewError:
                    request.retry_count += 1
                    assert request.retry_count <= self.options["max_request_retries"]
                    pending.appendleft(request)
    monkeypatch.setattr(RequestQueue, "open", staticmethod(open_queue))
    monkeypatch.setattr("crawlee.crawlers.BasicCrawler", FakeCrawler)
    class Session:
        driver = None
        def __init__(self):
            self.driver, self.alive = object(), True
        def start(self):
            return self.driver
        def is_alive(self):
            return self.alive
        def close(self):
            self.alive = False
    class Factory:
        sessions = []
        def create(self, identity):
            session = Session()
            self.sessions.append(session)
            return session
    stores = deque(Store(str(i), str(i), str(i)) for i in range(3))
    claims, calls, finished = [], [], []
    def provider():
        assert len(dropped) == len([identity for identity in claims if identity is not None])
        store = stores.popleft() if stores else None
        claims.append(store.id if store else None)
        return store
    def crawl(store, driver):
        calls.append((store.id, driver))
        if store.id == "0":
            raise AuthRequiredError("synthetic terminal failure")
        if store.id == "2" and sum(identity == "2" for identity, _ in calls) == 1:
            error = LimitedReviewViewError("synthetic limited")
            error.smoke_store_id = store.id
            raise error
        return CrawlResult(store.id, CrawlStatus.COMPLETE)
    factory = Factory()
    adapter = runner.CrawleeAdapter(factory, crawl, retry_policy=SmokeRetryPolicy(), work_provider=provider,
                                   on_result=lambda store, *args: finished.append(store.id),
                                   on_terminal_failure=lambda store, *args: finished.append(store.id))
    asyncio.run(adapter.run(()))
    assert claims == ["0", "1", "2", None]
    assert [identity for identity, _ in calls] == ["0", "1", "2", "2"]
    assert finished == ["0", "1", "2"]
    assert calls[0][1] is calls[1][1] is calls[2][1]  # same healthy browser across places
    assert calls[3][1] is not calls[2][1]  # only the LIMITED retry recreates it
    assert len(factory.sessions) == 2 and all(not s.alive for s in factory.sessions)
    assert len(set(queue_names)) == len(dropped) == 3


def local_crawlee_worker(worker_id, payloads, directory, configuration, events, place_queue):
    """Real filesystem RequestQueues/BasicCrawler, fake browser; zero network."""
    from pathlib import Path
    from crawlee import service_locator
    from crawlee.configuration import Configuration
    from crawlee.storage_clients import FileSystemStorageClient
    from crawlee.storages import RequestQueue
    from crawl_experiment.core.models import Store
    service_locator.set_configuration(Configuration(storage_dir=str(Path(directory) / worker_id / "storage"),
                                                     purge_on_start=False))
    service_locator.set_storage_client(FileSystemStorageClient())
    def emit(event):
        events.put(dict(event, worker_id=worker_id, timestamp=runner.now(), monotonic_seconds=time.monotonic(),
                        queue_remaining=max(0, place_queue.qsize() - 1)))
    handler = runner.RequestQueueCorruptionHandler(emit)
    import logging
    logging.getLogger("crawlee").addHandler(handler)
    class Session:
        alive = True
        driver = object()
        def start(self):
            return self.driver
        def is_alive(self):
            return self.alive
        def close(self):
            self.alive = False
    class Factory:
        count = 0
        def create(self, identity):
            self.count += 1
            return Session()
    factory = Factory()
    current = None
    names = []
    closed = []
    original_open = RequestQueue.open
    original_drop = RequestQueue.drop
    async def tracked_open(**kwargs):
        assert kwargs["name"].startswith("benchmark-job-")
        names.append(kwargs["name"])
        return await original_open(**kwargs)
    RequestQueue.open = staticmethod(tracked_open)
    async def tracked_drop(storage):
        assert await storage.is_finished()
        await original_drop(storage)
        closed.append(storage.name)
    RequestQueue.drop = tracked_drop
    def provider():
        nonlocal current
        assert len(closed) == len(names)  # No next claim until real queue cleanup.
        if current:
            emit(dict(event="place_released", store_id=current.id))
            emit(dict(event="worker_released_job", store_id=current.id))
        payload = place_queue.get()
        if payload is None:
            current = None
            emit(dict(event="worker_idle", store_id=None))
            return None
        p = runner.CanonicalPlace.from_dict(payload)
        current = Store(runner.store_id(p), p.name, p.name)
        emit(dict(event="place_claimed", store_id=current.id))
        emit(dict(event="worker_claimed_job", store_id=current.id))
        return current
    adapter = runner.CrawleeAdapter(factory, lambda store, driver: CrawlResult(store.id, CrawlStatus.COMPLETE),
                                   work_provider=provider, event_observer=emit, worker_id=worker_id,
                                   on_attempt=lambda store, n: emit(dict(event="attempt_started", store_id=store.id, attempt=n)),
                                   on_result=lambda store, *args: emit(dict(event="place_finished", store_id=store.id, status="COMPLETE")))
    try:
        asyncio.run(asyncio.wait_for(adapter.run(()), timeout=60))
        assert len(names) == len(set(names)) == len(payloads)
        assert closed == names
        assert factory.count == 1
        assert not list((Path(directory) / worker_id / "storage" / "request_queues").glob("*"))
    except BaseException as exc:
        emit(dict(event="worker_error", error_type=type(exc).__name__, error_message=str(exc)))
    finally:
        logging.getLogger("crawlee").removeHandler(handler)
        emit(dict(event="worker_finished"))


def test_real_local_queue_lifecycle_three_dynamic_jobs_exits(monkeypatch, tmp_path):
    monkeypatch.setattr(runner, "worker_entry", local_crawlee_worker)
    directory = tmp_path / "runs" / "local-queues"
    final = runner.run_benchmark([place(str(i)) for i in range(3)], directory,
                                 {**config(), "no_progress_timeout_seconds": 60})
    assert not final["worker_errors"]
    assert final["places_crawled_successfully"] == 3
    events = [json.loads(line) for line in (directory / "events.jsonl").read_text().splitlines()]
    assert not any(e["event"] in {"request_queue_corruption", "worker_stalled"} for e in events)
    assert sum(e["event"] == "worker_finished_job" for e in events) == 3
    assert sum(e["event"] == "worker_released_job" for e in events) == 3
    assert sum(e["event"] == "worker_idle" for e in events) == 1


def silent_worker(worker_id, payloads, directory, configuration, events, place_queue):
    time.sleep(30)  # Parent watchdog must end this synthetic process promptly.


def corrupt_worker(worker_id, payloads, directory, configuration, events, place_queue):
    import logging
    handler = runner.RequestQueueCorruptionHandler(lambda e: events.put(dict(e, worker_id=worker_id)))
    for _ in range(3):
        handler.handle(logging.LogRecord("crawlee", logging.WARNING, "", 0,
                                        'Request file for "stale-id" is missing or invalid, skipping.', (), None))
    time.sleep(30)


@pytest.mark.parametrize("target,corruption", [(silent_worker, False), (corrupt_worker, True)])
def test_parent_watchdog_aborts_stalled_or_corrupt_worker(monkeypatch, tmp_path, target, corruption):
    monkeypatch.setattr(runner, "worker_entry", target)
    directory = tmp_path / "runs" / target.__name__
    started = time.monotonic()
    final = runner.run_benchmark([place("one")], directory,
                                 {**config(), "no_progress_timeout_seconds": 8 if corruption else 2})
    assert time.monotonic() - started < 15
    assert len(final["worker_errors"]) == 1
    assert final["worker_errors"][0]["error_type"] == "BenchmarkWorkerStalled"
    events = [json.loads(line) for line in (directory / "events.jsonl").read_text().splitlines()]
    assert any(e["event"] == "worker_stalled" for e in events)
    if corruption:
        assert sum(e["event"] == "request_queue_corruption" for e in events) == 3


def test_browser_lifecycle_events_have_worker_identity_and_isolated_drivers(caplog):
    import logging
    events = []
    class Session:
        def __init__(self):
            self.driver = object()
        def start(self):
            return self.driver
        def is_alive(self):
            return True
        def close(self):
            pass
    class Factory:
        def create(self, identity):
            return Session()
    managers = [BrowserSessionManager(Factory(), event_observer=events.append, worker_id=f"worker-{i}")
                for i in (1, 2)]
    with caplog.at_level(logging.INFO):
        drivers = [m.acquire("a", 1).driver for m in managers]
        assert drivers[0] is not drivers[1]
        assert all(m.acquire("b", 1).driver is driver for m, driver in zip(managers, drivers))
        for m in managers:
            m.close()
    assert {e["worker_id"] for e in events} == {"worker-1", "worker-2"}
    assert all("worker_id=" in r.getMessage() for r in caplog.records if r.getMessage().startswith("event="))


def test_new_benchmark_sample_reads_canonical_artifact_only(monkeypatch, tmp_path):
    from crawl_experiment.storage import place_catalog as storage
    places = [place(str(i), group=("restaurant", "cafe_coffee")[i % 2]) for i in range(12)]
    output = tmp_path / "canonical.parquet"
    storage.persist_catalog(storage.PlaceCatalog(places, {}, {"canonical_persisted": len(places)}), output, {})
    monkeypatch.setattr(storage, "_build_catalog", lambda *a, **kw: pytest.fail("downstream must not rebuild"))
    monkeypatch.setattr(storage, "administrative_polygon", lambda *a: pytest.fail("downstream must not parse polygon"))
    sample, fingerprint = runner.load_sample(tmp_path / "sample.json", 10, output, tmp_path / "missing-ward")
    assert sample == runner.sample_places(places, 10)
    assert fingerprint == runner.load_sample(tmp_path / "sample.json", 10, tmp_path / "absent", None)[1]
