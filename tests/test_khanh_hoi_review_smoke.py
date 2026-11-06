import asyncio
import csv
import json
from dataclasses import replace
from pathlib import Path

import pytest
import pyarrow.parquet as pq

from crawl_experiment.core.errors import PlaceResolutionError
from crawl_experiment.core.models import Review, Store, CrawlResult
from crawl_experiment.core.place_catalog import CanonicalPlace
from crawl_experiment.core.statuses import CrawlStatus
from crawl_experiment.orchestration.retry_policy import SmokeRetryPolicy
from crawl_experiment.sources.google_maps import place_resolution
from crawl_experiment.storage.parquet_review_writer import ParquetReviewWriter
from scripts import khanh_hoi_review_smoke as smoke


def place(identity="one", group="restaurant", confidence=0.99):
    return CanonicalPlace(
        place_key=f"overture:{identity}", source="overture", source_place_id=identity,
        name=f"Place {identity}", lat=10.76, lng=106.70, address="123 Street",
        primary_category=group, categories=[group], category_hierarchy=["food_and_drink", group],
        product_category_group=group, category_source="overture", confidence=confidence,
        admin_area_id="ward", admin_area_name="Ward", first_seen_at="now", last_seen_at="now",
    )


def resolution(p):
    return dict(resolved_place_title=p.name, resolved_place_address=p.address,
                resolved_url="https://www.google.com/maps/place/test/data=!1s0x1:0x2!3d10.76!4d106.70",
                place_entity_confirmed=True)


def test_selection_is_deterministic_diverse_high_confidence():
    places = [place("r"), place("c", "cafe_coffee"), place("b", "bakery_dessert"),
              place("tea", "beverage"), replace(place("seafood"), primary_category="seafood_restaurant"),
              place("low", confidence=0.1), replace(place("chain"), name="Place r")]
    selected = smoke.select_places(places)
    assert selected == smoke.select_places(list(reversed(places)))
    assert len(selected) == 5
    assert {p.product_category_group for p in selected} == {"restaurant", "cafe_coffee", "bakery_dessert", "beverage"}
    assert len({smoke.normalized(p.name) for p in selected}) == 5
    assert all(p.confidence >= 0.8 for p in selected)
    with pytest.raises(ValueError):
        smoke.select_places(places, limit=6)


def test_conservative_resolution_gate_and_no_viewport_coordinates():
    p = place()
    diagnostic = resolution(p)
    assert place_resolution.assess_resolution(p, diagnostic)["status"] == "RESOLVED"
    for update in (
        {"resolved_place_title": "Another business"}, {"place_entity_confirmed": False},
        {"resolved_url": "https://www.google.com/maps/place/test/@10.76,106.70,18z"},
        {"resolved_url": "https://www.google.com/maps/place/test/data=!1s0x1:0x2!3d11.0!4d106.70"},
        {"resolved_url": "https://www.google.com/maps/place/test/data=!3d10.76!4d106.70"},
    ):
        assert place_resolution.assess_resolution(p, {**diagnostic, **update})["status"] == "AMBIGUOUS"
    assert place_resolution.coordinate_distance(p, "https://google.com/maps/@10.76,106.70,18z") is None


def test_bridge_opens_exact_resolved_entity_and_rejects_redirect():
    class Driver:
        def get(self, url):
            self.current_url = url
    driver = Driver()
    p = place()
    result = place_resolution.assess_resolution(p, resolution(p))
    store = Store("one", p.name, "query")
    bridge = place_resolution.ResolvedPlaceDriver(driver, store, result)
    bridge.get(bridge.search_url)
    assert driver.current_url == result["resolved_url"]
    driver.get = lambda url: setattr(driver, "current_url", "https://google.com/maps/place/other/!1s0x3:0x4!")
    with pytest.raises(PlaceResolutionError, match="identity"):
        bridge.get(bridge.search_url)


def test_inspection_cap_does_not_limit_persistence_and_includes_unchanged(tmp_path):
    writer = ParquetReviewWriter(tmp_path / "lake", "run", "2026-10-01")
    repo = smoke.InspectionReviewRepository(tmp_path / "state.sqlite3", run_id="run", parquet_writer=writer)
    reviews = [Review("google_maps", str(i), "store", text=f"Text {i}") for i in range(25)]
    for r in reviews:
        repo.upsert_review_state(r)
    repo.flush()
    assert repo.count() == 25
    assert len(repo.inspection_rows()) == 20
    assert sum(pq.ParquetFile(f).metadata.num_rows for f in writer.output_path.glob("*.parquet")) == 25
    repo.upsert_review_state(reviews[0])
    assert repo.inspection_rows()[0]["change_type"] == "UNCHANGED"
    repo.close()


@pytest.mark.parametrize("terminal_status", ["AMBIGUOUS", "NOT_FOUND"])
def test_offline_pipeline_only_resolved_is_crawled(monkeypatch, tmp_path, terminal_status):
    monkeypatch.setattr(smoke, "ROOT", tmp_path)
    selected = [place("one"), place("two")]
    crawled = []
    resolution_calls = []

    class FakeCrawler:
        def __init__(self, repo, *args, **kwargs):
            self.repo = repo
        def crawl(self, store, driver, **kwargs):
            crawled.append(store.id)
            self.repo.upsert_review_state(Review("google_maps", "review", store.id, text="Synthetic only"))
            return CrawlResult(store.id, CrawlStatus.PARTIAL_LIMIT, reviews_seen=1, reviews_written=1)

    class FakeAdapter:
        lifecycle_summary = {"browser_launch_count": 0}
        def __init__(self, factory, callback, **options):
            self.callback, self.options = callback, options
        async def run(self, stores):
            for store in stores:
                self.options["on_attempt"](store, 1)
                try:
                    result = self.callback(store, object())
                except PlaceResolutionError as exc:
                    pytest.fail(f"deterministic outcome entered retry flow: {exc}")
                else:
                    self.options["on_result"](store, result, 1)

    def resolve(p, driver):
        resolution_calls.append(p.place_key)
        row = smoke.assess_resolution(p, resolution(p))
        if p.source_place_id == "two":
            row["status"] = terminal_status
        return row

    monkeypatch.setattr(smoke, "GoogleMapsCrawler", FakeCrawler)
    monkeypatch.setattr(smoke, "CrawleeAdapter", FakeAdapter)
    monkeypatch.setattr(smoke, "resolve_place", resolve)
    output = tmp_path / "output"
    output.mkdir()
    summary = asyncio.run(smoke.execute(selected, output, 682, 90, 5))
    assert crawled == [smoke.store_id(selected[0])]
    assert summary["resolved"] == summary["crawled_successfully"] == 1
    assert summary["ambiguous" if terminal_status == "AMBIGUOUS" else "not_found"] == 1
    assert summary["total_reviews_written"] == 1
    assert summary["finished"]
    assert len(list(csv.DictReader((output / "reviews_sample.csv").open(encoding="utf-8-sig")))) == 1
    assert json.loads((output / "summary.json").read_text())["total_reviews_written"] == 1
    assert resolution_calls == [p.place_key for p in selected]
    skipped_id = smoke.store_id(selected[1])
    metadata = json.loads((Path(summary["run_directory"]) / "metadata" / f"{skipped_id}.json").read_text())
    assert metadata["attempts_started"] == 1
    assert metadata["terminal"] and metadata["review_crawling_skipped"]
    assert not metadata["dlq"]
    assert metadata["status"] == terminal_status
    rows = list(csv.DictReader((output / "google_resolution.csv").open(encoding="utf-8-sig")))
    assert rows[1]["status"] == terminal_status
    assert float(rows[1]["match_score"]) == 1.0


def test_smoke_limited_retry_once_per_place_even_after_navigation_retries():
    from crawl_experiment.core.errors import LimitedReviewViewError
    from crawl_experiment.orchestration.retry_policy import RetryAction
    policy = SmokeRetryPolicy()
    first = LimitedReviewViewError("limited review surface")
    first.smoke_store_id = "one"
    decision = policy.decide(first, 2)
    assert decision.action == RetryAction.RETRY_FRESH_SESSION
    assert decision.reason == "smoke_limited_review_retry"
    assert decision.status == CrawlStatus.LIMITED
    second = LimitedReviewViewError("still limited")
    second.smoke_store_id = "one"
    assert policy.decide(second, 3).action == RetryAction.STOP
    second.smoke_store_id = "two"
    assert policy.decide(second, 0).action == RetryAction.RETRY_FRESH_SESSION
    assert policy.max_session_retries >= 3


def test_smoke_policy_preserves_default_auth_challenge_and_other_decisions():
    from crawl_experiment.core.errors import (
        AuthRequiredError, ChallengeError, LimitedReviewViewError, LimitedViewError,
        BrowserSessionError, NavigationError, SortError,
    )
    from crawl_experiment.orchestration.retry_policy import RetryPolicy
    policy, default = SmokeRetryPolicy(), RetryPolicy()
    for error_type in (AuthRequiredError, ChallengeError, LimitedReviewViewError,
                       LimitedViewError, BrowserSessionError, NavigationError, SortError):
        for attempt in range(4):
            error = error_type("synthetic")
            assert policy.decide(error, attempt) == default.decide(error, attempt)


@pytest.mark.parametrize("body,status", [("No results found", "NOT_FOUND"), ("Other candidates", "AMBIGUOUS")])
def test_resolution_terminal_evidence_keeps_canonical_place(monkeypatch, body, status):
    p = place()
    before = p.to_dict()
    calls = []
    class Navigator:
        last_resolution_diagnostics = {**resolution(p), "place_entity_confirmed": False}
        def __init__(self, driver):
            pass
        def open_store(self, store):
            calls.append(store)
            raise PlaceResolutionError("synthetic candidate not confirmed")
    monkeypatch.setattr(place_resolution, "GoogleMapsNavigator", Navigator)
    driver = type("Driver", (), {"execute_script": lambda self, script: body})()
    row = place_resolution.resolve_place(p, driver)
    assert row["status"] == status
    assert row["error"] == "synthetic candidate not confirmed"
    assert row["google_place_id"] == "0x1:0x2" and row["match_score"] == 1.0
    assert calls[0].query == "Place one 123 Street Ward"
    assert p.to_dict() == before


def test_resolution_transient_failure_still_propagates_to_policy(monkeypatch):
    from crawl_experiment.core.errors import NavigationError
    class Navigator:
        def __init__(self, driver):
            pass
        def open_store(self, store):
            raise NavigationError("synthetic timeout")
    monkeypatch.setattr(place_resolution, "GoogleMapsNavigator", Navigator)
    with pytest.raises(NavigationError, match="timeout"):
        place_resolution.resolve_place(place(), object())


def test_model_and_legacy_import_compatibility():
    from crawl_experiment.core.models import CrawlJob, ResolutionResult
    from crawl_experiment.core.statuses import ResolutionStatus
    assert CrawlJob is Store
    assert set(s.value for s in ResolutionStatus) == {"RESOLVED", "AMBIGUOUS", "NOT_FOUND", "ERROR"}
    assert "status" in ResolutionResult.__annotations__
    assert smoke.resolve_place is place_resolution.resolve_place
    assert smoke.ResolvedPlaceDriver is place_resolution.ResolvedPlaceDriver
    assert smoke.SmokeRetryPolicy is SmokeRetryPolicy


def test_smoke_cli_reuses_canonical_artifact_without_raw_or_polygon(monkeypatch, tmp_path):
    from crawl_experiment.storage import place_catalog as storage
    selected = [place("r"), place("c", "cafe_coffee"), place("b", "bakery_dessert"),
                place("tea", "beverage"), place("fast", "fast_food")]
    catalog = tmp_path / "canonical.parquet"
    storage.persist_catalog(storage.PlaceCatalog(selected, {}, {"canonical_persisted": 5}), catalog, {})
    monkeypatch.setattr(storage, "_build_catalog", lambda *a, **kw: pytest.fail("smoke must not rebuild"))
    monkeypatch.setattr(storage, "administrative_polygon", lambda *a: pytest.fail("smoke must not parse polygon"))
    monkeypatch.setattr(smoke, "execute", lambda *a, **kw: pytest.fail("prepare-only must not crawl"))
    output = tmp_path / "output"
    assert smoke.main(["--catalog", str(catalog), "--ward", str(tmp_path / "absent.json"),
                       "--output", str(output), "--prepare-only"]) == 0
    rows = list(csv.DictReader((output / "selected_places.csv").open(encoding="utf-8-sig")))
    assert [r["source_place_id"] for r in rows] == [p.source_place_id for p in smoke.select_places(selected)]
