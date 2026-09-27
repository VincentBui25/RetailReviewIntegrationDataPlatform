import json
from pathlib import Path

from crawl_experiment.sources.google_maps.review_surface import ReviewSurfaceState
from scripts import google_maps_cooldown_probe as probe


def record(timestamp, state, **extra):
    return {
        "timestamp": timestamp,
        "access_state": state,
        "recovery_observed": False,
        **extra,
    }


def test_limited_to_limited_has_no_recovery():
    previous = [record("2026-09-27T00:00:00+00:00", "LIMITED")]

    detected, elapsed, timestamp = probe.recovery_observation(
        previous, "LIMITED", "2026-09-27T01:00:00+00:00"
    )

    assert (detected, elapsed, timestamp) == (False, None, None)


def test_limited_to_full_detects_recovery_and_elapsed_time():
    previous = [
        record("2026-09-27T00:00:00+00:00", "LIMITED"),
        record("2026-09-27T00:30:00+00:00", "LIMITED"),
    ]

    detected, elapsed, timestamp = probe.recovery_observation(
        previous, "FULL", "2026-09-27T01:30:00+00:00"
    )

    assert detected is True
    assert elapsed == 5400
    assert timestamp == "2026-09-27T01:30:00+00:00"


def test_auth_required_to_full_detects_recovery():
    previous = [record("2026-09-27T02:00:00+00:00", "AUTH_REQUIRED")]

    detected, elapsed, _ = probe.recovery_observation(
        previous, "FULL", "2026-09-27T02:15:00+00:00"
    )

    assert detected is True
    assert elapsed == 900


def test_full_without_prior_degraded_state_has_no_recovery():
    previous = [record("2026-09-27T00:00:00+00:00", "FULL")]

    assert probe.recovery_observation(
        previous, "FULL", "2026-09-27T01:00:00+00:00"
    ) == (False, None, None)


def test_recovery_summary_counts_states_and_preserves_first_recovery():
    records = [
        record("2026-09-27T00:00:00+00:00", "LIMITED"),
        record("2026-09-27T00:30:00+00:00", "AUTH_REQUIRED"),
        record(
            "2026-09-27T01:00:00+00:00",
            "FULL",
        ),
    ]

    summary = probe.build_recovery_summary(records)

    assert summary == {
        "first_probe_at": "2026-09-27T00:00:00+00:00",
        "latest_probe_at": "2026-09-27T01:00:00+00:00",
        "initial_state": "LIMITED",
        "latest_state": "FULL",
        "recovery_observed": True,
        "first_full_after_limited_seconds": 3600,
        "first_full_after_limited_at": "2026-09-27T01:00:00+00:00",
        "cooldown_confirmed": False,
        "benchmark_conditions_controlled": False,
        "recommended_manual_schedule_minutes": [0, 15, 30, 60],
        "probe_count": 3,
        "full_count": 1,
        "limited_count": 1,
        "auth_required_count": 1,
    }


class FakeDriver:
    capabilities = {"browserName": "chrome", "browserVersion": "140.0"}

    def execute_script(self, _script):
        return {"width": 1280, "height": 720}


class FakeManaged:
    instance_id = "browser-001"
    driver = FakeDriver()


class FakeManager:
    instances = []

    def __init__(self, _browser_factory, warm_up, logger):
        self.warm_up = warm_up
        self.acquire_count = 0
        self.closed = False
        self.__class__.instances.append(self)

    def acquire(self, store_id, attempt):
        assert store_id == "coles-berowra"
        assert attempt == 1
        self.acquire_count += 1
        self.warm_up(FakeManaged.driver)
        return FakeManaged()

    def close(self):
        self.closed = True

    def summary(self):
        return {
            "browsers_created": 1,
            "browsers_retired": int(self.closed),
            "browser_restarts": 0,
            "warm_up_count": 1,
        }


class FakeNavigator:
    warm_up_count = 0
    open_count = 0

    def __init__(self, driver):
        assert driver is FakeManaged.driver

    def warm_up(self):
        self.__class__.warm_up_count += 1

    def open_store(self, store):
        assert store.id == "coles-berowra"
        self.__class__.open_count += 1


class ProbeOnlySurface:
    prepare_count = 0

    def __init__(self, driver):
        assert driver is FakeManaged.driver

    def prepare_and_classify(self):
        self.__class__.prepare_count += 1
        return ReviewSurfaceState.FULL, object(), {
            "sort_control": True,
            "cards": 20,
        }


def test_one_shot_probe_classifies_once_without_pagination_or_extraction(tmp_path):
    FakeManager.instances.clear()
    FakeNavigator.warm_up_count = FakeNavigator.open_count = 0
    ProbeOnlySurface.prepare_count = 0
    ticks = iter([10.0, 11.5])

    result, summary = probe.execute_probe(
        output_root=tmp_path,
        now=lambda: "2026-09-27T03:00:00+00:00",
        monotonic=lambda: next(ticks),
        manager_factory=FakeManager,
        browser_factory=object(),
        navigator_factory=FakeNavigator,
        surface_factory=ProbeOnlySurface,
    )

    manager = FakeManager.instances[-1]
    assert manager.acquire_count == 1
    assert manager.closed is True
    assert FakeNavigator.warm_up_count == 1
    assert FakeNavigator.open_count == 1
    assert ProbeOnlySurface.prepare_count == 1
    assert result["access_state"] == "FULL"
    assert result["browser_engine"] == "chrome 140.0"
    assert result["browser_instance_created"] is True
    assert result["viewport"] == {"width": 1280, "height": 720}
    assert result["elapsed_since_first_probe"] == 0
    assert result["cards_count"] == 20
    assert result["review_pane_present"] is True
    assert result["sort_control_present"] is True
    assert result["sort_menu_openable"] is None
    assert result["browser_reused"] is False
    assert result["network_identity_known_unchanged"] is False
    assert result["cooldown_confirmed"] is False
    assert summary["probe_count"] == 1
    lines = (tmp_path / probe.RESULTS_NAME).read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    assert json.loads(lines[0])["access_state"] == "FULL"
    assert not (tmp_path / "reviews.sqlite3").exists()
    assert not (tmp_path / "checkpoints").exists()


def test_main_invokes_exactly_one_probe_and_exits(monkeypatch, tmp_path):
    calls = []

    def fake_execute_probe(*, output_root):
        calls.append(output_root)
        return {"access_state": "LIMITED"}, {"probe_count": 1}

    monkeypatch.setattr(probe, "execute_probe", fake_execute_probe)

    assert probe.main(["--output-root", str(tmp_path)]) == 0
    assert calls == [Path(tmp_path)]
