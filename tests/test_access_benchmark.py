import json

from crawl_experiment.observability.access_benchmark import AccessBenchmark


class Clock:
    def __init__(self):
        self.value = 0.0

    def __call__(self):
        self.value += 1.0
        return self.value


def benchmark(tmp_path):
    item = AccessBenchmark(tmp_path / "access_state_timeline.jsonl", "run-1", clock=Clock())
    item.browser_created("browser-001")
    item.warm_up_completed()
    item.begin_store("store-1", 1)
    return item


def test_full_to_full_does_not_stop(tmp_path):
    item = benchmark(tmp_path)
    item.classify("FULL")
    item.classify("FULL")
    assert not item.should_stop


def test_full_to_limited_stops_and_includes_counters(tmp_path):
    item = benchmark(tmp_path)
    item.classify("FULL")
    item.observe("navigation", {})
    item.observe("review_surface_classified", {"access_state": "LIMITED", "opened": True})
    assert item.should_stop
    line = json.loads(item.path.read_text(encoding="utf-8").splitlines()[-1])
    assert line["transition"] == "FULL_TO_LIMITED"
    assert line["cumulative_navigations"] == 1
    assert line["cumulative_review_surface_opens"] == 1


def test_full_to_auth_required_stops(tmp_path):
    item = benchmark(tmp_path)
    item.classify("FULL")
    item.classify("AUTH_REQUIRED")
    assert item.should_stop
    assert item.summary()["transition_type"] == "FULL_TO_AUTH_REQUIRED"


def test_browser_restart_invalidates_benchmark(tmp_path):
    item = benchmark(tmp_path)
    item.browser_created("browser-002")
    assert item.summary()["benchmark_valid"] is False


def test_same_browser_retry_navigation_remains_valid(tmp_path):
    item = benchmark(tmp_path)
    item.retry_navigation()
    assert item.summary()["benchmark_valid"] is True
    assert item.summary()["retry_count"] == 1


def test_no_degradation_has_false_transition(tmp_path):
    item = benchmark(tmp_path)
    item.classify("FULL")
    item.complete_store()
    assert item.summary()["access_transition_detected"] is False
