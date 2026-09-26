import json

from scripts.google_maps_smoke_test import STORE, build_summary, pagination_progressed


def test_summary_does_not_claim_runtime_stages_without_events(tmp_path):
    summary = build_summary(
        tmp_path,
        started_at="start",
        finished_at="finish",
        hard_stopped=False,
        worker_exit_code=1,
    )

    checks = summary["checks"]
    assert checks["browser_started"] is False
    assert checks["warm_up_completed"] is False
    assert checks["store_resolved"] is False
    assert checks["reviews_surface_opened"] is False
    assert checks["sort_newest_applied"] is False
    assert checks["review_pane_found"] is False


def test_summary_uses_emitted_runtime_stages(tmp_path):
    events = [
        {"store_id": STORE.id, "stage": "browser_started"},
        {"store_id": STORE.id, "stage": "warm_up_completed"},
    ]
    (tmp_path / "progress.log").write_text(
        "\n".join(json.dumps(event) for event in events),
        encoding="utf-8",
    )

    summary = build_summary(
        tmp_path,
        started_at="start",
        finished_at="finish",
        hard_stopped=False,
        worker_exit_code=1,
    )

    checks = summary["checks"]
    assert checks["browser_started"] is True
    assert checks["warm_up_completed"] is True
    assert checks["store_resolved"] is False
    assert checks["reviews_surface_opened"] is False


def test_pagination_progress_requires_actual_runtime_progress(tmp_path):
    (tmp_path / "progress.log").write_text(
        '[smoke] INFO pagination scroll: '
        '{"scroll_top_changed": false, "cards_found": 0, '
        '"new_review_ids": 0}\n',
        encoding="utf-8",
    )

    assert pagination_progressed(tmp_path / "progress.log") is False
