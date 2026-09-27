"""Manual one-shot cooldown probe for Google Maps guest review access."""

from __future__ import annotations

import argparse
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from crawl_experiment.browser.browser_factory import BrowserFactory
from crawl_experiment.browser.session_manager import BrowserSessionManager
from crawl_experiment.core.errors import ChallengeError, LimitedViewError
from crawl_experiment.sources.google_maps.navigator import GoogleMapsNavigator
from crawl_experiment.sources.google_maps.review_surface import (
    ReviewSurface,
    ReviewSurfaceState,
)
from crawl_experiment.observability.run_artifacts import write_json
from scripts.coles_small_set_full_crawl import STORES

OUTPUT_ROOT = Path("data/access_benchmark/google_maps_cooldown")
RESULTS_NAME = "cooldown_probe_results.jsonl"
SUMMARY_NAME = "recovery_summary.json"
STORE_ID = "coles-berowra"
DEGRADED_STATES = {"LIMITED", "AUTH_REQUIRED"}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def load_records(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    records = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            records.append(value)
    return records


def append_record(path: Path, record: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(record, default=str, sort_keys=True) + "\n")
        stream.flush()


def recovery_observation(
    previous_records: list[dict[str, Any]],
    current_state: str,
    current_timestamp: str,
) -> tuple[bool, float | None, str | None]:
    if not previous_records or current_state != "FULL":
        return False, None, None
    if previous_records[-1].get("access_state") not in DEGRADED_STATES:
        return False, None, None
    degraded_suffix = []
    for record in reversed(previous_records):
        if record.get("access_state") not in DEGRADED_STATES:
            break
        degraded_suffix.append(record)
    started_at = degraded_suffix[-1]["timestamp"]
    elapsed = max(
        0.0,
        (parse_timestamp(current_timestamp) - parse_timestamp(started_at)).total_seconds(),
    )
    return True, elapsed, current_timestamp


def build_recovery_summary(records: list[dict[str, Any]]) -> dict[str, Any]:
    if not records:
        return {
            "first_probe_at": None,
            "latest_probe_at": None,
            "initial_state": None,
            "latest_state": None,
            "recovery_observed": False,
            "first_full_after_limited_seconds": None,
            "first_full_after_limited_at": None,
            "cooldown_confirmed": False,
            "benchmark_conditions_controlled": False,
            "recommended_manual_schedule_minutes": [0, 15, 30, 60],
            "probe_count": 0,
            "full_count": 0,
            "limited_count": 0,
            "auth_required_count": 0,
        }
    observation = None
    for index, current in enumerate(records[1:], 1):
        if (
            current.get("access_state") == "FULL"
            and records[index - 1].get("access_state") in DEGRADED_STATES
        ):
            _observed, elapsed, timestamp = recovery_observation(
                records[:index], "FULL", current["timestamp"]
            )
            observation = (elapsed, timestamp)
            break
    return {
        "first_probe_at": records[0]["timestamp"],
        "latest_probe_at": records[-1]["timestamp"],
        "initial_state": records[0].get("access_state"),
        "latest_state": records[-1].get("access_state"),
        "recovery_observed": observation is not None,
        "first_full_after_limited_seconds": observation[0] if observation else None,
        "first_full_after_limited_at": observation[1] if observation else None,
        "cooldown_confirmed": False,
        "benchmark_conditions_controlled": False,
        "recommended_manual_schedule_minutes": [0, 15, 30, 60],
        "probe_count": len(records),
        "full_count": sum(r.get("access_state") == "FULL" for r in records),
        "limited_count": sum(r.get("access_state") == "LIMITED" for r in records),
        "auth_required_count": sum(
            r.get("access_state") == "AUTH_REQUIRED" for r in records
        ),
    }


def browser_engine(driver: Any) -> str:
    capabilities = getattr(driver, "capabilities", {}) or {}
    name = capabilities.get("browserName") or capabilities.get("browser_name")
    version = capabilities.get("browserVersion") or capabilities.get("version")
    return " ".join(str(value) for value in (name, version) if value) or "unknown"


def browser_viewport(driver: Any) -> dict[str, int] | None:
    try:
        value = driver.execute_script(
            "return {width: window.innerWidth, height: window.innerHeight};"
        )
        if isinstance(value, dict) and value.get("width") and value.get("height"):
            return {"width": int(value["width"]), "height": int(value["height"])}
    except Exception:  # noqa: BLE001 - metadata must not fail the probe
        pass
    try:
        value = driver.get_window_size()
        return {"width": int(value["width"]), "height": int(value["height"])}
    except Exception:  # noqa: BLE001 - viewport may be unavailable
        return None


def execute_probe(
    *,
    output_root: Path = OUTPUT_ROOT,
    now: Callable[[], str] = utc_now,
    monotonic: Callable[[], float] = time.monotonic,
    manager_factory: Callable[..., Any] = BrowserSessionManager,
    browser_factory: Any | None = None,
    navigator_factory: Callable[[Any], Any] = GoogleMapsNavigator,
    surface_factory: Callable[[Any], Any] = ReviewSurface,
) -> tuple[dict[str, Any], dict[str, Any]]:
    output_root.mkdir(parents=True, exist_ok=True)
    results_path = output_root / RESULTS_NAME
    summary_path = output_root / SUMMARY_NAME
    previous_records = load_records(results_path)
    timestamp = now()
    run_id = parse_timestamp(timestamp).strftime("%Y%m%d-%H%M%S-%f")
    first_timestamp = previous_records[0]["timestamp"] if previous_records else timestamp
    store = next(store for store in STORES if store.id == STORE_ID)
    logger = logging.getLogger("cooldown_probe")
    manager = manager_factory(
        browser_factory or BrowserFactory(headless=False),
        warm_up=lambda driver: navigator_factory(driver).warm_up(),
        logger=logger,
    )
    managed = None
    access_state = "UNKNOWN"
    reason = "UNKNOWN_REVIEW_VIEW"
    evidence: dict[str, Any] = {}
    sort_control_present = False
    auth_requirement_detected = False
    challenge_detected = False
    engine = "unknown"
    viewport = None
    cards_count = 0
    review_pane_present = False
    started = monotonic()
    try:
        managed = manager.acquire(store.id, 1)
        engine = browser_engine(managed.driver)
        viewport = browser_viewport(managed.driver)
        navigator_factory(managed.driver).open_store(store)
        state, pane, evidence = surface_factory(
            managed.driver
        ).prepare_and_classify()
        access_state = str(state)
        cards_count = int(evidence.get("cards", 0) or 0)
        review_pane_present = pane is not None
        sort_control_present = bool(evidence.get("sort_control"))
        auth_requirement_detected = state is ReviewSurfaceState.AUTH_REQUIRED
        reason = f"{access_state}_REVIEW_VIEW"
    except LimitedViewError as exc:
        access_state = "LIMITED"
        reason = "LIMITED_NAVIGATION_VIEW"
        evidence = {"navigation_error": str(exc).splitlines()[0]}
    except ChallengeError as exc:
        challenge_detected = True
        reason = "CHALLENGE_DETECTED"
        evidence = {"challenge_error": str(exc).splitlines()[0]}
    except Exception as exc:  # noqa: BLE001 - every manual probe must be recorded
        reason = f"{type(exc).__name__}: {str(exc).splitlines()[0]}"
        evidence = {"probe_error": reason}
    finally:
        manager.close()

    lifecycle = manager.summary()
    recovery_observed, first_full_seconds, first_full_at = recovery_observation(
        previous_records, access_state, timestamp
    )
    elapsed_since_first = max(
        0.0,
        (parse_timestamp(timestamp) - parse_timestamp(first_timestamp)).total_seconds(),
    )
    record = {
        "timestamp": timestamp,
        "run_id": run_id,
        "probe_index": len(previous_records) + 1,
        "store_id": store.id,
        "browser_instance_id": getattr(managed, "instance_id", None),
        "browser_engine": engine,
        "browser_instance_created": lifecycle.get("browsers_created", 0) == 1,
        "viewport": viewport,
        "access_state": access_state,
        "previous_access_state": (
            previous_records[-1].get("access_state") if previous_records else None
        ),
        "elapsed_since_first_probe": elapsed_since_first,
        "probe_elapsed_seconds": round(max(0.0, monotonic() - started), 3),
        "browser_reused": False,
        "browser_restarted": lifecycle.get("browser_restarts", 0) > 0,
        "warm_up_count": lifecycle.get("warm_up_count", 0),
        "cards_count": cards_count,
        "review_pane_present": review_pane_present,
        "sort_control_present": sort_control_present,
        "sort_menu_openable": None,
        "auth_requirement_detected": auth_requirement_detected,
        "challenge_detected": challenge_detected,
        "network_mode": "direct",
        "network_identity_known_unchanged": False,
        "recovery_observed": recovery_observed,
        "first_full_after_limited_seconds": first_full_seconds,
        "first_full_after_limited_at": first_full_at,
        "cooldown_confirmed": False,
        "reason": reason,
        "evidence": evidence,
    }
    append_record(results_path, record)
    records = [*previous_records, record]
    summary = build_recovery_summary(records)
    write_json(summary_path, summary)
    return record, summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="One-shot Google Maps guest access cooldown probe"
    )
    parser.add_argument("--output-root", type=Path, default=OUTPUT_ROOT)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    record, _summary = execute_probe(output_root=args.output_root)
    print(json.dumps(record, default=str, sort_keys=True))
    print(f"Probe results: {args.output_root / RESULTS_NAME}")
    print(f"Recovery summary: {args.output_root / SUMMARY_NAME}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
