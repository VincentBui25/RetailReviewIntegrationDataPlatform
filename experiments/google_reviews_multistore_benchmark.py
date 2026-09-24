from __future__ import annotations

import json
import logging
import re
import statistics
import sys
import tempfile
import threading
import time
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4


ROOT = Path(__file__).resolve().parents[1]
UPSTREAM = ROOT / "experiments" / "google_reviews_scraper_pro_benchmark" / "upstream"
sys.path.insert(0, str(UPSTREAM))

from modules.scraper import GoogleReviewsScraper  # noqa: E402


UPSTREAM_COMMIT = "09bfa6215cb37edecc777bb40055d100e88ef767"
UPSTREAM_VERSION = "1.2.3"
MAX_SCROLL_ATTEMPTS = 100
IDLE_STOP = 8
MAX_RUNTIME_SECONDS = 15 * 60


def classify_store_status(
    *,
    upstream_success: bool,
    unique_ids: set,
    challenge: bool,
    rate_limit: bool,
    limited_view: bool,
    timed_out: bool,
    reviews_tab: bool,
    error: str | None,
) -> str:
    """Return an explicit status replacing the misleading boolean success.

    Statuses:
        COMPLETE           – upstream reported success, reviews extracted, no anomaly
        PARTIAL_TIMEOUT    – reviews were extracted but the safety timer fired
        LIMITED            – Google returned a limited (unsigned) view
        NAVIGATION_FAILED  – reviews tab or place never resolved
        CHALLENGE          – CAPTCHA / human verification encountered
        RATE_LIMITED       – Google rate-limit (/sorry/) encountered
        ERROR              – unhandled exception
    """
    if error:
        return "ERROR"
    if challenge:
        return "CHALLENGE"
    if rate_limit:
        return "RATE_LIMITED"
    if limited_view:
        return "LIMITED"
    if not reviews_tab or not unique_ids:
        return "NAVIGATION_FAILED"
    if timed_out:
        return "PARTIAL_TIMEOUT"
    return "COMPLETE"


class Capture(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.messages: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.messages.append(record.getMessage())


class ObservedScraper(GoogleReviewsScraper):
    def __init__(self, config, cancel_event=None):
        super().__init__(config, cancel_event=cancel_event)
        self.observed = {
            "resolved_url": None,
            "resolved_title": None,
            "limited_view": False,
            "challenge": False,
            "rate_limit": False,
            "reviews_tab": False,
            "advertised_review_count": None,
            "initial_cards": None,
            "sort_confirmed": False,
        }

    def navigate_to_place(self, driver, url, wait):
        result = super().navigate_to_place(driver, url, wait)
        self.observed["resolved_url"] = driver.current_url
        self.observed["resolved_title"] = driver.title
        self.observed["limited_view"] = bool(self._is_limited_view(driver))
        lowered_url = (driver.current_url or "").lower()
        self.observed["challenge"] = any(x in lowered_url for x in ("/sorry/", "captcha", "recaptcha"))
        self.observed["rate_limit"] = "/sorry/" in lowered_url
        return result

    def click_reviews_tab(self, driver):
        result = super().click_reviews_tab(driver)
        self.observed["reviews_tab"] = bool(result)
        try:
            body = driver.find_element("tag name", "body").text or ""
            counts = re.findall(r"([\d,.]+)\s+(?:reviews?|ratings?)", body, re.I)
            if counts:
                self.observed["advertised_review_count"] = max(int(x.replace(",", "").replace(".", "")) for x in counts)
            self.observed["initial_cards"] = len(driver.find_elements("css selector", "div[data-review-id]"))
        except Exception:
            pass
        return result

    def set_sort(self, driver, sort_by):
        result = super().set_sort(driver, sort_by)
        self.observed["sort_confirmed"] = bool(result and sort_by == "newest")
        return result


def _place_input_url(name: str) -> str:
    # The upstream method extracts this path segment, warms google.com, then
    # performs /maps/search/<place_name>/. It is not used as primary direct navigation.
    return f"https://www.google.com/maps/place/{urllib.parse.quote_plus(name)}/"


def _feature_id(url: str | None) -> str | None:
    if not url:
        return None
    matches = re.findall(r"0x[0-9a-f]+:0x[0-9a-f]+", url, re.I)
    return matches[-1] if matches else None


def _review_text(value):
    if isinstance(value, dict):
        for key in ("text", "description", "full", "original"):
            if value.get(key):
                return value[key]
        return next((v for v in value.values() if isinstance(v, str) and v), "")
    return value or ""


def _scroll_metrics(messages: list[str]) -> tuple[int, bool, int | None, list[int]]:
    events: list[int] = []
    for message in messages:
        match = re.search(r"Found (\d+) new reviews in this iteration", message)
        if match:
            events.append(int(match.group(1)))
        elif "No new reviews in this iteration" in message:
            events.append(0)
    last_success = max((i for i, count in enumerate(events, start=1) if count > 0), default=None)
    idle_stop = any(f"No new reviews found after {IDLE_STOP} scroll attempts" in m for m in messages)
    return len(events), idle_stop, last_success, events


def main() -> int:
    stores_doc = json.loads((ROOT / "config" / "known_coles_stores.json").read_text(encoding="utf-8"))
    stores = stores_doc["stores"]
    if len(stores) != 5:
        raise RuntimeError(f"expected exactly 5 configured stores, found {len(stores)}")
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "-" + uuid4().hex[:8]
    run_dir = ROOT / "data" / "google-reviews-multistore-benchmark" / run_id
    reviews_dir = run_dir / "reviews"
    reviews_dir.mkdir(parents=True, exist_ok=False)
    reviews_dir.mkdir(parents=True, exist_ok=False)
    results = []

    logger = logging.getLogger("scraper")
    logger.setLevel(logging.INFO)
    for order, store in enumerate(stores, start=1):
        capture = Capture()
        logger.addHandler(capture)
        started = time.perf_counter()
        cancel = threading.Event()
        timer = threading.Timer(MAX_RUNTIME_SECONDS, cancel.set)
        timer.daemon = True
        timer.start()
        safe_key = f"coles-{store['retailer_store_id']}"
        rows = []
        error = None
        success = False
        with tempfile.TemporaryDirectory(prefix=f"grsp-{safe_key}-") as temp_dir:
            db_path = str(Path(temp_dir) / "reviews.sqlite")
            config = {
                "url": _place_input_url(store["store_name"]),
                "headless": True,
                "sort_by": "newest",
                "scrape_mode": "full",
                "stop_threshold": 0,
                "max_reviews": 0,
                "max_scroll_attempts": MAX_SCROLL_ATTEMPTS,
                "scroll_idle_limit": IDLE_STOP,
                "db_path": db_path,
                "use_mongodb": False,
                "backup_to_json": False,
                "download_images": False,
                "convert_dates": False,
                "resilience": {
                    "retry_on_session_death": 0,
                    "retry_on_navigation_failure": 0,
                    "retry_backoff_base_seconds": 3,
                    "rate_limit_cooldown_seconds": 0,
                },
            }
            scraper = ObservedScraper(config, cancel_event=cancel)
            try:
                success = bool(scraper.scrape())
                places = scraper.review_db.list_places()
                place = places[0] if places else {}
                resolved_key = place.get("place_id")
                if resolved_key:
                    rows = scraper.review_db.get_reviews(resolved_key, include_deleted=False)
            except Exception as exc:
                error = f"{type(exc).__name__}: {exc}"
            finally:
                timer.cancel()
                try:
                    scraper.review_db.close()
                except Exception:
                    pass
        logger.removeHandler(capture)
        runtime = round(time.perf_counter() - started, 2)
        scroll_iterations, idle_reached, last_success_scroll, new_counts = _scroll_metrics(capture.messages)
        challenge = bool(scraper.observed["challenge"] or any("captcha" in m.lower() or "/sorry/" in m.lower() for m in capture.messages))
        rate_limit = bool(scraper.observed["rate_limit"] or any("rate-limit" in m.lower() for m in capture.messages))
        normalized = []
        for row in rows:
            normalized.append({
                "place_key": safe_key,
                "retailer_store_id": store["retailer_store_id"],
                "place_name": store["store_name"],
                "google_maps_feature_id": _feature_id(scraper.observed["resolved_url"]),
                "source_review_id": row.get("review_id"),
                "author": row.get("author"),
                "rating": row.get("rating"),
                "review_text": _review_text(row.get("review_text")),
                "published_text": row.get("raw_date") or row.get("review_date"),
            })
        unique_ids = {r["source_review_id"] for r in normalized if r["source_review_id"]}
        duplicates = max(0, len(normalized) - len(unique_ids))
        (reviews_dir / f"{safe_key}.jsonl").write_text(
            "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in normalized), encoding="utf-8"
        )
        results.append({
            "order": order,
            "place_key": safe_key,
            "retailer_store_id": store["retailer_store_id"],
            "place_name": store["store_name"],
            "google_maps_feature_id": _feature_id(scraper.observed["resolved_url"]),
            "resolved_title": scraper.observed["resolved_title"],
            "advertised_review_count": scraper.observed["advertised_review_count"],
            "total_unique_reviews": len(unique_ids),
            "stable_review_ids": bool(unique_ids) and len(unique_ids) == len(normalized),
            "scroll_iterations": scroll_iterations,
            "new_review_counts_by_iteration": new_counts,
            "idle_stop_reached": idle_reached,
            "runtime_seconds": runtime,
            "timed_out": cancel.is_set(),
            "limited_view": bool(scraper.observed["limited_view"]),
            "captcha_challenge": challenge,
            "rate_limit": rate_limit,
            "duplicates_observed": duplicates,
            "last_successful_new_review_scroll": last_success_scroll,
            "reviews_tab": bool(scraper.observed["reviews_tab"]),
            "initial_review_cards": scraper.observed["initial_cards"],
            "sort_newest_confirmed": bool(scraper.observed["sort_confirmed"]),
            "status": classify_store_status(
                upstream_success=bool(success),
                unique_ids=unique_ids,
                challenge=challenge,
                rate_limit=rate_limit,
                limited_view=bool(scraper.observed["limited_view"]),
                timed_out=cancel.is_set(),
                reviews_tab=bool(scraper.observed["reviews_tab"]),
                error=error,
            ),
            "error": error,
        })
        print(json.dumps({"store": store["store_name"], "status": results[-1]["status"], "reviews": len(unique_ids), "runtime_seconds": runtime, "challenge": challenge, "rate_limit": rate_limit}), flush=True)
        if challenge or rate_limit:
            # Stop this store only; continue the bounded five-store benchmark.
            continue

    from collections import Counter
    runtimes = [row["runtime_seconds"] for row in results]
    total_reviews = sum(row["total_unique_reviews"] for row in results)
    status_counts = dict(Counter(row["status"] for row in results))
    summary = {
        "run_id": run_id,
        "upstream_commit": UPSTREAM_COMMIT,
        "upstream_version": UPSTREAM_VERSION,
        "proxy": None,
        "login": False,
        "navigation": "google.com warm-up -> /maps/search/<place_name>/ -> resolved place -> Reviews",
        "stores_attempted": len(results),
        "status_counts": status_counts,
        "total_unique_reviews": total_reviews,
        "average_reviews_per_store": round(total_reviews / len(results), 2) if results else 0,
        "median_runtime_seconds": round(statistics.median(runtimes), 2) if runtimes else 0,
        "challenge_count": sum(bool(row["captcha_challenge"]) for row in results),
        "rate_limit_count": sum(bool(row["rate_limit"]) for row in results),
        "limited_view_count": sum(bool(row["limited_view"]) for row in results),
        "max_reviews_one_store": max((row["total_unique_reviews"] for row in results), default=0),
        "stores": results,
        "artifact_dir": str(run_dir),
        "finished_at": datetime.now(timezone.utc).isoformat(),
    }
    (run_dir / "stores.json").write_text(json.dumps(results, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    (run_dir / "summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps({k: summary[k] for k in ("artifact_dir", "stores_attempted", "status_counts", "total_unique_reviews", "median_runtime_seconds", "challenge_count", "rate_limit_count")}, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
