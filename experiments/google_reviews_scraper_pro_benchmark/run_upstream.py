from __future__ import annotations

import json
import logging
import re
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

UPSTREAM = Path(__file__).resolve().parent / "upstream"
sys.path.insert(0, str(UPSTREAM))

from modules.scraper import GoogleReviewsScraper  # noqa: E402

FEATURE_ID = "0x6b12ae3738d8eb85:0x558b61a1bffd3ab7"
PLACE_URL = (
    "https://www.google.com/maps/place/Coles+World+Square/@-33.8772854,151.2067106,17z/"
    "data=!3m1!5s0x6b13eb099503ba87:0x20b19b8a7efcbdf4!4m8!3m7!1s0x6b12ae3738d8eb85:0x558b61a1bffd3ab7!"
    "8m2!3d-33.8772854!4d151.2067106!9m1!1b1!16s%2Fg%2F1tjtjfbm?entry=ttu"
)
ROOT = Path(__file__).resolve().parents[2]


class Capture(logging.Handler):
    def __init__(self):
        super().__init__()
        self.records: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record.getMessage())


def main() -> int:
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = ROOT / "data" / "google-reviews-scraper-pro-benchmark" / run_id
    out.mkdir(parents=True, exist_ok=False)
    started = time.perf_counter()
    capture = Capture()
    logger = logging.getLogger("scraper")
    logger.setLevel(logging.INFO)
    logger.addHandler(capture)
    db_tmp = tempfile.NamedTemporaryFile(prefix="grsp-", suffix=".sqlite", delete=False)
    db_tmp.close()
    config = {
        "url": PLACE_URL,
        "headless": True,
        "sort_by": "newest",
        "scrape_mode": "full",
        "stop_threshold": 0,
        "max_reviews": 30,
        "max_scroll_attempts": 15,
        "scroll_idle_limit": 5,
        "db_path": db_tmp.name,
        "use_mongodb": False,
        "backup_to_json": False,
        "download_images": False,
        "convert_dates": False,
        "log_level": "INFO",
    }
    scraper = GoogleReviewsScraper(config)
    success = False
    error = None
    try:
        success = bool(scraper.scrape())
    except Exception as exc:  # upstream handles expected failures internally
        error = f"{type(exc).__name__}: {exc}"
    try:
        places = scraper.review_db.list_places()
        place = places[0] if places else {}
        resolved_place_id = place.get("place_id")
        if resolved_place_id:
            reviews = scraper.review_db.get_reviews(resolved_place_id, include_deleted=False)
        else:
            reviews = []
    except Exception as exc:
        reviews = []
        error = error or f"DB read: {type(exc).__name__}: {exc}"
    finally:
        try:
            scraper.review_db.close()
        except Exception:
            pass
        logger.removeHandler(capture)
    elapsed = round(time.perf_counter() - started, 2)

    def _classify_status() -> str:
        if error:
            return "ERROR"
        if challenge:
            return "CHALLENGE"
        if limited:
            return "LIMITED"
        if not success or len(unique_ids) == 0:
            return "NAVIGATION_FAILED"
        return "COMPLETE"

    normalized = []
    for review in reviews[:30]:
        text = review.get("review_text", "")
        if isinstance(text, dict):
            text = text.get("text", "") or text.get("full", "")
        normalized.append({
            "source_review_id": review.get("review_id", ""),
            "author": review.get("author", ""),
            "rating": review.get("rating", 0),
            "review_text": text,
            "published_text": review.get("raw_date", "") or review.get("review_date", ""),
        })
    ids = [r["source_review_id"] for r in normalized if r["source_review_id"]]
    unique_ids = list(dict.fromkeys(ids))
    log_text = "\n".join(capture.records)
    limited = any("limited view" in line.lower() for line in capture.records)
    challenge = any(x in log_text.lower() for x in ("/sorry/", "captcha", "recaptcha"))
    navigation = "search_based" if any("search-based navigation successful" in line.lower() for line in capture.records) else "unknown_or_fallback"
    initial_cards = None
    for line in capture.records:
        m = re.search(r"search-based navigation found (\d+) review cards", line, re.I)
        if m:
            initial_cards = int(m.group(1))
            break
    scroll_logs = [line for line in capture.records if "Found " in line and "new reviews" in line]
    summary = {
        "run_id": run_id,
        "upstream_commit": "09bfa6215cb37edecc777bb40055d100e88ef767",
        "upstream_version": "1.2.3",
        "google_maps_feature_id": FEATURE_ID,
        "status": _classify_status(),
        "initial_page_state": "limited_view" if limited else "not_observed",
        "limited_view": limited,
        "challenge_detected": challenge,
        "navigation_path": navigation,
        "reviews_tab_appeared": any("reviews tab" in line.lower() for line in capture.records),
        "review_pane_appeared": any("found reviews pane" in line.lower() for line in capture.records),
        "initial_review_card_count": initial_cards,
        "scroll_iterations_with_new_reviews": len(scroll_logs),
        "scroll_attempt_bound": 15,
        "scroll_idle_limit": 5,
        "total_unique_reviews": len(unique_ids),
        "stable_review_ids": bool(unique_ids),
        "duplicate_cards_observed": len(ids) - len(set(ids)),
        "sort_by": "newest",
        "sort_confirmed": any("sort" in line.lower() and "success" in line.lower() for line in capture.records),
        "extraction_stalled": not success or len(unique_ids) == 0,
        "runtime_seconds": elapsed,
        "error": error,
        "log_observations": [line for line in capture.records if any(k in line.lower() for k in ("navigat", "reviews tab", "reviews pane", "limited", "finished", "total unique", "scroll", "sort", "error"))][-80:],
    }
    (out / "summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    (out / "reviews.jsonl").write_text("\n".join(json.dumps(r, ensure_ascii=False) for r in normalized) + ("\n" if normalized else ""), encoding="utf-8")
    print(json.dumps({"artifact_dir": str(out), "status": summary["status"], "reviews": len(unique_ids), "navigation": navigation, "limited_view": limited, "runtime_seconds": elapsed}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
