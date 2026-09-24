from __future__ import annotations

import json
from pathlib import Path
import shutil
import unittest
from uuid import uuid4

from crawl_experiment.cli import build_parser
from crawl_experiment.http import FetchResult
from crawl_experiment.models import ReviewRecord, StoreRecord
from crawl_experiment.parsers import load_review_batches, parse_review_batches
from crawl_experiment.runner import CrawlRunner, build_tasks
from crawl_experiment.storage import Storage


ROOT = Path(__file__).parent
TEST_TMP_ROOT = ROOT.parent / ".test-tmp"
TEST_TMP_ROOT.mkdir(exist_ok=True)


def test_directory() -> Path:
    path = TEST_TMP_ROOT / uuid4().hex
    path.mkdir()
    return path


class IdentityTests(unittest.TestCase):
    def test_store_unique_key(self) -> None:
        store = StoreRecord("COLES", "710", "World Square", "https://example.test/store-710")
        self.assertEqual(("coles", "710"), store.logical_key)

    def test_review_unique_key_prefers_source_id(self) -> None:
        review = ReviewRecord("Google", "abc", "coles", "710")
        self.assertEqual(("google", "abc"), review.logical_key)

    def test_review_fallback_is_deterministic(self) -> None:
        fields = dict(source_code="google", source_review_id=None, retailer_code="coles", retailer_store_id="710", author="A", published_at="2026-01-01", rating=5, text="Same")
        self.assertEqual(ReviewRecord(**fields).logical_key, ReviewRecord(**fields).logical_key)


class StorageTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_path = test_directory()
        self.storage = Storage(self.temp_path / "crawl.sqlite3")

    def tearDown(self) -> None:
        self.storage.close()
        shutil.rmtree(self.temp_path)

    def test_duplicate_store_discovery_and_rerun(self) -> None:
        store = StoreRecord("coles", "710", "World Square", "https://example.test/store-710")
        self.assertTrue(self.storage.upsert_store(store))
        self.assertFalse(self.storage.upsert_store(store))
        self.assertEqual(1, self.storage.count("stores"))

    def test_two_review_batches_deduplicate(self) -> None:
        paths = [str(ROOT / "fixtures" / "reviews_batch_1.json"), str(ROOT / "fixtures" / "reviews_batch_2.json")]
        reviews = parse_review_batches(load_review_batches(paths), source_code="google", retailer_store_id="710")
        self.assertEqual(4, len(reviews))
        self.assertEqual(3, self.storage.upsert_reviews(reviews))
        self.assertEqual(0, self.storage.upsert_reviews(reviews))
        self.assertEqual(3, self.storage.count("reviews"))


class RunnerTests(unittest.TestCase):
    def test_max_stores_default_is_ten(self) -> None:
        self.assertEqual(10, build_parser().parse_args([]).max_stores)
        config = {"store_pages": [{"name": str(i), "url": f"https://www.coles.com.au/find-stores/coles/nsw/store-{i}"} for i in range(1, 12)]}
        self.assertEqual(10, len(build_tasks(config, 10)))

    def test_duplicate_discovery_is_removed(self) -> None:
        config = {"store_pages": [
            {"name": "One", "url": "https://www.coles.com.au/find-stores/coles/nsw/a-1"},
            {"name": "One duplicate", "url": "https://www.coles.com.au/find-stores/coles/nsw/b-1"},
        ]}
        self.assertEqual(1, len(build_tasks(config, 10)))

    def test_failure_is_isolated(self) -> None:
        temp_dir = test_directory()
        try:
            storage = Storage(temp_dir / "crawl.sqlite3")
            html = (ROOT / "fixtures" / "store_world_square.html").read_bytes()

            def fetch(url: str) -> FetchResult:
                if url.endswith("-999"):
                    return FetchResult(url, 503, b"unavailable", 12.5, 1, "HTTP 503")
                return FetchResult(url, 200, html, 10.0, 0)

            tasks = [
                {"task_key": "710", "url": "https://www.coles.com.au/find-stores/coles/nsw/world-square-710", "name": "World Square"},
                {"task_key": "999", "url": "https://www.coles.com.au/find-stores/coles/nsw/failure-999", "name": "Failure"},
            ]
            summary = CrawlRunner(storage=storage, data_dir=temp_dir, concurrency=2, fetch=fetch).run(tasks)
            self.assertEqual(1, summary["tasks_succeeded"])
            self.assertEqual(1, summary["tasks_failed"])
            self.assertEqual(1, storage.count("stores"))
            storage.close()
        finally:
            shutil.rmtree(temp_dir)

    def test_rerunning_identical_input_does_not_duplicate_store(self) -> None:
        temp_dir = test_directory()
        try:
            storage = Storage(temp_dir / "crawl.sqlite3")
            html = (ROOT / "fixtures" / "store_world_square.html").read_bytes()
            task = {"task_key": "710", "url": "https://www.coles.com.au/find-stores/coles/nsw/world-square-710", "name": "World Square"}
            fetch = lambda url: FetchResult(url, 200, html, 10.0, 0)
            runner = CrawlRunner(storage=storage, data_dir=temp_dir, concurrency=1, fetch=fetch)
            runner.run([task])
            runner.run([task])
            self.assertEqual(1, storage.count("stores"))
            storage.close()
        finally:
            shutil.rmtree(temp_dir)


if __name__ == "__main__":
    unittest.main()
