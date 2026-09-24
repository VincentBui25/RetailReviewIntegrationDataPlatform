from __future__ import annotations

import argparse
import json
from pathlib import Path
import shutil
import unittest
from uuid import uuid4

from crawl_experiment.discover import build_parser, candidate_limit
from crawl_experiment.discovery import DiscoveryRunner, discover_from_sitemap, enrich_from_store_html
from crawl_experiment.http import FetchResult
from crawl_experiment.storage import Storage


ROOT = Path(__file__).parent
TEST_TMP_ROOT = ROOT.parent / ".test-tmp"
TEST_TMP_ROOT.mkdir(exist_ok=True)
SITEMAP = (ROOT / "fixtures" / "store_sitemap.xml").read_bytes()
DETAIL = (ROOT / "fixtures" / "store_detail_jsonld.html").read_bytes()
KEYWORDS = ["world-square", "central-wynyard", "sydney-cbd", "surry-hills"]


class DiscoveryTests(unittest.TestCase):
    def test_sitemap_discovery_and_deduplication(self) -> None:
        stores, metrics = discover_from_sitemap(SITEMAP, area_path_keywords=KEYWORDS, max_candidates=20)
        self.assertEqual(5, metrics["candidates_seen"])
        self.assertEqual(1, metrics["duplicate_candidates"])
        self.assertEqual(1, metrics["invalid_candidates"])
        self.assertEqual(["710", "840", "7671"], [store.retailer_store_id for store in stores])

    def test_detail_enrichment_captures_location_fields(self) -> None:
        stores, _ = discover_from_sitemap(SITEMAP, area_path_keywords=KEYWORDS, max_candidates=1)
        store = enrich_from_store_html(stores[0], DETAIL)
        self.assertEqual("Coles World Square", store.store_name)
        self.assertEqual("Sydney", store.suburb)
        self.assertEqual("2000", store.postcode)
        self.assertEqual(-33.8777, store.latitude)
        self.assertEqual(151.2068, store.longitude)

    def test_limit_is_at_most_twenty(self) -> None:
        self.assertEqual(10, build_parser().parse_args([]).max_candidates)
        with self.assertRaises(argparse.ArgumentTypeError):
            candidate_limit("21")

    def test_runner_emits_metrics_and_is_idempotent(self) -> None:
        temp_dir = TEST_TMP_ROOT / uuid4().hex
        temp_dir.mkdir()
        try:
            storage = Storage(temp_dir / "crawl.sqlite3")

            def fetch(url: str) -> FetchResult:
                body = SITEMAP if url.endswith("sitemap-stores.xml") else DETAIL
                return FetchResult(url, 200, body, 10.0, 0)

            config = {
                "area": "Sydney CBD test",
                "discovery": {"sitemap_url": "https://www.coles.com.au/sitemap/sitemap-stores.xml", "area_path_keywords": KEYWORDS},
                "store_pages": [],
            }
            runner = DiscoveryRunner(storage=storage, data_dir=temp_dir, fetch=fetch)
            first = runner.run(config, 20)
            second = runner.run(config, 20)
            self.assertEqual(3, first["unique_stores"])
            self.assertEqual(4, first["discovery_requests"])
            self.assertEqual(5, first["candidates_seen"])
            self.assertEqual(1, first["duplicate_candidates"])
            self.assertEqual(1, first["invalid_candidates"])
            self.assertEqual(3, storage.count("stores"))
            records = [json.loads(line) for line in Path(first["stores_path"]).read_text(encoding="utf-8").splitlines()]
            self.assertEqual("official_store_sitemap", records[0]["discovery_source"])
            self.assertEqual(3, second["unique_stores"])
            self.assertEqual(3, storage.count("stores"))
            storage.close()
        finally:
            shutil.rmtree(temp_dir)

    def test_challenge_stops_discovery_and_records_metrics(self) -> None:
        temp_dir = TEST_TMP_ROOT / uuid4().hex
        temp_dir.mkdir()
        try:
            storage = Storage(temp_dir / "crawl.sqlite3")
            calls = []

            def fetch(url: str) -> FetchResult:
                calls.append(url)
                body = b'<script src="/_Incapsula_Resource"></script>'
                return FetchResult(url, 403, body, 8.0, 0, "HTTP 403", 1, 0)

            config = {
                "area": "Sydney CBD test",
                "discovery": {"sitemap_url": "https://www.coles.com.au/sitemap/sitemap-stores.xml", "area_path_keywords": KEYWORDS},
                "store_pages": [],
            }
            summary = DiscoveryRunner(storage=storage, data_dir=temp_dir, fetch=fetch).run(config, 10)
            self.assertEqual("failed", summary["status"])
            self.assertEqual(1, summary["discovery_requests"])
            self.assertEqual(1, summary["http_403_count"])
            self.assertEqual(1, summary["challenge_count"])
            self.assertEqual([], calls[1:])
            storage.close()
        finally:
            shutil.rmtree(temp_dir)


if __name__ == "__main__":
    unittest.main()
