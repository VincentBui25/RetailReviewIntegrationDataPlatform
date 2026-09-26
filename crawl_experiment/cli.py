from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

from .browser.browser_factory import BrowserFactory
from .core.models import Store
from .orchestration.crawlee_adapter import CrawleeAdapter
from .sources.google_maps.crawler import GoogleMapsCrawler
from .storage.checkpoint_repository import CheckpointRepository
from .storage.review_repository import ReviewRepository

DEFAULT_MANIFEST = "config/known_coles_stores.json"
DEFAULT_DATABASE = "data/reviews.sqlite3"
DEFAULT_CHECKPOINT_DIRECTORY = "data/checkpoints"
MAX_CONCURRENCY = 10


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Modular Google Maps review crawler")
    parser.add_argument("--manifest", default=DEFAULT_MANIFEST)
    parser.add_argument("--database", default=DEFAULT_DATABASE)
    parser.add_argument("--checkpoints", default=DEFAULT_CHECKPOINT_DIRECTORY)
    parser.add_argument("--concurrency", type=int, default=1)
    parser.add_argument("--headed", action="store_true")
    return parser


def load_stores(path: str | Path) -> list[Store]:
    manifest = json.loads(Path(path).read_text(encoding="utf-8"))
    return [
        Store(
            id=f"coles-{item['retailer_store_id']}",
            name=item["store_name"],
            query=item["store_name"],
            retailer="coles",
            expected_address=item.get("address"),
        )
        for item in manifest["stores"]
    ]


async def run(args: argparse.Namespace) -> None:
    review_repository = ReviewRepository(args.database)
    try:
        crawler = GoogleMapsCrawler(
            review_repository,
            CheckpointRepository(args.checkpoints),
        )
        adapter = CrawleeAdapter(
            BrowserFactory(headless=not args.headed),
            crawler.crawl,
            max_concurrency=args.concurrency,
        )
        await adapter.run(load_stores(args.manifest))
        print(json.dumps([result.__dict__ for result in adapter.results], indent=2, default=str))
    finally:
        review_repository.close()


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if not 1 <= args.concurrency <= MAX_CONCURRENCY:
        raise SystemExit(f"concurrency must be between 1 and {MAX_CONCURRENCY}")
    asyncio.run(run(args))
    return 0
