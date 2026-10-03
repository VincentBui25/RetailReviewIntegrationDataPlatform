from __future__ import annotations

import argparse

from crawl_experiment.core.models import Store
from crawl_experiment.runners.full_crawl_runner import (
    DEFAULT_CONCURRENCY,
    DEFAULT_MAX_SCROLLS,
    DEFAULT_OUTPUT_ROOT,
    DEFAULT_PARQUET_ROOT,
    DEFAULT_STATE_DATABASE,
    DEFAULT_TIMEOUT_SECONDS,
    run_full_crawl,
)
# Compatibility aliases for existing external imports.
OUTPUT_ROOT = DEFAULT_OUTPUT_ROOT
CRAWL_TIMEOUT_SECONDS = DEFAULT_TIMEOUT_SECONDS
MAX_SCROLLS = DEFAULT_MAX_SCROLLS
CONCURRENCY = DEFAULT_CONCURRENCY
STATE_DATABASE = DEFAULT_STATE_DATABASE
PARQUET_ROOT = DEFAULT_PARQUET_ROOT

STORES = (
    Store(
        "restaurant-no-1-malatang-world-square",
        "No.1 Malatang - World Square",
        "No.1 Malatang - World Square",
        "restaurant",
    ),
    Store(
        "restaurant-central-ma-la-town",
        "Central Ma La Town",
        "Central Ma La Town",
        "restaurant",
    ),
    Store(
        "restaurant-zhang-liang-malatang-haymarket",
        "Zhang Liang Malatang Haymarket",
        "Zhang Liang Malatang Haymarket",
        "restaurant",
    ),
    Store("coles-local-newport", "Coles Local Newport", "Coles Local Newport", "coles"),
    Store("coles-chullora", "Coles Chullora", "Coles Chullora", "coles"),
    Store(
        "coles-local-sydney-cbd-york-street",
        "Coles Local Sydney CBD - York Street",
        "Coles Local Sydney CBD - York Street",
        "coles",
        "68 York Street, Sydney NSW 2000, Australia",
    ),
    Store("coles-berowra", "Coles Berowra", "Coles Berowra", "coles"),
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Observable Google Maps full crawl")
    parser.add_argument("--store-limit", type=int, default=len(STORES))
    parser.add_argument("--timeout-seconds", type=float, default=CRAWL_TIMEOUT_SECONDS)
    parser.add_argument("--max-scrolls", type=int, default=MAX_SCROLLS)
    parser.add_argument(
        "--store-id",
        action="append",
        choices=[store.id for store in STORES],
    )
    parser.add_argument("--validation-mode", action="store_true")
    parser.add_argument('--crawl-mode',choices=('REVIEW_FULL','REVIEW_INCREMENTAL','REVIEW_RECONCILIATION'),default='REVIEW_FULL')
    parser.add_argument('--known-streak-threshold',type=int,default=5)
    parser.add_argument('--incremental-max-scrolls',type=int,default=20)
    parser.add_argument('--recent-review-limit',type=int,default=100)
    parser.add_argument('--deletion-miss-threshold',type=int,default=3)
    return parser


def select_stores(args: argparse.Namespace) -> tuple[Store, ...]:
    if args.store_id:
        stores_by_id = {store.id: store for store in STORES}
        return tuple(stores_by_id[store_id] for store_id in args.store_id)
    return STORES[: args.store_limit]


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if not 1 <= args.store_limit <= len(STORES):
        raise SystemExit(f"store-limit must be between 1 and {len(STORES)}")
    if args.timeout_seconds <= 0 or args.max_scrolls <= 0:
        raise SystemExit("timeout-seconds and max-scrolls must be positive")
    from crawl_experiment.orchestration.review_incremental import IncrementalConfig
    from crawl_experiment.orchestration.review_reconciliation import ReconciliationConfig
    if args.known_streak_threshold<=0 or args.incremental_max_scrolls<=0:
        raise SystemExit('Incremental threshold and max-scrolls must be positive')
    return run_full_crawl(
        select_stores(args),
        output_root=OUTPUT_ROOT,
        state_database=STATE_DATABASE,
        parquet_root=PARQUET_ROOT,
        timeout_seconds=args.timeout_seconds,
        max_scrolls=args.max_scrolls,
        concurrency=CONCURRENCY,
        validation_mode=args.validation_mode,
        crawl_mode=args.crawl_mode,
        incremental_config=IncrementalConfig(args.known_streak_threshold,args.incremental_max_scrolls),
        reconciliation_config=ReconciliationConfig(args.recent_review_limit,args.incremental_max_scrolls,args.deletion_miss_threshold),
    )


if __name__ == "__main__":
    raise SystemExit(main())
