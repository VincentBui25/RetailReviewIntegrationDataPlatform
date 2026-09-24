from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

from .parsers import load_review_batches, parse_review_batches
from .runner import CrawlRunner, build_tasks, client_from_config
from .storage import Storage


def max_stores_value(value: str) -> int:
    parsed = int(value)
    if not 1 <= parsed <= 10:
        raise argparse.ArgumentTypeError("must be between 1 and 10")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Bounded Coles Crawl Experiment v0")
    parser.add_argument("--config", default="config/sydney_cbd.json")
    parser.add_argument("--max-stores", type=max_stores_value, default=10)
    parser.add_argument("--concurrency", type=int)
    parser.add_argument("--request-interval", type=float)
    parser.add_argument("--max-retries", type=int)
    parser.add_argument("--database", default="data/crawl_v0.sqlite3")
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--resume-run-id")
    parser.add_argument("--review-batch", action="append", default=[])
    parser.add_argument("--review-store-id")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = json.loads(Path(args.config).read_text(encoding="utf-8"))
    concurrency = args.concurrency if args.concurrency is not None else int(config.get("concurrency", 1))
    interval = args.request_interval if args.request_interval is not None else float(config.get("request_interval_seconds", 2.0))
    retries = args.max_retries if args.max_retries is not None else int(config.get("max_retries", 1))
    if concurrency < 1 or concurrency > 10:
        raise SystemExit("concurrency must be between 1 and 10")
    if interval < 0:
        raise SystemExit("request interval must be non-negative")
    if retries < 0 or retries > 5:
        raise SystemExit("max retries must be between 0 and 5")

    storage = Storage(args.database)
    try:
        if args.review_batch:
            if not args.review_store_id:
                raise SystemExit("--review-store-id is required with --review-batch")
            reviews = parse_review_batches(
                load_review_batches(args.review_batch),
                source_code=str(config.get("source_code", "google")),
                retailer_store_id=args.review_store_id,
            )
            inserted = storage.upsert_reviews(reviews)
            print(json.dumps({"reviews_processed": len(reviews), "reviews_inserted": inserted}, indent=2))
            return 0

        if args.resume_run_id:
            tasks = storage.failed_tasks(args.resume_run_id)
            if not tasks:
                print(json.dumps({"message": "no failed tasks to resume", "run_id": args.resume_run_id}, indent=2))
                return 0
        else:
            tasks = build_tasks(config, args.max_stores)
        client = client_from_config(config, interval, retries)
        runner = CrawlRunner(
            storage=storage,
            data_dir=args.data_dir,
            concurrency=concurrency,
            fetch=client.fetch,
        )
        summary = runner.run(tasks, args.resume_run_id)
        print(json.dumps(summary, indent=2, sort_keys=True))
        return 0 if summary["tasks_failed"] == 0 else 2
    finally:
        storage.close()


if __name__ == "__main__":
    sys.exit(main())
