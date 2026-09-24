from __future__ import annotations

import argparse
import json
from pathlib import Path

from .discovery import DiscoveryRunner
from .runner import client_from_config
from .storage import Storage


def candidate_limit(value: str) -> int:
    parsed = int(value)
    if not 1 <= parsed <= 20:
        raise argparse.ArgumentTypeError("must be between 1 and 20")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Dynamic Coles store discovery")
    parser.add_argument("--config", default="config/sydney_cbd.json")
    parser.add_argument("--max-candidates", type=candidate_limit, default=10)
    parser.add_argument("--request-interval", type=float)
    parser.add_argument("--max-retries", type=int)
    parser.add_argument("--database", default="data/crawl_v0.sqlite3")
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--no-enrich", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = json.loads(Path(args.config).read_text(encoding="utf-8"))
    interval = args.request_interval if args.request_interval is not None else float(config.get("request_interval_seconds", 2.0))
    retries = args.max_retries if args.max_retries is not None else int(config.get("max_retries", 1))
    if interval < 0:
        raise SystemExit("request interval must be non-negative")
    if retries < 0 or retries > 5:
        raise SystemExit("max retries must be between 0 and 5")
    storage = Storage(args.database)
    try:
        client = client_from_config(config, interval, retries)
        summary = DiscoveryRunner(storage=storage, data_dir=args.data_dir, fetch=client.fetch).run(
            config, args.max_candidates, enrich=not args.no_enrich
        )
        print(json.dumps(summary, indent=2, sort_keys=True))
        return 0 if summary["status"] == "completed" else 2
    finally:
        storage.close()


if __name__ == "__main__":
    raise SystemExit(main())

