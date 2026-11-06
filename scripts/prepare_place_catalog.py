"""Prepare one reusable ward catalog from a cached snapshot or explicit Overture release."""
from __future__ import annotations

import argparse
import importlib.metadata
import json
from datetime import datetime, timezone
from pathlib import Path

from crawl_experiment.storage.place_catalog import (
    DEFAULT_CATALOG, DEFAULT_SOURCE, DEFAULT_SOURCE_METADATA, DEFAULT_WARD, prepare_catalog,
)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--source-metadata", type=Path, default=DEFAULT_SOURCE_METADATA)
    parser.add_argument("--ward", type=Path, default=DEFAULT_WARD)
    parser.add_argument("--output", type=Path, default=DEFAULT_CATALOG)
    parser.add_argument("--download", action="store_true", help="Explicit bounded bbox access, not latest-release discovery")
    parser.add_argument("--release", help="Actual release to query; required with --download")
    args = parser.parse_args(argv)
    observed_at = datetime.now(timezone.utc).isoformat()
    reader = None
    if args.download:
        if not args.release:
            parser.error("--download requires an explicit --release")
        from overturemaps import record_batch_reader
        lineage = dict(source_release=args.release, client_version=importlib.metadata.version("overturemaps"),
                       retrieved_at=observed_at, source_catalog="https://stac.overturemaps.org/catalog.json",
                       license_attribution="Overture Maps source-specific attribution: https://docs.overturemaps.org/attribution/")
        def reader(*, bbox, release):
            batches = record_batch_reader("place", bbox=bbox, release=release, stac=True,
                                          connect_timeout=10, request_timeout=60)
            if batches is None:
                raise RuntimeError("Overture returned no reader")
            return batches.read_all()
    else:
        lineage = json.loads(args.source_metadata.read_text(encoding="utf-8"))
    _, manifest = prepare_catalog(args.source, args.ward, args.output, lineage,
                                 observed_at=observed_at, reader=reader)
    print(json.dumps(dict(catalog_path=str(args.output), **manifest), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
