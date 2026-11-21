"""Publish immutable read-only BI snapshots of an already verified dbt build."""

import argparse
import json
from pathlib import Path
import shutil
import sys
import time

import duckdb

from . import warehouse as wh


def check(connection, manifest):
    """Reconcile verified table hashes, business grains and Gold accounting."""
    primitive = {
        "VARCHAR",
        "BIGINT",
        "INTEGER",
        "SMALLINT",
        "BOOLEAN",
        "DOUBLE",
        "FLOAT",
        "DATE",
    }
    for name, spec in manifest["tables"].items():
        wh.require(
            name in {*wh.MODELS, *wh.MARTS, "release_audit"},
            "Unexpected warehouse table",
        )
        actual = connection.execute(f"describe dimensional.{name}").fetchall()
        projection = ",".join(
            (
                f'"{column}"'
                if kind in primitive
                else f'cast(to_json("{column}") as varchar)'
            )
            for column, kind, *_ in actual
        )
        rows = connection.execute(
            f"select {projection} from dimensional.{name} order by all"
        ).fetchall()
        wh.require(
            len(rows) == spec["rows"]
            and wh.logical_hash(rows) == spec["logical_sha256"],
            "DuckDB content mismatch: " + name,
        )
    kpis = dict(
        connection.execute("select metric,value from bi.v_data_quality").fetchall()
    )
    expected = manifest["expected"]
    for metric, field in {
        "canonical_records": "canonical_store_count",
        "accepted_links": "accepted_mapping_count",
        "source_only_records": "source_only_store_count",
        "source_mapping_rows": "source_mapping_rows",
        "logical_reviews": "current_logical_reviews",
        "attached_reviews": "attached_reviews",
        "unmatched_canonical_reviews": "unmatched_reviews",
        "nonaccepted_candidate_pairs": "unresolved_mapping_count",
    }.items():
        wh.require(
            kpis[metric] == expected[field], "DuckDB/Gold KPI mismatch: " + metric
        )
    for name, keys in wh.MODELS.items():
        group = ",".join(keys)
        duplicates = connection.execute(
            f"select count(*) from (select {group} from dimensional.{name} group by {group} having count(*)<>1)"
        ).fetchone()[0]
        wh.require(duplicates == 0, "Duplicate grain: " + name)
    wh.require(
        connection.execute(
            "select count(*) from (select source_id_kind,source_id from dimensional.bridge_store_source where source='google' group by all having count(distinct store_key)>1)"
        ).fetchone()[0]
        == 0,
        "Google mapping fanout",
    )
    return kpis


def publish(release, output):
    release, output = Path(release).resolve(), Path(output).resolve()
    manifest = wh.verify_bundle(release)
    source = release / "warehouse.duckdb"
    wh.require(source.is_file(), "Missing closed dbt working database: " + str(source))
    source_sha = wh.sha(source)
    sql_path = wh.ROOT / "docker/analytics/serving.sql"
    identity = wh.dump(
        {
            "bundle": manifest["bundle_id"],
            "working_sha256": source_sha,
            "views_sha256": wh.sha(sql_path),
            "publisher_sha256": wh.sha(Path(__file__)),
        }
    )
    import hashlib

    snapshot_id = hashlib.sha256(identity.encode()).hexdigest()[:24]
    destination = output / "serving" / snapshot_id
    from fnb_platform.publication import validate_publication_paths

    validate_publication_paths(destination, [release])
    database = destination / "warehouse.duckdb"
    evidence = destination / "snapshot.json"
    started = time.perf_counter()
    if destination.exists():
        saved = json.loads(evidence.read_text())
        wh.require(
            wh.sha(database) == saved["sha256"], "Existing serving snapshot changed"
        )
    else:
        partial = output / (".snapshot-" + snapshot_id)
        partial.mkdir(parents=True, exist_ok=True)
        target = partial / database.name
        # The working DB is closed by dbt/export. Never replace an open serving file.
        shutil.copyfile(source, target)
        wh.require(
            wh.sha(source) == source_sha == wh.sha(target),
            "Working database changed during copy",
        )
        with duckdb.connect(str(target)) as connection:
            connection.execute("create schema if not exists bi")
            connection.execute(sql_path.read_text())
            kpis = check(connection, manifest)
            connection.execute("checkpoint")
        with duckdb.connect(str(target), read_only=True) as connection:
            wh.require(
                check(connection, manifest) == kpis,
                "Read-only snapshot reconciliation differs",
            )
        saved = dict(
            snapshot_id=snapshot_id,
            bundle_id=manifest["bundle_id"],
            gold_release_id=manifest["gold_release_id"],
            sha256=wh.sha(target),
            database=str(database),
            kpis=kpis,
            expected=manifest["expected"],
            dbt=manifest["dbt"],
            source_manifest_sha256=wh.sha(release / "manifest.json"),
            seconds=round(time.perf_counter() - started, 3),
        )
        (partial / evidence.name).write_text(
            json.dumps(saved, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        destination.parent.mkdir(parents=True, exist_ok=True)
        partial.rename(destination)
    # The URI uses this immutable filename, not a symlink/open file replacement.
    pointer = output / "serving-current.tmp"
    pointer.write_text(
        json.dumps(saved, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    pointer.replace(output / "serving-current.json")
    return saved


def probe(output):
    saved = json.loads((Path(output) / "serving-current.json").read_text())
    queries = {
        "store_count": "select count(*) from bi.v_store_overview",
        "logical_review_count": "select count(*) from bi.v_review_profile",
        "category": "select category_label,count(*) from bi.v_catalog_profile group by all",
        "reference": "select observed_stores_per_km2 from bi.v_area_market",
    }
    timings = {}
    for name, query in queries.items():
        times = []
        for _ in range(5):
            started = time.perf_counter()
            with duckdb.connect(saved["database"], read_only=True) as connection:
                connection.execute(query).fetchall()
            times.append(round((time.perf_counter() - started) * 1000, 3))
        timings[name] = times
    report = dict(
        snapshot_id=saved["snapshot_id"],
        query_ms=timings,
        scope="Read-only DuckDB probe including connection open/close; five runs per query",
    )
    if sys.platform.startswith("linux"):
        import resource

        report["probe_process_peak_mib"] = round(
            resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 2
        )
    (Path(output) / "query-timings.json").write_text(
        json.dumps(report, indent=2), encoding="utf-8"
    )
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--release", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--probe", action="store_true")
    args = parser.parse_args()
    if args.probe:
        print(json.dumps(probe(args.output)))
    else:
        if not args.release:
            parser.error("--release is required unless --probe is selected")
        result = publish(args.release, args.output)
        print(
            json.dumps(
                {k: result[k] for k in ("snapshot_id", "bundle_id", "gold_release_id")}
            )
        )
