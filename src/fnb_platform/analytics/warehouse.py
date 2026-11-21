"""Build a versioned DuckDB warehouse from pinned publications. No upstream execution."""

import argparse
from collections import Counter
from datetime import date, datetime, timezone
import hashlib
import json
import os
import re
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[3]
PROJECT = ROOT / "pipelines/dimensional_analytics"
MODELS = {
    "dim_store": ["store_key"],
    "dim_category": ["category_key"],
    "dim_area": ["area_key"],
    "dim_date": ["date_key"],
    "fact_store_snapshot": ["gold_release_id", "store_key"],
    "fact_review": ["logical_review_id"],
    "bridge_store_source": ["gold_release_id", "source", "source_id_kind", "source_id"],
}
MARTS = ("mart_store_business", "mart_category_verification", "mart_area_catalog")
KIND = "FNB_DIMENSIONAL_ANALYTICS_V1"


def require(condition, message):
    if not condition:
        raise ValueError(message)


def sha(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def dump(value):
    return json.dumps(
        value,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    )


def logical_hash(rows):
    def canonical(v):
        if isinstance(v, datetime):
            return v.astimezone(timezone.utc).isoformat() if v.tzinfo else v.isoformat()
        return v.isoformat() if isinstance(v, date) else v

    return hashlib.sha256(
        dump([[canonical(v) for v in row] for row in rows]).encode()
    ).hexdigest()


def demographic_input(release, manifest_sha, ward_code):
    """Admit an immutable reference, independently of the old catalog enrichment."""
    import pyarrow.parquet as pq
    import pyarrow as pa

    requested = Path(release)
    require(
        requested.parent.name == "releases"
        and re.fullmatch("[a-f0-9]{24}", requested.name),
        "Demographics must select an explicit published release, not latest/current",
    )
    release = requested.resolve()
    require(
        release.name == requested.name, "Demographics release path identity mismatch"
    )
    manifest_path = release / "manifest.json"
    require(
        manifest_path.is_file() and sha(manifest_path) == manifest_sha,
        "Demographics manifest SHA256 mismatch/missing",
    )
    manifest = json.loads(manifest_path.read_text())
    require(
        manifest.get("publication_kind") == "AREA_DEMOGRAPHICS_SILVER"
        and manifest["signature"].get("publication_kind") == "AREA_DEMOGRAPHICS_SILVER"
        and manifest.get("release_id") == release.name
        and hashlib.sha256(dump(manifest["signature"]).encode()).hexdigest()[:24]
        == release.name,
        "Invalid demographics publication/release identity",
    )
    pins = {str(manifest_path): manifest_sha}
    require(
        {"area_demographics.parquet", "summary.json", "dq_summary.json"}
        <= set(manifest["artifacts"]),
        "Missing mandatory demographics publication artifacts",
    )
    for name, spec in manifest["artifacts"].items():
        path = (release / name).resolve()
        require(
            path.is_relative_to(release)
            and path.is_file()
            and sha(path) == spec["sha256"],
            "Demographics artifact SHA256 mismatch: " + name,
        )
        pins[str(path)] = spec["sha256"]
    path = release / "area_demographics.parquet"
    fields = [
        "ward_code",
        "ward_name",
        "city_code",
        "legal_code_status",
        "area_km2",
        "population",
        "reference_year",
        "source_version",
        "source_url",
        "source_record_url",
        "population_basis",
        "area_basis",
        "data_quality_status",
        "provenance_json",
    ]
    schema = pq.read_schema(path)
    require(set(fields) <= set(schema.names), "Incomplete demographics schema")
    require(
        (
            pa.types.is_integer(schema.field("population").type)
            or pa.types.is_null(schema.field("population").type)
        )
        and pa.types.is_integer(schema.field("reference_year").type),
        "Invalid demographics numeric schema",
    )
    rows = pq.read_table(path, columns=fields).to_pylist()
    require(
        len({r["ward_code"] for r in rows}) == len(rows),
        "Duplicate demographics ward grain",
    )
    selected = [
        r for r in rows if r["ward_code"] == ward_code and r["city_code"] == "79"
    ]
    require(
        len(selected) == 1, "Missing/ambiguous demographics for the selected boundary"
    )
    row = selected[0]
    require(
        row["legal_code_status"] == "CODE_AND_NAME_AGREE",
        "Demographics legal code/name conflict",
    )
    require(
        row["reference_year"]
        and row["source_version"]
        and row["source_url"]
        and row["provenance_json"],
        "Missing demographics provenance/year",
    )
    require(
        row["population_basis"] == "2025_REORGANIZATION_PROPOSAL_NOT_CENSUS"
        and row["reference_year"] == 2025,
        "Unsupported demographics population reference basis",
    )
    require(
        row["area_basis"] and row["data_quality_status"],
        "Missing demographics quality/basis",
    )
    require(
        (row["area_km2"] is None or row["area_km2"] > 0)
        and (row["population"] is None or row["population"] >= 0),
        "Invalid demographics denominators",
    )
    return row, pins


def inputs(gold, analytics, gold_sha, analytics_sha, demographics, demographics_sha):
    """Verify only published analytics inputs, not raw captures or changing current pointers."""
    import pyarrow.parquet as pq

    gold, analytics = Path(gold).resolve(), Path(analytics).resolve()
    pins = {}

    def tracked(path, expected):
        require(path.is_file(), "Missing input: " + str(path))
        require(sha(path) == expected, "SHA256 mismatch: " + str(path))
        pins[str(path)] = expected
        return path

    tracked(gold / "manifest.json", gold_sha)
    tracked(analytics / "manifest.json", analytics_sha)
    gm = json.loads((gold / "manifest.json").read_text())
    am = json.loads((analytics / "manifest.json").read_text())
    require(
        gm["release_id"] == gold.name
        and gm["publication_kind"] == "CANONICAL_GOLD_STAGING_NOT_PRODUCTION",
        "Expected pinned Gold publication",
    )
    require(
        am["release_id"] == analytics.name
        and am["signature"]["gold_release_id"] == gold.name,
        "Analytics/Gold release mismatch",
    )
    require(
        am["signature"]["inputs_sha256"].get(str(gold / "manifest.json")) == gold_sha,
        "Analytics/Gold manifest lineage mismatch",
    )
    for relative in (
        "config/admin_areas/phuong_khanh_hoi.geojson",
        "config/admin_areas/phuong_khanh_hoi_metadata.json",
    ):
        path = ROOT / relative
        require(
            str(path) in am["signature"]["inputs_sha256"],
            "Boundary not pinned by reused analytics",
        )
        tracked(path, am["signature"]["inputs_sha256"][str(path)])
    files = {}
    for key, name in [
        ("gold_stores", "dim_store.parquet"),
        ("gold_mappings", "store_source_mapping.parquet"),
        ("gold_attached", "review_attachment.parquet"),
        ("gold_unmatched", "unmatched_review_accounting.parquet"),
    ]:
        files[key] = tracked(gold / name, gm["artifacts"][name]["sha256"])
    tracked(gold / "summary.json", gm["artifacts"]["summary.json"]["sha256"])
    link_path = tracked(
        gold / "link_accounting.parquet",
        gm["artifacts"]["link_accounting.parquet"]["sha256"],
    )
    for name in MARTS:
        files[name] = tracked(
            analytics / (name + ".parquet"),
            am["artifacts"][name + ".parquet"]["sha256"],
        )
    pp = [
        Path(p)
        for p in gm["signature"]["inputs_sha256"]
        if Path(p).name == "google_places.parquet"
    ]
    require(len(pp) == 1, "Missing/ambiguous pinned Places artifact")
    places = pp[0]
    files["google_places"] = tracked(
        places, gm["signature"]["inputs_sha256"][str(places)]
    )
    places_release = places.parent.parent
    lineages = {
        r["gold_review_lineage_json"]
        for key in ("gold_attached", "gold_unmatched")
        for r in pq.read_table(
            files[key], columns=["gold_review_lineage_json"]
        ).to_pylist()
    }
    require(len(lineages) == 1, "Review release lineage differs within Gold")
    lineage = json.loads(next(iter(lineages)))
    tracked(places_release / "manifest.json", lineage["places_manifest_sha256"])
    pm = json.loads((places_release / "manifest.json").read_text())
    require(
        pm["release_id"] == places_release.name
        and Path(lineage["google_places_silver_release"]).name == places_release.name,
        "Places release lineage mismatch",
    )
    reviews_manifest = (
        gold.parents[2]
        / "reviews/releases"
        / lineage["reviews_release_id"]
        / "manifest.json"
    )
    tracked(reviews_manifest, lineage["reviews_manifest_sha256"])
    require(
        json.loads(reviews_manifest.read_text())["release_id"]
        == lineage["reviews_release_id"],
        "Reviews release ID mismatch",
    )
    expected = json.loads((gold / "summary.json").read_text())
    metadata = Counter(
        r["metadata_status"]
        for r in pq.read_table(places, columns=["metadata_status"]).to_pylist()
    )
    expected.update(
        google_places_count=sum(metadata.values()),
        google_available=metadata["AVAILABLE"],
        google_incomplete=metadata["INCOMPLETE_METADATA"],
        google_missing=metadata["MISSING_METADATA"],
    )
    decisions = Counter(
        (
            r["effective_status"]
            if r["effective_status"] in ("AUTO_ACCEPTED", "HUMAN_ACCEPTED")
            else r["final_decision"]
        )
        for r in pq.read_table(
            link_path, columns=["effective_status", "final_decision"]
        ).to_pylist()
    )
    expected.update(
        er_candidate_pairs=sum(decisions.values()), er_decision_counts=dict(decisions)
    )
    meta = json.loads(
        (ROOT / "config/admin_areas/phuong_khanh_hoi_metadata.json").read_text()
    )
    area, population_pins = demographic_input(
        demographics, demographics_sha, meta["official_unit_code"]
    )
    pins.update(population_pins)
    area_reference = dict(
        reference_area_km2=area["area_km2"],
        reference_population=area["population"],
        reference_year=area["reference_year"],
        reference_source_version=area["source_version"],
        reference_source_url=area["source_url"],
        reference_record_url=area["source_record_url"],
        reference_population_basis=area["population_basis"],
        reference_area_basis=area["area_basis"],
        reference_quality_status=area["data_quality_status"],
        reference_provenance_json=area["provenance_json"],
        reference_release_id=Path(demographics).name,
    )
    expected["area_reference"] = dict(
        area_business_key=meta["admin_area_id"], **area_reference
    )
    return dict(
        gold_release_id=gold.name,
        places_release_id=places_release.name,
        reviews_release_id=lineage["reviews_release_id"],
        files=files,
        pins=pins,
        expected=expected,
        area_reference=area_reference,
    )


def verify_bundle(root):
    root = Path(root)
    manifest = json.loads((root / "manifest.json").read_text())
    require(
        manifest["kind"] == KIND and root.name == manifest["bundle_id"],
        "Invalid warehouse bundle identity",
    )
    require(
        hashlib.sha256(dump(manifest["signature"]).encode()).hexdigest()[:24]
        == manifest["bundle_id"],
        "Warehouse release ID/signature mismatch",
    )
    for relative, expected in manifest["artifacts"].items():
        path = (root / relative).resolve()
        require(
            path.is_relative_to(root.resolve()) and sha(path) == expected,
            "Warehouse artifact checksum mismatch: " + relative,
        )
    return manifest


def build(
    gold,
    analytics,
    output,
    gold_sha,
    analytics_sha,
    demographics,
    demographics_sha,
    *,
    demo=False,
):
    import duckdb
    import pyarrow as pa
    import pyarrow.parquet as pq
    from fnb_platform.gold.business_mvp import boundary
    from fnb_platform import publication

    started = time.perf_counter()
    from .demo import context as demo_context

    output = Path(output).resolve()
    context = (
        demo_context(output)
        if demo
        else inputs(
            gold, analytics, gold_sha, analytics_sha, demographics, demographics_sha
        )
    )
    code = [
        Path(__file__),
        Path(publication.__file__),
        ROOT / "docker/analytics/serving.sql",
        *sorted(PROJECT.rglob("*.sql")),
        *sorted(PROJECT.rglob("*.yml")),
    ]
    if demo:
        code.append(Path(__file__).with_name("demo.py"))
    publication.validate_publication_paths(
        output / "releases",
        [*([] if demo else [gold, analytics]), *context["pins"], *code],
    )
    signature = dict(
        version=KIND,
        inputs=context["pins"],
        code={str(p.relative_to(ROOT)): sha(p) for p in code},
    )
    bundle_id = hashlib.sha256(dump(signature).encode()).hexdigest()[:24]
    release = output / "releases" / bundle_id
    if release.exists():
        verify_bundle(release)
        print(dump(dict(status="REUSED_VERIFIED", bundle_id=bundle_id)))
    else:
        stage = output / (".partial-" + bundle_id)
        stage.mkdir(parents=True, exist_ok=True)
        geo = ROOT / "config/admin_areas/phuong_khanh_hoi.geojson"
        meta_path = geo.with_name("phuong_khanh_hoi_metadata.json")
        _, meta = boundary(geo, meta_path)
        reference = dict(
            area_id=meta["admin_area_id"],
            area_name="Khánh Hội",
            boundary_source=meta["source"],
            boundary_url=meta["source_object_url"],
            boundary_snapshot=meta["retrieval_date"],
            boundary_license=meta["license"],
            boundary_sha256=sha(geo),
            boundary_geojson=dump(json.loads(geo.read_text())),
            **context["area_reference"],
        )
        pq.write_table(
            pa.Table.from_pylist([reference]), stage / "area-reference.parquet"
        )
        variables = {
            k: str(v)
            for k, v in context["files"].items()
            if k.startswith("gold_") or k == "google_places"
        }
        variables.update(
            gold_release_id=context["gold_release_id"],
            places_release_id=context["places_release_id"],
            business_mart=str(context["files"]["mart_store_business"]),
            area_reference=str(stage / "area-reference.parquet"),
        )
        env = dict(
            os.environ,
            FNB_WAREHOUSE_DUCKDB=str(stage / "warehouse.duckdb"),
            DBT_SEND_ANONYMOUS_USAGE_STATS="false",
            DBT_PARTIAL_PARSE="false",
        )
        args = [
            sys.executable,
            "-B",
            "-c",
            "from dbt.cli.main import cli; cli()",
            "build",
            "--project-dir",
            str(PROJECT),
            "--profiles-dir",
            str(PROJECT),
            "--target-path",
            str(stage / "dbt-target"),
            "--log-path",
            str(stage / "dbt-logs"),
            "--vars",
            dump(variables),
            "--no-use-colors",
        ]
        with (stage / "dbt.log").open("w") as log:
            result = subprocess.run(
                args, env=env, stdout=log, stderr=subprocess.STDOUT, timeout=600
            )
        require(
            result.returncode == 0,
            "Dimensional dbt build failed; retained " + str(stage / "dbt.log"),
        )
        results = json.loads((stage / "dbt-target/run_results.json").read_text())[
            "results"
        ]
        require(
            results and all(r["status"] in ("success", "pass") for r in results),
            "Dimensional dbt result failed/skipped",
        )
        require(
            sum(r["unique_id"].startswith("model.") for r in results) == len(MODELS)
            and sum(r["unique_id"].startswith("test.") for r in results) > 0,
            "Missing dimensional models or real dbt data tests",
        )
        tables = {}
        with duckdb.connect(str(stage / "warehouse.duckdb")) as con:
            for name in MARTS:
                path = str(context["files"][name]).replace("'", "''")
                con.execute(
                    f"create or replace table dimensional.{name} as select * from read_parquet('{path}')"
                )
            payload = dump(context["expected"])
            con.execute(
                "create or replace table dimensional.release_audit (gold_release_id varchar, payload varchar)"
            )
            con.execute(
                "insert into dimensional.release_audit values (?,?)",
                [context["gold_release_id"], payload],
            )
            for name in [*MODELS, *MARTS, "release_audit"]:
                columns = con.execute(f"describe dimensional.{name}").fetchall()
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
                projection = ",".join(
                    (
                        f'"{col}"'
                        if typ in primitive
                        else f'cast(to_json("{col}") as varchar) as "{col}"'
                    )
                    for col, typ, *_ in columns
                )
                query = f"select {projection} from dimensional.{name} order by all"
                cursor = con.execute(query)
                description = [(c[0], str(c[1])) for c in cursor.description]
                rows = cursor.fetchall()
                tables[name] = dict(
                    columns=description,
                    rows=len(rows),
                    logical_sha256=logical_hash(rows),
                    order="ALL",
                )
        # Recheck exactly the admitted immutable files after all reads; no upstream writes.
        for path, expected in context["pins"].items():
            require(
                sha(path) == expected, "Published input changed during analytics build"
            )
        artifacts = {"warehouse.duckdb": sha(stage / "warehouse.duckdb")}
        artifacts["dbt-target/run_results.json"] = sha(
            stage / "dbt-target/run_results.json"
        )
        manifest = dict(
            kind=KIND,
            bundle_id=bundle_id,
            gold_release_id=context["gold_release_id"],
            places_release_id=context["places_release_id"],
            reviews_release_id=context["reviews_release_id"],
            signature=signature,
            tables=tables,
            artifacts=artifacts,
            expected=context["expected"],
            dbt_vars=variables,
            dbt=dict(
                models=sum(r["unique_id"].startswith("model.") for r in results),
                tests=sum(r["unique_id"].startswith("test.") for r in results),
            ),
            total_seconds=round(time.perf_counter() - started, 3),
        )
        if sys.platform.startswith("linux"):
            import resource

            manifest["build_driver_peak_mib"] = round(
                resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 2
            )
            manifest["dbt_child_peak_mib"] = round(
                resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss / 1024, 2
            )
        (stage / "manifest.json").write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        release.parent.mkdir(parents=True, exist_ok=True)
        stage.rename(release)
        print(dump(dict(status="BUILT", bundle_id=bundle_id, dbt=manifest["dbt"])))
    pointer = output / "current.tmp"
    pointer.write_text(
        dump(dict(bundle_id=bundle_id, manifest_sha256=sha(release / "manifest.json"))),
        encoding="utf-8",
    )
    pointer.replace(output / "current.json")
    return release


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gold", type=Path)
    parser.add_argument("--analytics", type=Path)
    parser.add_argument("--gold-sha")
    parser.add_argument("--analytics-sha")
    parser.add_argument("--demographics", type=Path)
    parser.add_argument("--demographics-sha")
    parser.add_argument("--output", type=Path, default=Path("/analytics-out"))
    parser.add_argument("--check-only", action="store_true")
    parser.add_argument(
        "--demo",
        action="store_true",
        help="Only invented, explicitly labeled demo data",
    )
    parser.add_argument(
        "--serve",
        action="store_true",
        help="Publish an independent read-only DuckDB snapshot",
    )
    args = parser.parse_args()
    if not args.demo and not all(
        [
            args.gold,
            args.analytics,
            args.gold_sha,
            args.analytics_sha,
            args.demographics,
            args.demographics_sha,
        ]
    ):
        parser.error(
            "Real mode requires all six explicit input/release hash arguments; no demo fallback"
        )
    if args.demo and args.check_only:
        parser.error("Demo is a separate synthetic build, never real-input admission")
    if args.check_only:
        value = inputs(
            args.gold,
            args.analytics,
            args.gold_sha,
            args.analytics_sha,
            args.demographics,
            args.demographics_sha,
        )
        print(
            dump(
                dict(
                    status="INPUTS_VERIFIED",
                    gold_release=value["gold_release_id"],
                    reviews_release=value["reviews_release_id"],
                    places_release=value["places_release_id"],
                    published_files_verified=len(value["pins"]),
                )
            )
        )
    else:
        release = build(
            args.gold,
            args.analytics,
            args.output,
            args.gold_sha,
            args.analytics_sha,
            args.demographics,
            args.demographics_sha,
            demo=args.demo,
        )
        if args.serve:
            from .duckdb_serving import publish

            snapshot = publish(release, args.output)
            print(
                dump(
                    dict(
                        status="SERVING_READY",
                        snapshot_id=snapshot["snapshot_id"],
                        bundle_id=snapshot["bundle_id"],
                    )
                )
            )
