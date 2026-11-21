"""Offline business projection of immutable Gold; no matching or source rewriting."""

import argparse
from collections import Counter
import json
import math
import os
from pathlib import Path
import subprocess
import sys

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from shapely.geometry import shape, Point

from .. import publication
from ..google import places_silver as silver
from ..overture_incremental import _digest
from . import staging

ROOT = Path(__file__).resolve().parents[3]
PROJECT = ROOT / "pipelines/business_analytics"
VERSION = "business-analytics-mvp-v1"
AREA_SCHEMA = pa.schema(
    [(k, pa.string()) for k in ("store_id", "area_id", "area_name", "spatial_status")]
    + [("valid_coordinates", pa.bool_())]
)


def gold_facts(stores, mappings, links, attached, unmatched, source_metrics):
    """Count at each original grain; never join source/review rows into store KPIs."""
    ids = {v["store_id"] for v in stores}
    publication.require(len(ids) == len(stores), "Duplicate canonical IDs")
    source_keys = {(v["source"], v["source_id_kind"], v["source_id"]) for v in mappings}
    publication.require(
        len(source_keys) == len(mappings)
        and all(v["store_id"] in ids for v in mappings),
        "Source mapping duplication/FK mismatch",
    )
    source_counts = {
        source: len({v["store_id"] for v in mappings if v["source"] == source})
        for source in sorted({v["source"] for v in mappings})
    }
    review_key = lambda v: (v["source"], v["source_review_id"])
    publication.require(
        len({review_key(v) for v in [*attached, *unmatched]})
        == len(attached) + len(unmatched),
        "Duplicate/fanout logical reviews",
    )
    publication.require(
        all(v["store_id"] in ids for v in attached),
        "Attached review canonical FK mismatch",
    )
    link_keys = {
        (v["overture_id"], v["google_id_kind"], v["google_identity_value"])
        for v in links
    }
    publication.require(len(link_keys) == len(links), "Duplicate link accounting")
    facts = dict(
        canonical_store_count=len(stores),
        source_mapping_rows=len(mappings),
        accepted_mapping_count=sum(
            v["effective_status"] in staging.ACCEPTED for v in links
        ),
        unresolved_mapping_count=sum(
            v["effective_status"] in ("PENDING", "CONFLICT") for v in links
        ),
        source_only_store_count=sum(
            v["identity_status"] == "SOURCE_ONLY" for v in stores
        ),
        attached_reviews=len(attached),
        unmatched_reviews=len(unmatched),
        current_logical_reviews=len(attached) + len(unmatched),
    )
    publication.require(
        all(source_metrics[k] == v for k, v in facts.items()),
        "Dashboard/Gold summary reconciliation mismatch",
    )
    publication.require(
        source_counts.get("overture") == len(stores)
        and source_counts.get("google", 0) == facts["accepted_mapping_count"],
        "Source contribution/accepted mappings mismatch",
    )
    return dict(
        accepted_links=facts["accepted_mapping_count"],
        unresolved_links=facts["unresolved_mapping_count"],
        source_only_stores=facts["source_only_store_count"],
        logical_reviews=facts["current_logical_reviews"],
        gold_category_status_counts=dict(
            Counter(v["category_validation_status"] for v in stores)
        ),
        source_contribution=source_counts,
        link_status_counts=dict(Counter(v["effective_status"] for v in links)),
    )


def boundary(geo_path, meta_path):
    """Use attributed existing geometry; approximate metadata is not a trusted denominator."""
    if not geo_path.is_file() or not meta_path.is_file():
        return None, dict(
            status="BOUNDARY_MISSING", density_status="NO_TRUSTED_AREA_DENOMINATOR"
        )
    feature = json.loads(geo_path.read_text(encoding="utf-8-sig"))
    meta = json.loads(meta_path.read_text(encoding="utf-8-sig"))
    polygon = shape(feature["geometry"])
    publication.require(
        polygon.geom_type in ("Polygon", "MultiPolygon")
        and polygon.is_valid
        and not polygon.is_empty
        and polygon.area > 0,
        "Invalid boundary geometry",
    )
    publication.require(
        meta["crs"].startswith("EPSG:4326")
        and meta.get("source_object_url")
        and meta.get("license")
        and meta.get("retrieval_date"),
        "Missing boundary CRS/provenance",
    )
    publication.require(
        feature["properties"]["admin_area_id"] == meta["admin_area_id"],
        "Boundary identity mismatch",
    )
    meta = dict(
        meta,
        status="AVAILABLE",
        feature=feature,
        density_status="NO_TRUSTED_AREA_DENOMINATOR",
        density_reason="Metadata chỉ có approximate_area_km2; chưa có phương pháp tính/diện tích hành chính được xác thực.",
    )
    return polygon, meta


def assign_area(stores, polygon, meta):
    rows = []
    for row in stores:
        lat, lon = row["latitude"], row["longitude"]
        valid = all(
            isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v)
            for v in (lat, lon)
        )
        valid = valid and -90 <= lat <= 90 and -180 <= lon <= 180
        if not valid:
            code, name, status = (
                "MISSING_COORDINATES",
                "Thiếu tọa độ",
                "MISSING_COORDINATES",
            )
        elif polygon is None:
            code, name, status = "UNASSIGNED", "Chưa có ranh giới", "NO_BOUNDARY"
        elif polygon.covers(Point(lon, lat)):
            code, name, status = meta["admin_area_id"], "Khánh Hội", "INSIDE_BOUNDARY"
        else:
            code, name, status = (
                "OUTSIDE_SUPPORTED_BOUNDARY",
                "Ngoài polygon Khánh Hội",
                "OUTSIDE_BOUNDARY",
            )
        rows.append(
            dict(
                store_id=row["store_id"],
                area_id=code,
                area_name=name,
                spatial_status=status,
                valid_coordinates=valid,
            )
        )
    return pa.Table.from_pylist(rows, schema=AREA_SCHEMA)


def build(gold_release, output, geo_path, meta_path):
    gold = Path(gold_release).resolve()
    manifest = silver.verify_release(gold)
    publication.require(manifest["release_id"] == gold.name, "Gold release ID mismatch")
    publication.require(
        manifest["publication_kind"] == "CANONICAL_GOLD_STAGING_NOT_PRODUCTION",
        "Expected pilot Gold contract",
    )
    pins = dict(manifest["signature"]["inputs_sha256"])
    publication.check_hashes({**pins, **manifest["signature"]["code_sha256"]})
    pins[str(gold / "manifest.json")] = silver.sha(gold / "manifest.json")
    for name, spec in manifest["artifacts"].items():
        pins[str(gold / name)] = spec["sha256"]
    norm_paths = [
        Path(p)
        for p in manifest["signature"]["inputs_sha256"]
        if Path(p).name == "category_normalization.parquet"
    ]
    publication.require(
        len(norm_paths) == 1, "Missing/ambiguous pinned Category normalization"
    )
    normalization = norm_paths[0]
    stores = pq.read_table(gold / "dim_store.parquet").to_pylist()
    publication.require(
        len(stores) == len({v["store_id"] for v in stores}), "Duplicate Gold stores"
    )
    polygon, meta = boundary(geo_path, meta_path)
    for path in (geo_path, meta_path):
        if path.is_file():
            pins[str(path.resolve())] = silver.sha(path)
    area = assign_area(stores, polygon, meta)
    source_metrics = json.loads((gold / "summary.json").read_text())
    facts = gold_facts(
        stores,
        pq.read_table(gold / "store_source_mapping.parquet").to_pylist(),
        pq.read_table(gold / "link_accounting.parquet").to_pylist(),
        pq.read_table(
            gold / "review_attachment.parquet",
            columns=["source", "source_review_id", "store_id"],
        ).to_pylist(),
        pq.read_table(
            gold / "unmatched_review_accounting.parquet",
            columns=["source", "source_review_id"],
        ).to_pylist(),
        source_metrics,
    )
    files = [
        Path(__file__),
        Path(publication.__file__),
        *[
            p
            for p in PROJECT.rglob("*")
            if p.is_file() and p.suffix in (".sql", ".yml")
        ],
    ]
    signature = dict(
        rule_version=VERSION,
        publication_kind="BUSINESS_ANALYTICS_PILOT_NOT_PRODUCTION",
        gold_release_id=gold.name,
        inputs_sha256=pins,
        code_sha256={str(p.resolve()): silver.sha(p) for p in files},
        category_policy="SOURCE_ONLY_SINGLE_CLEAN_OVERTURE_PROVISIONAL; linked disagreements remain unknown",
        density_policy="NO_TRUSTED_AREA_DENOMINATOR",
    )

    def writer(stage, rid):
        input_area = stage / "input_store_area.parquet"
        pq.write_table(area, input_area)
        variables = dict(
            gold_stores_path=str(gold / "dim_store.parquet"),
            category_normalization_path=str(normalization),
            store_area_path=str(input_area),
        )
        env = dict(
            os.environ,
            FNB_DBT_DATABASE=str(stage / "analytics.duckdb"),
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
            json.dumps(variables),
            "--no-use-colors",
        ]
        with (stage / "dbt-build.log").open("w") as log:
            result = subprocess.run(
                args, env=env, stdout=log, stderr=subprocess.STDOUT, timeout=120
            )
        publication.require(
            result.returncode == 0,
            "dbt build failed; retained " + str(stage / "dbt-build.log"),
        )
        results = json.loads((stage / "dbt-target/run_results.json").read_text())[
            "results"
        ]
        publication.require(
            results and all(v["status"] in ("success", "pass") for v in results),
            "dbt test failure/skip",
        )
        dbt = dict(
            models=sum(v["unique_id"].startswith("model.") for v in results),
            tests=sum(v["unique_id"].startswith("test.") for v in results),
            failures=0,
        )
        con = duckdb.connect(str(stage / "analytics.duckdb"), read_only=True)
        try:
            names = (
                "mart_store_business",
                "mart_category_verification",
                "mart_area_catalog",
            )
            tables = {
                name
                + ".parquet": con.execute(
                    "select * from business_analytics."
                    + name
                    + " order by "
                    + ("store_id" if name == "mart_store_business" else "1,2,3")
                ).to_arrow_table()
                for name in names
            }
        finally:
            con.close()
        rows = tables["mart_store_business.parquet"].to_pylist()
        publication.require(
            len(rows) == len(stores) == len({v["store_id"] for v in rows}),
            "Dashboard/Gold KPI mismatch",
        )
        before = {v["store_id"]: v for v in stores}
        for row in rows:
            original = before[row["store_id"]]
            publication.require(
                all(
                    row[k] == original[k]
                    for k in (
                        "overture_id",
                        "latitude",
                        "longitude",
                        "identity_status",
                        "is_identity_human_verified",
                    )
                ),
                "Gold identity changed",
            )
        summary = dict(
            release_id=rid,
            gold_release_id=gold.name,
            gold_manifest_sha256=silver.sha(gold / "manifest.json"),
            canonical_stores=len(rows),
            dbt=dbt,
            original_gold_unknown=sum(
                v["canonical_category"] == "unknown" for v in stores
            ),
            analysis_unknown=sum(v["analysis_category"] == "unknown" for v in rows),
            recovered_source_categories=sum(
                v["analysis_category_basis"] == "OVERTURE_SOURCE_PROVISIONAL"
                for v in rows
            ),
            category_counts=dict(Counter(v["analysis_category"] for v in rows)),
            verification_counts=dict(Counter(v["verification_status"] for v in rows)),
            area_counts=dict(Counter(v["area_name"] for v in rows)),
            valid_coordinates=sum(v["valid_coordinates"] for v in rows),
            source_mapping_rows=source_metrics["source_mapping_rows"],
            attached_reviews=source_metrics["attached_reviews"],
            unmatched_reviews=source_metrics["unmatched_reviews"],
            density_status=meta["density_status"],
            production_gold=False,
            **facts,
        )
        return (
            tables,
            summary,
            [
                "input_store_area.parquet",
                "analytics.duckdb",
                "dbt-build.log",
                "dbt-target/run_results.json",
                "dbt-target/manifest.json",
            ],
        )

    return publication.publish_release(
        output,
        signature,
        writer,
        read_active=silver.read_active,
        verify_release=silver.verify_release,
        digest=_digest,
        dump=silver.dump,
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gold-release", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--boundary",
        type=Path,
        default=ROOT / "config/admin_areas/phuong_khanh_hoi.geojson",
    )
    parser.add_argument(
        "--boundary-metadata",
        type=Path,
        default=ROOT / "config/admin_areas/phuong_khanh_hoi_metadata.json",
    )
    args = parser.parse_args()
    release, summary = build(
        args.gold_release, args.output, args.boundary, args.boundary_metadata
    )
    print(
        json.dumps(
            dict(status="PASS", release=str(release), summary=summary),
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
