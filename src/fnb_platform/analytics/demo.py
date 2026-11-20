"""Invented, non-PII demo sources; never substitute these for real admission."""

from datetime import datetime, timezone
from collections import Counter
from pathlib import Path
import json
import os
import subprocess
import sys

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq


def context(output):
    """Build the existing three business marts on explicitly synthetic inputs."""
    from . import warehouse as wh

    folder = Path(output) / "demo-sources"
    rows = create_sources(folder)
    meta = json.loads(
        (wh.ROOT / "config/admin_areas/phuong_khanh_hoi_metadata.json").read_text()
    )
    area_id = meta["admin_area_id"]
    for row in rows["stores"]:
        row.update(
            overture_id=row["store_id"],
            google_identity_value=(
                "synthetic-google-a"
                if row["identity_status"] == "AUTO_ACCEPTED"
                else None
            ),
            category_relation=(
                "CONSISTENT" if row["identity_status"] == "AUTO_ACCEPTED" else None
            ),
        )
    pq.write_table(pa.Table.from_pylist(rows["stores"]), folder / "stores.parquet")
    normal = [
        dict(
            source="overture",
            source_id=r["store_id"],
            normalized_categories=["cafe_coffee" if i == 0 else "restaurant"],
            dq_flags=[],
            source_provenance_json='{"synthetic":true}',
        )
        for i, r in enumerate(rows["stores"])
    ]
    areas = [
        dict(
            store_id=r["store_id"],
            area_id=area_id,
            area_name="Khánh Hội",
            spatial_status="SYNTHETIC_DEMO",
            valid_coordinates=True,
        )
        for r in rows["stores"]
    ]
    for name, data in [("normal", normal), ("store_area", areas)]:
        pq.write_table(pa.Table.from_pylist(data), folder / (name + ".parquet"))
    project = wh.ROOT / "pipelines/business_analytics"
    target = folder / "business-target"
    variables = dict(
        gold_stores_path=str(folder / "stores.parquet"),
        category_normalization_path=str(folder / "normal.parquet"),
        store_area_path=str(folder / "store_area.parquet"),
    )
    env = dict(
        os.environ,
        FNB_DBT_DATABASE=str(folder / "business.duckdb"),
        DBT_SEND_ANONYMOUS_USAGE_STATS="false",
    )
    command = [
        sys.executable,
        "-B",
        "-c",
        "from dbt.cli.main import cli;cli()",
        "build",
        "--project-dir",
        str(project),
        "--profiles-dir",
        str(project),
        "--target-path",
        str(target),
        "--log-path",
        str(folder / "business-logs"),
        "--vars",
        json.dumps(variables),
    ]
    with (folder / "business-build.log").open("w") as log:
        result = subprocess.run(
            command, env=env, stdout=log, stderr=subprocess.STDOUT, timeout=120
        )
    wh.require(
        result.returncode == 0,
        "Demo business dbt build failed; inspect business-build.log",
    )
    results = json.loads((target / "run_results.json").read_text())["results"]
    wh.require(
        results and all(r["status"] in ("success", "pass") for r in results),
        "Demo business dbt tests failed/skipped",
    )
    files = {
        "gold_stores": folder / "stores.parquet",
        "gold_mappings": folder / "mappings.parquet",
        "gold_attached": folder / "attached.parquet",
        "gold_unmatched": folder / "unmatched.parquet",
        "google_places": folder / "places.parquet",
        "demo_decisions": folder / "decisions.parquet",
    }
    with duckdb.connect(str(folder / "business.duckdb"), read_only=True) as con:
        for name in wh.MARTS:
            path = folder / (name + ".parquet")
            con.execute(
                "copy (select * from business_analytics."
                + name
                + " order by all) to '"
                + str(path).replace("'", "''")
                + "' (format parquet)"
            )
            files[name] = path
    reference = {
        key: value
        for key, value in rows["areas"][0].items()
        if key.startswith("reference_")
    }
    reference.update(
        reference_population_basis="SYNTHETIC_DEMO_NOT_REAL_POPULATION",
        reference_area_basis="SYNTHETIC_DEMO_NOT_REAL_AREA",
        reference_quality_status="SYNTHETIC",
    )
    expected = dict(
        canonical_store_count=len(rows["stores"]),
        accepted_mapping_count=sum(r["source"] == "google" for r in rows["mappings"]),
        source_only_store_count=sum(
            r["identity_status"] == "SOURCE_ONLY" for r in rows["stores"]
        ),
        source_mapping_rows=len(rows["mappings"]),
        unresolved_mapping_count=sum(
            r["effective_status"] not in {"AUTO_ACCEPTED", "HUMAN_ACCEPTED"}
            for r in rows["decisions"]
        ),
        current_logical_reviews=len(rows["attached"]) + len(rows["unmatched"]),
        attached_reviews=len(rows["attached"]),
        unmatched_reviews=len(rows["unmatched"]),
        category_validation_counts=dict(
            Counter(r["category_validation_status"] for r in rows["stores"])
        ),
        er_candidate_pairs=len(rows["decisions"]),
        er_decision_counts=dict(
            Counter(r["effective_status"] for r in rows["decisions"])
        ),
        area_reference=dict(area_business_key=area_id, **reference),
        synthetic_demo=True,
    )
    return dict(
        gold_release_id="synthetic-gold",
        places_release_id="synthetic-places",
        reviews_release_id="synthetic-reviews",
        files=files,
        pins={str(p): wh.sha(p) for p in files.values()},
        expected=expected,
        area_reference=reference,
    )


def create_sources(folder):
    folder.mkdir(parents=True, exist_ok=True)
    s = [
        dict(
            store_id="synthetic-a",
            store_name="Synthetic A",
            address="Example address",
            latitude=10.76,
            longitude=106.70,
            identity_status="AUTO_ACCEPTED",
            is_identity_human_verified=False,
            field_provenance_json="{}",
            category_validation_status="PROVISIONAL_SOURCE_AGREEMENT",
            canonical_category="cafe_coffee",
        ),
        dict(
            store_id="synthetic-b",
            store_name="Synthetic B",
            address=None,
            latitude=10.761,
            longitude=106.701,
            identity_status="SOURCE_ONLY",
            is_identity_human_verified=False,
            field_provenance_json="{}",
            category_validation_status="PENDING",
            canonical_category="unknown",
        ),
    ]
    b = [
        dict(
            store_id=r["store_id"],
            analysis_category="cafe_coffee" if i == 0 else "restaurant",
            area_id="synthetic-area",
            verification_status=(
                "MACHINE_ACCEPTED_LINK" if i == 0 else "UNVERIFIED_SOURCE_ONLY"
            ),
            analysis_category_basis=(
                "SOURCE_AGREEMENT" if i == 0 else "OVERTURE_SOURCE_PROVISIONAL"
            ),
            analysis_category_provenance_json="{}",
        )
        for i, r in enumerate(s)
    ]
    mappings = []
    for r in s:
        mappings.append(
            dict(
                store_id=r["store_id"],
                source="overture",
                source_id_kind="overture_id",
                source_id=r["store_id"],
                link_status="SOURCE_ONLY",
                decision_source="SOURCE_ANCHOR",
                policy_version="synthetic",
                evidence_hash="synthetic",
                provenance_json="{}",
            )
        )
    mappings.append(
        dict(
            store_id="synthetic-a",
            source="google",
            source_id_kind="place_id",
            source_id="synthetic-google-a",
            link_status="AUTO_ACCEPTED",
            decision_source="CANONICAL_POLICY",
            policy_version="synthetic",
            evidence_hash="synthetic",
            provenance_json="{}",
        )
    )
    lineage = json.dumps(dict(reviews_release_id="synthetic-reviews"))
    reviews = [
        dict(
            logical_review_id=f"synthetic-review-{i}",
            source="google",
            source_review_id=f"synthetic-review-{i}",
            google_id_kind="place_id",
            google_identity_value=(
                "synthetic-google-a" if i < 2 else "synthetic-google-unmatched"
            ),
            published_at=(
                datetime(2026, 1, i + 1, tzinfo=timezone.utc) if i != 3 else None
            ),
            rating=float([1, 5, 4, 9][i]),
            timestamp_status="VERIFIED" if i != 3 else "MISSING",
            timestamp_source="SYNTHETIC_TEST",
            provenance_json="{}",
            gold_review_lineage_json=lineage,
        )
        for i in range(4)
    ]
    attached = [dict(r, store_id="synthetic-a") for r in reviews[:2]]
    places = [
        dict(
            google_id_kind="place_id",
            google_identity_value="synthetic-google-a",
            rating=4.7,
            review_count=200,
        )
    ]
    areas = [
        dict(
            area_id="synthetic-area",
            area_name="Synthetic Area",
            boundary_source="Synthetic test only",
            boundary_url="synthetic://test",
            boundary_snapshot="2026-01-01",
            boundary_license="TEST_FIXTURE",
            boundary_sha256="synthetic",
            boundary_geojson="{}",
            reference_area_km2=2.0,
            reference_population=4000,
            reference_year=2025,
            reference_source_version="synthetic-ref",
            reference_source_url="synthetic://reference",
            reference_record_url="synthetic://ward",
            reference_population_basis="2025_REORGANIZATION_PROPOSAL_NOT_CENSUS",
            reference_area_basis="PROVIDER_REORGANIZATION_REFERENCE_NOT_INDEPENDENTLY_VERIFIED",
            reference_quality_status="WARNING",
            reference_provenance_json="{}",
            reference_release_id="synthetic-reference",
        )
    ]
    datasets = dict(
        stores=s,
        business=b,
        mappings=mappings,
        attached=attached,
        unmatched=reviews[2:],
        places=places,
        areas=areas,
        decisions=[
            dict(overture_id="synthetic-a", effective_status="AUTO_ACCEPTED"),
            dict(overture_id="synthetic-b", effective_status="REVIEW_REQUIRED"),
        ],
    )
    for name, rows in datasets.items():
        pq.write_table(pa.Table.from_pylist(rows), folder / (name + ".parquet"))
    return datasets
