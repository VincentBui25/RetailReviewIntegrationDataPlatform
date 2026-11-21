"""Synthetic analytics contracts only; no upstream publications are rebuilt."""

import hashlib
import importlib.util
import json
from pathlib import Path
import sys

import duckdb
from jinja2 import Environment, StrictUndefined
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from fnb_platform.analytics import warehouse as wh
from fnb_platform.analytics.demo import create_sources as fixture_inputs

ROOT = Path(__file__).resolve().parents[1]


def native_dataset(name):
    """Read the same native dataset definition imported by Superset."""
    from zipfile import ZipFile

    yaml = pytest.importorskip("yaml")
    with ZipFile(ROOT / "docker/analytics/market_overview.zip") as bundle:
        datasets = [
            yaml.safe_load(bundle.read(path))
            for path in bundle.namelist()
            if "/datasets/" in path and path.endswith(".yaml")
        ]
    return next(dataset for dataset in datasets if dataset["table_name"] == name)


def test_warehouse_output_guard_rejects_overlap_but_allows_sibling_publications(
    tmp_path,
):
    from fnb_platform.publication import validate_publication_paths

    source = tmp_path / "sources"
    source.mkdir()
    artifact = source / "input.parquet"
    artifact.touch()
    validate_publication_paths(tmp_path / "warehouse", [source, artifact])
    validate_publication_paths(source / "warehouse", [artifact])
    for output, inputs in [
        (source, [artifact]),
        (source / "warehouse", [source]),
        (artifact, [artifact]),
    ]:
        with pytest.raises(ValueError, match="overlaps"):
            validate_publication_paths(output, inputs)


def model_connection(folder):
    con = duckdb.connect()
    con.execute("set TimeZone='UTC'")
    env = Environment(undefined=StrictUndefined)
    macro = (wh.PROJECT / "macros/current_reviews.sql").read_text()
    source = (
        lambda package, name: f"read_parquet('{(folder/(name+'.parquet')).as_posix()}')"
    )
    variables = {
        "gold_release_id": "synthetic-gold",
        "places_release_id": "synthetic-places",
    }
    for name in [
        "dim_store",
        "dim_category",
        "dim_area",
        "dim_date",
        "bridge_store_source",
        "fact_store_snapshot",
        "fact_review",
    ]:
        template = env.from_string(
            macro + "\n" + (wh.PROJECT / "models" / (name + ".sql")).read_text()
        )
        query = template.render(
            source=source, ref=lambda value: value, var=lambda key: variables[key]
        )
        con.execute(f"create table {name} as {query}")
    return con, env, source, variables


def gate(con, env, source, variables):
    return con.execute(
        env.from_string(
            (wh.PROJECT / "tests/grain_and_accounting.sql").read_text()
        ).render(source=source, ref=lambda value: value, var=lambda key: variables[key])
    ).fetchall()


@pytest.fixture
def models(tmp_path):
    folder = tmp_path / "sources"
    fixture_inputs(folder)
    con, env, source, variables = model_connection(folder)
    yield con, env, source, variables, folder
    con.close()


def test_two_distinct_fact_grains_and_reconciliation(models):
    con, env, source, variables, _ = models
    assert not gate(con, env, source, variables)
    assert con.execute(
        "select count(*),sum(store_count),sum(accepted_link_flag),sum(source_only_flag) from fact_store_snapshot"
    ).fetchone() == (2, 2, 1, 1)
    assert con.execute(
        "select count(*),count(store_key) from fact_review"
    ).fetchone() == (4, 2)
    assert con.execute("select count(*) from bridge_store_source").fetchone()[0] == 3


def test_place_rating_is_not_average_collected_review_rating(models):
    con, *_ = models
    assert con.execute(
        "select max(google_rating),max(google_rating_count) from fact_store_snapshot"
    ).fetchone() == (4.7, 200)
    assert (
        con.execute(
            "select avg(review_rating) from fact_review where store_key is not null"
        ).fetchone()[0]
        == 3.0
    )
    assert con.execute(
        "select google_rating,google_rating_count from fact_store_snapshot where source_only_flag=1"
    ).fetchone() == (None, None)


def test_area_reference_keeps_provider_basis_without_claiming_trusted_area(models):
    con, env, source, variables, _ = models
    assert con.execute(
        "select reference_population,reference_area_km2,reference_quality_status,trusted_area_km2 from dim_area"
    ).fetchone() == (4000, 2.0, "WARNING", None)
    con.execute("update dim_area set trusted_area_km2=reference_area_km2")
    assert ("invalid_area_reference",) in gate(con, env, source, variables)


def test_unknown_date_and_invalid_rating_are_retained(models):
    con, *_ = models
    assert con.execute(
        "select review_date_key,review_date_status,review_rating,review_rating_status from fact_review where logical_review_id='synthetic-review-3'"
    ).fetchone() == (None, "UNKNOWN", None, "INVALID")
    assert con.execute("select count(*) from dim_date").fetchone()[0] == 3


def test_source_only_category_remains_provisional_not_verified(models):
    con, *_ = models
    assert con.execute(
        "select f.gold_canonical_category,c.category_business_key,f.gold_category_evidence_status,s.is_identity_human_verified from fact_store_snapshot f join dim_category c using(category_key) join dim_store s using(store_key) where f.source_only_flag=1"
    ).fetchone() == ("unknown", "restaurant", "PENDING", False)


def test_unverified_timestamp_does_not_create_a_date(tmp_path):
    folder = tmp_path / "sources"
    datasets = fixture_inputs(folder)
    datasets["unmatched"][0]["timestamp_status"] = "AMBIGUOUS"
    pq.write_table(
        pa.Table.from_pylist(datasets["unmatched"]), folder / "unmatched.parquet"
    )
    con, *_ = model_connection(folder)
    try:
        assert con.execute(
            "select review_date_key,review_date_status from fact_review where logical_review_id='synthetic-review-2'"
        ).fetchone() == (None, "UNKNOWN")
    finally:
        con.close()


def test_duplicate_review_is_rejected_by_contract(models):
    con, env, source, variables, _ = models
    con.execute("insert into fact_review select * from fact_review limit 1")
    assert ("review_source_grain",) in gate(con, env, source, variables)


def test_review_cannot_be_assigned_to_source_only_store(models):
    con, env, source, variables, _ = models
    con.execute(
        "update fact_review set store_key=md5('synthetic-b') where store_key is not null"
    )
    assert ("unauthorized_review_association",) in gate(con, env, source, variables)


def test_lost_records_and_bad_flags_rejected(models):
    con, env, source, variables, _ = models
    con.execute("delete from fact_store_snapshot where source_only_flag=1")
    assert ("lost_stores",) in gate(con, env, source, variables)


def test_bundle_paths_are_not_arbitrary(tmp_path):
    signature = {"fixture_only": True, "purpose": "path guard"}
    rid = hashlib.sha256(wh.dump(signature).encode()).hexdigest()[:24]
    root = tmp_path / rid
    root.mkdir()
    (root / "manifest.json").write_text(
        json.dumps(
            dict(
                kind=wh.KIND,
                bundle_id=rid,
                signature=signature,
                artifacts={"../outside": "bad"},
            )
        )
    )
    with pytest.raises(ValueError, match="checksum"):
        wh.verify_bundle(root)


def test_release_signature_mismatch_rejected(tmp_path):
    root = tmp_path / ("a" * 24)
    root.mkdir()
    (root / "manifest.json").write_text(
        json.dumps(
            dict(
                kind=wh.KIND,
                bundle_id=root.name,
                signature={"fixture_only": True},
                artifacts={},
            )
        )
    )
    with pytest.raises(ValueError, match="signature"):
        wh.verify_bundle(root)


def test_map_never_defaults_to_city_wide_raw_points():
    from jinja2 import Environment

    env = Environment()
    env.filters["where_in"] = (
        lambda values: "("
        + ",".join("'" + v.replace("'", "''") + "'" for v in values)
        + ")"
    )
    template = env.from_string(native_dataset("map_catalog")["sql"])
    for values, expected in [
        ({}, "false"),
        ({"area_name": ["Synthetic Area"]}, "true"),
    ]:
        query = template.render(filter_values=lambda key, **kw: values.get(key, []))
        assert f"where {expected} and n <= 10000" in query
        assert "floor(latitude/0.01)" in query


def test_bi_verification_checks_real_payloads_and_nested_errors():
    spec = importlib.util.spec_from_file_location(
        "verify_superset", ROOT / "scripts/verify_superset.py"
    )
    verifier = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(verifier)
    failed, levels, numbers = verifier.chart_evidence(
        {
            "result": [
                {
                    "status": "success",
                    "data": [
                        {"canonical_records": 2},
                        {"accepted_links": 1},
                        {"source_only": 1},
                    ],
                }
            ],
            "data": {"features": [{"extraProps": {"map_level": "GRID"}}]},
        }
    )
    assert not failed and levels == {"GRID"} and numbers["canonical_records"] == {2}
    assert verifier.chart_evidence(
        {"result": [{"status": "failed", "error": "SYNTHETIC SQL failure"}]}
    )[0]


def test_actual_dbt_build_on_synthetic_inputs(tmp_path):
    import os, subprocess

    folder = tmp_path / "sources"
    fixture_inputs(folder)
    names = {
        "gold_stores": "stores",
        "gold_mappings": "mappings",
        "gold_attached": "attached",
        "gold_unmatched": "unmatched",
        "google_places": "places",
        "business_mart": "business",
        "area_reference": "areas",
    }
    variables = {k: str(folder / (v + ".parquet")) for k, v in names.items()}
    variables.update(
        gold_release_id="synthetic-gold", places_release_id="synthetic-places"
    )
    env = dict(
        os.environ,
        FNB_WAREHOUSE_DUCKDB=str(tmp_path / "models.duckdb"),
        DBT_SEND_ANONYMOUS_USAGE_STATS="false",
        DBT_PARTIAL_PARSE="false",
    )
    command = [
        sys.executable,
        "-B",
        "-c",
        "from dbt.cli.main import cli; cli()",
        "build",
        "--project-dir",
        str(wh.PROJECT),
        "--profiles-dir",
        str(wh.PROJECT),
        "--target-path",
        str(tmp_path / "dbt-target"),
        "--log-path",
        str(tmp_path / "dbt-logs"),
        "--vars",
        json.dumps(variables),
    ]
    with (tmp_path / "dbt.log").open("w") as log:
        result = subprocess.run(
            command, env=env, stdout=log, stderr=subprocess.STDOUT, timeout=90
        )
    assert result.returncode == 0, (tmp_path / "dbt.log").read_text()[-2500:]
    results = json.loads((tmp_path / "dbt-target/run_results.json").read_text())[
        "results"
    ]
    assert results and all(r["status"] in ("success", "pass") for r in results)
    assert sum(r["unique_id"].startswith("model.") for r in results) == 7
