"""Owned synthetic snapshots only; never alter published inputs or live BI."""

import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import uuid

import duckdb
import pytest

from fnb_platform.analytics import duckdb_serving as serving
from fnb_platform.analytics import warehouse as wh

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location(
    "dimensional_fixture", ROOT / "tests/test_dimensional_analytics.py"
)
fixture = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fixture)


@pytest.mark.parametrize(
    "project",
    ["dimensional_analytics", "business_analytics", "gold_staging", "area_enrichment"],
)
def test_project_profile_uses_the_runner_database_without_manual_path_edits(project):
    from jinja2 import Environment

    yaml = pytest.importorskip("yaml")
    folder = ROOT / "pipelines" / project
    model = yaml.safe_load((folder / "dbt_project.yml").read_text())
    profile = yaml.safe_load(
        Environment()
        .from_string((folder / "profiles.yml").read_text())
        .render(
            env_var=lambda key: (
                "/verify/catalog-working.duckdb"
                if key in ("FNB_WAREHOUSE_DUCKDB", "FNB_DBT_DATABASE")
                else pytest.fail("Unexpected profile variable")
            )
        )
    )
    target = profile[model["profile"]]["outputs"]["local"]
    assert (
        target["type"] == "duckdb"
        and target["path"] == "/verify/catalog-working.duckdb"
    )


def test_docs_serve_keeps_logs_outside_readonly_project_source(tmp_path, monkeypatch):
    spec = importlib.util.spec_from_file_location(
        "dbt_docs_cli", ROOT / "scripts/dbt_docs.py"
    )
    docs = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(docs)
    output = tmp_path / "warehouse"
    release = output / "releases/fixture"
    (release / "dbt-target").mkdir(parents=True)
    (output / "current.json").write_text(json.dumps({"bundle_id": "fixture"}))
    (release / "manifest.json").write_text("{}")
    (release / "warehouse.duckdb").touch()
    (release / "dbt-target/run_results.json").write_text(
        json.dumps({"args": {"vars": {}}})
    )
    target = output / "docs/dimensional_analytics"
    target.mkdir(parents=True)
    for name in ["catalog.json", "manifest.json", "index.html"]:
        (target / name).write_text(
            "{}" if name.endswith(".json") else "<html>Owned docs fixture</html>"
        )
    for name in [
        "FNB_WAREHOUSE_DUCKDB",
        "FNB_DBT_DATABASE",
        "DBT_SEND_ANONYMOUS_USAGE_STATS",
    ]:
        monkeypatch.setenv(name, "fixture-before-exec")
    captured = []

    class ReachedServe(Exception):
        pass

    def execute(binary, args):
        captured.extend(args)
        raise ReachedServe

    monkeypatch.setattr(docs.os, "execv", execute)
    with pytest.raises(ReachedServe):
        docs.run("dimensional_analytics", output, serve=True)
    assert captured[captured.index("--log-path") + 1] == str(target / "logs")
    assert captured[captured.index("--target-path") + 1] == str(target / "public")
    assert not (target / "public/warehouse.duckdb").exists()


def source(folder, population=4000):
    sources = folder / "sources"
    datasets = fixture.fixture_inputs(sources)
    datasets["areas"][0]["reference_population"] = population
    fixture.pq.write_table(
        fixture.pa.Table.from_pylist(datasets["areas"]), sources / "areas.parquet"
    )
    con, *_ = fixture.model_connection(sources)
    expected = dict(
        canonical_store_count=2,
        accepted_mapping_count=1,
        source_only_store_count=1,
        source_mapping_rows=3,
        unresolved_mapping_count=1,
        current_logical_reviews=4,
        attached_reviews=2,
        unmatched_reviews=2,
        category_validation_counts={"PROVISIONAL_SOURCE_AGREEMENT": 1, "PENDING": 1},
        google_places_count=1,
        google_available=1,
        google_incomplete=0,
        google_missing=0,
    )
    con.execute("create table release_audit(gold_release_id varchar,payload varchar)")
    con.execute(
        "insert into release_audit values (?,?)",
        ["synthetic-gold", json.dumps(expected)],
    )
    for name in wh.MARTS:
        con.execute(f"create table {name} as select * from dim_store")
    area = con.execute("select * from dim_area").fetchone()
    area = dict(zip([c[0] for c in con.description], area))
    expected["area_reference"] = {
        k: v
        for k, v in area.items()
        if k == "area_business_key" or k.startswith("reference_")
    }
    signature = {"fixture_only": True, "nonce": uuid.uuid4().hex}
    import hashlib

    rid = hashlib.sha256(wh.dump(signature).encode()).hexdigest()[:24]
    root = folder / rid
    root.mkdir()
    target = str(root / "warehouse.duckdb").replace("'", "''")
    con.execute(f"attach '{target}' as exported")
    con.execute("create schema exported.dimensional")
    tables = {}
    artifacts = {}
    for name in [*wh.MODELS, *wh.MARTS, "release_audit"]:
        cursor = con.execute(f"select * from {name} order by all")
        description = [(c[0], str(c[1])) for c in cursor.description]
        rows = cursor.fetchall()
        con.execute(
            f"create table exported.dimensional.{name} as select * from main.{name}"
        )
        tables[name] = dict(
            columns=description, rows=len(rows), logical_sha256=wh.logical_hash(rows)
        )
    con.execute("detach exported")
    con.close()
    artifacts["warehouse.duckdb"] = wh.sha(root / "warehouse.duckdb")
    manifest = dict(
        kind=wh.KIND,
        bundle_id=rid,
        gold_release_id="synthetic-gold",
        places_release_id="synthetic-places",
        reviews_release_id="synthetic-reviews",
        signature=signature,
        tables=tables,
        artifacts=artifacts,
        expected=expected,
        fixture_only=True,
        dbt={"fixture_only": True},
    )
    (root / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    return root


def test_snapshot_is_readonly_idempotent_and_detects_corruption(tmp_path):
    release = source(tmp_path / "inputs")
    output = tmp_path / "output"
    one = serving.publish(release, output)
    with duckdb.connect(one["database"], read_only=True) as reader:
        two = serving.publish(release, output)
        assert one["snapshot_id"] == two["snapshot_id"]
        assert (
            reader.execute("select count(*) from bi.v_review_profile").fetchone()[0]
            == 4
        )
        with pytest.raises(duckdb.InvalidInputException):
            reader.execute("create table forbidden(a int)")
    path = Path(one["database"])
    with path.open("ab") as stream:
        stream.write(b"CORRUPT_OWNED_FIXTURE")
    with pytest.raises(ValueError, match="changed"):
        serving.publish(release, output)


def test_new_snapshot_keeps_old_readers_and_preserves_unmatched_reviews(tmp_path):
    first = source(tmp_path / "first")
    second = source(tmp_path / "second")
    output = tmp_path / "output"
    one = serving.publish(first, output)
    with duckdb.connect(one["database"], read_only=True) as old:
        two = serving.publish(second, output)
        assert one["snapshot_id"] != two["snapshot_id"]
        with duckdb.connect(two["database"], read_only=True) as new:
            for reader in [old, new]:
                assert (
                    reader.execute(
                        "select count(*) from dimensional.fact_review where store_key is null and google_identity_value is not null"
                    ).fetchone()[0]
                    == 2
                )
                assert (
                    reader.execute(
                        "select count(*) from bi.v_store_overview"
                    ).fetchone()[0]
                    == 2
                )
    pointer = json.loads((output / "serving-current.json").read_text())
    assert pointer["snapshot_id"] == two["snapshot_id"]


def test_modified_working_database_cannot_publish_false_success(tmp_path):
    release = source(tmp_path / "inputs")
    with duckdb.connect(str(release / "warehouse.duckdb")) as con:
        con.execute("delete from dimensional.fact_review where store_key is null")
    output = tmp_path / "output"
    with pytest.raises(ValueError, match="checksum|content mismatch"):
        serving.publish(release, output)
    assert not (output / "serving-current.json").exists()


def test_failed_refresh_preserves_the_active_snapshot(tmp_path):
    first, second = source(tmp_path / "first"), source(tmp_path / "second")
    output = tmp_path / "output"
    published = serving.publish(first, output)
    before = (output / "serving-current.json").read_bytes()
    with duckdb.connect(str(second / "warehouse.duckdb")) as connection:
        connection.execute("delete from dimensional.fact_review")
    with pytest.raises(ValueError, match="checksum"):
        serving.publish(second, output)
    assert (output / "serving-current.json").read_bytes() == before
    with duckdb.connect(published["database"], read_only=True) as reader:
        assert (
            reader.execute("select count(*) from dimensional.fact_review").fetchone()[0]
            == 4
        )


def test_readonly_process_remains_connected_during_new_snapshot_publication(tmp_path):
    first, second = source(tmp_path / "first"), source(tmp_path / "second")
    output = tmp_path / "output"
    old = serving.publish(first, output)
    code = "import duckdb,sys;c=duckdb.connect(sys.argv[1],read_only=True,config={'threads':2});print('READY',flush=True);sys.stdin.readline();print(c.execute('select count(*) from dimensional.fact_review').fetchone()[0]);c.close()"
    child = subprocess.Popen(
        [sys.executable, "-B", "-c", code, old["database"]],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        assert child.stdout.readline().strip() == "READY"
        new = serving.publish(second, output)
        assert new["snapshot_id"] != old["snapshot_id"]
        stdout, stderr = child.communicate("finish\n", timeout=15)
        assert child.returncode == 0 and stdout.strip() == "4", stderr
    finally:
        if child.poll() is None:
            child.kill()
            child.communicate(timeout=5)
