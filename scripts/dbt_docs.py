"""Open one real dbt project's docs; select profiles and DuckDB paths automatically."""

import argparse
import json
import hashlib
import os
from pathlib import Path
import shutil
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
PROJECTS = (
    "dimensional_analytics",
    "business_analytics",
    "gold_staging",
    "area_enrichment",
)


def run(project, output, serve=False):
    output = Path(output)
    pointer = json.loads((output / "current.json").read_text())
    release = output / "releases" / pointer["bundle_id"]
    bundle = json.loads((release / "manifest.json").read_text())
    if project == "dimensional_analytics":
        results, database = (
            release / "dbt-target/run_results.json",
            release / "warehouse.duckdb",
        )
    elif project == "business_analytics" and bundle["expected"].get("synthetic_demo"):
        results = output / "demo-sources/business-target/run_results.json"
        database = results.parent.parent / "business.duckdb"
    else:
        suffix = (
            "mart_store_business.parquet"
            if project == "business_analytics"
            else "dim_store.parquet"
        )
        if project == "area_enrichment":
            configured = os.environ.get("FNB_AREA_DBT_RELEASE")
            pointer = Path("/area-dbt-input/current.json")
            if not configured and pointer.is_file():
                active = json.loads(pointer.read_text())
                source = (pointer.parent / "releases" / active["release_id"]).resolve()
                if not source.is_relative_to((pointer.parent / "releases").resolve()):
                    raise ValueError("Area Docs release escapes the mounted publication root")
                manifest_hash = hashlib.sha256((source / "manifest.json").read_bytes()).hexdigest()
                if manifest_hash != active["manifest_sha256"]:
                    raise ValueError("Area Docs manifest SHA256 mismatch")
                configured = str(source)
            candidates = [Path(configured)] if configured else []
        else:
            candidates = [
                Path(p).parent
                for p in bundle["signature"]["inputs"]
                if Path(p).name == suffix
            ]
        if len(candidates) != 1:
            raise ValueError(
                "No unambiguous real dbt build for "
                + project
                + "; run that project first. Area accepts FNB_AREA_DBT_RELEASE; no fabricated lineage."
            )
        source = candidates[0]
        results = source / "dbt-target/run_results.json"
        database = (
            source
            / {
                "business_analytics": "analytics.duckdb",
                "gold_staging": "staging.duckdb",
                "area_enrichment": "area_catalog.duckdb",
            }[project]
        )
    if not results.is_file() or not database.is_file():
        raise ValueError(
            "Required real dbt results/database missing: "
            + str(results)
            + "; "
            + str(database)
        )
    target = output / "docs" / project
    target.mkdir(parents=True, exist_ok=True)
    # Catalog generation never opens an immutable build/serving database for writes.
    working = target / "catalog-working.duckdb"
    variables = json.loads(results.read_text()).get("args", {}).get("vars", {})
    if isinstance(variables, str):
        variables = json.loads(variables)
    env = dict(
        os.environ,
        FNB_WAREHOUSE_DUCKDB=str(working),
        FNB_DBT_DATABASE=str(working),
        DBT_SEND_ANONYMOUS_USAGE_STATS="false",
    )
    if not serve or not (target / "catalog.json").is_file():
        shutil.copyfile(database, working)
        shutil.copyfile(results.parent / "manifest.json", target / "manifest.json")
        args = [
            sys.executable,
            "-B",
            "-c",
            "from dbt.cli.main import cli;cli()",
            "docs",
            "generate",
            "--no-compile",
            "--project-dir",
            str(ROOT / "pipelines" / project),
            "--profiles-dir",
            str(ROOT / "pipelines" / project),
            "--target-path",
            str(target),
            "--log-path",
            str(target / "logs"),
            "--vars",
            json.dumps(variables),
        ]
        with (target / "generate.log").open("w") as log:
            result = subprocess.run(
                args, env=env, stdout=log, stderr=subprocess.STDOUT, timeout=120
            )
        if result.returncode or not all(
            (target / n).is_file()
            for n in ["manifest.json", "catalog.json", "index.html"]
        ):
            raise ValueError(
                "dbt docs generation failed; inspect " + str(target / "generate.log")
            )
    print(
        json.dumps(dict(status="DOCS_READY", project=project, target=str(target))),
        flush=True,
    )
    if serve:
        # Serve only generated dbt docs, not the source data directory.
        public = target / "public"
        public.mkdir(exist_ok=True)
        for name in ["index.html", "manifest.json", "catalog.json"]:
            shutil.copyfile(target / name, public / name)
        os.environ.update(env)
        args = [
            sys.executable,
            "-B",
            "-c",
            "from dbt.cli.main import cli;cli()",
            "docs",
            "serve",
            "--host",
            "0.0.0.0",
            "--port",
            "8081",
            "--no-browser",
            "--project-dir",
            str(ROOT / "pipelines" / project),
            "--profiles-dir",
            str(ROOT / "pipelines" / project),
            "--target-path",
            str(public),
            "--log-path",
            str(target / "logs"),
            "--vars",
            json.dumps(variables),
        ]
        os.execv(sys.executable, args)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", choices=PROJECTS, default="dimensional_analytics")
    parser.add_argument("--output", type=Path, default=Path("/analytics-out"))
    parser.add_argument("--serve", action="store_true")
    args = parser.parse_args()
    run(args.project, args.output, args.serve)
