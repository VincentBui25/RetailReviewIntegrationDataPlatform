"""Import boundary regression: ORM/encryption initialization belongs to create_app."""

import importlib.util
from pathlib import Path
import sys
import types
import pytest


@pytest.fixture
def bootstrap(monkeypatch):
    superset = types.ModuleType("superset")
    superset.__path__ = []
    superset.db = object()
    superset.security_manager = object()
    app = types.ModuleType("superset.app")

    def create_app():
        raise AssertionError(
            "Importing the bootstrap must not initialize an app/database"
        )

    app.create_app = create_app
    sqlalchemy = types.ModuleType("sqlalchemy")
    sqlalchemy.__path__ = []
    engine = types.ModuleType("sqlalchemy.engine")
    engine.URL = object()
    for name, module in [
        ("superset", superset),
        ("superset.app", app),
        ("sqlalchemy", sqlalchemy),
        ("sqlalchemy.engine", engine),
    ]:
        monkeypatch.setitem(sys.modules, name, module)
    # Model modules are deliberately unavailable until app initialization. The
    # original top-level ORM imports fail here, as they did in the real image.
    path = (
        Path(__file__).resolve().parents[1] / "docker/analytics/configure_superset.py"
    )
    spec = importlib.util.spec_from_file_location("fnb_bootstrap_import_test", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_bootstrap_import_is_safe_without_initialized_superset_app(bootstrap):
    module = bootstrap
    assert callable(module.configure)
    assert module.MARKER == "Managed by fnb analytics v1"


def test_setup_generates_missing_secrets_without_replacing_existing_values(tmp_path):
    import shutil
    import subprocess

    scripts = tmp_path / "scripts"
    scripts.mkdir()
    script = scripts / "init_analytics_secrets.py"
    shutil.copyfile(
        Path(__file__).resolve().parents[1] / "scripts" / script.name, script
    )
    args = [sys.executable, "-B", str(script)]
    first = subprocess.run(args, capture_output=True, text=True, check=True)
    target = tmp_path / ".auth/fnb"
    before = {p.name: p.read_bytes() for p in target.iterdir()}
    assert len(before) == 4 and "admin_password" in before
    assert all(len(value) == 64 for value in before.values())
    second = subprocess.run(args, capture_output=True, text=True, check=True)
    assert before == {p.name: p.read_bytes() for p in target.iterdir()}
    assert all(
        value.decode() not in first.stdout + second.stdout for value in before.values()
    )


@pytest.fixture
def assets():
    from zipfile import ZipFile

    yaml = pytest.importorskip("yaml")
    with ZipFile(
        Path(__file__).resolve().parents[1] / "docker/analytics/market_overview.zip"
    ) as bundle:
        return {
            name: yaml.safe_load(bundle.read(name))
            for name in bundle.namelist()
            if name.endswith(".yaml")
        }


def test_native_bundle_has_one_dashboard_and_complete_dataset_references(assets):
    dashboards = [v for k, v in assets.items() if "/dashboards/" in k]
    charts = [v for k, v in assets.items() if "/charts/" in k]
    datasets = [v for k, v in assets.items() if "/datasets/" in k]
    assert len(dashboards) == 1 and len(charts) == 20 and len(datasets) == 6
    assert len({v["uuid"] for v in charts}) == len(charts)
    assert {v["dataset_uuid"] for v in charts} == {v["uuid"] for v in datasets}
    physical = [v for v in datasets if v["table_name"] != "map_catalog"]
    assert all(v["sql"] is None and v["schema"] == "bi" for v in physical)


def test_native_connection_has_no_embedded_password_or_write_access(assets):
    from urllib.parse import urlsplit

    databases = [v for k, v in assets.items() if "/databases/" in k]
    assert len(databases) == 1
    database = databases[0]
    uri = urlsplit(database["sqlalchemy_uri"])
    assert uri.password is None and uri.scheme == "duckdb"
    assert not database["allow_dml"] and not database["expose_in_sqllab"]


def test_duckdb_native_import_extra_is_mapping_and_orm_extra_is_json(assets, bootstrap):
    import json

    yaml = pytest.importorskip("yaml")
    original = next(v for k, v in assets.items() if "/databases/" in k)
    uri = original["sqlalchemy_uri"]
    configured = bootstrap.duckdb_database_config(
        original, "/analytics-out/serving/fixture/warehouse.duckdb"
    )
    loaded = yaml.safe_load(yaml.safe_dump(configured))
    assert isinstance(loaded["extra"], dict)
    assert loaded["extra"]["engine_params"]["connect_args"]["read_only"] is True
    assert json.loads(json.dumps(configured["extra"])) == loaded["extra"]
    assert (
        loaded["uuid"] == original["uuid"]
        and loaded["database_name"] == original["database_name"]
    )
    assert loaded["sqlalchemy_uri"].startswith("duckdb:////analytics-out/serving/")
    assert original["sqlalchemy_uri"] == uri


def test_native_tabs_cover_each_selected_chart_once(assets, bootstrap):
    dashboard = next(v for k, v in assets.items() if "/dashboards/" in k)
    layout = dashboard["position"]
    tabset = next(
        layout[k] for k in layout["GRID_ID"]["children"] if layout[k]["type"] == "TABS"
    )
    tabs = tabset["children"]
    assert [layout[k]["meta"]["text"] for k in tabs] == [
        "Tổng quan",
        "Loại hình & địa điểm",
        "Reviews quan sát",
    ]
    ids = [value for key in tabs for value in bootstrap.chart_ids(layout, key)]
    assert len(ids) == len(set(ids)) == 20
    layout_names = {
        v["meta"]["sliceName"]
        for v in layout.values()
        if isinstance(v, dict) and v.get("type") == "CHART"
    }
    assert layout_names == {
        v["slice_name"] for k, v in assets.items() if "/charts/" in k
    }
    nodes = [
        v for v in layout.values() if isinstance(v, dict) and v.get("type") == "CHART"
    ]
    assert len(nodes) == 20
    assert {v["meta"]["uuid"] for v in nodes} == {
        v["uuid"] for k, v in assets.items() if "/charts/" in k
    }
    for node in layout.values():
        if not isinstance(node, dict):
            continue
        assert all(child in layout for child in node.get("children", []))


def test_map_uses_registered_metric_and_bounded_native_dataset(assets):
    import json
    from urllib.parse import urlsplit

    chart = next(
        v
        for k, v in assets.items()
        if "/charts/" in k and v["viz_type"] == "deck_scatter"
    )
    params = chart["params"]
    if isinstance(params, str):
        params = json.loads(params)
    # Retain the low-clutter M5 basemap. Verification must use the real local UI
    # origin via the Superset network namespace, not change the visual for tests.
    tile = urlsplit(params["mapbox_style"].removeprefix("tile://"))
    assert tile.scheme == "https" and tile.hostname == "tiles.stadiamaps.com"
    assert "/alidade_smooth/" in tile.path
    assert not tile.query and tile.username is None and tile.password is None
    dataset = next(
        v
        for k, v in assets.items()
        if "/datasets/" in k and v["uuid"] == chart["dataset_uuid"]
    )
    metric = params["point_radius_fixed"]
    assert metric["type"] == "metric"
    assert metric["value"] in {m["metric_name"] for m in dataset["metrics"]}
    assert "n <= 10000" in dataset["sql"]


@pytest.fixture
def native_runner(tmp_path):
    """Load only runner functions; never start its Docker/build workflow."""
    import shutil
    import subprocess

    pwsh = shutil.which("pwsh")
    if pwsh is None:
        pytest.skip("PowerShell required for native runner regression")
    root = Path(__file__).resolve().parents[1]

    def quoted(value):
        return "'" + str(value).replace("'", "''") + "'"

    prefix = f"""
$ErrorActionPreference='Stop'
$ast=[System.Management.Automation.Language.Parser]::ParseFile({quoted(root / 'scripts/Invoke-DuckDbPilot.ps1')},[ref]$null,[ref]$null)
$functions=$ast.FindAll({{param($node) $node -is [System.Management.Automation.Language.FunctionDefinitionAst]}},$true)
if(@($functions|Where-Object{{$_.Name -in 'Python','Docker'}}).Count){{throw 'Native executable name is shadowed'}}
foreach($function in $functions|Where-Object{{$_.Name -in 'Invoke-DockerCommand','Read-Settings','Setting','Restore-Environment','Assert-Ports','Save-State','Step'}}){{. ([scriptblock]::Create($function.Extent.Text))}}
$run={quoted(tmp_path)}
$checkpoint=Join-Path $run 'checkpoint.json'
$state=@{{stages=@{{}};status='RUNNING'}}
"""

    def execute(body):
        result = subprocess.run(
            [pwsh, "-NoProfile", "-Command", prefix + body + "\nexit 0\n"],
            cwd=root,
            capture_output=True,
            text=True,
            timeout=25,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        return result

    return execute


def test_runner_calls_native_executables_and_rejects_nonzero_exit(native_runner):
    import shutil

    if shutil.which("docker") is None:
        pytest.skip("Docker CLI required; this test only calls --version")
    result = native_runner(
        """
function Python {throw 'Unexpected Python function dispatch'}
function Docker {throw 'Unexpected Docker function dispatch'}
Invoke-DockerCommand -Arguments @('--version')
$failed=$false
try {Invoke-DockerCommand -Arguments @('--fnb-invalid-flag')}
catch {if($_.Exception.Message -notmatch 'Docker failed'){throw};$failed=$true}
if(-not $failed){throw 'Nonzero native exit was accepted'}
"""
    )
    assert "Docker version" in result.stdout


def test_runner_settings_preserve_literal_values_without_executing_them(native_runner):
    native_runner(
        """
$path=Join-Path $run 'settings.env'
@('# comment','FNB_SUPERSET_PORT=18101','FNB_PATH="folder with spaces"','FNB_LITERAL=$(throw bad)','IGNORED=value')|Set-Content $path
$settings=Read-Settings $path
if($settings.FNB_PATH -ne 'folder with spaces' -or $settings.FNB_LITERAL -ne '$(throw bad)' -or $settings.Count -ne 3){throw 'Settings were evaluated or parsed incorrectly'}
"""
    )


def test_runner_rejects_occupied_ports_before_building(native_runner):
    native_runner(
        """
$project='fixture'
function Invoke-DockerCommand {$global:LASTEXITCODE=0}
$listener=[Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback,0)
$listener.Start()
try {
    $port=$listener.LocalEndpoint.Port
    $rejected=$false
    try {Assert-Ports @{superset=$port}}catch{if($_.Exception.Message -notmatch 'busy'){throw};$rejected=$true}
    if(-not $rejected){throw 'Occupied port accepted'}
}finally{$listener.Stop()}
"""
    )


def test_environment_roundtrip_keeps_dotenv_defaults_stable_in_one_process(native_runner):
    native_runner(
        """
$settings=@{FNB_DX_TEST_VOLUME='fixture-volume';FNB_DX_TEST_INPUT='./config';FNB_DX_TEST_PORT='18103'}
$original=@{}
foreach($name in $settings.Keys){$original[$name]=[Environment]::GetEnvironmentVariable($name);Remove-Item -LiteralPath ('Env:'+$name) -ErrorAction SilentlyContinue}
try {
    $fingerprints=@()
    foreach($attempt in 1..2){
        $fingerprints+=(@($settings.Keys|Sort-Object|ForEach-Object{Setting $_}) -join '|')
        $prior=@{}
        foreach($name in $settings.Keys){$prior[$name]=[Environment]::GetEnvironmentVariable($name);[Environment]::SetEnvironmentVariable($name,'runtime-value')}
        Restore-Environment $prior
        foreach($name in $settings.Keys){if(Test-Path -LiteralPath ('Env:'+$name)){throw 'Absent environment became an empty override'}}
    }
    if($fingerprints[0] -ne $fingerprints[1] -or $fingerprints[0] -notmatch 'fixture-volume'){throw 'Rerun configuration drifted'}
    [Environment]::SetEnvironmentVariable('FNB_DX_TEST_EMPTY','temporary')
    Restore-Environment @{FNB_DX_TEST_EMPTY=''}
    if([Environment]::GetEnvironmentVariable('FNB_DX_TEST_EMPTY') -ne ''){throw 'Explicit empty value was not preserved'}
}finally{Restore-Environment $original;Remove-Item Env:FNB_DX_TEST_EMPTY -ErrorAction SilentlyContinue}
"""
    )


def test_frozen_resume_rejects_changed_export_before_touching_checkpoint():
    import hashlib
    import json
    import re
    import shutil
    import subprocess
    import uuid

    pwsh = shutil.which("pwsh")
    if pwsh is None:
        pytest.skip("PowerShell required for frozen-source resume regression")
    root = Path(__file__).resolve().parents[1]
    runner = root / "scripts/Invoke-DuckDbPilot.ps1"
    inventory = runner.read_text().split("$files=@(", 1)[1].split(")\n", 1)[0]
    paths = re.findall(r"'([^']+)'", inventory)
    run_name = "resume-guard-" + uuid.uuid4().hex[:12]
    owned = root / ".local/mentor-verification" / run_name
    source = owned / "source"
    hashes = []
    for name in paths:
        target = source / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(b"owned resume fixture")
        hashes.append({"Hash": hashlib.sha256(target.read_bytes()).hexdigest()})
    (owned / "source-identity.json").write_text(json.dumps({"revision": "fixture", "files": hashes}))
    evidence = owned / "verification.json"
    before = b'{"status":"FAIL","fixture_only":true,"stages":{"FIRST_SETUP":"PASS"}}'
    evidence.write_bytes(before)
    (source / paths[0]).write_bytes(b"changed owned fixture")
    result = subprocess.run(
        [pwsh, "-NoProfile", "-File", str(runner), "-FreshClone", "-ResumeFresh", "-RunName", run_name],
        cwd=root, capture_output=True, text=True, timeout=25,
    )
    assert result.returncode != 0 and "Frozen source changed" in result.stdout + result.stderr
    assert evidence.read_bytes() == before


def test_runner_writes_actual_failure_to_stage_log(native_runner, tmp_path):
    native_runner(
        """
try {Step 'NEGATIVE' {throw 'fixture step failure'}}
catch {if($_.Exception.Message -notmatch 'Stage NEGATIVE failed'){throw}}
if($state.stages.NEGATIVE -ne 'FAIL'){throw 'Native failure was not recorded'}
"""
    )
    logs = list(tmp_path.glob("NEGATIVE-*.log"))
    assert len(logs) == 1 and "ERROR: fixture step failure" in logs[0].read_text(
        encoding="utf-8-sig"
    )


def test_runner_rejected_code_change_preserves_existing_checkpoint():
    import json
    import shutil
    import subprocess
    import uuid

    pwsh = shutil.which("pwsh")
    if pwsh is None:
        pytest.skip("PowerShell required for runner checkpoint regression")
    root = Path(__file__).resolve().parents[1]
    run_name = "guard-test-" + uuid.uuid4().hex[:12]
    folder = root / ".local/duckdb-final" / run_name
    folder.mkdir(parents=True)
    checkpoint = folder / "checkpoint.json"
    before = json.dumps(
        {
            "signature": "OLD_SOURCE",
            "mode": "Demo",
            "status": "PASS",
            "stages": {"BUILD_IMAGES": "PASS"},
        }
    ).encode()
    checkpoint.write_bytes(before)
    verification = folder / "verification.json"
    verification.write_bytes(b'{"status":"PASS","fixture_only":true}')
    result = subprocess.run(
        [
            pwsh,
            "-NoProfile",
            "-File",
            str(root / "scripts/Invoke-DuckDbPilot.ps1"),
            "-Mode",
            "Demo",
            "-RunName",
            run_name,
        ],
        cwd=root,
        capture_output=True,
        text=True,
        timeout=25,
    )
    assert (
        result.returncode != 0
        and "Code/options changed" in result.stdout + result.stderr
    )
    assert checkpoint.read_bytes() == before
    assert verification.read_bytes() == b'{"status":"PASS","fixture_only":true}'


def test_runner_fingerprint_is_stable_across_powershell_processes():
    import shutil
    import subprocess

    pwsh = shutil.which("pwsh")
    if pwsh is None:
        pytest.skip("PowerShell required for runner fingerprint regression")
    root = Path(__file__).resolve().parents[1]
    source = (root / "scripts/Invoke-DuckDbPilot.ps1").read_text()
    parameters = source[source.index("param(") : source.index("\n)\n") + 3]
    start = source.index("    $code=@(")
    end = source.index("    if(Test-Path -LiteralPath $checkpoint)", start)
    script = (
        parameters
        + "\n$repo=(Get-Location).Path\n"
        + source[start:end]
        + "\nWrite-Output $signature\n"
    )
    signatures = [
        subprocess.run(
            [pwsh, "-NoProfile", "-Command", script],
            cwd=root,
            capture_output=True,
            text=True,
            check=True,
            timeout=20,
        ).stdout.strip()
        for _ in range(3)
    ]
    assert len(set(signatures)) == 1 and len(signatures[0]) == 64


def test_airflow_verifier_command_resolves_through_the_declared_dags_mount():
    import re
    from pathlib import PurePosixPath

    yaml = pytest.importorskip("yaml")
    root = Path(__file__).resolve().parents[1]
    compose = root / "compose.yaml"
    if not compose.is_file():
        pytest.skip(
            "Host-only Compose mount contract; root Compose is not packaged in the warehouse image"
        )
    config = yaml.safe_load(compose.read_text())
    mounts = config["services"]["airflow-scheduler"]["volumes"]
    mount = next(
        value
        for value in mounts
        if isinstance(value, str) and value.startswith("./dags:")
    )
    source, destination, *_ = mount.split(":")
    runner = (root / "scripts/Invoke-DuckDbPilot.ps1").read_text()
    paths = [
        PurePosixPath(p)
        for p in re.findall(r"'([^']*verify_orchestration\.py)'", runner)
        if p.startswith("/")
    ]
    assert len(paths) == 1
    relative = paths[0].relative_to(PurePosixPath(destination))
    assert (root / source / relative).is_file()
