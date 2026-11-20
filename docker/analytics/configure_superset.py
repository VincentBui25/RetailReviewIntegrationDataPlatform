"""Import native Superset assets and configure their read-only connection."""

import json
import hashlib
from pathlib import Path
from zipfile import ZipFile

from superset import db, security_manager
from superset.app import create_app

MARKER = "Managed by fnb analytics v1"
SLUG = "hcmc-fnb-market-overview"


def secret(name):
    return (Path("/run/secrets") / name).read_text().strip()


def duckdb_database_config(config, path):
    """Native import expects an extra mapping; the ORM stores its JSON string."""
    return dict(
        config,
        sqlalchemy_uri="duckdb:///" + str(path),
        extra={
            "engine_params": {
                "connect_args": {"read_only": True, "config": {"threads": 2}}
            }
        },
    )


def chart_ids(layout, node_id):
    """Read the native layout inventory; never generate chart definitions."""
    node = layout[node_id]
    if node["type"] == "CHART":
        return [node["meta"]["chartId"]]
    return [
        value
        for child in node.get("children", [])
        for value in chart_ids(layout, child)
    ]


def configure():
    # ORM imports require Superset's initialized encrypted-field factory.
    from flask import g
    from superset.commands.chart.importers.dispatcher import ImportChartsCommand
    from superset.commands.dashboard.importers.dispatcher import ImportDashboardsCommand
    from superset.commands.importers.v1.utils import get_contents_from_bundle
    from superset.models.core import Database
    from superset.models.dashboard import Dashboard

    dashboard = db.session.query(Dashboard).filter_by(slug=SLUG).one_or_none()
    if dashboard is not None and MARKER not in (dashboard.description or ""):
        raise ValueError("Existing dashboard is not managed by this project; preserved")
    user = security_manager.find_user(username="portfolio_admin")
    if user is None:
        user = security_manager.add_user(
            username="portfolio_admin",
            first_name="Portfolio",
            last_name="Admin",
            email="portfolio-admin@localhost.invalid",
            role=security_manager.find_role("Admin"),
            password=secret("superset_admin_password"),
        )
    g.user = user
    with ZipFile(Path(__file__).with_name("market_overview.zip")) as bundle:
        contents = get_contents_from_bundle(bundle)
    import yaml

    snapshot = json.loads(Path("/analytics-out/serving-current.json").read_text())
    path = Path(snapshot["database"]).resolve()
    if not path.is_relative_to(Path("/analytics-out/serving")) or not path.is_file():
        raise ValueError(
            "Expected an immutable serving snapshot under /analytics-out/serving"
        )
    with path.open("rb") as stream:
        digest = hashlib.sha256()
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    if digest.hexdigest() != snapshot["sha256"]:
        raise ValueError("Serving snapshot SHA256 mismatch")
    for name, content in list(contents.items()):
        if name.startswith("databases/"):
            config = duckdb_database_config(yaml.safe_load(content), path)
            contents[name] = yaml.safe_dump(config, allow_unicode=True)
        elif name.startswith("dashboards/") and snapshot["expected"].get(
            "synthetic_demo"
        ):
            config = yaml.safe_load(content)
            config["description"] = (
                MARKER
                + "; SYNTHETIC DEMO: invented stores, reviews and reference denominators; not real market evidence."
            )
            for node in config["position"].values():
                if isinstance(node, dict) and node.get("type") == "MARKDOWN":
                    node["meta"]["code"] = (
                        "**SYNTHETIC DEMO — all catalog/review/reference values are invented.**\n\n"
                        + node["meta"].get("code", "")
                    )
            contents[name] = yaml.safe_dump(config, allow_unicode=True)
    ImportDashboardsCommand(contents, overwrite=True).run()
    # Superset 6 overwrites the dashboard but deliberately retains existing child
    # charts. Import the same native charts explicitly so reruns apply bundle fixes.
    chart_contents = {k: v for k, v in contents.items() if not k.startswith("dashboards/")}
    metadata = yaml.safe_load(chart_contents["metadata.yaml"])
    metadata["type"] = "Slice"
    chart_contents["metadata.yaml"] = yaml.safe_dump(metadata)
    ImportChartsCommand(chart_contents, overwrite=True).run()

    database = (
        db.session.query(Database)
        .filter_by(database_name="FNB Analytics (read only)")
        .one()
    )
    database.sqlalchemy_uri = "duckdb:///" + snapshot["database"]
    database.extra = json.dumps(
        duckdb_database_config({}, snapshot["database"])["extra"]
    )
    database.allow_dml = False
    database.expose_in_sqllab = False
    db.session.commit()

    dashboard = db.session.query(Dashboard).filter_by(slug=SLUG).one()
    layout = json.loads(dashboard.position_json)
    tabset = next(
        layout[key]
        for key in layout["GRID_ID"]["children"]
        if layout[key]["type"] == "TABS"
    )
    tabs = [
        {"label": layout[key]["meta"]["text"], "chart_ids": chart_ids(layout, key)}
        for key in tabset["children"]
    ]
    ordered_ids = [value for tab in tabs for value in tab["chart_ids"]]
    charts = {chart.id: chart for chart in dashboard.slices}
    nodes = [
        node
        for node in layout.values()
        if isinstance(node, dict) and node.get("type") == "CHART"
    ]
    if (
        len(ordered_ids) != len(set(ordered_ids))
        or set(ordered_ids) != set(charts)
        or len(nodes) != len(ordered_ids)
        or any(
            node["meta"].get("uuid") != str(charts[node["meta"]["chartId"]].uuid)
            for node in nodes
        )
    ):
        raise ValueError("Imported chart inventory differs from the native dashboard")
    filters = json.loads(dashboard.json_metadata)["native_filter_configuration"]
    report = dict(
        status="CONFIGURED",
        dashboard_id=dashboard.id,
        slug=SLUG,
        charts=[
            dict(id=value, viz_type=charts[value].viz_type) for value in ordered_ids
        ],
        tabs=tabs,
        native_filters=len(filters),
        default_areas=filters[0]
        .get("defaultDataMask", {})
        .get("filterState", {})
        .get("value", []),
        market_metrics=[
            "reference_population",
            "reference_area_km2",
            "observed_stores_per_km2",
            "observed_stores_per_1000_residents",
        ],
        credentials="Metadata secrets in files; DuckDB serving snapshot is read-only",
        map_status="AWAITING_BROWSER_VERIFICATION",
        backend="duckdb",
        snapshot_id=snapshot["snapshot_id"],
    )
    Path("/analytics-out/superset-assets.json").write_text(
        json.dumps(report, indent=2), encoding="utf-8"
    )
    print(
        json.dumps(
            dict(status="CONFIGURED", dashboard_id=dashboard.id, charts=len(charts))
        )
    )


if __name__ == "__main__":
    app = create_app()
    with app.app_context():
        configure()
