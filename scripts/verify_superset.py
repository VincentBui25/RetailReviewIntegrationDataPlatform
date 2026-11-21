"""Operator-run BI browser verification; credentials stay in memory and local secret files."""

import argparse
import os
import json
import math
from pathlib import Path
import time
from urllib.parse import urlsplit

MAP_CANVAS_SELECTOR = "#deckgl-overlay,.mapboxgl-canvas,.maplibregl-canvas"
NUMERIC_METRICS = (
    "canonical_records",
    "accepted_links",
    "source_only",
    "observed_store_count",
    "reference_population",
    "reference_area_km2",
    "observed_stores_per_km2",
    "observed_stores_per_1000_residents",
    "logical_reviews",
    "stores_with_reviews",
    "store_review_coverage_pct",
    "review_sample_mean",
)


def left_login_page(current_url):
    """Playwright's URL predicate receives a string, not a parsed URL object."""
    return "/login" not in urlsplit(current_url).path


def select_dashboard_tab(page, label):
    # Superset adds a "Copy URL" button to each tab's accessible name.
    # Match the visible title within the tab, rather than its whole name.
    page.get_by_role("tab").filter(has=page.get_by_text(label, exact=True)).click()


def apply_native_filter(page, filter_id, label):
    """Select a native dropdown option, never identically named table text."""
    from playwright.sync_api import expect

    area = page.get_by_role("combobox", name=filter_id, exact=True)
    area.click()
    dropdown = page.locator(".ant-select-dropdown:visible")
    option = dropdown.locator(".ant-select-item-option").filter(
        has=page.get_by_text(label, exact=True)
    )
    expect(option).to_have_count(1)
    option.click()
    area.press("Escape")
    selection = (
        page.locator(".ant-select")
        .filter(has=area)
        .locator(".ant-select-selection-item")
    )
    expect(selection).to_have_count(1)
    expect(selection).to_contain_text(label)
    apply = page.get_by_role("button", name="Apply filters", exact=True)
    expect(apply).to_be_enabled()
    apply.click()


def apply_area_filter(page, label):
    apply_native_filter(page, "NATIVE_FILTER-fnb-0", label)


def rendered_chart_errors(page):
    """Keep the chart title and frontend error even when its SQL returned 200."""
    return page.locator("[data-test=chart-error],.ant-alert-error").evaluate_all(
        """els=>els.map(e=>({
        message:e.innerText,
        chart:e.closest('.dashboard-component-chart-holder')?.innerText || null
    }))"""
    )


def chart_evidence(body):
    """Handle both legacy Deck.gl payloads and modern chart-data result arrays."""
    levels = set()
    numbers = {name: set() for name in NUMERIC_METRICS}
    failed = False
    pending = [body]
    while pending:
        value = pending.pop()
        if isinstance(value, dict):
            if value.get("error") or value.get("status") in ("failed", "error"):
                failed = True
            if value.get("map_level") in ("GRID", "DETAIL"):
                levels.add(value["map_level"])
            for key, result in value.items():
                if (
                    key in numbers
                    and isinstance(result, (int, float))
                    and not isinstance(result, bool)
                ):
                    numbers[key].add(result)
                if isinstance(result, (dict, list)):
                    pending.append(result)
        elif isinstance(value, list):
            pending.extend(value)
    return failed, levels, numbers


def verify(url, output, chrome):
    from playwright.sync_api import sync_playwright, expect

    root = Path(__file__).resolve().parents[1]
    analytics_root = Path(
        os.environ.get("FNB_ANALYTICS_OUTPUT", root / ".local/analytics")
    )
    assets = json.loads((analytics_root / "superset-assets.json").read_text())
    pointer = json.loads((analytics_root / "current.json").read_text())
    manifest = json.loads(
        (
            analytics_root / "releases" / pointer["bundle_id"] / "manifest.json"
        ).read_text()
    )
    checks = []
    tile_statuses = []
    chart_responses = []
    errors = []
    map_levels = set()
    map_queries = []
    numeric_metrics = {name: set() for name in NUMERIC_METRICS}

    def require(ok, message):
        if not ok:
            raise ValueError(message)
        checks.append(message)
        print("BI_CHECK: " + message, flush=True)

    output.mkdir(parents=True, exist_ok=True)
    started = time.perf_counter()
    result = dict(
        status="NOT_RUN",
        dashboard_url=url + "/superset/dashboard/hcmc-fnb-market-overview/",
    )
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                **({"executable_path": str(chrome)} if chrome else {}),
                headless=True,
                args=[
                    "--disable-extensions",
                    "--disable-background-networking",
                    "--enable-webgl",
                    "--use-angle=swiftshader",
                ],
            )
            context = browser.new_context(viewport={"width": 1600, "height": 1100})
            page = context.new_page()

            def response(resp):
                path = urlsplit(resp.url)
                if path.hostname in ("tile.openstreetmap.org", "tiles.stadiamaps.com"):
                    tile_statuses.append(resp.status)
                if (
                    "/api/v1/chart/data" in path.path
                    or "/superset/explore_json/" in path.path
                ):
                    try:
                        body = resp.json()
                        failed, levels, numbers = chart_evidence(body)
                        chart_responses.append(
                            dict(
                                http=resp.status,
                                error=failed,
                                status=body.get("status"),
                            )
                        )
                        map_levels.update(levels)
                        if levels:
                            map_queries.append(levels)
                        for name, values in numbers.items():
                            numeric_metrics[name].update(values)
                    except Exception:
                        chart_responses.append(dict(http=resp.status, error=True))

            page.on("response", response)
            page.on("pageerror", lambda error: errors.append(str(error)))
            page.goto(url + "/login/")
            page.locator("#username").fill("portfolio_admin")
            page.locator("#password").fill(
                (
                    Path(os.environ.get("FNB_SECRETS_ROOT", root / ".auth/fnb"))
                    / "superset_admin_password"
                )
                .read_text()
                .strip()
            )
            page.locator("input[type=submit],button[type=submit]").first.click()
            page.wait_for_url(left_login_page, timeout=60000)
            page.goto(result["dashboard_url"])
            page.get_by_text("HCMC F&B — Market Overview", exact=True).first.wait_for(
                timeout=60000
            )
            page.wait_for_function(
                "document.querySelectorAll('.chart-container,[data-test=chart-container]').length > 0",
                timeout=60000,
            )
            tabs = assets.get("tabs")
            if not tabs:
                raise ValueError("Missing market/review tab layout evidence")
            for number, tab in enumerate(tabs):
                print("BI_TAB: " + tab["label"], flush=True)
                select_dashboard_tab(page, tab["label"])
                selector = (
                    ".dashboard-component-chart-holder:has(.dashboard-chart):visible"
                )
                containers = page.locator(selector)
                containers.first.wait_for(state="visible", timeout=30000)
                expect(containers).to_have_count(len(tab["chart_ids"]), timeout=30000)
                require(
                    containers.count() == len(tab["chart_ids"]),
                    "All saved chart holders exist: " + tab["label"],
                )
                for container in containers.all():
                    container.scroll_into_view_if_needed()
                    page.wait_for_timeout(250)
                # Query/render failures in inactive tabs must also fail sign-off.
                expect(containers.filter(has_text="Waiting on FNB")).to_have_count(
                    0, timeout=60000
                )
                alerts = rendered_chart_errors(page)
                require(
                    not alerts,
                    "No chart errors in "
                    + tab["label"]
                    + ": "
                    + json.dumps(alerts, ensure_ascii=False),
                )
                page.evaluate("window.scrollTo(0,0)")
                page.screenshot(
                    path=str(output / f"superset-tab-{number}.png"), full_page=True
                )
            select_dashboard_tab(page, tabs[0]["label"])
            # The standalone operator owns bounded waits and checkpoints; no agent polling required.
            deadline = time.monotonic() + 120
            while time.monotonic() < deadline and (
                len(chart_responses) < len(assets["charts"]) or not tile_statuses
            ):
                page.wait_for_timeout(500)
            require(
                len(chart_responses) >= len(assets["charts"]),
                "All saved charts returned real query responses",
            )
            require(
                all(
                    v["http"] == 200
                    and not v["error"]
                    and v.get("status") not in ("failed", "error")
                    for v in chart_responses
                ),
                "All chart queries succeeded",
            )
            for metric, field in [
                ("observed_store_count", "canonical_store_count"),
                ("accepted_links", "accepted_mapping_count"),
                ("source_only", "source_only_store_count"),
                ("logical_reviews", "current_logical_reviews"),
            ]:
                require(
                    manifest["expected"][field] in numeric_metrics[metric],
                    "Rendered KPI matches Gold: " + metric,
                )
            require(
                any(1 <= v <= 5 for v in numeric_metrics["review_sample_mean"]),
                "Logical-review sample mean rendered with valid rating units",
            )
            reference = manifest["expected"]["area_reference"]
            for metric in assets["market_metrics"]:
                if metric.startswith("reference_"):
                    expected = reference[metric]
                else:
                    denominator = reference[
                        (
                            "reference_area_km2"
                            if metric.endswith("_km2")
                            else "reference_population"
                        )
                    ]
                    expected = (
                        manifest["expected"]["canonical_store_count"]
                        * (1 if metric.endswith("_km2") else 1000)
                        / denominator
                        if denominator
                        else None
                    )
                require(
                    (
                        (not numeric_metrics[metric])
                        if expected is None
                        else any(
                            math.isclose(v, expected, rel_tol=1e-10)
                            for v in numeric_metrics[metric]
                        )
                    ),
                    "Rendered market KPI matches admitted reference: " + metric,
                )
            page.locator(MAP_CANVAS_SELECTOR).first.wait_for(
                state="visible", timeout=30000
            )
            require(
                page.locator(MAP_CANVAS_SELECTOR).first.is_visible(),
                "Street map canvas rendered",
            )
            require(
                any(200 <= status < 300 for status in tile_statuses),
                "Real street basemap tiles loaded",
            )
            default_areas = assets.get("default_areas", [])
            default_level = "DETAIL" if len(default_areas) == 1 else "GRID"
            require(
                default_level in map_levels,
                "Default map returned " + default_level + " for its area scope",
            )
            require(
                page.get_by_text("Đọc bản đồ", exact=True).count() == 1,
                "Map reading guide and legend exist",
            )
            boxes = page.get_by_role("combobox")
            require(boxes.count() >= 3, "Area/category/source filter controls exist")
            page.evaluate("window.scrollTo(0,0)")
            if default_areas:
                start = len(map_queries)
                page.get_by_role("button", name="Clear all", exact=True).click()
                page.get_by_role("button", name="Apply filters", exact=True).click()
                deadline = time.monotonic() + 60
                while time.monotonic() < deadline and not any(
                    "GRID" in levels for levels in map_queries[start:]
                ):
                    page.wait_for_timeout(500)
                require(
                    any("GRID" in levels for levels in map_queries[start:]),
                    "Cleared area filter returned server-side GRID cells",
                )
            start = len(map_queries)
            apply_area_filter(page, default_areas[0] if default_areas else "Khánh Hội")
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline and not any(
                "DETAIL" in levels for levels in map_queries[start:]
            ):
                page.wait_for_timeout(500)
            require(
                any("DETAIL" in levels for levels in map_queries[start:]),
                "Small-area filter returned bounded DETAIL points",
            )
            require(
                all(v["http"] == 200 and not v["error"] for v in chart_responses),
                "Filtered chart queries succeeded",
            )
            result["chart_errors"] = rendered_chart_errors(page)
            if result["chart_errors"]:
                page.screenshot(
                    path=str(output / "superset-failure.png"), full_page=True
                )
            require(
                not result["chart_errors"],
                "No rendered chart error alerts: "
                + json.dumps(result["chart_errors"], ensure_ascii=False),
            )
            require(not errors, "No JavaScript errors in extension-free browser")
            page.evaluate("window.scrollTo(0,0)")
            page.screenshot(path=str(output / "superset-dashboard.png"), full_page=True)
            result.update(
                status="PASS",
                checks=checks,
                chart_query_responses=len(chart_responses),
                tile_successes=sum(200 <= s < 300 for s in tile_statuses),
                map_levels=sorted(map_levels),
                canonical_records_expected_from_gold=manifest["expected"][
                    "canonical_store_count"
                ],
                note="Numeric KPI conservation is enforced by the DuckDB snapshot publisher; no formulas duplicated here.",
                elapsed_seconds=round(time.perf_counter() - started, 2),
            )
            browser.close()
    except Exception as exc:
        result.update(
            status="FAIL", checks=checks, error=str(exc), map_levels=sorted(map_levels)
        )
        raise
    finally:
        (output / "superset-verification.json").write_text(
            json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
        )
    print(
        json.dumps(
            dict(
                status=result["status"],
                checks=len(checks),
                map_levels=result["map_levels"],
            )
        )
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:18089")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--chrome",
        type=Path,
        default=os.environ.get("FNB_CHROME_BIN"),
        help="Optional browser override; Docker uses its bundled Chromium",
    )
    args = parser.parse_args()
    verify(args.url.rstrip("/"), args.output, args.chrome)
