"""URL boundary regression for the operator-run Superset browser verifier."""

import importlib.util
from pathlib import Path

import pytest

spec = importlib.util.spec_from_file_location(
    "verify_superset", Path(__file__).parents[1] / "scripts/verify_superset.py"
)
verifier = importlib.util.module_from_spec(spec)
spec.loader.exec_module(verifier)


@pytest.mark.parametrize(
    "url,expected",
    [
        ("http://127.0.0.1:18088/login/", False),
        ("http://127.0.0.1:18088/login/?next=/superset/welcome/", False),
        ("http://127.0.0.1:18088/superset/welcome/", True),
        ("http://127.0.0.1:18088/superset/dashboard/hcmc-fnb-market-overview/", True),
        ("http://127.0.0.1:18088/superset/welcome/?next=/login/", True),
    ],
)
def test_playwright_url_predicate_accepts_strings(url, expected):
    assert verifier.left_login_page(url) is expected


def test_frontend_rendering_error_is_reported_with_chart_title():
    playwright = pytest.importorskip("playwright.sync_api")
    chrome = Path("C:/Program Files/Google/Chrome/Application/chrome.exe")
    if not chrome.is_file():
        pytest.skip("System Chrome is required for the DOM regression")
    with playwright.sync_playwright() as p:
        browser = p.chromium.launch(
            executable_path=str(chrome), headless=True, args=["--disable-extensions"]
        )
        page = browser.new_page()
        page.set_content(
            '<section class="dashboard-component-chart-holder">Category distribution</section>'
        )
        assert verifier.rendered_chart_errors(page) == []
        # SQL may have returned 200 while the frontend plugin failed to render.
        page.set_content(
            """<section class="dashboard-component-chart-holder">
            <h3>Category distribution</h3><div class="ant-alert-error">Plugin not registered</div>
            </section><div data-test="chart-error">Another rendering error</div>"""
        )
        errors = verifier.rendered_chart_errors(page)
        assert len(errors) == 2
        assert errors[0]["message"] == "Plugin not registered"
        assert "Category distribution" in errors[0]["chart"]
        assert errors[1] == {"message": "Another rendering error", "chart": None}
        page.set_content(
            """<div role="tab" onclick="this.dataset.clicked='yes'">
            <span>Thị trường F&amp;B</span><button aria-label="Copy URL">Link</button></div>
            <div role="tab"><span>Reviews &amp; chất lượng</span><button aria-label="Copy URL">Link</button></div>"""
        )
        verifier.select_dashboard_tab(page, "Thị trường F&B")
        assert page.get_by_role("tab").first.get_attribute("data-clicked") == "yes"
        browser.close()


def test_area_option_is_scoped_away_from_duplicate_table_text():
    playwright = pytest.importorskip("playwright.sync_api")
    chrome = Path("C:/Program Files/Google/Chrome/Application/chrome.exe")
    if not chrome.is_file():
        pytest.skip("System Chrome is required for the DOM regression")
    with playwright.sync_playwright() as p:
        browser = p.chromium.launch(
            executable_path=str(chrome), headless=True, args=["--disable-extensions"]
        )
        page = browser.new_page()
        page.set_content(
            """
            <input role="combobox" aria-label="Select page size">
            <div class="ant-select">
              <input role="combobox" aria-label="NATIVE_FILTER-fnb-0"
                onclick="document.querySelector('.ant-select-dropdown').style.display='block'">
              <span class="ant-select-selection-item"></span>
            </div>
            <div class="ant-select-dropdown" style="display:none">
              <div class="ant-select-item-option" onclick="
                document.querySelector('.ant-select-selection-item').textContent='Khánh Hội';
                document.querySelector('#apply').disabled=false;">
                <span>Khánh Hội</span>
              </div>
            </div>
            <table><tr><td onclick="this.dataset.clicked='yes'">Khánh Hội</td></tr></table>
            <button id="apply" disabled onclick="this.dataset.applied='yes'">Apply filters</button>
        """
        )
        verifier.apply_area_filter(page, "Khánh Hội")
        assert page.locator("#apply").get_attribute("data-applied") == "yes"
        assert page.locator("td").get_attribute("data-clicked") is None
        assert page.locator(".ant-select-selection-item").inner_text() == "Khánh Hội"
        browser.close()
