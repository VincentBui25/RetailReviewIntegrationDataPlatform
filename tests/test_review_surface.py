import pytest

from crawl_experiment.core.errors import SortError
from crawl_experiment.core.statuses import CrawlStatus
from crawl_experiment.sources.google_maps import review_surface as review_surface_module
from crawl_experiment.sources.google_maps import selectors
from crawl_experiment.sources.google_maps.crawler import GoogleMapsCrawler
from crawl_experiment.sources.google_maps.review_surface import ReviewSurface


class Element:
    def __init__(self, text="", **attributes):
        self.text = text
        self.attributes = attributes
        self.click_count = 0

    def get_attribute(self, name):
        return self.attributes.get(name)

    def click(self):
        self.click_count += 1


class Driver:
    def __init__(self, menu_items):
        self.sort_button = Element()
        self.menu_items = menu_items

    def find_elements(self, _, selector):
        if selector == selectors.SORT_BUTTON:
            return [self.sort_button]
        if selector == selectors.SORT_MENU_ITEMS:
            return self.menu_items
        return []


class OpeningDriver:
    def __init__(self, *, normal=True, javascript=False, actions=False):
        self.menu_open = False
        self.normal = normal
        self.javascript = javascript
        self.actions = actions
        self.sort_button = Element(
            "",
            **{
                "aria-label": "Sort reviews",
                "role": "button",
                "aria-haspopup": "true",
            },
        )
        self.sort_button.click = self._normal_click

    def _normal_click(self):
        if not self.normal:
            raise RuntimeError("normal click blocked")
        self.menu_open = True

    def execute_script(self, script, *_):
        if "click" in script and self.javascript:
            self.menu_open = True

    def find_elements(self, _, selector):
        if selector == selectors.SORT_BUTTON:
            return [self.sort_button]
        if not self.menu_open:
            return []
        if selector == "[role='menu']":
            return [Element(role="menu")]
        if selector == selectors.SORT_MENU_ITEMS:
            return [Element("Newest", role="menuitemradio")]
        return []


class LocalRetryDriver:
    def __init__(self, opens_on_attempt, *, stale_first=False):
        self.opens_on_attempt = opens_on_attempt
        self.stale_first = stale_first
        self.lookups = 0
        self.menu_open = False
        self.review_pane = object()

    def execute_script(self, script, element):
        if "click" in script and element.attempt >= self.opens_on_attempt:
            self.menu_open = True
        if "scrollHeight" in script:
            return {"scrollHeight": 1200, "clientHeight": 500, "scrollTop": 0}

    def find_elements(self, _, selector):
        if selector == selectors.SORT_BUTTON:
            self.lookups += 1
            return [LocalButton(self, self.lookups)]
        if selector == selectors.REVIEWS_TAB_CSS:
            return [Element("Reviews")]
        if selector in (
            selectors.REVIEW_PANE_PRIMARY,
            *selectors.REVIEW_PANE_FALLBACKS,
        ):
            return [self.review_pane]
        if selector == "[role='menu']" and self.menu_open:
            return [Element(role="menu")]
        if selector == selectors.SORT_MENU_ITEMS and self.menu_open:
            return [Element("Newest", role="menuitemradio")]
        return []


class LocalButton(Element):
    def __init__(self, driver, attempt):
        super().__init__("", **{"aria-label": "Sort reviews"})
        self.driver = driver
        self.attempt = attempt

    def click(self):
        if self.driver.stale_first and self.attempt == 1:
            raise RuntimeError("stale element")
        if self.attempt >= self.driver.opens_on_attempt:
            self.driver.menu_open = True

    def send_keys(self, _):
        pass


def test_sort_menu_opens_on_second_local_attempt():
    driver = LocalRetryDriver(opens_on_attempt=2)

    ReviewSurface(driver, retry_delay=0, sleeper=lambda _: None).sort_newest()

    assert driver.lookups >= 2
    assert driver.menu_open is True


def test_sort_re_finds_button_after_stale_first_button():
    driver = LocalRetryDriver(opens_on_attempt=2, stale_first=True)

    ReviewSurface(driver, retry_delay=0, sleeper=lambda _: None).sort_newest()

    assert driver.lookups >= 2


def test_sort_reenters_reviews_after_local_attempts_fail():
    driver = LocalRetryDriver(opens_on_attempt=4)

    reopened_pane = ReviewSurface(
        driver,
        retry_delay=0,
        sleeper=lambda _: None,
    ).sort_newest()

    assert reopened_pane is not None
    assert driver.lookups >= 4
    assert driver.menu_open is True


def test_sort_menu_opens_after_normal_click():
    driver = OpeningDriver()

    ReviewSurface(driver).sort_newest()

    assert driver.menu_open is True


def test_sort_menu_uses_javascript_click_fallback():
    driver = OpeningDriver(normal=False, javascript=True)

    ReviewSurface(driver).sort_newest()

    assert driver.menu_open is True


def test_sort_menu_uses_action_chains_fallback(monkeypatch):
    driver = OpeningDriver(normal=False, javascript=False)

    class FakeActionChains:
        def __init__(self, current_driver):
            self.current_driver = current_driver

        def move_to_element(self, _):
            return self

        def click(self):
            return self

        def perform(self):
            self.current_driver.menu_open = True

    monkeypatch.setattr(review_surface_module, "ActionChains", FakeActionChains)

    ReviewSurface(driver).sort_newest()

    assert driver.menu_open is True


def test_sort_menu_never_opens_reports_explicit_diagnostics():
    driver = OpeningDriver(normal=False, javascript=False)

    with pytest.raises(SortError) as captured:
        ReviewSurface(driver).sort_newest()

    message = str(captured.value)
    assert "sort menu failed to open" in message
    assert "Sort reviews" in message
    assert "click_strategies_attempted" in message
    assert "menu_structure_appeared" in message


@pytest.mark.parametrize("label", ["Newest", "Mới nhất"])
def test_sort_newest_matches_localized_text(label):
    newest = Element(label, role="menuitemradio")
    surface = ReviewSurface(Driver([newest]))

    surface.sort_newest()

    assert newest.click_count == 1


def test_sort_newest_matches_aria_label_and_deduplicates_entries():
    first = Element(role="menuitemradio", **{"aria-label": "Newest"})
    duplicate = Element(role="menuitemradio", **{"aria-label": "Newest"})
    surface = ReviewSurface(Driver([first, duplicate]))

    surface.sort_newest()

    assert first.click_count == 1
    assert duplicate.click_count == 0


def test_sort_newest_uses_second_radio_item_for_identified_standard_menu():
    relevant = Element(
        "Pertinence",
        role="menuitemradio",
        **{"aria-checked": "true"},
    )
    newest = Element("Les plus récents", role="menuitemradio")
    surface = ReviewSurface(Driver([relevant, newest]))

    surface.sort_newest()

    assert relevant.click_count == 0
    assert newest.click_count == 1


def test_sort_newest_failure_has_discovered_item_diagnostics():
    item = Element("Most relevant", role="menuitem", **{"aria-label": "Relevant"})
    surface = ReviewSurface(Driver([item]))

    with pytest.raises(SortError) as captured:
        surface.sort_newest()

    message = str(captured.value)
    assert "discovered sort menu items" in message
    assert "most relevant" in message
    assert "relevant" in message
    assert GoogleMapsCrawler._failure_status(captured.value) == (
        CrawlStatus.SORT_FAILED,
        "review_surface",
    )


def test_sort_button_uses_container_fallback_and_rejects_negative_button():
    class ContainerDriver(OpeningDriver):
        def find_elements(self, method, selector):
            if selector == selectors.SORT_BUTTON:
                return []
            if selector == "div[role='main'] button[aria-haspopup='true']":
                return [self.sort_button]
            return super().find_elements(method, selector)

    driver = ContainerDriver()
    ReviewSurface(driver).sort_newest()

    assert driver.menu_open is True


def test_sort_button_uses_localized_xpath_fallback():
    class XPathDriver(OpeningDriver):
        def find_elements(self, method, selector):
            if method == "xpath":
                return [self.sort_button]
            if self.menu_open and selector == "[role='menu']":
                return [Element(role="menu")]
            if self.menu_open and selector == selectors.SORT_MENU_ITEMS:
                return [Element("Newest", role="menuitemradio")]
            return []

    driver = XPathDriver()
    ReviewSurface(driver).sort_newest()

    assert driver.menu_open is True


def test_m_lu_xec_text_container_resolves_parent_radio():
    text_container = Element(
        "Newest",
        **{"class": "mLuXec", "id": "text-node"},
    )
    parent = Element("Newest", role="menuitemradio", id="radio-1")

    class InnerTextDriver(Driver):
        def execute_script(self, script, element):
            if "closest" in script:
                return parent

    surface = ReviewSurface(InnerTextDriver([text_container]))

    surface.sort_newest()

    assert parent.click_count == 1
    assert text_container.click_count == 0


def test_selenium_element_id_deduplicates_menu_entries():
    first = Element("Newest", role="menuitemradio")
    duplicate = Element("Newest", role="menuitemradio")
    first.id = duplicate.id = "same-remote-element"
    surface = ReviewSurface(Driver([first, duplicate]))

    surface.sort_newest()

    assert first.click_count == 1
    assert duplicate.click_count == 0
