import pytest

from crawl_experiment.core.errors import SortError
from crawl_experiment.core.statuses import CrawlStatus
from crawl_experiment.sources.google_maps import review_surface as review_surface_module
from crawl_experiment.sources.google_maps import selectors
from crawl_experiment.sources.google_maps.crawler import GoogleMapsCrawler
from crawl_experiment.sources.google_maps.review_surface import ReviewSurface
from crawl_experiment.sources.google_maps.review_surface import ReviewSurfaceState


class Element:
    def __init__(self, text="", **attributes):
        self.text = text
        self.attributes = attributes
        self.click_count = 0
        self.tag_name = attributes.pop("tag_name", "button")
        self.rect = attributes.pop(
            "rect", {"x": 10, "y": 10, "width": 100, "height": 30}
        )

    def get_attribute(self, name):
        return self.attributes.get(name)

    def click(self):
        self.click_count += 1
        if self.attributes.get("role") in {"menuitemradio", "option"}:
            self.attributes["aria-checked"] = "true"

    def is_displayed(self):
        return True

    def is_enabled(self):
        return True


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


def test_reviews_tab_already_selected_is_not_clicked(monkeypatch):
    tab = Element("Reviews", **{"aria-selected": "true"})
    pane = object()

    class SelectedTabDriver:
        def find_elements(self, _, selector):
            return [tab] if selector == selectors.REVIEWS_TAB_CSS else []

    surface = ReviewSurface(SelectedTabDriver())
    monkeypatch.setattr(surface, "resolve_review_pane", lambda: pane)

    assert surface.open_reviews() is pane
    assert tab.click_count == 0


def test_sort_control_diagnostics_detect_center_overlay():
    overlay = {
        "tag": "div",
        "role": None,
        "aria_label": None,
        "class": "mYFZJb",
    }

    class OverlayDriver(OpeningDriver):
        def execute_script(self, script, *args):
            if "elementFromPoint" in script:
                return overlay
            return super().execute_script(script, *args)

    driver = OverlayDriver()
    surface = ReviewSurface(driver)
    surface._last_sort_selector = selectors.SORT_BUTTON
    diagnostics = surface._button_diagnostics(driver.sort_button)

    assert diagnostics["tag"] == "button"
    assert diagnostics["interactive_control_confirmed"] is True
    assert diagnostics["overlay"] == overlay
    assert diagnostics["rect"]["width"] == 100


def test_semantic_listbox_is_detected_after_click():
    newest = Element("Newest", role="option")

    class ListboxDriver(OpeningDriver):
        def find_elements(self, method, selector):
            if selector == selectors.SORT_BUTTON:
                return [self.sort_button]
            if not self.menu_open:
                return []
            if selector == review_surface_module.SORT_MENU_SELECTOR:
                return [Element(role="listbox")]
            if selector == selectors.SORT_MENU_ITEMS:
                return [newest]
            return []

    ReviewSurface(ListboxDriver(), render_wait=0).sort_newest()

    assert newest.get_attribute("aria-checked") == "true"


def test_newest_must_be_positively_confirmed():
    class UnconfirmedOption(Element):
        def click(self):
            self.click_count += 1

    option = UnconfirmedOption("Newest", role="menuitemradio")
    surface = ReviewSurface(Driver([option]), retry_delay=0, render_wait=0)

    with pytest.raises(SortError) as captured:
        surface.sort_newest()

    assert "not confirmed" in str(captured.value)


def test_confirmed_newest_selection_returns_success():
    newest = Element("Newest", role="menuitemradio")
    surface = ReviewSurface(Driver([newest]), render_wait=0)

    assert surface.sort_newest() is None
    assert newest.get_attribute("aria-checked") == "true"


class ConfirmationOption(Element):
    def __init__(self, driver, text="Newest", **attributes):
        super().__init__(text, **attributes)
        self.driver = driver

    def click(self):
        self.click_count += 1
        self.driver.menu_open = False

    def find_elements(self, *_):
        return []


class DateCard(Element):
    def __init__(self, date):
        super().__init__("review", tag_name="div")
        self.date = Element(date, tag_name="span")

    def find_elements(self, _, selector):
        return [self.date] if selector == selectors.DATE else []


class DatePane(Element):
    def __init__(self, dates):
        super().__init__(tag_name="div")
        self.cards = [DateCard(date) for date in dates]

    def find_elements(self, _, selector):
        return self.cards if selector == selectors.REVIEW_CARD else []


class CurrentDomDriver(OpeningDriver):
    def __init__(self, dates, *, selected_class=""):
        super().__init__()
        self.review_pane = DatePane(dates)
        self.newest = ConfirmationOption(
            self,
            role="menuitemradio",
            **{"aria-checked": "false", "class": selected_class},
        )

    def find_elements(self, method, selector):
        if selector == selectors.SORT_BUTTON:
            return [self.sort_button]
        if selector == review_surface_module.SORT_MENU_SELECTOR:
            return [Element(role="menu")] if self.menu_open else []
        if selector == selectors.SORT_MENU_ITEMS:
            return [self.newest] if self.menu_open else []
        return []


def test_false_aria_checked_but_selected_class_confirms_newest():
    driver = CurrentDomDriver([], selected_class="menu-option selected")
    driver.menu_open = True
    surface = ReviewSurface(driver, render_wait=0)

    surface._sort_newest_once()

    assert driver.newest.click_count == 1


def test_closed_menu_and_newest_first_dates_confirm_newest():
    driver = CurrentDomDriver(["2 days ago", "a week ago", "3 weeks ago"])
    driver.menu_open = True
    surface = ReviewSurface(driver, render_wait=0)
    surface._last_pane = driver.review_pane

    surface._sort_newest_once()

    assert driver.newest.click_count == 1


def test_closed_menu_with_inconclusive_duplicate_dates_confirms_click_transition():
    driver = CurrentDomDriver(
        ["2 weeks ago", "2 weeks ago", "edited a month ago", "a month ago"]
    )
    driver.menu_open = True
    surface = ReviewSurface(driver, render_wait=0)
    surface._last_pane = driver.review_pane

    surface._sort_newest_once()

    assert driver.newest.click_count == 1


def test_false_aria_checked_without_other_evidence_is_not_confirmed():
    driver = CurrentDomDriver([])
    driver.menu_open = True
    surface = ReviewSurface(driver, retry_delay=0, render_wait=0)
    surface._last_pane = driver.review_pane

    with pytest.raises(SortError, match="not confirmed"):
        surface._sort_newest_once()


def test_review_dates_not_newest_first_do_not_confirm():
    driver = CurrentDomDriver(["3 weeks ago", "2 days ago", "a week ago"])
    driver.menu_open = True
    surface = ReviewSurface(driver, retry_delay=0, render_wait=0)
    surface._last_pane = driver.review_pane

    with pytest.raises(SortError, match="not confirmed"):
        surface._sort_newest_once()


def test_open_menu_requires_explicit_selected_signal():
    driver = CurrentDomDriver(["2 days ago", "a week ago"])

    def click_without_closing():
        driver.newest.click_count += 1

    driver.newest.click = click_without_closing
    driver.menu_open = True
    surface = ReviewSurface(driver, retry_delay=0, render_wait=0)
    surface._last_pane = driver.review_pane

    with pytest.raises(SortError, match="not confirmed"):
        surface._sort_newest_once()


class ClassificationPane:
    def __init__(self, cards):
        self.cards = cards

    def find_elements(self, _, selector):
        return self.cards if selector == selectors.REVIEW_CARD else []


class ClassificationDriver:
    def __init__(
        self,
        *,
        tabs=None,
        cards=None,
        main_text="",
        dialogs=None,
        header_sign_in=False,
        sort_button=None,
    ):
        self.tabs = tabs or []
        self.cards = cards or []
        self.main = [Element(main_text, tag_name="div")] if main_text else []
        self.dialogs = dialogs or []
        self.header_sign_in = header_sign_in
        self.sort_button = sort_button

    def find_elements(self, _, selector):
        if selector == selectors.REVIEWS_TAB_CSS:
            return self.tabs
        if selector == selectors.REVIEW_CARD:
            return self.cards
        if selector == review_surface_module.MAIN_SURFACE_SELECTOR:
            return self.main
        if selector == review_surface_module.AUTH_DIALOG_SELECTOR:
            return self.dialogs
        if selector == selectors.SIGN_IN_PROMPT:
            return [Element("Sign in")] if self.header_sign_in else []
        if selector == selectors.SORT_BUTTON and self.sort_button:
            return [self.sort_button]
        return []


def test_classifies_no_reviews_from_explicit_surface_evidence():
    state, evidence = ReviewSurface(
        ClassificationDriver(main_text="No reviews yet")
    ).classify_state()

    assert state is ReviewSurfaceState.NO_REVIEWS
    assert evidence["no_reviews_evidence"]


def test_classifies_limited_guest_review_view():
    state, evidence = ReviewSurface(
        ClassificationDriver(
            tabs=[Element("Reviews")],
            cards=[Element("Review")],
            main_text="Limited review view",
        )
    ).classify_state()

    assert state is ReviewSurfaceState.LIMITED
    assert evidence["limited_evidence"] == ["limited review view"]


def test_classifies_auth_required_review_dialog():
    dialog = Element("Sign in to continue and sort reviews", role="dialog")
    state, evidence = ReviewSurface(
        ClassificationDriver(dialogs=[dialog])
    ).classify_state()

    assert state is ReviewSurfaceState.AUTH_REQUIRED
    assert evidence["auth_dialog_evidence"]


def test_header_sign_in_alone_is_not_auth_required():
    state, evidence = ReviewSurface(
        ClassificationDriver(header_sign_in=True)
    ).classify_state()

    assert state is ReviewSurfaceState.UNKNOWN
    assert evidence["header_sign_in_ignored"] is True


def test_classifies_full_review_surface_with_header_sign_in_ignored():
    cards = [Element("Review")]
    pane = ClassificationPane(cards)
    sort_button = Element("", **{"aria-label": "Sort reviews"})
    state, evidence = ReviewSurface(
        ClassificationDriver(
            tabs=[Element("Reviews")],
            cards=cards,
            header_sign_in=True,
            sort_button=sort_button,
        )
    ).classify_state(pane)

    assert state is ReviewSurfaceState.FULL
    assert evidence["cards"] == 1
    assert evidence["sort_control"] is True


def test_missing_full_controls_without_restriction_is_unknown():
    cards = [Element("Review")]
    state, evidence = ReviewSurface(
        ClassificationDriver(cards=cards)
    ).classify_state(ClassificationPane(cards))

    assert state is ReviewSurfaceState.UNKNOWN
    assert evidence["sort_control"] is False


def test_auth_dialog_after_sort_access_is_not_sort_failed():
    class AuthAfterClickDriver(OpeningDriver):
        def find_elements(self, method, selector):
            if selector == review_surface_module.AUTH_DIALOG_SELECTOR:
                return [Element("Sign in to continue and sort reviews", role="dialog")]
            return super().find_elements(method, selector)

    with pytest.raises(review_surface_module.AuthRequiredError):
        ReviewSurface(
            AuthAfterClickDriver(normal=False, javascript=False),
            render_wait=0,
            retry_delay=0,
        ).sort_newest()
