import pytest

from crawl_experiment.core.errors import LimitedViewError
from crawl_experiment.core.models import Store
from crawl_experiment.sources.google_maps import selectors
from crawl_experiment.sources.google_maps.health import (
    HealthState,
    PageEvidence,
    classify_page,
)
from crawl_experiment.sources.google_maps.navigator import GoogleMapsNavigator


def test_true_limited_page_requires_strong_evidence():
    evidence = PageEvidence(has_sign_in_prompt=True)

    assert classify_page(
        "https://www.google.com/maps/search/Store/",
        "Sign in to see this place in limited view",
        evidence=evidence,
    ) is HealthState.LIMITED


def test_healthy_place_ui_overrides_limited_prompt_text():
    evidence = PageEvidence(
        has_place_tabs=True,
        has_place_shell=True,
        has_sign_in_prompt=True,
    )

    assert classify_page(
        "https://www.google.com/maps/place/Store/",
        "Sign in to see more. Limited view.",
        evidence=evidence,
    ) is HealthState.HEALTHY


def test_resolving_page_is_not_limited_without_structural_prompt():
    assert classify_page(
        "https://www.google.com/maps/search/Store/",
        "Sign in to see more",
        evidence=PageEvidence(),
    ) is HealthState.DEGRADED


class ResolvingDriver:
    title = "Coles World Square - Google Maps"

    def __init__(self):
        self.resolved = False

    @property
    def current_url(self):
        page = "place" if self.resolved else "search"
        return f"https://www.google.com/maps/{page}/Coles+World+Square/"

    def get(self, _):
        pass

    def find_element(self, *_):
        return Element("Sign in to see more")

    def find_elements(self, _, selector):
        if self.resolved and selector == selectors.PLACE_TABS:
            return [Element("Reviews")]
        return []


def test_navigator_waits_for_resolving_page_instead_of_marking_limited():
    driver = ResolvingDriver()
    navigator = GoogleMapsNavigator(
        driver,
        resolve_timeout_seconds=1,
        clock=lambda: 0,
        sleeper=lambda _: setattr(driver, "resolved", True),
    )

    resolved_url = navigator.open_store(
        Store("coles-710", "Coles World Square", "Coles World Square")
    )

    assert "/maps/place/" in resolved_url


class Element:
    def __init__(self, text=""):
        self.text = text


class LimitedDriver:
    current_url = "https://www.google.com/maps/search/Store/"
    title = "Store - Google Maps"

    def get(self, _):
        pass

    def find_element(self, *_):
        return Element("Sign in to see this place in limited view")

    def find_elements(self, _, selector):
        if selector == selectors.SIGN_IN_PROMPT:
            return [Element()]
        return []


def test_navigator_limited_error_contains_url_and_trigger_signals():
    navigator = GoogleMapsNavigator(
        LimitedDriver(),
        resolve_timeout_seconds=0,
        sleeper=lambda _: None,
    )

    with pytest.raises(LimitedViewError) as captured:
        navigator.open_store(Store("s", "Store", "Store"))

    message = str(captured.value)
    assert "maps/search/Store" in message
    assert "text:limited view" in message
    assert "ui:sign_in_prompt" in message
