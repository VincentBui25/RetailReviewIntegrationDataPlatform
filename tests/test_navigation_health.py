import pytest

from crawl_experiment.core.errors import LimitedReviewViewError, PlaceResolutionError
from crawl_experiment.core.models import Store
from crawl_experiment.core.statuses import CrawlStatus
from crawl_experiment.sources.google_maps import selectors
from crawl_experiment.sources.google_maps.health import (
    HealthState,
    PageEvidence,
    classify_page,
)
from crawl_experiment.sources.google_maps.navigator import GoogleMapsNavigator
from crawl_experiment.storage.review_repository import ReviewRepository


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


class Element:
    def __init__(self, text="", attributes=None, on_click=None):
        self.text = text
        self.attributes = attributes or {}
        self.on_click = on_click

    def get_attribute(self, name):
        return self.attributes.get(name)

    def click(self):
        if self.on_click:
            self.on_click()


class PlaceDriver:
    title = "Google Maps"

    def __init__(self, *, candidates=(), direct=False, preview_text=""):
        self.entity_open = direct
        self.candidates = list(candidates)
        self.preview_text = preview_text
        self.click_count = 0

    @property
    def current_url(self):
        if self.entity_open:
            return "https://www.google.com/maps/place/Coles+World+Square/"
        return "https://www.google.com/maps/search/Coles+World+Square/"

    def get(self, _url):
        pass

    def open_entity(self):
        self.click_count += 1
        self.entity_open = True

    def find_element(self, *_args):
        return Element(
            "Coles World Square 650 George Street Sydney " + self.preview_text
        )

    def find_elements(self, _by, selector):
        if selector == selectors.SEARCH_RESULT_CANDIDATES and not self.entity_open:
            return [
                Element(
                    attributes={
                        "href": candidate[2],
                        "aria-label": candidate[0],
                        "data-address": candidate[1],
                    },
                    on_click=self.open_entity,
                )
                for candidate in self.candidates
            ]
        if self.entity_open and selector == selectors.PLACE_TITLE:
            return [Element("Coles World Square")]
        if self.entity_open and selector == selectors.PLACE_ADDRESS:
            return [Element("650 George Street, Sydney NSW 2000, Australia")]
        if self.entity_open and selector == selectors.PLACE_SHELL:
            return [Element("Coles World Square")]
        if self.entity_open and selector == selectors.PLACE_DETAIL_PANE:
            return [Element("Place details")]
        if self.entity_open and selector == selectors.PLACE_ENTITY_MARKER:
            return [Element("Address")]
        if self.entity_open and selector == selectors.PLACE_TABS:
            return [Element("Reviews")]
        if not self.entity_open and selector == selectors.REVIEW_CARD:
            return [Element("preview review")]
        if not self.entity_open and selector == selectors.SIGN_IN_PROMPT:
            return [Element("Sign in")]
        return []


TARGET = Store(
    "coles-710",
    "Coles World Square",
    "Coles World Square",
    "coles",
    "650 George Street, Sydney NSW 2000, Australia",
)


def test_search_preview_with_limited_reviews_is_not_classified_as_access_limited():
    driver = PlaceDriver(preview_text="Sign in to see this place in limited view")
    navigator = GoogleMapsNavigator(driver, resolve_timeout_seconds=0)

    with pytest.raises(PlaceResolutionError):
        navigator.open_store(TARGET)

    assert navigator.last_resolution_diagnostics["navigation_state"] == "SEARCH_PREVIEW"
    assert navigator.last_resolution_diagnostics["place_entity_confirmed"] is False


def test_exact_candidate_click_opens_entity_before_review_access_can_be_classified():
    driver = PlaceDriver(
        candidates=(
            (
                "Coles World Square",
                "650 George Street, Sydney NSW 2000, Australia",
                "https://www.google.com/maps/place/Coles+World+Square/",
            ),
        )
    )
    navigator = GoogleMapsNavigator(driver, resolve_timeout_seconds=1)

    resolved_url = navigator.open_store(TARGET)

    assert driver.click_count == 1
    assert "/maps/place/" in resolved_url
    assert navigator.last_resolution_diagnostics == {
        "initial_url": "https://www.google.com/maps/search/Coles+World+Square/",
        "navigation_state": "PLACE_ENTITY",
        "search_candidate_count": 1,
        "selected_candidate": {
            "text": "Coles World Square",
            "address": "650 George Street, Sydney NSW 2000, Australia",
            "url": "https://www.google.com/maps/place/Coles+World+Square/",
        },
        "resolved_place_title": "Coles World Square",
        "resolved_place_address": "650 George Street, Sydney NSW 2000, Australia",
        "resolved_url": resolved_url,
        "place_entity_confirmed": True,
    }


def test_wrong_candidate_is_rejected_without_click():
    driver = PlaceDriver(
        candidates=(
            (
                "Coles World Square",
                "Broadway, NSW",
                "https://www.google.com/maps/place/Coles+Broadway/",
            ),
        )
    )
    navigator = GoogleMapsNavigator(driver, resolve_timeout_seconds=0)

    with pytest.raises(PlaceResolutionError):
        navigator.open_store(TARGET)

    assert driver.click_count == 0
    assert navigator.last_resolution_diagnostics["search_candidate_count"] == 1
    assert navigator.last_resolution_diagnostics["place_entity_confirmed"] is False


def test_single_direct_entity_resolves_without_unnecessary_click():
    driver = PlaceDriver(direct=True)
    navigator = GoogleMapsNavigator(driver, resolve_timeout_seconds=0)

    navigator.open_store(TARGET)

    assert driver.click_count == 0
    assert navigator.last_resolution_diagnostics["navigation_state"] == "PLACE_ENTITY"
    assert navigator.last_resolution_diagnostics["place_entity_confirmed"] is True


def install_confirmed_entity_flow(monkeypatch, access_state):
    from crawl_experiment.sources.google_maps import crawler as module

    entity = {"confirmed": False}

    class Navigator:
        def __init__(self, _driver):
            self.last_resolution_diagnostics = {}

        def warm_up(self):
            pass

        def open_store(self, _store):
            entity["confirmed"] = True
            self.last_resolution_diagnostics = {
                "navigation_state": "PLACE_ENTITY",
                "place_entity_confirmed": True,
                "resolved_url": "https://www.google.com/maps/place/Store/",
            }
            return self.last_resolution_diagnostics["resolved_url"]

    class Surface:
        def __init__(self, _driver):
            pass

        def prepare_and_classify(self):
            assert entity["confirmed"] is True
            pane = object() if str(access_state) == "FULL" else None
            return access_state, pane, {}

        def sort_newest(self):
            pass

        def resolve_review_pane(self):
            return object()

    class Paginator:
        def __init__(self, *_args, **_kwargs):
            self.state = type(
                "State",
                (),
                {
                    "seen_ids": set(),
                    "last_new_ids": set(),
                    "scroll_count": 0,
                    "last_progress_at": None,
                    "idle_count": 0,
                },
            )()

        def cards(self):
            return []

        def observe(self, _cards):
            return 0

        def stop_reason(self):
            return "natural_end"

    monkeypatch.setattr(module, "GoogleMapsNavigator", Navigator)
    monkeypatch.setattr(module, "ReviewSurface", Surface)
    monkeypatch.setattr(module, "ReviewPaginator", Paginator)


class Checkpoints:
    def __init__(self):
        self.saved = []

    def save(self, value):
        self.saved.append(value)


class Metrics:
    def emit(self, _value):
        pass


def test_exact_place_entity_is_confirmed_before_full_access_classification(monkeypatch):
    from crawl_experiment.sources.google_maps.crawler import GoogleMapsCrawler
    from crawl_experiment.sources.google_maps.review_surface import ReviewSurfaceState

    install_confirmed_entity_flow(monkeypatch, ReviewSurfaceState.FULL)
    events = []
    repository = ReviewRepository(":memory:")
    try:
        result = GoogleMapsCrawler(
            repository,
            Checkpoints(),
            Metrics(),
            activity_observer=lambda event, details: events.append((event, details)),
        ).crawl(Store("s", "Store", "Store"), object(), warm_up=False)
    finally:
        repository.close()

    assert result.status is CrawlStatus.COMPLETE
    assert next(
        details for event, details in events if event == "place_resolution"
    )["place_entity_confirmed"] is True
    assert next(
        details
        for event, details in events
        if event == "review_access_after_entity_open"
    )["access_state_after_place_resolution"] == "FULL"


def test_confirmed_place_entity_can_still_be_genuinely_limited(monkeypatch):
    from crawl_experiment.sources.google_maps.crawler import GoogleMapsCrawler
    from crawl_experiment.sources.google_maps.review_surface import ReviewSurfaceState

    install_confirmed_entity_flow(monkeypatch, ReviewSurfaceState.LIMITED)
    events = []
    checkpoints = Checkpoints()
    repository = ReviewRepository(":memory:")
    try:
        with pytest.raises(LimitedReviewViewError):
            GoogleMapsCrawler(
                repository,
                checkpoints,
                Metrics(),
                activity_observer=lambda event, details: events.append((event, details)),
            ).crawl(Store("s", "Store", "Store"), object(), warm_up=False)
    finally:
        repository.close()

    assert checkpoints.saved[-1].status is CrawlStatus.LIMITED
    assert next(
        details
        for event, details in events
        if event == "review_access_after_entity_open"
    )["access_state_after_place_resolution"] == "LIMITED"
