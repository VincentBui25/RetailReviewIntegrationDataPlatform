import pytest

from crawl_experiment.core.errors import ReviewSurfaceError
from crawl_experiment.sources.google_maps import selectors
from crawl_experiment.sources.google_maps.review_surface import ReviewSurface


class Card:
    def __init__(self, review_id):
        self.review_id = review_id

    def get_attribute(self, name):
        return self.review_id if name == "data-review-id" else None


class Pane:
    _next_id = 0

    def __init__(self, driver, *, client_height, scroll_height, cards=(), visible=True):
        Pane._next_id += 1
        self.id = f"pane-{Pane._next_id}"
        self.driver = driver
        self.client_height = client_height
        self.scroll_height = scroll_height
        self.cards = [Card(value) for value in cards]
        self.visible = visible

    def is_displayed(self):
        return self.visible

    def find_elements(self, _by, selector):
        if selector == selectors.REVIEW_CARD:
            return list(self.cards)
        return []


class Driver:
    def __init__(self, panes=None, mapping=None):
        self.panes = panes or []
        self.mapping = mapping

    def find_elements(self, _by, selector):
        if selector in (selectors.REVIEW_PANE_PRIMARY, *selectors.REVIEW_PANE_FALLBACKS):
            if self.mapping is not None:
                return list(self.mapping.get(selector, []))
            return list(self.panes)
        return []

    def execute_script(self, _script, pane):
        return {
            "scrollTop": 0,
            "scrollHeight": pane.scroll_height,
            "clientHeight": pane.client_height,
        }


def surface(driver):
    return ReviewSurface(driver, render_wait=0, sleeper=lambda _seconds: None)


def test_resolve_review_pane_reacquires_after_sort_rerender():
    driver = Driver([])
    old_pane = Pane(driver, client_height=49, scroll_height=49)
    new_pane = Pane(driver, client_height=600, scroll_height=1800, cards=["new-1"])
    driver.panes = [old_pane]
    with pytest.raises(ReviewSurfaceError):
        surface(driver).resolve_review_pane()

    driver.panes = [new_pane]
    assert surface(driver).resolve_review_pane() is new_pane


def test_tiny_non_scrollable_candidate_is_rejected():
    driver = Driver([Pane(driver=None, client_height=49, scroll_height=49)])
    with pytest.raises(ReviewSurfaceError, match="review pane unavailable"):
        surface(driver).resolve_review_pane()


def test_best_scrollable_pane_is_selected_among_candidates():
    driver = Driver(mapping={})
    primary = Pane(driver, client_height=700, scroll_height=1900, cards=["primary"])
    fallback = Pane(driver, client_height=500, scroll_height=1600, cards=["fallback"])
    driver.mapping = {
        selectors.REVIEW_PANE_PRIMARY: [primary],
        selectors.REVIEW_PANE_FALLBACKS[0]: [fallback],
    }

    assert surface(driver).resolve_review_pane() is primary


def test_no_valid_pane_is_review_surface_failure_not_pagination_stall():
    driver = Driver([Pane(driver=None, client_height=49, scroll_height=49)])
    with pytest.raises(ReviewSurfaceError):
        surface(driver).resolve_review_pane()


def test_stale_candidate_is_skipped_when_a_valid_pane_is_available():
    driver = Driver([])
    stale = Pane(driver, client_height=500, scroll_height=1200)
    valid = Pane(driver, client_height=500, scroll_height=1200, cards=["r-1"])

    def stale_metrics(_script, pane):
        if pane is stale:
            raise RuntimeError("stale element reference")
        return {"scrollTop": 0, "scrollHeight": 1200, "clientHeight": 500}

    driver.mapping = {
        selectors.REVIEW_PANE_PRIMARY: [stale],
        selectors.REVIEW_PANE_FALLBACKS[0]: [valid],
    }
    driver.execute_script = stale_metrics

    assert surface(driver).resolve_review_pane() is valid
