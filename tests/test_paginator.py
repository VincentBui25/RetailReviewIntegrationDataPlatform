from crawl_experiment.sources.google_maps.paginator import ReviewPaginator


class Card:
    def __init__(self, review_id):
        self.review_id = review_id

    def get_attribute(self, name):
        return self.review_id if name == "data-review-id" else None


class Pane:
    def __init__(self, owner):
        self.owner = owner

    def find_elements(self, _, __):
        return list(self.owner.cards)


class Driver:
    def __init__(self, *, add_card_on_scroll=False, moves=True):
        self.cards = []
        self.scroll_top = 0
        self.scroll_height = 100
        self.client_height = 50
        self.add_card_on_scroll = add_card_on_scroll
        self.moves = moves
        self.pane = Pane(self)

    def execute_script(self, script, _pane):
        if "scrollBy" in script:
            if self.moves:
                self.scroll_top += 50
            if self.add_card_on_scroll:
                self.cards = [Card("r-1")]
            return {
                "before": {
                    "scrollTop": self.scroll_top - (50 if self.moves else 0),
                    "scrollHeight": self.scroll_height,
                    "clientHeight": self.client_height,
                },
                "after": {
                    "scrollTop": self.scroll_top,
                    "scrollHeight": self.scroll_height,
                    "clientHeight": self.client_height,
                },
            }
        return {
            "scrollTop": self.scroll_top,
            "scrollHeight": self.scroll_height,
            "clientHeight": self.client_height,
        }


def test_initial_zero_cards_then_cards_appear_after_first_scroll():
    driver = Driver(add_card_on_scroll=True)
    paginator = ReviewPaginator(driver, driver.pane, wait_seconds=0, sleeper=lambda _: None)

    assert paginator.observe(paginator.cards()) == 0
    diagnostics = paginator.scroll()
    assert diagnostics["cards_found"] == 1
    assert paginator.observe(paginator.cards()) == 1
    assert paginator.state.idle_count == 0


def test_scroll_top_change_is_reported():
    driver = Driver()
    paginator = ReviewPaginator(driver, driver.pane, wait_seconds=0, sleeper=lambda _: None)

    diagnostics = paginator.scroll()

    assert diagnostics["scroll_top_before"] == 0
    assert diagnostics["scroll_top_after"] == 50
    assert diagnostics["scroll_top_changed"] is True


def test_new_review_ids_reset_idle_count_after_scroll_cycle():
    driver = Driver()
    paginator = ReviewPaginator(driver, driver.pane, wait_seconds=0, sleeper=lambda _: None)
    paginator.observe([Card("r-1")])
    paginator.state.scroll_count = 1
    assert paginator.observe([Card("r-1")]) == 0
    assert paginator.state.idle_count == 1
    assert paginator.observe([Card("r-2")]) == 1
    assert paginator.state.idle_count == 0


def test_repeated_zero_growth_cycles_eventually_stall():
    driver = Driver()
    paginator = ReviewPaginator(
        driver,
        driver.pane,
        idle_limit=3,
        wait_seconds=0,
        sleeper=lambda _: None,
    )
    paginator.observe([])
    for _ in range(3):
        paginator.scroll()
        paginator.observe([])

    assert paginator.state.idle_count == 3
    assert paginator.stop_reason() == "stalled"


def test_non_scrollable_pane_is_detected():
    driver = Driver(moves=False)
    driver.scroll_height = driver.client_height
    paginator = ReviewPaginator(driver, driver.pane, wait_seconds=0, sleeper=lambda _: None)

    paginator.observe([])
    diagnostics = paginator.scroll()

    assert diagnostics["scroll_top_changed"] is False
    assert paginator.state.non_scrollable is True
    assert paginator.stop_reason() == "stalled"
