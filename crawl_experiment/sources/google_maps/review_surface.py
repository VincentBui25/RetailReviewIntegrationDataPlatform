import logging
import time

from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys

from crawl_experiment.core.errors import ReviewSurfaceError, SortError

from . import selectors

logger = logging.getLogger(__name__)

SORT_BUTTON_FALLBACKS = (
    "div[role='main'] button[aria-label*='Sort' i]",
    "div[role='main'] [data-review-id] button[aria-haspopup='true']",
    "div[role='main'] button[aria-haspopup='true']",
    "button[aria-label*='Sort' i]",
    "button[aria-haspopup='true']",
)
NEGATIVE_BUTTON_LABELS = (
    "menu", "overview", "photos", "directions", "share", "save",
    "close", "back", "next", "about", "updates", "products",
)
SORT_MENU_SELECTOR = "[role='menu']"
SORT_OPTION_FALLBACKS = (
    selectors.SORT_MENU_ITEMS,
    "div.fxNQSd",
    "div.mLuXec",
)


def _normalized(value: str | None) -> str:
    return " ".join((value or "").split()).casefold()


def _menu_item_details(element) -> dict[str, str]:
    is_displayed = getattr(element, "is_displayed", None)
    return {
        "role": _normalized(element.get_attribute("role")),
        "text": _normalized(getattr(element, "text", "")),
        "aria_label": _normalized(element.get_attribute("aria-label")),
        "aria_checked": _normalized(element.get_attribute("aria-checked")),
        "displayed": str(is_displayed() if is_displayed else True).lower(),
    }


def _unique_menu_items(elements) -> list[tuple[object, dict[str, str]]]:
    unique = []
    positions = {}
    element_ids = set()
    for element in elements:
        details = _menu_item_details(element)
        element_id = getattr(element, "id", None)
        if element_id is not None:
            if element_id in element_ids:
                continue
            element_ids.add(element_id)
        key = (details["role"], details["text"], details["aria_label"])
        if key in positions:
            position = positions[key]
            existing_details = unique[position][1]
            if (
                existing_details["displayed"] != "true"
                and details["displayed"] == "true"
            ):
                unique[position] = (element, details)
            continue
        positions[key] = len(unique)
        unique.append((element, details))
    return unique


def _described(items: list[tuple[object, dict[str, str]]]) -> list[dict[str, str]]:
    return [details for _, details in items]


class ReviewSurface:
    def __init__(
        self,
        driver,
        *,
        retry_delay: float = 0.25,
        render_wait: float = 3.0,
        render_poll: float = 0.25,
        clock=time.monotonic,
        sleeper=time.sleep,
    ):
        self.driver = driver
        self.retry_delay = retry_delay
        self.render_wait = render_wait
        self.render_poll = render_poll
        self.clock = clock
        self.sleeper = sleeper
        self._last_pane = None

    def open_reviews(self):
        try:
            buttons = self.driver.find_elements("css selector", selectors.REVIEWS_TAB_CSS)
            if not buttons:
                buttons = self.driver.find_elements("xpath", selectors.REVIEWS_TAB_XPATH)
            if not buttons:
                raise ReviewSurfaceError("Reviews tab not found")
            buttons[0].click()
            return self.resolve_review_pane()
        except ReviewSurfaceError:
            raise
        except Exception as exc:
            raise ReviewSurfaceError(str(exc)) from exc

    def resolve_review_pane(self):
        """Wait for and select the current, scrollable Reviews pane."""
        self.sleeper(min(0.5, self.render_wait))
        deadline = self.clock() + self.render_wait
        best = None
        diagnostics = []
        while True:
            candidates = self._pane_candidates()
            scored = [self._score_pane(pane, selector) for pane, selector in candidates]
            diagnostics = [entry[1] for entry in scored]
            valid = [entry for entry in scored if entry[0] is not None]
            if valid:
                best = max(valid, key=lambda entry: entry[0]["score"])
                self._last_pane = best[0]["pane"]
                logger.info("Selected review pane: %s", best[1])
                return self._last_pane
            if self.clock() >= deadline:
                break
            self.sleeper(self.render_poll)

        raise ReviewSurfaceError(
            "review pane unavailable after render; candidates: "
            f"{diagnostics}"
        )

    def _pane_candidates(self):
        candidates = []
        seen_ids = set()
        for selector in (selectors.REVIEW_PANE_PRIMARY, *selectors.REVIEW_PANE_FALLBACKS):
            try:
                panes = self.driver.find_elements("css selector", selector)
            except Exception as exc:  # noqa: BLE001 - transient/stale browser state
                logger.debug("Review pane lookup failed for %s: %s", selector, exc)
                continue
            for pane in panes:
                pane_id = getattr(pane, "id", None) or id(pane)
                if pane_id in seen_ids:
                    continue
                seen_ids.add(pane_id)
                candidates.append((pane, selector))
        return candidates

    def _score_pane(self, pane, selector):
        try:
            is_displayed = getattr(pane, "is_displayed", None)
            visible = is_displayed() if is_displayed else True
            metrics = self.driver.execute_script(
                """
                const pane = arguments[0];
                return {
                    scrollHeight: pane.scrollHeight,
                    clientHeight: pane.clientHeight,
                    scrollTop: pane.scrollTop,
                };
                """,
                pane,
            ) or {}
            client_height = metrics.get("clientHeight", 0) or 0
            scroll_height = metrics.get("scrollHeight", 0) or 0
            pane_find = getattr(pane, "find_elements", None)
            cards = (
                pane_find("css selector", selectors.REVIEW_CARD)
                if pane_find
                else []
            )
        except Exception as exc:  # noqa: BLE001 - stale element during Maps re-render
            return None, {"selector": selector, "stale": True, "error": str(exc)}
        overflow = scroll_height > client_height
        plausible_size = visible and client_height >= 100
        dimensions_valid = scroll_height >= client_height
        plausible_scroll = dimensions_valid and (overflow or bool(cards))
        details = {
            "selector": selector,
            "visible": visible,
            "clientHeight": client_height,
            "scrollHeight": scroll_height,
            "cards": len(cards),
            "overflow": overflow,
            "dimensions_valid": dimensions_valid,
        }
        if not (plausible_size and plausible_scroll):
            return None, details
        score = (
            (100 if selector == selectors.REVIEW_PANE_PRIMARY else 0)
            + (50 if overflow else 0)
            + min(len(cards), 20)
            + min(client_height / 1000, 10)
        )
        details["score"] = score
        return {"pane": pane, "score": score}, details

    def sort_newest(self):
        last_error = None
        for attempt in range(3):
            try:
                self._sort_newest_once()
                return None
            except SortError as exc:
                last_error = exc
                logger.info("Sort menu local attempt %d/3 failed: %s", attempt + 1, exc)
                if attempt < 2:
                    self.sleeper(self.retry_delay)

        logger.info("Re-entering Reviews surface after local sort retries")
        try:
            review_pane = self.open_reviews()
        except ReviewSurfaceError as exc:
            raise SortError(
                f"{last_error}; Reviews re-entry failed: {exc}"
            ) from exc
        try:
            self._sort_newest_once()
            return review_pane
        except SortError as exc:
            raise SortError(
                f"sort failed after Reviews re-entry; last local error: {last_error}; "
                f"re-entry error: {exc}"
            ) from exc

    def _sort_newest_once(self) -> None:
        try:
            button = self._find_sort_button()
            if button is None:
                raise SortError("sort button not found")

            diagnostics = self._button_diagnostics(button)
            self._scroll_into_view(button)
            self.sleeper(0.3)
            strategies = (
                ("javascript", lambda: self.driver.execute_script(
                    "arguments[0].click();", button
                )),
                ("normal", button.click),
                ("action_chains", lambda: ActionChains(self.driver)
                 .move_to_element(button).click().perform()),
                ("action_chains_center", lambda: ActionChains(self.driver)
                 .move_to_element_with_offset(
                     button,
                     button.size["width"] // 2,
                     button.size["height"] // 2,
                 ).click().perform()),
                ("keyboard_enter", lambda: button.send_keys(Keys.ENTER)),
            )
            attempted = []
            menu_open = False
            for name, click in strategies:
                attempted.append(name)
                try:
                    click()
                except Exception as exc:  # noqa: BLE001
                    logger.debug("Sort button %s click failed: %s", name, exc)
                if self._sort_menu_is_open():
                    menu_open = True
                    break

            if not menu_open:
                diagnostics.update(
                    {
                        "click_strategies_attempted": attempted,
                        "menu_structure_appeared": False,
                    }
                )
                logger.warning("Sort menu failed to open: %s", diagnostics)
                raise SortError(f"sort menu failed to open; diagnostics: {diagnostics}")

            items = _unique_menu_items(
                self._visible_sort_options()
            )
            newest_labels = {_normalized(label) for label in selectors.NEWEST_LABELS}
            for element, details in items:
                labels = (details["text"], details["aria_label"])
                if any(
                    target == label or target in label
                    for label in labels
                    for target in newest_labels
                ):
                    element.click()
                    return

            if self._has_standard_radio_menu(items):
                items[1][0].click()
                return

            raise SortError(
                "Newest option not found; discovered sort menu items: "
                f"{_described(items)}"
            )
        except SortError:
            raise
        except Exception as exc:
            raise SortError(str(exc)) from exc

    def _find_sort_button(self):
        selectors_to_try = (selectors.SORT_BUTTON, *SORT_BUTTON_FALLBACKS)
        for css_selector in selectors_to_try:
            candidates = self.driver.find_elements("css selector", css_selector)
            viable = [button for button in candidates if self._button_is_viable(button)]
            sort_labeled = [button for button in viable if self._is_sort_labeled(button)]
            if sort_labeled:
                return sort_labeled[0]
            if css_selector == selectors.SORT_BUTTON and viable:
                return viable[0]

        for xpath_selector in (selectors.SORT_BUTTON_XPATH,):
            candidates = self.driver.find_elements("xpath", xpath_selector)
            viable = [button for button in candidates if self._button_is_viable(button)]
            if viable:
                return viable[0]
        return None

    @staticmethod
    def _is_sort_labeled(button) -> bool:
        label = _normalized(button.get_attribute("aria-label"))
        if any(token in label for token in NEGATIVE_BUTTON_LABELS):
            return False
        return any(
            token in label
            for token in ("sort", "sắp xếp", "trier", "orden", "sortieren")
        )

    @staticmethod
    def _button_is_viable(button) -> bool:
        is_displayed = getattr(button, "is_displayed", None)
        is_enabled = getattr(button, "is_enabled", None)
        return (is_displayed() if is_displayed else True) and (
            is_enabled() if is_enabled else True
        )

    def _scroll_into_view(self, button) -> None:
        execute_script = getattr(self.driver, "execute_script", None)
        if execute_script:
            execute_script(
                "arguments[0].scrollIntoView({block: 'center', inline: 'nearest'});",
                button,
            )

    def _sort_menu_is_open(self) -> bool:
        if self._visible_elements(SORT_MENU_SELECTOR):
            return True
        visible_options = self._visible_sort_options()
        return bool(visible_options)

    def _visible_sort_options(self) -> list[object]:
        options = []
        for css_selector in SORT_OPTION_FALLBACKS:
            for element in self._visible_elements(css_selector):
                class_name = _normalized(element.get_attribute("class"))
                if "mluxec" in class_name:
                    parent = self.driver.execute_script(
                        "return arguments[0].closest('[role=\"menuitemradio\"]');",
                        element,
                    )
                    if parent is not None:
                        element = parent
                options.append(element)
        return options

    def _visible_elements(self, css_selector: str) -> list[object]:
        elements = self.driver.find_elements("css selector", css_selector)
        visible = []
        for element in elements:
            is_displayed = getattr(element, "is_displayed", None)
            if is_displayed is None or is_displayed():
                visible.append(element)
        return visible

    @staticmethod
    def _button_diagnostics(button) -> dict[str, object]:
        return {
            "aria_label": button.get_attribute("aria-label"),
            "role": button.get_attribute("role"),
            "class": button.get_attribute("class"),
            "aria-haspopup": button.get_attribute("aria-haspopup"),
            "aria-expanded": button.get_attribute("aria-expanded"),
        }

    @staticmethod
    def _has_standard_radio_menu(
        items: list[tuple[object, dict[str, str]]],
    ) -> bool:
        return (
            2 <= len(items) <= 4
            and all(details["role"] == "menuitemradio" for _, details in items)
            and all(
                details["text"] or details["aria_label"] for _, details in items
            )
            and items[0][1]["aria_checked"] == "true"
        )
