import logging
import re
import time
from enum import StrEnum

from selenium.webdriver.common.action_chains import ActionChains

from crawl_experiment.core.errors import (
    AuthRequiredError,
    LimitedReviewViewError,
    ReviewSurfaceError,
    SortError,
)

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
SORT_MENU_SELECTOR = (
    "[role='menu'], [role='listbox'], "
    "[role='dialog'] [role='menuitemradio'], "
    "[role='dialog'] [role='option']"
)
SORT_OPTION_FALLBACKS = (
    selectors.SORT_MENU_ITEMS,
    "div.fxNQSd",
    "div.mLuXec",
)
AUTH_DIALOG_SELECTOR = "[role='dialog'], [aria-modal='true']"
MAIN_SURFACE_SELECTOR = "[role='main']"
AUTH_REVIEW_TERMS = ("review", "sort", "continue", "account")
LIMITED_REVIEW_TERMS = (
    "limited review view",
    "limited view",
    "some reviews aren't available",
    "some reviews are not available",
    "sign in to see all reviews",
)
NO_REVIEWS_TERMS = (
    "no reviews",
    "no reviews yet",
    "be the first to review",
)


class ReviewSurfaceState(StrEnum):
    NO_REVIEWS = "NO_REVIEWS"
    LIMITED = "LIMITED"
    AUTH_REQUIRED = "AUTH_REQUIRED"
    FULL = "FULL"
    UNKNOWN = "UNKNOWN"


def _normalized(value: str | None) -> str:
    return " ".join((value or "").split()).casefold()


def _menu_item_details(element) -> dict[str, object]:
    is_displayed = getattr(element, "is_displayed", None)
    is_enabled = getattr(element, "is_enabled", None)
    get_property = getattr(element, "get_property", None)
    try:
        dataset = get_property("dataset") if get_property else None
    except Exception:  # noqa: BLE001 - diagnostic property may be unavailable
        dataset = None
    try:
        selected_descendants = len(
            element.find_elements(
                "css selector",
                "[aria-selected='true'], [aria-checked='true'], "
                "[aria-current]:not([aria-current='false']), "
                "[data-selected='true'], [data-checked='true'], "
                "[aria-label*='checked' i], [title*='checked' i]",
            )
        )
    except Exception:  # noqa: BLE001 - diagnostic child audit is best effort
        selected_descendants = 0
    return {
        "role": _normalized(element.get_attribute("role")),
        "text": _normalized(getattr(element, "text", "")),
        "aria_label": _normalized(element.get_attribute("aria-label")),
        "aria_checked": _normalized(element.get_attribute("aria-checked")),
        "aria_selected": _normalized(element.get_attribute("aria-selected")),
        "aria_current": _normalized(element.get_attribute("aria-current")),
        "class": _normalized(element.get_attribute("class")),
        "tabindex": _normalized(element.get_attribute("tabindex")),
        "data": dataset if isinstance(dataset, dict) else {},
        "selected_descendants": selected_descendants,
        "displayed": str(is_displayed() if is_displayed else True).lower(),
        "enabled": str(is_enabled() if is_enabled else True).lower(),
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
        self._last_sort_selector = None

    def open_reviews(self):
        try:
            buttons = self.driver.find_elements("css selector", selectors.REVIEWS_TAB_CSS)
            if not buttons:
                buttons = self.driver.find_elements("xpath", selectors.REVIEWS_TAB_XPATH)
            if not buttons:
                raise ReviewSurfaceError("Reviews tab not found")
            selected = _normalized(buttons[0].get_attribute("aria-selected")) == "true"
            if not selected:
                buttons[0].click()
            else:
                logger.info("Reviews tab already selected; reacquiring pane without click")
            return self.resolve_review_pane()
        except ReviewSurfaceError:
            raise
        except Exception as exc:
            raise ReviewSurfaceError(str(exc).splitlines()[0]) from exc

    def prepare_and_classify(self):
        """Open Reviews when possible, then classify before sort is attempted."""
        initial_state, initial = self.classify_state()
        if initial_state in {
            ReviewSurfaceState.NO_REVIEWS,
            ReviewSurfaceState.LIMITED,
            ReviewSurfaceState.AUTH_REQUIRED,
        }:
            logger.info(
                "Review surface classified state=%s evidence=%s",
                initial_state,
                initial,
            )
            return initial_state, None, initial
        try:
            pane = self.open_reviews()
        except ReviewSurfaceError as exc:
            state, evidence = self.classify_state()
            evidence["open_error"] = str(exc).splitlines()[0]
            logger.info(
                "Review surface classified state=%s evidence=%s", state, evidence
            )
            return state, None, evidence
        before_classification = getattr(self, "before_access_classification", None)
        if before_classification is not None:
            before_classification()
        state, evidence = self.classify_state(pane)
        logger.info(
            "Review surface classified state=%s evidence=%s", state, evidence
        )
        return state, pane, evidence

    def classify_state(self, pane=None):
        review_tabs = self._safe_find(selectors.REVIEWS_TAB_CSS)
        cards = self._cards_for_classification(pane)
        auth_dialogs = self._visible_elements(AUTH_DIALOG_SELECTOR)
        auth_texts = [self._element_text(element) for element in auth_dialogs]
        auth_evidence = [
            text
            for text in auth_texts
            if "sign in" in text
            and any(term in text for term in AUTH_REVIEW_TERMS)
        ]
        main_text = " ".join(
            self._element_text(element)
            for element in self._visible_elements(MAIN_SURFACE_SELECTOR)
        )
        limited_evidence = [
            term for term in LIMITED_REVIEW_TERMS if term in main_text
        ]
        no_reviews_evidence = [
            term for term in NO_REVIEWS_TERMS if term in main_text
        ]
        sort_button = self._find_sort_button() if pane is not None else None
        sort_control = bool(
            sort_button
            and self._interactive_sort_control_confirmed(sort_button)
        )
        evidence = {
            "review_tabs": len(review_tabs),
            "cards": len(cards),
            "pane": pane is not None,
            "sort_control": sort_control,
            "sort_selector": self._last_sort_selector if sort_control else None,
            "auth_dialog_evidence": auth_evidence,
            "limited_evidence": limited_evidence,
            "no_reviews_evidence": no_reviews_evidence,
            "header_sign_in_ignored": bool(
                self._safe_find(selectors.SIGN_IN_PROMPT)
            ),
        }
        if auth_evidence:
            return ReviewSurfaceState.AUTH_REQUIRED, evidence
        if limited_evidence and (cards or review_tabs):
            return ReviewSurfaceState.LIMITED, evidence
        if no_reviews_evidence and not cards:
            return ReviewSurfaceState.NO_REVIEWS, evidence
        if pane is not None and cards and sort_control:
            return ReviewSurfaceState.FULL, evidence
        return ReviewSurfaceState.UNKNOWN, evidence

    def _cards_for_classification(self, pane):
        try:
            if pane is not None and hasattr(pane, "find_elements"):
                return pane.find_elements("css selector", selectors.REVIEW_CARD)
            return self._safe_find(selectors.REVIEW_CARD)
        except Exception:  # noqa: BLE001
            return []

    def _safe_find(self, selector):
        try:
            return self.driver.find_elements("css selector", selector)
        except Exception:  # noqa: BLE001
            return []

    @staticmethod
    def _element_text(element):
        try:
            return _normalized(getattr(element, "text", ""))
        except Exception:  # noqa: BLE001
            return ""

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
            self._wait_until_control_stable(button)
            menu_open, attempted = self._open_menu_with_bounded_clicks(button)

            if not menu_open:
                state, state_evidence = self.classify_state(self._last_pane)
                if state is ReviewSurfaceState.AUTH_REQUIRED:
                    raise AuthRequiredError(
                        f"review sign-in required after sort access: {state_evidence}"
                    )
                if state is ReviewSurfaceState.LIMITED:
                    raise LimitedReviewViewError(
                        f"limited review surface after sort access: {state_evidence}"
                    )
                diagnostics.update(
                    {
                        "click_strategies_attempted": attempted,
                        "menu_structure_appeared": False,
                        "menu_option_texts": self._menu_option_texts(),
                        "reviews_tab_selected": self._reviews_tab_selected(),
                    }
                )
                logger.warning("Sort menu failed to open: %s", diagnostics)
                raise SortError(f"sort menu failed to open; diagnostics: {diagnostics}")

            items = _unique_menu_items(
                self._visible_sort_options()
            )
            logger.info("Sort menu options: %s", self._menu_option_texts(items))
            logger.info("Sort option DOM audit: %s", _described(items))
            newest_labels = {_normalized(label) for label in selectors.NEWEST_LABELS}
            for element, details in items:
                labels = (details["text"], details["aria_label"])
                if any(
                    target == label or target in label
                    for label in labels
                    for target in newest_labels
                ):
                    element.click()
                    if self._confirm_newest(button, element, newest_labels):
                        return
                    raise SortError(
                        "Newest option click was not confirmed; menu items: "
                        f"{_described(items)}"
                    )

            if self._has_standard_radio_menu(items):
                element = items[1][0]
                element.click()
                if self._confirm_newest(button, element, newest_labels):
                    return
                raise SortError(
                    "Newest fallback option click was not confirmed; menu items: "
                    f"{_described(items)}"
                )

            raise SortError(
                "Newest option not found; discovered sort menu items: "
                f"{_described(items)}"
            )
        except (AuthRequiredError, LimitedReviewViewError):
            raise
        except SortError:
            raise
        except Exception as exc:
            raise SortError(str(exc).splitlines()[0]) from exc

    def _find_sort_button(self):
        selectors_to_try = (selectors.SORT_BUTTON, *SORT_BUTTON_FALLBACKS)
        for css_selector in selectors_to_try:
            candidates = self.driver.find_elements("css selector", css_selector)
            viable = [button for button in candidates if self._button_is_viable(button)]
            sort_labeled = [button for button in viable if self._is_sort_labeled(button)]
            if sort_labeled:
                self._last_sort_selector = css_selector
                return sort_labeled[0]
            if css_selector == selectors.SORT_BUTTON and viable:
                self._last_sort_selector = css_selector
                return viable[0]

        for xpath_selector in (selectors.SORT_BUTTON_XPATH,):
            candidates = self.driver.find_elements("xpath", xpath_selector)
            viable = [button for button in candidates if self._button_is_viable(button)]
            if viable:
                self._last_sort_selector = xpath_selector
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

    def _wait_until_control_stable(self, button) -> None:
        previous = None
        deadline = self.clock() + min(self.render_wait, 1.0)
        while True:
            if self._button_is_viable(button):
                current = self._element_rect(button)
                if current is None:
                    return
                if current and current == previous:
                    return
                previous = current
            if self.clock() >= deadline:
                return
            self.sleeper(min(self.render_poll, 0.1))

    def _open_menu_with_bounded_clicks(self, button):
        strategies = [
            ("normal", button.click),
            (
                "action_chains",
                lambda: ActionChains(self.driver)
                .move_to_element(button)
                .click()
                .perform(),
            ),
        ]
        if self._interactive_sort_control_confirmed(button):
            strategies.append(
                (
                    "javascript",
                    lambda: self.driver.execute_script("arguments[0].click();", button),
                )
            )
        attempted = []
        for name, click in strategies:
            attempted.append(name)
            try:
                click()
            except Exception as exc:  # noqa: BLE001 - strategy fallback
                logger.debug("Sort button %s click failed: %s", name, str(exc).splitlines()[0])
            if self._wait_for_sort_menu():
                return True, attempted
        return False, attempted

    def _wait_for_sort_menu(self) -> bool:
        if self._sort_menu_is_open():
            return True
        self.sleeper(min(self.render_poll, 0.25))
        return self._sort_menu_is_open()

    def _confirm_newest(self, button, option, newest_labels) -> bool:
        self.sleeper(min(self.render_poll, 0.25))
        menu_open = self._sort_menu_is_open()
        selected, source = self._newest_selected_signal(
            button, option, newest_labels
        )
        visible_dates = []
        order_consistent = None
        pane_available = False

        if not selected and not menu_open:
            pane = self._available_review_pane()
            pane_available = pane is not None
            if pane_available:
                visible_dates = self._visible_review_dates(pane)
                order_consistent = self._dates_are_newest_first(visible_dates)

        menu_transition_confirmed = (
            not menu_open
            and pane_available
            and bool(visible_dates)
            and order_consistent is not False
        )
        confirmed = selected or menu_transition_confirmed
        confirmation_basis = (
            source
            if selected
            else "menu_closed_with_review_dates"
            if menu_transition_confirmed
            else None
        )
        diagnostics = {
            "newest_click_succeeded": True,
            "menu_closed": not menu_open,
            "selected_signal": selected or None,
            "selected_signal_source": source,
            "review_pane_available": pane_available,
            "visible_review_dates": visible_dates,
            "review_order_consistent": order_consistent,
            "confirmation_basis": confirmation_basis,
            "final_sort_confirmed": confirmed,
        }
        logger.info("sort_confirmation=%s", diagnostics)
        return confirmed

    def _newest_selected_signal(self, button, option, newest_labels):
        candidates = [(option, "clicked_option")]
        candidates.extend(
            (element, "visible_menu_option")
            for element, _ in _unique_menu_items(self._visible_sort_options())
        )
        for candidate, prefix in candidates:
            selected, source = self._option_selected_signal(
                candidate, newest_labels
            )
            if selected:
                return True, f"{prefix}.{source}"
        if self._control_reports_newest(button, newest_labels):
            return True, "sort_control_label"
        return False, None

    def _option_selected_signal(self, option, newest_labels):
        try:
            details = _menu_item_details(option)
        except Exception:  # noqa: BLE001 - option may rerender after click
            return False, None
        label = f"{details['text']} {details['aria_label']}"
        if not any(target in label for target in newest_labels):
            return False, None
        if details["aria_selected"] == "true":
            return True, "aria-selected"
        if details["aria_current"] not in {"", "false"}:
            return True, "aria-current"
        if details["aria_checked"] == "true":
            return True, "aria-checked"
        class_tokens = set(re.split(r"[^a-z0-9_-]+", details["class"]))
        for token in ("selected", "checked", "is-selected", "is-checked"):
            if token in class_tokens:
                return True, f"class:{token}"
        data = details["data"]
        if isinstance(data, dict):
            for key in ("selected", "checked"):
                if _normalized(str(data.get(key, ""))) == "true":
                    return True, f"data-{key}"
            if _normalized(str(data.get("state", ""))) in {"selected", "checked"}:
                return True, "data-state"
        try:
            marked_children = option.find_elements(
                "css selector",
                "[aria-selected='true'], [aria-checked='true'], "
                "[aria-current]:not([aria-current='false']), "
                "[data-selected='true'], [data-checked='true'], "
                "[aria-label*='checked' i], [title*='checked' i]",
            )
        except Exception:  # noqa: BLE001 - child audit is best effort
            marked_children = []
        if any(self._is_visible(child) for child in marked_children):
            return True, "selected_descendant"
        return False, None

    def _available_review_pane(self):
        pane = self._last_pane
        try:
            if pane is not None and self._is_visible(pane):
                return pane
        except Exception:  # noqa: BLE001 - pane may have rerendered
            pass
        try:
            return self.resolve_review_pane()
        except ReviewSurfaceError:
            return None

    def _visible_review_dates(self, pane, limit=5):
        try:
            cards = pane.find_elements("css selector", selectors.REVIEW_CARD)
        except Exception:  # noqa: BLE001 - pane may rerender
            return []
        dates = []
        for card in cards:
            if len(dates) >= limit:
                break
            try:
                if not self._is_visible(card):
                    continue
                labels = card.find_elements("css selector", selectors.DATE)
                text = next(
                    (
                        _normalized(getattr(label, "text", ""))
                        for label in labels
                        if self._is_visible(label)
                        and _normalized(getattr(label, "text", ""))
                    ),
                    "",
                )
            except Exception:  # noqa: BLE001 - one card must not spoil evidence
                continue
            if text:
                dates.append(text)
        return dates

    @classmethod
    def _dates_are_newest_first(cls, labels):
        ages = [cls._relative_age_seconds(label) for label in labels]
        if len(ages) < 2 or any(age is None for age in ages):
            return None
        return all(left <= right for left, right in zip(ages, ages[1:]))

    @staticmethod
    def _relative_age_seconds(label):
        text = _normalized(label)
        if text in {"just now", "today"}:
            return 0
        if text == "yesterday":
            return 86400
        match = re.fullmatch(
            r"(?:(\d+)|a|an|one)\s+"
            r"(minute|hour|day|week|month|year)s?\s+ago",
            text,
        )
        if not match:
            return None
        amount = int(match.group(1)) if match.group(1) else 1
        seconds = {
            "minute": 60,
            "hour": 3600,
            "day": 86400,
            "week": 7 * 86400,
            "month": 30 * 86400,
            "year": 365 * 86400,
        }
        return amount * seconds[match.group(2)]

    @staticmethod
    def _is_visible(element):
        is_displayed = getattr(element, "is_displayed", None)
        return is_displayed() if is_displayed else True

    @staticmethod
    def _option_is_selected_newest(option, newest_labels) -> bool:
        try:
            details = _menu_item_details(option)
        except Exception:  # noqa: BLE001 - menu may rerender after selection
            return False
        label = f"{details['text']} {details['aria_label']}"
        selected = details["aria_checked"] == "true" or _normalized(
            option.get_attribute("aria-selected")
        ) == "true"
        return selected and any(target in label for target in newest_labels)

    @staticmethod
    def _control_reports_newest(button, newest_labels) -> bool:
        try:
            state = " ".join(
                filter(
                    None,
                    (
                        button.get_attribute("aria-label"),
                        button.get_attribute("data-value"),
                        getattr(button, "text", ""),
                    ),
                )
            ).casefold()
        except Exception:  # noqa: BLE001 - control may rerender
            return False
        return any(target in state for target in newest_labels)

    def _interactive_sort_control_confirmed(self, button) -> bool:
        try:
            tag = _normalized(getattr(button, "tag_name", ""))
            role = _normalized(button.get_attribute("role"))
            return (tag == "button" or role == "button") and self._is_sort_labeled(button)
        except Exception:  # noqa: BLE001
            return False

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

    def _button_diagnostics(self, button) -> dict[str, object]:
        overlay = self._center_overlay(button)
        return {
            "selector": self._last_sort_selector,
            "tag": getattr(button, "tag_name", None),
            "aria_label": button.get_attribute("aria-label"),
            "role": button.get_attribute("role"),
            "class": button.get_attribute("class"),
            "aria-haspopup": button.get_attribute("aria-haspopup"),
            "aria-expanded": button.get_attribute("aria-expanded"),
            "visible": self._button_is_viable(button),
            "rect": self._element_rect(button),
            "overlay": overlay,
            "interactive_control_confirmed": self._interactive_sort_control_confirmed(button),
        }

    @staticmethod
    def _element_rect(element):
        try:
            return dict(element.rect)
        except Exception:  # noqa: BLE001
            return None

    def _center_overlay(self, element):
        try:
            return self.driver.execute_script(
                """
                const target = arguments[0];
                const rect = target.getBoundingClientRect();
                const x = rect.left + rect.width / 2;
                const y = rect.top + rect.height / 2;
                const top = document.elementFromPoint(x, y);
                if (!top || top === target || target.contains(top)) return null;
                return {
                    tag: top.tagName ? top.tagName.toLowerCase() : null,
                    role: top.getAttribute ? top.getAttribute('role') : null,
                    aria_label: top.getAttribute ? top.getAttribute('aria-label') : null,
                    class: top.className && typeof top.className === 'string'
                        ? top.className : null,
                };
                """,
                element,
            )
        except Exception as exc:  # noqa: BLE001
            return {"inspection_error": str(exc).splitlines()[0]}

    def _reviews_tab_selected(self):
        try:
            tabs = self.driver.find_elements("css selector", selectors.REVIEWS_TAB_CSS)
            return bool(tabs) and _normalized(
                tabs[0].get_attribute("aria-selected")
            ) == "true"
        except Exception:  # noqa: BLE001
            return None

    def _menu_option_texts(self, items=None):
        items = items if items is not None else _unique_menu_items(
            self._visible_sort_options()
        )
        return [
            details["text"] or details["aria_label"]
            for _, details in items
        ]

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
