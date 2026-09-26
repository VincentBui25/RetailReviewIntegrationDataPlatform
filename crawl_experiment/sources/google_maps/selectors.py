REVIEW_CARD = "div[data-review-id]"
REVIEW_ID_ATTRIBUTE = "data-review-id"
REVIEW_PANE_PRIMARY = "div[role='main'] div.m6QErb.DxyBCb.kA9KIf.dS8AEf"
REVIEW_PANE_FALLBACKS = (
    "div[role='main'] div.m6QErb",
    "div[role='main'] div[tabindex='-1']",
    "div[role='main']",
)
REVIEW_PANE = ", ".join((REVIEW_PANE_PRIMARY, *REVIEW_PANE_FALLBACKS))
PLACE_TABS = "[role='tab']"
PLACE_SHELL = "h1.DUwDvf, [data-item-id='address'], button[jsaction*='pane.reviewChart']"
SIGN_IN_PROMPT = "a[data-action='sign in'], a[href*='ServiceLogin']"
REVIEWS_TAB_CSS = "button[role='tab'][aria-label*='Reviews'], button[jsaction*='pane.reviewChart']"
REVIEWS_TAB_XPATH = "//button[contains(@aria-label,'Reviews') or .//*[contains(text(),'Reviews')]]"
SORT_BUTTON = (
    "button[aria-label*='Sort reviews' i], "
    "button[aria-label*='Sort' i], "
    "button[aria-label*='Sắp xếp' i], "
    "button[data-value='Sort']"
)
SORT_BUTTON_XPATH = (
    "//button[contains(translate(@aria-label, "
    "'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'sort') "
    "or contains(@aria-label, 'Sắp xếp')]"
)
SORT_MENU_ITEMS = (
    "div[role='menu'] [role='menuitem'], "
    "li[role='menuitem'], "
    "[role='menuitemradio'], "
    "[role='menuitem'], "
    "[role='option']"
)
NEWEST_LABELS = (
    "newest", "החדשות ביותר", "ใหม่ที่สุด", "最新", "más recientes", "最近",
    "mais recentes", "neueste", "plus récent", "più recenti", "nyeste",
    "новые", "nieuwste", "جديد", "uusimmat", "najnowsze", "senaste",
    "terbaru", "yakın zamanlı", "mới nhất", "नवीनतम",
)
AUTHOR = ".d4r55, [class*='author']"
RATING = "span[role='img'][aria-label*='star']"
TEXT = ".wiI7pd, .MyEned"
DATE = ".rsqaWe"
OWNER_RESPONSE = ".CDe7pd .wiI7pd, .owner-response"
SCROLL_TO_END_SCRIPT = """
    const pane = arguments[0];
    const before = {
        scrollTop: pane.scrollTop,
        scrollHeight: pane.scrollHeight,
        clientHeight: pane.clientHeight,
    };
    pane.scrollBy(0, pane.scrollHeight);
    return {
        before: before,
        after: {
            scrollTop: pane.scrollTop,
            scrollHeight: pane.scrollHeight,
            clientHeight: pane.clientHeight,
        },
    };
"""
LIMITED_VIEW_TEXT = ("limited view", "vue limitée", "eingeschränkte ansicht")
LIMITED_PROMPT_TEXT = ("sign in to see", "can't show")
CHALLENGE_TEXT = ("unusual traffic", "not a robot", "recaptcha")
