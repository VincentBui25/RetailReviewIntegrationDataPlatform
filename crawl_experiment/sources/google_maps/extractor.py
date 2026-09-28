import html
import re
from urllib.parse import urlparse

from crawl_experiment.core.errors import ReviewParseError
from crawl_experiment.core.models import Review

from . import selectors


def _text(card, selector: str) -> str | None:
    elements = card.find_elements("css selector", selector)
    if not elements:
        return None
    return (elements[0].text or "").strip() or None


def _review_url(card) -> str | None:
    for element in card.find_elements("css selector", selectors.REVIEW_URL):
        url = (element.get_attribute("href") or "").strip()
        if url and ("/maps/reviews/" in url or "review_id=" in url or "reviewId=" in url):
            return url
    return None


def _image_urls(card) -> list[str]:
    urls: list[str] = []
    seen: set[str] = set()
    for element in card.find_elements("css selector", selectors.REVIEW_MEDIA):
        resolved_src = element.get_attribute("src")
        if resolved_src:
            candidates = [resolved_src]
        else:
            candidates = [
                element.get_attribute("data-src"),
                element.get_attribute("data-lazy-src"),
                element.get_attribute("data-original"),
            ]
            style = element.get_attribute("style") or ""
            candidates.extend(re.findall(r"url\((?:['\"])?(.*?)(?:['\"])?\)", style))
        for candidate in candidates:
            url = html.unescape(candidate or "").strip().strip("'\"")
            parsed = urlparse(url)
            if (
                parsed.scheme in {"http", "https"}
                and parsed.netloc
                and url not in seen
            ):
                seen.add(url)
                urls.append(url)
    return urls


class ReviewExtractor:
    def extract(self, card, store_id: str) -> Review:
        try:
            review_id = (card.get_attribute(selectors.REVIEW_ID_ATTRIBUTE) or "").strip()
            if not review_id:
                raise ReviewParseError("review card has no data-review-id")

            ratings = card.find_elements("css selector", selectors.RATING)
            rating_label = ratings[0].get_attribute("aria-label") if ratings else None
            match = re.search(r"([0-5](?:\.\d+)?)", rating_label or "")
            return Review(
                source="google_maps",
                source_review_id=review_id,
                store_id=store_id,
                author=_text(card, selectors.AUTHOR),
                rating=float(match.group(1)) if match else None,
                text=_text(card, selectors.TEXT),
                displayed_date=_text(card, selectors.DATE),
                owner_response=_text(card, selectors.OWNER_RESPONSE),
                review_url=_review_url(card),
                image_urls=_image_urls(card),
            )
        except ReviewParseError:
            raise
        except Exception as exc:
            raise ReviewParseError(str(exc)) from exc
