import re

from crawl_experiment.core.errors import ReviewParseError
from crawl_experiment.core.models import Review

from . import selectors


def _text(card, selector: str) -> str | None:
    elements = card.find_elements("css selector", selector)
    if not elements:
        return None
    return (elements[0].text or "").strip() or None


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
            )
        except ReviewParseError:
            raise
        except Exception as exc:
            raise ReviewParseError(str(exc)) from exc
