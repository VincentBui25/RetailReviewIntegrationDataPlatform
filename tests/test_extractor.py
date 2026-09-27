import pytest

from crawl_experiment.core.errors import ReviewParseError
from crawl_experiment.sources.google_maps import selectors
from crawl_experiment.sources.google_maps.extractor import ReviewExtractor


class Element:
    def __init__(self, text="", attrs=None): self.text, self.attrs = text, attrs or {}
    def get_attribute(self, name): return self.attrs.get(name)


class Card(Element):
    def __init__(self, review_id="r-1"):
        super().__init__(attrs={"data-review-id": review_id})
        self.children = {
            selectors.AUTHOR: [Element("Alice")], selectors.RATING: [Element(attrs={"aria-label": "5 stars"})],
            selectors.TEXT: [Element("Useful")], selectors.DATE: [Element("a week ago")], selectors.OWNER_RESPONSE: []}
    def find_elements(self, _, selector): return self.children.get(selector, [])


def test_extracts_canonical_review():
    review = ReviewExtractor().extract(Card(), "store-1")
    assert review.identity == ("google_maps", "r-1")
    assert review.author == "Alice" and review.rating == 5 and review.text == "Useful"


def test_missing_review_id_is_card_level_parse_failure():
    with pytest.raises(ReviewParseError): ReviewExtractor().extract(Card(""), "store-1")
