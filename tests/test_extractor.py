import pytest

from crawl_experiment.core.errors import ReviewParseError
from crawl_experiment.core.models import CrawlResult
from crawl_experiment.core.statuses import CrawlStatus
from crawl_experiment.sources.google_maps import selectors
from crawl_experiment.sources.google_maps.crawler import GoogleMapsCrawler
from crawl_experiment.sources.google_maps.extractor import ReviewExtractor
from crawl_experiment.storage.review_repository import ReviewChange


class Element:
    def __init__(self, text="", attrs=None): self.text, self.attrs = text, attrs or {}
    def get_attribute(self, name): return self.attrs.get(name)


class Card(Element):
    def __init__(self, review_id="r-1", review_url=None, media=None):
        super().__init__(attrs={"data-review-id": review_id})
        self.children = {
            selectors.AUTHOR: [Element("Alice")], selectors.RATING: [Element(attrs={"aria-label": "5 stars"})],
            selectors.TEXT: [Element("Useful")], selectors.DATE: [Element("a week ago")], selectors.OWNER_RESPONSE: [],
            selectors.REVIEW_URL: [Element(attrs={"href": review_url})] if review_url else [],
            selectors.REVIEW_MEDIA: media or []}
    def find_elements(self, _, selector): return self.children.get(selector, [])


def test_extracts_canonical_review():
    review = ReviewExtractor().extract(Card(), "store-1")
    assert review.identity == ("google_maps", "r-1")
    assert review.author == "Alice" and review.rating == 5 and review.text == "Useful"


def test_missing_review_id_is_card_level_parse_failure():
    with pytest.raises(ReviewParseError): ReviewExtractor().extract(Card(""), "store-1")


def test_extracts_review_url_and_deduplicated_review_media_in_dom_order():
    card = Card(
        review_url="https://www.google.com/maps/reviews/data=review-1",
        media=[
            Element(attrs={
                "src": "https://images.example/one.jpg?a=1&amp;b=2",
                "style": "background-image: url('https://images.example/not-preferred.jpg')",
            }),
            Element(attrs={"data-src": "https://images.example/two.jpg"}),
            Element(attrs={"style": "background-image: url('https://images.example/one.jpg?a=1&b=2')"}),
            Element(attrs={"src": "/relative-review-image.jpg"}),
            Element(attrs={"data-src": "malformed-image-url"}),
        ],
    )
    # An avatar is deliberately outside REVIEW_MEDIA and therefore not returned.
    card.children["img.avatar"] = [Element(attrs={"src": "https://images.example/avatar.jpg"})]

    review = ReviewExtractor().extract(card, "store-1")

    assert review.review_url == "https://www.google.com/maps/reviews/data=review-1"
    assert review.image_urls == [
        "https://images.example/one.jpg?a=1&b=2",
        "https://images.example/two.jpg",
    ]


def test_absent_review_url_and_media_use_optional_defaults():
    review = ReviewExtractor().extract(Card(), "store-1")
    assert review.review_url is None
    assert review.image_urls == []


def test_duplicate_dom_cards_produce_one_persistence_decision():
    class Repository:
        def __init__(self):
            self.review_ids = []

        def upsert_review_state(self, review):
            self.review_ids.append(review.source_review_id)
            return ReviewChange.INSERT

    crawler = object.__new__(GoogleMapsCrawler)
    crawler.reviews = Repository()
    crawler.clock = lambda: 0.0
    crawler.activity_observer = None
    result = CrawlResult("store-1", CrawlStatus.ERROR)

    crawler._extract_new_reviews(
        [Card("r-1"), Card("r-1")],
        "store-1",
        ReviewExtractor(),
        result,
        {"r-1"},
    )

    assert crawler.reviews.review_ids == ["r-1"]
    assert result.reviews_written == 1
