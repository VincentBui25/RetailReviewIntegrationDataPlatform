class CrawlError(RuntimeError):
    """Base for failures whose owning layer can make a recovery decision."""


class BrowserSessionError(CrawlError):
    pass


class NavigationError(CrawlError):
    pass


class PlaceResolutionError(NavigationError):
    pass


class LimitedViewError(NavigationError):
    pass


class RateLimitedError(NavigationError):
    pass


class ChallengeError(NavigationError):
    pass


class ReviewSurfaceError(CrawlError):
    pass


class SortError(ReviewSurfaceError):
    pass


class PaginationStalledError(CrawlError):
    pass


class ReviewParseError(CrawlError):
    pass
