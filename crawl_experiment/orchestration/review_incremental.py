"""ID/content-driven incremental stop policy; never parse relative date labels."""
from dataclasses import dataclass
from datetime import datetime

from crawl_experiment.storage.review_repository import ReviewChange


@dataclass(frozen=True)
class IncrementalConfig:
    known_streak_threshold: int = 5
    max_scrolls: int = 20
    safety_boundary: datetime | None = None
    # Optional trusted exact-time provider. Production DOM supplies no exact time.
    review_time: object = None

    def __post_init__(self):
        if self.known_streak_threshold<=0 or self.max_scrolls<=0:
            raise ValueError('Incremental threshold and max_scrolls must be positive')
        if self.safety_boundary and self.safety_boundary.tzinfo is None:
            raise ValueError('Safety boundary must be timezone-aware')


class IncrementalWindow:
    def __init__(self, repository, place_id, config):
        self.repository=repository;self.place_id=place_id;self.config=config
        state=repository.get_incremental_state(place_id) or {}
        self.boundary=config.safety_boundary or (datetime.fromisoformat(state['last_successful_incremental_at'])
            if state.get('last_successful_incremental_at') else None)
        self.streak=0;self.stopped=False;self.newest_id=None;self.newest_time=None
        self.metrics=dict(new_reviews=0,updated_reviews=0,known_unchanged_reviews=0,reviews_examined=0,
            streak_at_stop=0,stop_reason=None,scroll_count=0,elapsed_seconds=0)

    def observed(self,review,change):
        exact=self.config.review_time(review) if self.config.review_time else None
        if not isinstance(exact,datetime) or exact.tzinfo is None:
            exact=None
        if self.newest_id is None:
            self.newest_id=review.source_review_id
            self.newest_time=exact.isoformat() if exact else None
        key={ReviewChange.INSERT:'new_reviews',ReviewChange.UPDATE:'updated_reviews',ReviewChange.UNCHANGED:'known_unchanged_reviews'}[change]
        self.metrics[key]+=1;self.metrics['reviews_examined']+=1
        self.streak=self.streak+1 if change is ReviewChange.UNCHANGED else 0
        boundary_ok=exact is None or (self.boundary is not None and exact<=self.boundary)
        self.stopped=self.streak>=self.config.known_streak_threshold and boundary_ok
        return {ReviewChange.INSERT:'NEW',ReviewChange.UPDATE:'UPDATED',ReviewChange.UNCHANGED:'KNOWN_UNCHANGED'}[change]

    def finish(self,reason,scrolls,elapsed,successful):
        self.metrics.update(streak_at_stop=self.streak,stop_reason=reason,scroll_count=scrolls,elapsed_seconds=elapsed)
        self.repository.save_incremental_state(self.place_id,self.newest_id,self.newest_time,successful)
        return dict(self.metrics)
