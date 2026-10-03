"""Access deferral without sleeping browser workers or changing extraction."""
import random
from dataclasses import dataclass
from datetime import datetime,timedelta,timezone


@dataclass(frozen=True)
class AccessPolicyConfig:
    limited_ranges: tuple = ((900,1800),(3600,10800),(21600,43200),(43200,86400))
    unknown_initial_range: tuple = (1800,3600)
    max_backoff_seconds: float = 86400
    max_runtime_retries: int = 1
    max_browser_retries: int = 1

    def __post_init__(self):
        ranges=(*self.limited_ranges,self.unknown_initial_range)
        if not self.limited_ranges or any(low<=0 or high<low for low,high in ranges):
            raise ValueError('Backoff ranges must be positive and ordered')
        if self.max_backoff_seconds<=0 or min(self.max_runtime_retries,self.max_browser_retries)<0:
            raise ValueError('Invalid cap or retry budget')


@dataclass
class AccessDecision:
    access_state: str
    job_status: str | None
    retry_fresh: bool=False
    backoff_seconds: float=0


class AccessPolicy:
    def __init__(self, repository, config=None, clock=None, uniform=None):
        self.repository=repository
        self.config=config or AccessPolicyConfig()
        self.clock=clock or (lambda:datetime.now(timezone.utc))
        self.uniform=uniform or random.uniform

    def eligible(self,place_id):
        row=self.repository.get(place_id)
        return not row['next_eligible_at'] or datetime.fromisoformat(row['next_eligible_at'])<=self.clock()

    def attempt(self,place_id):
        now=self.clock().isoformat()
        return self.repository.update(place_id,lambda row:row.update(last_attempt_at=now))

    def full(self,place_id,successful=False):
        now=self.clock().isoformat()
        def change(row):
            row.update(last_access_state='FULL',consecutive_limited_count=0,consecutive_unknown_count=0,
                consecutive_runtime_error_count=0,next_eligible_at=None)
            if successful:
                row['last_successful_crawl_at']=now
        return self.repository.update(place_id,change)

    def failure(self,place_id,state,session_counts):
        decision=AccessDecision(state,None)
        now=self.clock()
        def change(row):
            row['last_access_state']=state
            if state=='LIMITED':
                row['consecutive_limited_count']+=1
                n=row['consecutive_limited_count']
                low,high=self.config.limited_ranges[min(n-1,len(self.config.limited_ranges)-1)]
                decision.job_status='DEFERRED_LIMITED'
            elif state=='UNKNOWN':
                row['consecutive_unknown_count']+=1
                if session_counts.get(state,0)==0:
                    decision.retry_fresh=True
                    return
                n=row['consecutive_unknown_count']
                multiplier=2**min(max(n-2,0),16)
                low,high=(v*multiplier for v in self.config.unknown_initial_range)
                decision.job_status='DEFERRED_UNKNOWN'
            else:
                row['consecutive_runtime_error_count']+=1
                budget=self.config.max_browser_retries if state=='BROWSER_ERROR' else self.config.max_runtime_retries
                decision.retry_fresh=session_counts.get(state,0)<budget
                decision.job_status=None if decision.retry_fresh else ('FAILED_BROWSER' if state=='BROWSER_ERROR' else 'FAILED_RUNTIME')
                return
            cap=self.config.max_backoff_seconds
            decision.backoff_seconds=self.uniform(min(low,cap),min(high,cap))
            row['next_eligible_at']=(now+timedelta(seconds=decision.backoff_seconds)).isoformat()
        self.repository.update(place_id,change)
        return decision

    def metrics(self,place_id,decision,attempt_number,network_count=None,dom_count=None):
        total=(network_count or 0)+(dom_count or 0)
        return dict(place_id=place_id,job_status=decision.job_status,access_state=decision.access_state,
            attempt_number=attempt_number,backoff_seconds=decision.backoff_seconds,
            fallback_rate=dom_count/total if total and dom_count is not None else None,
            network_review_count=network_count,dom_fallback_count=dom_count,
            runtime_error_count=self.repository.get(place_id)['consecutive_runtime_error_count'])
