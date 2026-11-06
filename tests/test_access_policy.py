from datetime import datetime,timedelta,timezone
import pytest

from crawl_experiment.storage.place_access_repository import PlaceAccessRepository
from crawl_experiment.orchestration.access_policy import AccessPolicy,AccessPolicyConfig


@pytest.fixture
def policy(tmp_path):
    repository=PlaceAccessRepository(tmp_path/'state.sqlite3')
    value=AccessPolicy(repository,clock=lambda:datetime(2026,10,3,tzinfo=timezone.utc),uniform=lambda a,b:a)
    yield value
    repository.close()


def test_limited_escalation_and_cap(policy):
    seconds=[policy.failure('p','LIMITED',{}).backoff_seconds for _ in range(6)]
    assert seconds==[900,3600,21600,43200,43200,43200]
    assert not policy.eligible('p')
    assert policy.repository.get('p')['consecutive_limited_count']==6


def test_unknown_one_fresh_retry_per_job(policy):
    first=policy.failure('p','UNKNOWN',{})
    assert first.retry_fresh
    second=policy.failure('p','UNKNOWN',{'UNKNOWN':1})
    assert not second.retry_fresh
    assert second.job_status=='DEFERRED_UNKNOWN'
    assert second.backoff_seconds==1800


def test_full_resets_and_success_preserves_timestamp(policy):
    policy.failure('p','LIMITED',{})
    policy.failure('p','RUNTIME_ERROR',{})
    row=policy.full('p',successful=True)
    assert row['consecutive_limited_count']==row['consecutive_unknown_count']==row['consecutive_runtime_error_count']==0
    assert row['next_eligible_at'] is None
    assert row['last_successful_crawl_at']==policy.clock().isoformat()
    policy.full('p')
    assert policy.repository.get('p')['last_successful_crawl_at']==row['last_successful_crawl_at']
    assert policy.eligible('p')


def test_runtime_and_browser_do_not_count_as_limited(policy):
    assert policy.failure('p','RUNTIME_ERROR',{}).retry_fresh
    assert policy.failure('p','RUNTIME_ERROR',{'RUNTIME_ERROR':1}).job_status=='FAILED_RUNTIME'
    assert policy.failure('p','BROWSER_ERROR',{'BROWSER_ERROR':1}).job_status=='FAILED_BROWSER'
    row=policy.repository.get('p')
    assert row['consecutive_limited_count']==0
    assert row['consecutive_unknown_count']==0
    assert row['next_eligible_at'] is None


def test_jitter_and_custom_cap(policy):
    policy.uniform=lambda a,b:b
    assert policy.failure('p','LIMITED',{}).backoff_seconds==1800
    policy.config=AccessPolicyConfig(max_backoff_seconds=2000)
    assert policy.failure('p','LIMITED',{}).backoff_seconds==2000


def test_persist_across_runs(tmp_path):
    path=tmp_path/'state.sqlite3'
    repository=PlaceAccessRepository(path)
    policy=AccessPolicy(repository)
    policy.attempt('p');policy.failure('p','LIMITED',{})
    repository.close()
    repository=PlaceAccessRepository(path)
    assert repository.get('p')['last_attempt_at']
    assert not AccessPolicy(repository).eligible('p')
    repository.close()


@pytest.mark.parametrize('state,expected_calls,status',[
    ('LIMITED',1,'DEFERRED_LIMITED'),('UNKNOWN',2,'DEFERRED_UNKNOWN'),
    ('RUNTIME_ERROR',2,'FAILED_RUNTIME'),('BROWSER_ERROR',2,'FAILED_BROWSER')])
def test_adapter_enforces_terminal_or_fresh_retry(monkeypatch,policy,state,expected_calls,status):
    import asyncio
    from types import SimpleNamespace
    from crawlee.storages import RequestQueue
    from crawl_experiment.core.models import Store
    from crawl_experiment.core.errors import LimitedReviewViewError,ReviewSurfaceError,BrowserSessionError
    from crawl_experiment.orchestration.crawlee_adapter import CrawleeAdapter
    pending=[]
    class Queue:
        async def add_requests(self,requests): pending.extend(requests)
    async def open_queue(**kwargs): return Queue()
    class Crawler:
        def __init__(self,**kwargs): self.options=kwargs
        async def run(self,**kwargs):
            while pending:
                request=pending.pop(0)
                try:
                    await self.options['request_handler'](SimpleNamespace(request=request,session=None))
                except Exception:
                    request.retry_count+=1
                    assert request.retry_count<=self.options['max_request_retries']
                    pending.insert(0,request)
    monkeypatch.setattr(RequestQueue,'open',staticmethod(open_queue))
    monkeypatch.setattr('crawlee.crawlers.BasicCrawler',Crawler)
    sessions=[];calls=[];deferred=[];terminal=[]
    class Session:
        def start(self): return object()
        def is_alive(self): return True
        def close(self): pass
    class Factory:
        def create(self,identity):
            session=Session(); sessions.append(session); return session
    def crawl(*args):
        calls.append(True)
        errors={'LIMITED':LimitedReviewViewError('limited'),
            'UNKNOWN':ReviewSurfaceError('review surface state UNKNOWN: synthetic'),
            'RUNTIME_ERROR':RuntimeError('synthetic'),'BROWSER_ERROR':BrowserSessionError('synthetic')}
        raise errors[state]
    adapter=CrawleeAdapter(Factory(),crawl,access_policy=policy,
        on_deferred=lambda *args:deferred.append(True),on_terminal_failure=lambda *args:terminal.append(True))
    asyncio.run(adapter.run([Store('p','p','p')]))
    assert len(calls)==expected_calls
    assert len(sessions)==expected_calls
    assert adapter.access_job_metrics[-1]['job_status']==status
    assert bool(deferred)==status.startswith('DEFERRED')
    assert bool(terminal)==status.startswith('FAILED')
    if deferred:
        asyncio.run(adapter.run([Store('p','p','p')]))
        assert len(calls)==expected_calls  # next run skips before acquiring browser
