from datetime import datetime,timedelta,timezone
from dataclasses import replace
import pytest

from crawl_experiment.core.models import Review
from crawl_experiment.storage.review_repository import ReviewRepository,ReviewChange
from crawl_experiment.orchestration.review_incremental import IncrementalWindow,IncrementalConfig
from crawl_experiment.orchestration.review_reconciliation import ReconciliationWindow,ReconciliationConfig
from crawl_experiment.sources.google_maps.crawler import GoogleMapsCrawler
from crawl_experiment.core.statuses import CrawlStatus


@pytest.fixture
def repository(tmp_path):
    repo=ReviewRepository(tmp_path/'state.sqlite3',run_id='test')
    yield repo
    repo.close()


def review(i,text='hello'):
    return Review('google_maps',str(i),'p',rating=5,text=text)


def ingest(window,reviews):
    for value in reviews:
        window.observed(value,window.repository.upsert_review_state(value))
        if window.stopped: break


def test_all_new_and_idempotent_known_streak(repository):
    values=[review(i) for i in range(10)]
    first=IncrementalWindow(repository,'p',IncrementalConfig())
    ingest(first,values)
    assert first.metrics['new_reviews']==10 and not first.stopped
    first.finish('END_OF_LIST',2,1,True)
    second=IncrementalWindow(repository,'p',IncrementalConfig())
    ingest(second,values)
    assert second.metrics['new_reviews']==0
    assert second.metrics['known_unchanged_reviews']==5
    assert second.stopped and second.streak==5
    second.finish('KNOWN_STREAK_BOUNDARY',0,1,True)
    assert repository.get_incremental_state('p')['newest_review_id_seen']=='0'


def test_update_and_new_reset_streak(repository):
    values=[review(i) for i in range(6)]
    for r in values: repository.upsert_review_state(r)
    window=IncrementalWindow(repository,'p',IncrementalConfig(known_streak_threshold=3))
    ingest(window,[values[0],replace(values[1],text='edited'),values[2],review('new'),values[3]])
    assert window.metrics['updated_reviews']==1
    assert window.metrics['new_reviews']==1
    assert window.streak==1 and not window.stopped
    assert repository.upsert_review_state(replace(values[1],text='edited')) is ReviewChange.UNCHANGED


def test_exact_safety_boundary_and_imprecise_time(repository):
    boundary=datetime(2026,10,3,tzinfo=timezone.utc)
    current=[boundary+timedelta(days=1)]
    config=IncrementalConfig(known_streak_threshold=2,safety_boundary=boundary,review_time=lambda _:current[0])
    window=IncrementalWindow(repository,'p',config)
    for i in range(2): window.observed(review(i),ReviewChange.UNCHANGED)
    assert not window.stopped
    current[0]=boundary
    window.observed(review(2),ReviewChange.UNCHANGED)
    assert window.stopped
    other=IncrementalWindow(repository,'p',IncrementalConfig(known_streak_threshold=1,review_time=lambda _:'3 months ago'))
    other.observed(review(0),ReviewChange.UNCHANGED)
    assert other.stopped and other.newest_time is None


def test_partial_guard_does_not_advance_success_boundary(repository):
    window=IncrementalWindow(repository,'p',IncrementalConfig(max_scrolls=2))
    ingest(window,[review(0)])
    result=window.finish('MAX_SCROLLS',2,1,False)
    assert result['scroll_count']==2
    assert repository.get_incremental_state('p')['last_successful_incremental_at'] is None


def test_config_guards():
    with pytest.raises(ValueError): IncrementalConfig(known_streak_threshold=0)
    with pytest.raises(ValueError): IncrementalConfig(max_scrolls=0)
    with pytest.raises(ValueError): IncrementalConfig(safety_boundary=datetime(2026,1,1))

def test_reconciliation_missing_threshold_and_reactivation(repository):
    value=review('old'); repository.upsert_review_state(value)
    config=ReconciliationConfig(recent_review_limit=1,deletion_miss_threshold=2)
    for _ in range(2):
        window=ReconciliationWindow(repository,'p',config); window.finish('VERIFIED_END_OF_LIST',2,1)
    assert repository.get_review_state('google_maps','old')['is_active']==0
    window=ReconciliationWindow(repository,'p',config); window.observed('old','UNCHANGED'); window.finish('VERIFIED_END_OF_LIST',1,1)
    assert repository.get_review_state('google_maps','old')['is_active']==1

def test_reconciliation_new_update_unchanged_and_window_limit(repository):
    repository.upsert_review_state(review('known','a'))
    window=ReconciliationWindow(repository,'p',ReconciliationConfig(recent_review_limit=1))
    repository.upsert_review_state(review('known','changed'))
    window.observed('known','UPDATED'); window.observed('new','NEW'); result=window.finish('PARTIAL_LIMIT',1,1)
    assert result['updated_reviews']==1 and result['missed_new_reviews']==1
    assert result['parquet_rows_written']==1

def test_reconciliation_metrics_match_decisions(repository):
    repository.upsert_review_state(review("kept"))
    repository.upsert_review_state(review("missing"))
    config=ReconciliationConfig(recent_review_limit=2,deletion_miss_threshold=3)
    window=ReconciliationWindow(repository,"p",config)
    window.observed("kept","UNCHANGED")
    result=window.finish("VERIFIED_END_OF_LIST",1,1)
    assert result["expected_reviews_in_window"] == 2
    assert result["observed_reviews_in_window"] == 1
    assert result["missing_reviews"] == 1
    assert result["miss_count_incremented"] == 1
    assert {row["review_id"] for row in result["decisions"]} == {"kept", "missing"}
    assert {row["classification"] for row in result["decisions"]} == {"UNCHANGED", "MISSING"}

def test_reconciliation_new_review_has_zero_previous_miss(repository):
    window=ReconciliationWindow(repository,"p",ReconciliationConfig())
    window.expected={"brand-new"}
    window.observed("brand-new","NEW")
    decision=window.finish("END_OF_LIST",1,1)["decisions"][0]
    assert decision["previous_miss_count"] == 0
    assert decision["new_miss_count"] == 0

def test_reconciliation_window_is_successful_completion():
    assert GoogleMapsCrawler._completion_status("reconciliation_window", 0) is CrawlStatus.COMPLETE

def test_reconciliation_no_growth_does_not_infer_missing(repository):
    window=ReconciliationWindow(repository,"p",ReconciliationConfig(recent_review_limit=100))
    window.expected={str(i) for i in range(100)}
    for i in range(70):
        window.observed(str(i),"UNCHANGED")
    result=window.finish("INCOMPLETE_COVERAGE",2,1)
    assert result["coverage_status"] == "INCOMPLETE_COVERAGE"
    assert result["missing_reviews"] == 0
    assert result["miss_count_incremented"] == 0
    assert result["possibly_deleted_reviews"] == 0
    assert result["missing_evaluation_performed"] is False
    assert result["miss_count_suppressed"] == 30
