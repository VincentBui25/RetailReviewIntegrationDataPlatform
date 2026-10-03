from dataclasses import dataclass

@dataclass(frozen=True)
class ReconciliationConfig:
    recent_review_limit: int = 100
    max_scrolls: int = 20
    deletion_miss_threshold: int = 3
    def __post_init__(self):
        if min(self.recent_review_limit,self.max_scrolls,self.deletion_miss_threshold)<=0:
            raise ValueError("Reconciliation limits must be positive")

class ReconciliationWindow:
    def __init__(self, repository, place_id, config):
        self.repository=repository; self.place_id=place_id; self.config=config
        self._initial_new = repository.new_reviews_written
        self._initial_changed = repository.changed_reviews_written
        self.expected={r["source_review_id"] for r in repository.reconciliation_candidates(place_id,config.recent_review_limit)}
        self.seen=set(); self.decisions=[]
        self.metrics=dict(reconciliation_reviews_examined=0,reconciliation_window_size=config.recent_review_limit,
            expected_reviews_in_window=len(self.expected),observed_reviews_in_window=0,missed_new_reviews=0,
            updated_reviews=0,verified_unchanged_reviews=0,missing_reviews=0,possibly_deleted_reviews=0,
            reactivated_reviews=0,miss_count_incremented=0,parquet_rows_written=0,durable_review_count_total=0,
            stop_reason=None,coverage_status="INCOMPLETE_COVERAGE",
            missing_evaluation_performed=False,unobserved_expected_reviews=0,miss_count_suppressed=0,
            content_hash_change_diagnostics=[])
    def observed(self, review_id, classification):
        previous=self.repository.get_review_state("google_maps",review_id) or {}
        previous_miss=int(previous.get("miss_count",0)); previous_active=bool(previous.get("is_active",1))
        prior=self.repository.consume_observed_previous(review_id)
        if prior is not None:
            previous_miss,previous_active=prior
        if classification == "NEW":
            previous_miss, previous_active = 0, True
        self.seen.add(review_id); self.metrics["reconciliation_reviews_examined"]+=1
        reactivated=not previous_active or self.repository.consume_reactivation(review_id)
        if reactivated:
            previous_active=False
            previous_miss=self.repository.reactivation_previous_miss_count(review_id)
        if reactivated and classification == "UNCHANGED":
            classification="REACTIVATED"; self.metrics["reactivated_reviews"]+=1
        self.repository.mark_reconciliation_seen("google_maps",review_id)
        key={"NEW":"missed_new_reviews","UPDATED":"updated_reviews","UNCHANGED":"verified_unchanged_reviews"}.get(classification)
        if key:self.metrics[key]+=1
        if classification == "UPDATED":
            self.metrics["content_hash_change_diagnostics"].append(
                {"review_id": review_id, "text_changed": None,
                 "rating_changed": None, "owner_response_changed": None,
                 "diagnostic_status": "PAYLOAD_NOT_RETAINED"})
        if review_id in self.expected:
            self.decisions.append(dict(review_id=review_id,expected_in_window=True,observed=True,
                classification=classification,previous_miss_count=previous_miss,new_miss_count=0,
                previous_is_active=previous_active,new_is_active=True))
        self.metrics["observed_reviews_in_window"]=len(self.seen & self.expected)
    def finish(self, stop_reason, scroll_count, elapsed):
        unobserved = self.expected - self.seen
        self.metrics["unobserved_expected_reviews"] = len(unobserved)
        complete_window = len(self.seen & self.expected) >= self.config.recent_review_limit
        verified_end = stop_reason == "VERIFIED_END_OF_LIST"
        self.metrics["coverage_status"] = (
            "COMPLETE_WINDOW" if complete_window else
            "VERIFIED_END_OF_LIST" if verified_end else "INCOMPLETE_COVERAGE"
        )
        evaluate_missing = complete_window or verified_end
        self.metrics["missing_evaluation_performed"] = evaluate_missing
        if not evaluate_missing:
            self.metrics["miss_count_suppressed"] = len(unobserved)
        for review_id in unobserved if evaluate_missing else ():
            self.metrics["missing_reviews"]+=1
            previous=self.repository.get_review_state("google_maps",review_id) or {}
            result=self.repository.mark_reconciliation_missing("google_maps",review_id,self.config.deletion_miss_threshold)
            if result:
                new_count,active=result
                self.metrics["miss_count_incremented"]+=1
                if not active:self.metrics["possibly_deleted_reviews"]+=1
                self.decisions.append(dict(review_id=review_id,expected_in_window=True,observed=False,
                    classification="POSSIBLY_DELETED" if not active else "MISSING",
                    previous_miss_count=int(previous.get("miss_count",0)),new_miss_count=new_count,
                    previous_is_active=bool(previous.get("is_active",1)),new_is_active=bool(active)))
        self.repository.set_reconciliation_state(self.place_id,self.repository.run_id,self.metrics["reconciliation_reviews_examined"])
        self.metrics["durable_review_count_total"]=self.repository.durable_review_count()
        self.metrics["parquet_rows_written"]=(self.repository.new_reviews_written-self._initial_new)+(
            self.repository.changed_reviews_written-self._initial_changed)
        self.metrics.update(stop_reason=stop_reason,scroll_count=scroll_count,elapsed_seconds=elapsed)
        return {**self.metrics,"decisions":list(self.decisions)}
