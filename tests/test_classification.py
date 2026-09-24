import unittest
import sys
from pathlib import Path

# Add experiments to path to import benchmark module
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))
try:
    from experiments.google_reviews_multistore_benchmark import classify_store_status
except ImportError:
    classify_store_status = None


class ClassificationTests(unittest.TestCase):
    def test_classification_logic(self):
        if not classify_store_status:
            self.skipTest("classify_store_status not available")
        
        # ERROR
        self.assertEqual("ERROR", classify_store_status(
            upstream_success=False, unique_ids=set(),
            challenge=False, rate_limit=False, limited_view=False,
            timed_out=False, reviews_tab=False, error="some error"
        ))
        
        # CHALLENGE
        self.assertEqual("CHALLENGE", classify_store_status(
            upstream_success=False, unique_ids=set(),
            challenge=True, rate_limit=False, limited_view=False,
            timed_out=False, reviews_tab=False, error=None
        ))
        
        # RATE_LIMITED
        self.assertEqual("RATE_LIMITED", classify_store_status(
            upstream_success=False, unique_ids=set(),
            challenge=False, rate_limit=True, limited_view=False,
            timed_out=False, reviews_tab=False, error=None
        ))
        
        # LIMITED
        self.assertEqual("LIMITED", classify_store_status(
            upstream_success=False, unique_ids=set(),
            challenge=False, rate_limit=False, limited_view=True,
            timed_out=False, reviews_tab=False, error=None
        ))
        
        # NAVIGATION_FAILED
        self.assertEqual("NAVIGATION_FAILED", classify_store_status(
            upstream_success=False, unique_ids=set(),
            challenge=False, rate_limit=False, limited_view=False,
            timed_out=False, reviews_tab=False, error=None
        ))
        
        # PARTIAL_TIMEOUT
        self.assertEqual("PARTIAL_TIMEOUT", classify_store_status(
            upstream_success=True, unique_ids={"abc"},
            challenge=False, rate_limit=False, limited_view=False,
            timed_out=True, reviews_tab=True, error=None
        ))
        
        # COMPLETE
        self.assertEqual("COMPLETE", classify_store_status(
            upstream_success=True, unique_ids={"abc"},
            challenge=False, rate_limit=False, limited_view=False,
            timed_out=False, reviews_tab=True, error=None
        ))


if __name__ == "__main__":
    unittest.main()
