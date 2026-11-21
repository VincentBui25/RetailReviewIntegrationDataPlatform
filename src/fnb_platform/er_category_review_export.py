"""Compatibility alias for fnb_platform.integration.er_category_review_export."""
import sys
from .integration import er_category_review_export as _implementation

if __name__ == "__main__":
    _implementation.main()
else:
    sys.modules[__name__] = _implementation
