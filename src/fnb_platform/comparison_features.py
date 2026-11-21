"""Compatibility alias for fnb_platform.integration.comparison_features."""
import sys
from .integration import comparison_features as _implementation

if __name__ == "__main__":
    _implementation.main()
else:
    sys.modules[__name__] = _implementation
