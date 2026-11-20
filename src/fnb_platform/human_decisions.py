"""Compatibility alias for fnb_platform.integration.human_decisions."""
import sys
from .integration import human_decisions as _implementation

if __name__ == "__main__":
    _implementation.main()
else:
    sys.modules[__name__] = _implementation
