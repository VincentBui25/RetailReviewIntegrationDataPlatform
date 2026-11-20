"""Compatibility alias for fnb_platform.integration.er_auto_accept."""
import sys
from .integration import er_auto_accept as _implementation

if __name__ == "__main__":
    _implementation.main()
else:
    sys.modules[__name__] = _implementation
