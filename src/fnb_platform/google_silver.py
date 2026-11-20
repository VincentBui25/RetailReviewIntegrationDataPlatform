"""Compatibility alias for fnb_platform.google.places_silver."""
import sys
from .google import places_silver as _implementation

if __name__ == "__main__":
    _implementation.main()
else:
    sys.modules[__name__] = _implementation
