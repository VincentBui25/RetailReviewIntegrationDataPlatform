"""Compatibility entrypoint; canonical implementation is fnb_platform.overture.snapshot."""
import sys
from .overture import snapshot as _implementation

sys.modules[__name__] = _implementation
