#!/usr/bin/env python3
"""
Compatibility wrapper for the modular NetBox importer (Phase 4).
"""

from __future__ import annotations

# Re-export core/resources symbols for compatibility with prior imports.
from import_core import *
from import_resources import *
from import_cli import main


if __name__ == "__main__":
    raise SystemExit(main())
