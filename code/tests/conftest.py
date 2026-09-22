"""Pytest configuration: add the code/ directory to sys.path so migrate/ is importable."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
