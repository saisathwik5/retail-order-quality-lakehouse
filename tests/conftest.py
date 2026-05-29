from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from order_quality.spark import create_spark


@pytest.fixture(scope="session")
def spark():
    session = create_spark("retail-order-quality-tests")
    yield session
    session.stop()

