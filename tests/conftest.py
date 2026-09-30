import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.spark_session import get_spark  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    session = get_spark("window-functions-tests", shuffle_partitions=2)
    yield session
    session.stop()
