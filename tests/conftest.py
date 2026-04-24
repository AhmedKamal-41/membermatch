import os
from pathlib import Path

import psycopg
import pytest

PROJECT_ROOT = Path(__file__).parent.parent


@pytest.fixture(scope="session")
def db_url():
    return os.environ.get(
        "DATABASE_URL",
        "postgresql://membermatch:membermatch@localhost:5432/membermatch",
    )


@pytest.fixture
def conn(db_url):
    """Fresh connection per test."""
    with psycopg.connect(db_url) as c:
        yield c


@pytest.fixture
def clean_db(conn):
    """Truncates all data layers before test runs. Yields conn."""
    from membermatch.ingest import truncate_all_layers

    truncate_all_layers(conn)
    yield conn


@pytest.fixture
def sources_dir():
    return PROJECT_ROOT / "data" / "sources"
