import os
from pathlib import Path

import psycopg
import pytest
from psycopg.types.json import Jsonb

PROJECT_ROOT = Path(__file__).parent.parent


def assert_dq(
    conn,
    test_name: str,
    category: str,
    schema: str,
    table: str,
    query: str,
    expected_failing: int = 0,
):
    """Run a DQ query. `query` MUST return rows that VIOLATE the rule
    (count of 0 = pass). Every run is logged to dq.test_results with a
    JSON sample of the first 5 failing rows for triage. Raises
    AssertionError if the failing-row count does not match the
    expectation (default: zero failures).
    """
    with conn.cursor() as cur:
        cur.execute(query)
        failing = cur.fetchall()
        failed_count = len(failing)

        if failing:
            columns = [d.name for d in cur.description]
            sample = [dict(zip(columns, row)) for row in failing[:5]]
            for row in sample:
                for k, v in row.items():
                    if hasattr(v, "isoformat"):
                        row[k] = v.isoformat()
        else:
            sample = None

        cur.execute(
            """
            INSERT INTO dq.test_results
              (test_name, category, schema_name, table_name,
               passed, failed_row_count, failure_sample)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                test_name,
                category,
                schema,
                table,
                failed_count == expected_failing,
                failed_count,
                Jsonb(sample) if sample else None,
            ),
        )
    conn.commit()
    assert failed_count == expected_failing, (
        f"{test_name} expected {expected_failing} failing rows, "
        f"got {failed_count}. Sample: {sample}"
    )


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


@pytest.fixture
def populated_db(clean_db, sources_dir):
    """Runs the full bronze->silver->gold pipeline. Yields conn."""
    from membermatch.ingest import ingest_all
    from membermatch.transform import run_silver_transform
    from membermatch.golden import run_golden_materialization

    ingest_all(clean_db, sources_dir)
    run_silver_transform(clean_db)
    run_golden_materialization(clean_db)
    yield clean_db
