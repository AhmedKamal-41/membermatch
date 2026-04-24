"""Gold-layer materialization: execute the silver->gold SQL query.

The dedup + lineage logic lives in sql/queries/golden_record.sql. This
module is a thin Python wrapper that runs the file and reports gold +
lineage row counts.
"""

from __future__ import annotations

from pathlib import Path

import psycopg

PROJECT_ROOT = Path(__file__).parent.parent.parent
QUERY_PATH = PROJECT_ROOT / "sql" / "queries" / "golden_record.sql"


def run_golden_materialization(conn: psycopg.Connection) -> dict:
    """Executes silver->gold materialization. Returns stats dict:
    {'gold_row_count': N, 'lineage_row_count': M}."""
    sql = QUERY_PATH.read_text()
    with conn.cursor() as cur:
        cur.execute(sql)
        cur.execute("SELECT COUNT(*) FROM gold.members_golden")
        gold_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM gold.member_sources")
        lineage_count = cur.fetchone()[0]
    conn.commit()
    return {"gold_row_count": gold_count, "lineage_row_count": lineage_count}
