"""Silver-layer transform: execute the bronze->silver SQL query.

The actual transformation logic lives in sql/queries/dedup_members.sql so
that it reads as a single SQL story (the interview talking point). This
module is a thin Python wrapper that runs the file and reports the
resulting silver row count.
"""

from __future__ import annotations

from pathlib import Path

import psycopg

PROJECT_ROOT = Path(__file__).parent.parent.parent
QUERY_PATH = PROJECT_ROOT / "sql" / "queries" / "dedup_members.sql"


def run_silver_transform(conn: psycopg.Connection) -> int:
    """Executes bronze->silver transform. Returns silver row count."""
    sql = QUERY_PATH.read_text()
    with conn.cursor() as cur:
        cur.execute(sql)
        cur.execute("SELECT COUNT(*) FROM silver.members_staged")
        count = cur.fetchone()[0]
    conn.commit()
    return count
