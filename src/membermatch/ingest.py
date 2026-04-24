"""Bronze-layer ingestion: load raw source rows into bronze.members_raw as JSONB.

Every ingest_source_* function commits its own transaction, so a failure in
one source does not corrupt another source's already-landed rows. The
ON CONFLICT clause on (source_system, source_row_number) makes reruns safe.
"""

from __future__ import annotations

import csv
import json
import os
from pathlib import Path

import psycopg
from psycopg.types.json import Jsonb


def get_connection() -> psycopg.Connection:
    """Return a psycopg connection read from the DATABASE_URL env var."""
    url = os.environ.get(
        "DATABASE_URL",
        "postgresql://membermatch:membermatch@localhost:5432/membermatch",
    )
    return psycopg.connect(url)


def _insert_bronze_row(cur, source_system: str, row_num: int, payload: dict) -> None:
    cur.execute(
        """
        INSERT INTO bronze.members_raw (source_system, source_row_number, raw_payload)
        VALUES (%s, %s, %s)
        ON CONFLICT (source_system, source_row_number) DO NOTHING
        """,
        (source_system, row_num, Jsonb(payload)),
    )


def ingest_source_a_csv(conn: psycopg.Connection, csv_path: Path) -> int:
    count = 0
    with csv_path.open(newline="") as f, conn.cursor() as cur:
        reader = csv.DictReader(f)
        for row_num, row in enumerate(reader, start=1):
            _insert_bronze_row(cur, "source_a", row_num, row)
            count += 1
    conn.commit()
    return count


def ingest_source_b_json(conn: psycopg.Connection, json_path: Path) -> int:
    with json_path.open() as f:
        records = json.load(f)
    count = 0
    with conn.cursor() as cur:
        for row_num, row in enumerate(records, start=1):
            _insert_bronze_row(cur, "source_b", row_num, row)
            count += 1
    conn.commit()
    return count


def ingest_source_c_csv(conn: psycopg.Connection, csv_path: Path) -> int:
    count = 0
    with csv_path.open(newline="") as f, conn.cursor() as cur:
        reader = csv.DictReader(f)
        for row_num, row in enumerate(reader, start=1):
            _insert_bronze_row(cur, "source_c", row_num, row)
            count += 1
    conn.commit()
    return count


def truncate_all_layers(conn: psycopg.Connection) -> None:
    """TRUNCATE bronze/silver/gold tables in dependency order for clean reruns."""
    with conn.cursor() as cur:
        cur.execute(
            """
            TRUNCATE TABLE
                gold.member_sources,
                gold.members_golden,
                silver.members_staged,
                bronze.members_raw
            RESTART IDENTITY CASCADE
            """
        )
    conn.commit()


def ingest_all(conn: psycopg.Connection, sources_dir: Path) -> dict:
    return {
        "source_a": ingest_source_a_csv(conn, sources_dir / "source_a_members.csv"),
        "source_b": ingest_source_b_json(conn, sources_dir / "source_b_members.json"),
        "source_c": ingest_source_c_csv(conn, sources_dir / "source_c_members.csv"),
    }
