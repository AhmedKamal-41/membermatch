from psycopg.types.json import Jsonb

from membermatch.ingest import ingest_all
from membermatch.transform import run_silver_transform


def test_silver_row_count_after_transform(clean_db, sources_dir):
    ingest_all(clean_db, sources_dir)
    count = run_silver_transform(clean_db)
    assert 1100 <= count <= 1250, (
        f"Expected silver count in [1100, 1250], got {count}"
    )


def test_no_duplicate_source_identifiers_in_silver(clean_db, sources_dir):
    ingest_all(clean_db, sources_dir)
    run_silver_transform(clean_db)

    with clean_db.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*) FROM (
                SELECT source_system, source_member_id
                FROM silver.members_staged
                GROUP BY source_system, source_member_id
                HAVING COUNT(*) > 1
            ) t
            """
        )
        assert cur.fetchone()[0] == 0


def test_match_key_format_for_known_row(clean_db):
    payload = {
        "member_id": "TEST_MATCH_KEY_001",
        "first_name": "John",
        "last_name": "Smith",
        "date_of_birth": "1985-06-15",
        "ssn_last4": "1234",
        "zip": "10001",
        "plan_code": "PPO_GOLD",
    }
    with clean_db.cursor() as cur:
        cur.execute(
            """
            INSERT INTO bronze.members_raw
                (source_system, source_row_number, raw_payload)
            VALUES ('source_a', 9999, %s)
            """,
            (Jsonb(payload),),
        )
    clean_db.commit()

    run_silver_transform(clean_db)

    with clean_db.cursor() as cur:
        cur.execute(
            """
            SELECT match_key FROM silver.members_staged
            WHERE source_system = 'source_a'
              AND source_member_id = 'TEST_MATCH_KEY_001'
            """
        )
        row = cur.fetchone()

    assert row is not None, "Known record did not land in silver"
    assert row[0] == "smith|1985-06-15|1234"
