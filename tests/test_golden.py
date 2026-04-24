from psycopg.types.json import Jsonb

from membermatch.golden import run_golden_materialization
from membermatch.transform import run_silver_transform


def test_gold_row_count_matches_distinct_match_keys(populated_db):
    """After full pipeline run, gold row count equals number of distinct
    match keys in silver (832 from Checkpoint 2 baseline)."""
    with populated_db.cursor() as cur:
        cur.execute(
            """
            SELECT
              (SELECT COUNT(DISTINCT match_key) FROM silver.members_staged),
              (SELECT COUNT(*) FROM gold.members_golden)
            """
        )
        distinct_keys, gold_count = cur.fetchone()
    assert gold_count == distinct_keys, (
        f"Gold row count {gold_count} should equal distinct silver match "
        f"keys {distinct_keys} -- each unique person should produce exactly "
        f"one golden record."
    )
    # Deterministic sanity check against Checkpoint 2 baseline:
    assert 820 <= gold_count <= 840, (
        f"Expected ~832 golden records based on Checkpoint 2 baseline, "
        f"got {gold_count}. If synthetic data generator changed, update "
        f"this assertion."
    )


def test_known_duplicate_collapses_with_priority_winner(clean_db):
    """Insert 3 bronze rows for the same person across all 3 sources with
    different first_name values. After the full pipeline, exactly one gold
    row exists and source_a's first_name wins (highest priority)."""
    with clean_db.cursor() as cur:
        cur.execute(
            """
            INSERT INTO bronze.members_raw
              (source_system, source_row_number, raw_payload)
            VALUES
              ('source_a', 1, %s),
              ('source_b', 1, %s),
              ('source_c', 1, %s)
            """,
            (
                Jsonb({
                    "member_id": "MEMBER_A_TEST",
                    "first_name": "Alice",
                    "last_name": "TestDuplicate",
                    "date_of_birth": "1990-01-01",
                    "ssn_last4": "9999",
                    "zip": "10001",
                    "plan_code": "PPO_GOLD",
                }),
                Jsonb({
                    "id": "MEMBER_B_TEST",
                    "firstName": "Al",
                    "lastName": "TestDuplicate",
                    "dob": "1990-01-01",
                    "ssnLast4": "9999",
                    "zipCode": "10001",
                    "plan": "GOLD",
                }),
                Jsonb({
                    "memberIdentifier": "MEMBER_C_TEST",
                    "givenName": "A",
                    "familyName": "TestDuplicate",
                    "birthDate": "1990-01-01",
                    "taxIdLast4": "9999",
                    "postal_code": "10001",
                    "planName": "Gold PPO",
                }),
            ),
        )
    clean_db.commit()

    run_silver_transform(clean_db)
    run_golden_materialization(clean_db)

    with clean_db.cursor() as cur:
        cur.execute(
            """
            SELECT golden_id, first_name, last_name, source_count
            FROM gold.members_golden
            WHERE match_key = 'testduplicate|1990-01-01|9999'
            """
        )
        rows = cur.fetchall()

    assert len(rows) == 1, f"Expected 1 gold row, got {len(rows)}"
    golden_id, first_name, last_name, source_count = rows[0]
    assert first_name == "Alice", (
        f"Expected source_a's 'Alice' to win priority; got '{first_name}'"
    )
    assert last_name == "Testduplicate", (
        f"Expected INITCAP('TestDuplicate') = 'Testduplicate'; got '{last_name}'"
    )
    assert source_count == 3, f"Expected source_count=3, got {source_count}"


def test_lineage_links_back_to_all_contributing_sources(clean_db):
    """Same setup as above: 3 bronze rows -> 1 gold row -> 3 lineage rows."""
    with clean_db.cursor() as cur:
        cur.execute(
            """
            INSERT INTO bronze.members_raw
              (source_system, source_row_number, raw_payload)
            VALUES
              ('source_a', 1, %s),
              ('source_b', 1, %s),
              ('source_c', 1, %s)
            """,
            (
                Jsonb({
                    "member_id": "MEMBER_A_TEST",
                    "first_name": "Alice",
                    "last_name": "LineageTest",
                    "date_of_birth": "1990-01-01",
                    "ssn_last4": "9999",
                    "zip": "10001",
                    "plan_code": "PPO_GOLD",
                }),
                Jsonb({
                    "id": "MEMBER_B_TEST",
                    "firstName": "Alicia",
                    "lastName": "LineageTest",
                    "dob": "1990-01-01",
                    "ssnLast4": "9999",
                    "zipCode": "10001",
                    "plan": "GOLD",
                }),
                Jsonb({
                    "memberIdentifier": "MEMBER_C_TEST",
                    "givenName": "ALICE",
                    "familyName": "LineageTest",
                    "birthDate": "1990-01-01",
                    "taxIdLast4": "9999",
                    "postal_code": "10001",
                    "planName": "Gold PPO",
                }),
            ),
        )
    clean_db.commit()

    run_silver_transform(clean_db)
    run_golden_materialization(clean_db)

    with clean_db.cursor() as cur:
        cur.execute(
            """
            SELECT ms.source_system
            FROM gold.member_sources ms
            JOIN gold.members_golden g ON g.golden_id = ms.golden_id
            WHERE g.match_key = 'lineagetest|1990-01-01|9999'
            ORDER BY ms.source_system
            """
        )
        systems = [row[0] for row in cur.fetchall()]

    assert systems == ["source_a", "source_b", "source_c"], (
        f"Expected lineage rows for all 3 sources, got {systems}"
    )
