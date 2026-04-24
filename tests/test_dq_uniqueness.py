from tests.conftest import assert_dq


def test_bronze_no_duplicate_source_rows(populated_db):
    assert_dq(
        populated_db,
        "bronze_no_duplicate_source_rows",
        "uniqueness",
        "bronze",
        "members_raw",
        """
        SELECT source_system, source_row_number, COUNT(*)
        FROM bronze.members_raw
        GROUP BY source_system, source_row_number
        HAVING COUNT(*) > 1
        """,
    )


def test_silver_unique_source_identifiers(populated_db):
    assert_dq(
        populated_db,
        "silver_unique_source_identifiers",
        "uniqueness",
        "silver",
        "members_staged",
        """
        SELECT source_system, source_member_id, COUNT(*)
        FROM silver.members_staged
        GROUP BY source_system, source_member_id
        HAVING COUNT(*) > 1
        """,
    )


def test_gold_match_key_unique(populated_db):
    assert_dq(
        populated_db,
        "gold_match_key_unique",
        "uniqueness",
        "gold",
        "members_golden",
        """
        SELECT match_key, COUNT(*) FROM gold.members_golden
        GROUP BY match_key HAVING COUNT(*) > 1
        """,
    )


def test_gold_golden_id_unique(populated_db):
    assert_dq(
        populated_db,
        "gold_golden_id_unique",
        "uniqueness",
        "gold",
        "members_golden",
        """
        SELECT golden_id, COUNT(*) FROM gold.members_golden
        GROUP BY golden_id HAVING COUNT(*) > 1
        """,
    )


def test_gold_source_count_positive(populated_db):
    assert_dq(
        populated_db,
        "gold_source_count_positive",
        "uniqueness",
        "gold",
        "members_golden",
        """
        SELECT golden_id, source_count FROM gold.members_golden
        WHERE source_count < 1
        """,
    )


def test_lineage_no_orphan_golden_ids(populated_db):
    assert_dq(
        populated_db,
        "lineage_no_orphan_golden_ids",
        "uniqueness",
        "gold",
        "member_sources",
        """
        SELECT ms.golden_id FROM gold.member_sources ms
        LEFT JOIN gold.members_golden g ON g.golden_id = ms.golden_id
        WHERE g.golden_id IS NULL
        """,
    )


def test_lineage_no_duplicate_links(populated_db):
    assert_dq(
        populated_db,
        "lineage_no_duplicate_links",
        "uniqueness",
        "gold",
        "member_sources",
        """
        SELECT golden_id, source_system, source_member_id, COUNT(*)
        FROM gold.member_sources
        GROUP BY golden_id, source_system, source_member_id
        HAVING COUNT(*) > 1
        """,
    )
