from tests.conftest import assert_dq


def test_bronze_raw_payload_not_null(populated_db):
    assert_dq(
        populated_db,
        "bronze_raw_payload_not_null",
        "completeness",
        "bronze",
        "members_raw",
        "SELECT id FROM bronze.members_raw WHERE raw_payload IS NULL",
    )


def test_silver_first_name_populated(populated_db):
    assert_dq(
        populated_db,
        "silver_first_name_populated",
        "completeness",
        "silver",
        "members_staged",
        """
        SELECT id FROM silver.members_staged
        WHERE first_name IS NULL OR first_name = ''
        """,
    )


def test_silver_last_name_populated(populated_db):
    assert_dq(
        populated_db,
        "silver_last_name_populated",
        "completeness",
        "silver",
        "members_staged",
        """
        SELECT id FROM silver.members_staged
        WHERE last_name IS NULL OR last_name = ''
        """,
    )


def test_silver_dob_not_null(populated_db):
    assert_dq(
        populated_db,
        "silver_dob_not_null",
        "completeness",
        "silver",
        "members_staged",
        "SELECT id FROM silver.members_staged WHERE date_of_birth IS NULL",
    )


def test_silver_match_key_not_null(populated_db):
    assert_dq(
        populated_db,
        "silver_match_key_not_null",
        "completeness",
        "silver",
        "members_staged",
        """
        SELECT id FROM silver.members_staged
        WHERE match_key IS NULL OR match_key = ''
        """,
    )


def test_gold_first_name_populated(populated_db):
    assert_dq(
        populated_db,
        "gold_first_name_populated",
        "completeness",
        "gold",
        "members_golden",
        """
        SELECT golden_id FROM gold.members_golden
        WHERE first_name IS NULL OR first_name = ''
        """,
    )


def test_gold_last_name_populated(populated_db):
    assert_dq(
        populated_db,
        "gold_last_name_populated",
        "completeness",
        "gold",
        "members_golden",
        """
        SELECT golden_id FROM gold.members_golden
        WHERE last_name IS NULL OR last_name = ''
        """,
    )


def test_gold_dob_not_null(populated_db):
    assert_dq(
        populated_db,
        "gold_dob_not_null",
        "completeness",
        "gold",
        "members_golden",
        "SELECT golden_id FROM gold.members_golden WHERE date_of_birth IS NULL",
    )


def test_every_gold_has_lineage(populated_db):
    assert_dq(
        populated_db,
        "every_gold_has_lineage",
        "completeness",
        "gold",
        "members_golden",
        """
        SELECT g.golden_id FROM gold.members_golden g
        LEFT JOIN gold.member_sources ms ON ms.golden_id = g.golden_id
        WHERE ms.golden_id IS NULL
        """,
    )
