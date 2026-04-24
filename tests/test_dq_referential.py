from tests.conftest import assert_dq


def test_lineage_fk_valid(populated_db):
    assert_dq(
        populated_db,
        "lineage_fk_valid",
        "referential",
        "gold",
        "member_sources",
        """
        SELECT ms.golden_id FROM gold.member_sources ms
        WHERE NOT EXISTS (
          SELECT 1 FROM gold.members_golden g WHERE g.golden_id = ms.golden_id
        )
        """,
    )


def test_every_silver_has_matching_gold(populated_db):
    assert_dq(
        populated_db,
        "every_silver_has_matching_gold",
        "referential",
        "silver",
        "members_staged",
        """
        SELECT s.id FROM silver.members_staged s
        LEFT JOIN gold.members_golden g ON g.match_key = s.match_key
        WHERE g.match_key IS NULL
        """,
    )


def test_plan_codes_in_known_set(populated_db):
    assert_dq(
        populated_db,
        "plan_codes_in_known_set",
        "referential",
        "silver",
        "members_staged",
        """
        SELECT DISTINCT plan_code FROM silver.members_staged
        WHERE plan_code NOT IN
          ('PPO_GOLD', 'HMO_SILVER', 'EPO_BRONZE', 'MEDICARE_A')
        """,
    )


def test_date_of_birth_reasonable(populated_db):
    assert_dq(
        populated_db,
        "date_of_birth_reasonable",
        "referential",
        "silver",
        "members_staged",
        """
        SELECT id FROM silver.members_staged
        WHERE date_of_birth < '1900-01-01' OR date_of_birth > CURRENT_DATE
        """,
    )
