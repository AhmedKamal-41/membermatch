from tests.conftest import assert_dq


def test_silver_ssn_format(populated_db):
    assert_dq(
        populated_db,
        "silver_ssn_format",
        "format",
        "silver",
        "members_staged",
        r"""
        SELECT id FROM silver.members_staged
        WHERE ssn_last4 IS NOT NULL AND ssn_last4 !~ '^\d{4}$'
        """,
    )


def test_silver_zip_format(populated_db):
    assert_dq(
        populated_db,
        "silver_zip_format",
        "format",
        "silver",
        "members_staged",
        r"""
        SELECT id FROM silver.members_staged
        WHERE zip_code IS NOT NULL AND zip_code !~ '^\d{5}$'
        """,
    )


def test_silver_first_name_no_whitespace(populated_db):
    assert_dq(
        populated_db,
        "silver_first_name_no_whitespace",
        "format",
        "silver",
        "members_staged",
        """
        SELECT id FROM silver.members_staged
        WHERE first_name <> TRIM(first_name)
        """,
    )


def test_silver_last_name_no_whitespace(populated_db):
    assert_dq(
        populated_db,
        "silver_last_name_no_whitespace",
        "format",
        "silver",
        "members_staged",
        """
        SELECT id FROM silver.members_staged
        WHERE last_name <> TRIM(last_name)
        """,
    )


def test_silver_last_name_title_cased(populated_db):
    assert_dq(
        populated_db,
        "silver_last_name_title_cased",
        "format",
        "silver",
        "members_staged",
        """
        SELECT id FROM silver.members_staged
        WHERE last_name = UPPER(last_name) OR last_name = LOWER(last_name)
        """,
    )


def test_gold_ssn_format(populated_db):
    assert_dq(
        populated_db,
        "gold_ssn_format",
        "format",
        "gold",
        "members_golden",
        r"""
        SELECT golden_id FROM gold.members_golden
        WHERE ssn_last4 IS NOT NULL AND ssn_last4 !~ '^\d{4}$'
        """,
    )


def test_gold_zip_format(populated_db):
    assert_dq(
        populated_db,
        "gold_zip_format",
        "format",
        "gold",
        "members_golden",
        r"""
        SELECT golden_id FROM gold.members_golden
        WHERE zip_code IS NOT NULL AND zip_code !~ '^\d{5}$'
        """,
    )
