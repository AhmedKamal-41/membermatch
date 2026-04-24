from membermatch.ingest import (
    ingest_all,
    ingest_source_a_csv,
)


def test_ingest_all_loads_expected_row_counts(clean_db, sources_dir):
    counts = ingest_all(clean_db, sources_dir)
    assert counts == {"source_a": 500, "source_b": 400, "source_c": 350}

    with clean_db.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM bronze.members_raw")
        assert cur.fetchone()[0] == 1250


def test_raw_payload_preserves_source_columns(clean_db, sources_dir):
    ingest_all(clean_db, sources_dir)

    expected_keys = {
        "source_a": {
            "member_id",
            "first_name",
            "last_name",
            "date_of_birth",
            "ssn_last4",
            "zip",
            "plan_code",
        },
        "source_b": {
            "id",
            "firstName",
            "lastName",
            "dob",
            "ssnLast4",
            "zipCode",
            "plan",
        },
        "source_c": {
            "memberIdentifier",
            "givenName",
            "familyName",
            "birthDate",
            "taxIdLast4",
            "postal_code",
            "planName",
        },
    }

    with clean_db.cursor() as cur:
        for source_system, keys in expected_keys.items():
            cur.execute(
                "SELECT raw_payload FROM bronze.members_raw "
                "WHERE source_system = %s LIMIT 1",
                (source_system,),
            )
            payload = cur.fetchone()[0]
            assert set(payload.keys()) == keys, (
                f"{source_system}: got {set(payload.keys())}, expected {keys}"
            )


def test_ingest_is_idempotent_via_unique_constraint(clean_db, sources_dir):
    path = sources_dir / "source_a_members.csv"
    first = ingest_source_a_csv(clean_db, path)
    second = ingest_source_a_csv(clean_db, path)

    assert first == 500
    assert second == 500

    with clean_db.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM bronze.members_raw WHERE source_system = %s",
            ("source_a",),
        )
        assert cur.fetchone()[0] == 500
