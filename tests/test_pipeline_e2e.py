from membermatch.pipeline import run_pipeline


def test_full_pipeline_completes_with_expected_counts(clean_db, sources_dir):
    stats = run_pipeline(clean_db, sources_dir)

    assert stats["bronze"]["source_a"] == 500
    assert stats["bronze"]["source_b"] == 400
    assert stats["bronze"]["source_c"] == 350
    assert stats["silver"]["row_count"] == 1218
    assert stats["gold"]["gold_row_count"] == 832
    assert stats["gold"]["lineage_row_count"] == 1218


def test_pipeline_is_idempotent(clean_db, sources_dir):
    """Running the pipeline twice on a clean DB produces identical counts."""
    stats_1 = run_pipeline(clean_db, sources_dir)
    stats_2 = run_pipeline(clean_db, sources_dir)
    assert stats_1["silver"]["row_count"] == stats_2["silver"]["row_count"]
    assert stats_1["gold"]["gold_row_count"] == stats_2["gold"]["gold_row_count"]
    assert (
        stats_1["gold"]["lineage_row_count"]
        == stats_2["gold"]["lineage_row_count"]
    )


def test_dq_test_results_populated_after_full_run(populated_db):
    """After running the populated_db fixture, DQ log table is populated.
    At least 27 DQ tests have run (one row per test execution)."""
    with populated_db.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM dq.test_results")
        count = cur.fetchone()[0]
    assert count >= 27, (
        f"Expected at least 27 DQ test results logged, got {count}. "
        f"DQ fixture may not be wired correctly."
    )


def test_reduction_ratio_in_expected_range(populated_db):
    """Bronze-to-gold reduction ratio is approximately 1.5x based on
    synthetic data overlap distribution."""
    with populated_db.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM bronze.members_raw")
        bronze = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM gold.members_golden")
        gold = cur.fetchone()[0]
    ratio = bronze / gold
    assert 1.4 <= ratio <= 1.6, (
        f"Expected reduction ratio 1.4-1.6x, got {ratio:.2f}x. "
        f"If synthetic data overlap changed, update assertion."
    )
