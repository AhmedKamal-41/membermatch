-- Data quality test results table.
-- Every test execution writes here with pass/fail + sample failures.
CREATE SCHEMA IF NOT EXISTS dq;

CREATE TABLE IF NOT EXISTS dq.test_results (
    id BIGSERIAL PRIMARY KEY,
    test_name VARCHAR(100) NOT NULL,
    category VARCHAR(30) NOT NULL CHECK (
        category IN ('uniqueness', 'completeness', 'referential', 'format', 'pipeline')
    ),
    schema_name VARCHAR(20) NOT NULL,
    table_name VARCHAR(50) NOT NULL,
    passed BOOLEAN NOT NULL,
    failed_row_count INT NOT NULL DEFAULT 0,
    failure_sample JSONB,
    ran_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dq_test_results_ran_at
    ON dq.test_results(ran_at DESC);
