-- Silver layer: typed, cleansed, unified schema. One row per source row
-- (not yet deduplicated across sources — that happens in gold).
CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.members_staged (
    id BIGSERIAL PRIMARY KEY,
    source_system VARCHAR(20) NOT NULL,
    source_member_id VARCHAR(50) NOT NULL,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    date_of_birth DATE NOT NULL,
    ssn_last4 VARCHAR(4),
    zip_code VARCHAR(10),
    plan_code VARCHAR(20),
    match_key VARCHAR(200) NOT NULL,
    staged_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (source_system, source_member_id)
);

COMMENT ON COLUMN silver.members_staged.match_key IS
    'Composite deduplication key: lower(last_name) || dob || ssn_last4';
