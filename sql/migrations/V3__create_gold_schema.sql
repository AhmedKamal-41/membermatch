-- Gold layer: one row per unique person across all sources.
-- match_key drives deduplication; golden_id is the stable business ID.
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.members_golden (
    golden_id BIGSERIAL PRIMARY KEY,
    match_key VARCHAR(200) NOT NULL UNIQUE,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    date_of_birth DATE NOT NULL,
    ssn_last4 VARCHAR(4),
    zip_code VARCHAR(10),
    plan_code VARCHAR(20),
    source_count INT NOT NULL CHECK (source_count >= 1),
    first_seen TIMESTAMP NOT NULL,
    last_seen TIMESTAMP NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Lineage: every golden record's contributing source rows.
-- Append-only; required for audit trails in real payer systems.
CREATE TABLE IF NOT EXISTS gold.member_sources (
    golden_id BIGINT NOT NULL REFERENCES gold.members_golden(golden_id),
    source_system VARCHAR(20) NOT NULL,
    source_member_id VARCHAR(50) NOT NULL,
    linked_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (golden_id, source_system, source_member_id)
);
