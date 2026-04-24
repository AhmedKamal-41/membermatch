-- Bronze layer: schema-on-read. Raw payloads from every source system
-- land here as JSONB. No schema enforcement beyond source tracking.
CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.members_raw (
    id BIGSERIAL PRIMARY KEY,
    source_system VARCHAR(20) NOT NULL,
    source_row_number INT NOT NULL,
    raw_payload JSONB NOT NULL,
    ingested_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (source_system, source_row_number)
);

COMMENT ON TABLE bronze.members_raw IS
    'Schema-on-read landing zone. Every source row preserved as JSONB.';
