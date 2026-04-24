-- Performance-critical indexes listed separately so they can be
-- dropped/rebuilt independently of schema changes.

-- Bronze: filtering by source_system during parsing
CREATE INDEX IF NOT EXISTS idx_bronze_source_system
    ON bronze.members_raw(source_system);

-- Silver: window-function dedup partitions by match_key
CREATE INDEX IF NOT EXISTS idx_silver_match_key
    ON silver.members_staged(match_key);

-- Silver: date-of-birth lookups for DQ checks
CREATE INDEX IF NOT EXISTS idx_silver_dob
    ON silver.members_staged(date_of_birth);

-- Gold: date-of-birth lookups
CREATE INDEX IF NOT EXISTS idx_gold_dob
    ON gold.members_golden(date_of_birth);

-- Gold: lineage queries (golden_id -> source rows)
CREATE INDEX IF NOT EXISTS idx_gold_sources_golden_id
    ON gold.member_sources(golden_id);
