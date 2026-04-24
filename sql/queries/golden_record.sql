-- ====================================================================
-- Silver -> Gold: golden record materialization
-- --------------------------------------------------------------------
-- For each match_key, exactly one silver row "wins" and its values
-- become the golden record. Winner selection uses ROW_NUMBER() with a
-- deterministic priority:
--   1. source_a (legacy system -- canonical)
--   2. source_c (API export -- structured)
--   3. source_b (partner feed -- least trusted)
-- Ties within a source broken by most recent staged_at.
--
-- source_count records how many distinct source systems recognized
-- this person. High counts = high confidence; singletons = unique
-- to one source.
--
-- Lineage is populated from scratch each run (TRUNCATE) because the
-- gold-layer PK is synthetic (golden_id), so rerunning may reassign
-- IDs and make the old lineage rows point at nothing.
-- ====================================================================

WITH ranked_candidates AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY match_key
            ORDER BY
                CASE source_system
                    WHEN 'source_a' THEN 1
                    WHEN 'source_c' THEN 2
                    WHEN 'source_b' THEN 3
                    ELSE 4
                END,
                staged_at DESC
        ) AS rn,
        COUNT(*) OVER (PARTITION BY match_key) AS sources_for_this_key
    FROM silver.members_staged
),

winners AS (
    SELECT * FROM ranked_candidates WHERE rn = 1
),

aggregated_timestamps AS (
    SELECT
        match_key,
        MIN(staged_at) AS first_seen,
        MAX(staged_at) AS last_seen
    FROM silver.members_staged
    GROUP BY match_key
)

INSERT INTO gold.members_golden (
    match_key, first_name, last_name, date_of_birth, ssn_last4,
    zip_code, plan_code, source_count, first_seen, last_seen
)
SELECT
    w.match_key,
    w.first_name,
    w.last_name,
    w.date_of_birth,
    w.ssn_last4,
    w.zip_code,
    w.plan_code,
    w.sources_for_this_key,
    a.first_seen,
    a.last_seen
FROM winners w
JOIN aggregated_timestamps a ON a.match_key = w.match_key
ON CONFLICT (match_key) DO UPDATE SET
    first_name = EXCLUDED.first_name,
    last_name = EXCLUDED.last_name,
    source_count = EXCLUDED.source_count,
    last_seen = EXCLUDED.last_seen;

-- Populate lineage. TRUNCATE first because golden_ids may shift on re-run.
TRUNCATE gold.member_sources;

INSERT INTO gold.member_sources (golden_id, source_system, source_member_id)
SELECT DISTINCT g.golden_id, s.source_system, s.source_member_id
FROM silver.members_staged s
JOIN gold.members_golden g ON g.match_key = s.match_key;
