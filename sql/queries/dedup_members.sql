-- ====================================================================
-- Bronze -> Silver: unified schema, format cleansing, match key
-- --------------------------------------------------------------------
-- This query is the SQL heart of MemberMatch. It:
--   1. Parses each source's JSONB payload with source-specific logic
--   2. Normalizes formats (date parsing, whitespace, case, zip padding)
--   3. Normalizes plan code vocabulary across sources
--   4. Computes a composite match key used by gold-layer deduplication
--
-- Why one query with CTEs instead of multiple statements:
--   - Atomicity: a partial silver load is never visible
--   - Readability: the three source transforms sit side-by-side,
--     making the normalization differences obvious
--   - Optimizer: the planner can inline the CTEs if useful
-- ====================================================================

WITH source_a_parsed AS (
    -- Source A is the "legacy system". CSV columns map close to the target
    -- schema. Main issues: MM/DD/YYYY dates, whitespace, lowercase names.
    SELECT
        'source_a' AS source_system,
        raw_payload->>'member_id' AS source_member_id,
        TRIM(INITCAP(raw_payload->>'first_name')) AS first_name,
        TRIM(INITCAP(raw_payload->>'last_name')) AS last_name,
        -- Handle both ISO and MM/DD/YYYY date formats
        CASE
            WHEN raw_payload->>'date_of_birth' ~ '^\d{4}-\d{2}-\d{2}$'
                THEN (raw_payload->>'date_of_birth')::DATE
            WHEN raw_payload->>'date_of_birth' ~ '^\d{2}/\d{2}/\d{4}$'
                THEN TO_DATE(raw_payload->>'date_of_birth', 'MM/DD/YYYY')
            ELSE NULL
        END AS date_of_birth,
        -- Empty string -> NULL for SSN
        NULLIF(TRIM(raw_payload->>'ssn_last4'), '') AS ssn_last4,
        -- ZIP+4 -> 5-digit
        LEFT(COALESCE(raw_payload->>'zip', ''), 5) AS zip_code,
        raw_payload->>'plan_code' AS plan_code
    FROM bronze.members_raw
    WHERE source_system = 'source_a'
),

source_b_parsed AS (
    -- Source B is the "partner feed". camelCase JSON keys, different
    -- plan vocabulary (GOLD/SILVER/BRONZE instead of PPO_GOLD etc).
    SELECT
        'source_b' AS source_system,
        raw_payload->>'id' AS source_member_id,
        TRIM(INITCAP(raw_payload->>'firstName')) AS first_name,
        TRIM(INITCAP(raw_payload->>'lastName')) AS last_name,
        (raw_payload->>'dob')::DATE AS date_of_birth,
        NULLIF(TRIM(raw_payload->>'ssnLast4'), '') AS ssn_last4,
        LEFT(raw_payload->>'zipCode', 5) AS zip_code,
        -- Plan code normalization: partner uses short codes
        CASE (raw_payload->>'plan')
            WHEN 'GOLD' THEN 'PPO_GOLD'
            WHEN 'SILVER' THEN 'HMO_SILVER'
            WHEN 'BRONZE' THEN 'EPO_BRONZE'
            ELSE raw_payload->>'plan'
        END AS plan_code
    FROM bronze.members_raw
    WHERE source_system = 'source_b'
),

source_c_parsed AS (
    -- Source C is the "API export". ALL CAPS names, Unix-timestamp dates
    -- for ~20% of rows, integer postal codes (dropping leading zeros).
    SELECT
        'source_c' AS source_system,
        raw_payload->>'memberIdentifier' AS source_member_id,
        TRIM(INITCAP(raw_payload->>'givenName')) AS first_name,
        TRIM(INITCAP(raw_payload->>'familyName')) AS last_name,
        -- Detect Unix timestamp vs ISO date
        CASE
            WHEN raw_payload->>'birthDate' ~ '^\d+$'
                THEN TO_TIMESTAMP((raw_payload->>'birthDate')::BIGINT)::DATE
            WHEN raw_payload->>'birthDate' ~ '^\d{4}-\d{2}-\d{2}$'
                THEN (raw_payload->>'birthDate')::DATE
            ELSE NULL
        END AS date_of_birth,
        NULLIF(TRIM(raw_payload->>'taxIdLast4'), '') AS ssn_last4,
        -- LPAD restores leading zeros if postal_code came through as int
        LPAD(raw_payload->>'postal_code', 5, '0') AS zip_code,
        -- Third plan vocabulary to normalize
        CASE
            WHEN raw_payload->>'planName' ILIKE '%gold%PPO%' THEN 'PPO_GOLD'
            WHEN raw_payload->>'planName' ILIKE '%silver%HMO%' THEN 'HMO_SILVER'
            WHEN raw_payload->>'planName' ILIKE '%bronze%EPO%' THEN 'EPO_BRONZE'
            WHEN raw_payload->>'planName' ILIKE '%medicare%' THEN 'MEDICARE_A'
            ELSE raw_payload->>'planName'
        END AS plan_code
    FROM bronze.members_raw
    WHERE source_system = 'source_c'
),

all_sources AS (
    SELECT * FROM source_a_parsed
    UNION ALL
    SELECT * FROM source_b_parsed
    UNION ALL
    SELECT * FROM source_c_parsed
),

with_match_key AS (
    -- Composite match key: lowercased last name + ISO DOB + ssn_last4.
    -- Chosen components:
    --   last_name: more stable than first name (no nickname variance)
    --   date_of_birth: extremely stable per-person identifier
    --   ssn_last4: 10,000 possible values but combined with dob + last
    --     name produces near-unique matching for synthetic data.
    --
    -- Rows with NULL in any match-key component are dropped here --
    -- they would produce false positive matches (all NULLs would
    -- collide). In a real system these would land in a quarantine
    -- table; for this project we just filter them out.
    SELECT
        *,
        LOWER(last_name) || '|' ||
        COALESCE(date_of_birth::TEXT, 'unknown') || '|' ||
        COALESCE(ssn_last4, 'unknown') AS match_key
    FROM all_sources
    WHERE date_of_birth IS NOT NULL
      AND last_name IS NOT NULL
      AND last_name <> ''
      AND first_name IS NOT NULL
      AND first_name <> ''
)

INSERT INTO silver.members_staged (
    source_system, source_member_id, first_name, last_name,
    date_of_birth, ssn_last4, zip_code, plan_code, match_key
)
SELECT
    source_system, source_member_id, first_name, last_name,
    date_of_birth, ssn_last4, zip_code, plan_code, match_key
FROM with_match_key
ON CONFLICT (source_system, source_member_id) DO NOTHING;
