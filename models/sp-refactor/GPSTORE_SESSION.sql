WITH raw_data AS (
    SELECT
        ADDR2,
        ADDR1,
        CITY,
        ZIP,
        STATE,
        LATITUDE,
        LONGITUDE,
        LOAD_TIMESTAMP,
        STORE_NUMBER,
        HASH(CAST(
            COALESCE(TRIM(ADDR2), '') || '&' ||
            COALESCE(TRIM(ADDR1), '') || '&' || '@' || '&' ||
            COALESCE(TRIM(CITY), '') || '&' ||
            COALESCE(LEFT(LEFT(REPLACE(TRIM(REPLACE(TRIM(ZIP), '-', ' ')), ' ', '') || '000000000', 9), 5), '') || '&' ||
            COALESCE(RIGHT(LEFT(REPLACE(TRIM(REPLACE(TRIM(ZIP), '-', ' ')), ' ', '') || '000000000', 9), 4), '') || '&' ||
            COALESCE(TRIM(STATE), '') || '&' || 'US' AS VARCHAR), 3) AS LOOKUP_HASH_ADDRESS_PHYSICAL
    FROM {{ source('edw_staging', 'cur_loc_gpstore') }} gpstore
),
ranked_data AS (
    SELECT
        COALESCE(TRIM(ADDR2), ' ') AS ADDR2,
        COALESCE(TRIM(ADDR1), ' ') AS ADDR1,
        COALESCE(TRIM(CITY), ' ') AS CITY,
        TRIM(ZIP) AS ZIP,
        TRIM(STATE) AS STATE,
        LATITUDE,
        LONGITUDE,
        LOAD_TIMESTAMP,
        LOOKUP_HASH_ADDRESS_PHYSICAL,
        STORE_NUMBER,
        ROW_NUMBER() OVER (PARTITION BY LOOKUP_HASH_ADDRESS_PHYSICAL ORDER BY LOAD_TIMESTAMP DESC) AS rn
    FROM raw_data
)
SELECT
    ADDR2,
    ADDR1,
    CITY,
    ZIP,
    STATE,
    LATITUDE,
    LONGITUDE,
    LOAD_TIMESTAMP,
    LOOKUP_HASH_ADDRESS_PHYSICAL,
    STORE_NUMBER
FROM ranked_data
WHERE rn = 1