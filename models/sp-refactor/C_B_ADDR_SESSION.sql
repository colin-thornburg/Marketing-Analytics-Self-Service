WITH hashed_addresses AS (
    SELECT
        ADDR_LN_1,
        ADDR_LN_2,
        SUITE,
        CITY,
        PSTL_CD,
        PSTL_CD_EXT,
        ST_PROV_CD,
        CNTRY_CD,
        LAST_UPDATE_DATE,
        ROWID_OBJECT,
        STORE_ADDR_FLG,
        VFYD_FLG,
        ANON_FLG,
        LATITUDE,
        LONGITUDE,
        HASH(CAST(
            COALESCE(COALESCE(TRIM(ADDR_LN_1), ''), '#') || '&' ||
            COALESCE(COALESCE(TRIM(ADDR_LN_2), ''), '$') || '&' ||
            COALESCE(COALESCE(TRIM(SUITE), ''), '@') || '&' ||
            COALESCE(COALESCE(TRIM(CITY), ''), 'D') || '&' ||
            COALESCE(COALESCE(LEFT(TRIM(PSTL_CD) || '00000', 5), ''), 'E') || '&' ||
            COALESCE(COALESCE(TRIM(PSTL_CD_EXT), ''), 'F') || '&' ||
            COALESCE(COALESCE(TRIM(ST_PROV_CD), ''), 'G') || '&' ||
            COALESCE(COALESCE(TRIM(CNTRY_CD), ''), 'H') AS VARCHAR), 3) AS LOOKUP_HASH_ADDRESS_PHYSICAL
    FROM {{ source('edw_staging', 'cur_mdm_c_b_addr') }}
),
ranked_addresses AS (
    SELECT
        COALESCE(TRIM(ADDR_LN_1), ' ') AS ADDR_LN_1,
        COALESCE(TRIM(ADDR_LN_2), ' ') AS ADDR_LN_2,
        COALESCE(TRIM(SUITE), ' ') AS SUITE,
        COALESCE(TRIM(CITY), ' ') AS CITY,
        TRIM(PSTL_CD) AS PSTL_CD,
        TRIM(PSTL_CD_EXT) AS PSTL_CD_EXT,
        COALESCE(TRIM(ST_PROV_CD), ' ') AS ST_PROV_CD,
        COALESCE(TRIM(CNTRY_CD), ' ') AS CNTRY_CD,
        LAST_UPDATE_DATE,
        LOOKUP_HASH_ADDRESS_PHYSICAL,
        STORE_ADDR_FLG,
        VFYD_FLG,
        ANON_FLG,
        LATITUDE,
        LONGITUDE,
        ROWID_OBJECT,
        ROW_NUMBER() OVER (PARTITION BY LOOKUP_HASH_ADDRESS_PHYSICAL ORDER BY LAST_UPDATE_DATE DESC) AS rn
    FROM hashed_addresses
)
SELECT
    ADDR_LN_1,
    ADDR_LN_2,
    SUITE,
    CITY,
    PSTL_CD,
    PSTL_CD_EXT,
    ST_PROV_CD,
    CNTRY_CD,
    LAST_UPDATE_DATE,
    LOOKUP_HASH_ADDRESS_PHYSICAL,
    STORE_ADDR_FLG,
    CAST(CASE WHEN LAST_UPDATE_DATE > '2022-07-06 00:00:00' THEN COALESCE(VFYD_FLG,'N')
    ELSE 'Y' END AS VARCHAR(1 OCTETS)) AS VFYD_FLG,
    ANON_FLG,
    LATITUDE,
    LONGITUDE,
    ROWID_OBJECT,
    rn
FROM ranked_addresses
WHERE rn = 1;