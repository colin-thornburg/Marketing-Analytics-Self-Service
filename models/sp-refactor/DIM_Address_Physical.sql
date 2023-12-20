{{
    config(
        materialized='incremental',
        unique_key='LOOKUP_HASH_ADDRESS_PHYSICAL'  -- Assuming this is the unique key for your dataset
    )
}}

SELECT
			CAST(EDW_PROCESS.SEQ_MASTER_MEMBER_ID_H0.NEXTVAL AS BIGINT) AS ADDRESS_PHYSICAL_ID,
			CAST(''Y'' AS VARCHAR(1 OCTETS)) AS ACTIVE_FLAG,
			CAST(''1900-01-01-00.00.00'' AS TIMESTAMP) AS EFFECTIVE_START_DATETIME,
			CAST(NULL AS TIMESTAMP) AS EFFECTIVE_END_DATETIME,
			c_b_addr_ses.LOOKUP_HASH_ADDRESS_PHYSICAL AS LOOKUP_HASH_ADDRESS_PHYSICAL,
			CAST(c_b_addr_ses.ADDR_LN_1 AS VARCHAR(128 OCTETS)) AS PHYSICAL_ADDRESS_LINE_1,
			CAST(COALESCE(c_b_addr_ses.ADDR_LN_2, '''') AS VARCHAR(128 OCTETS)) AS PHYSICAL_ADDRESS_LINE_2,
			CAST(COALESCE(c_b_addr_ses.SUITE, '''') AS VARCHAR(128 OCTETS)) AS PHYSICAL_ADDRESS_SUITE,
			CAST(c_b_addr_ses.CITY AS VARCHAR(128 OCTETS)) AS PHYSICAL_ADDRESS_CITY,
			CAST(LEFT(TRIM(c_b_addr_ses.PSTL_CD) || ''00000'', 5) AS VARCHAR(64 OCTETS)) AS PHYSICAL_ADDRESS_POSTAL_CODE,
			CAST(c_b_addr_ses.PSTL_CD_EXT AS VARCHAR(64 OCTETS)) AS PHYSICAL_ADDRESS_POSTAL_CODE_EXTENSION,
			CAST(c_b_addr_ses.ST_PROV_CD AS VARCHAR(64 OCTETS)) AS PHYSICAL_ADDRESS_STATE_PROVINCE_CODE,
			CAST(c_b_addr_ses.CNTRY_CD AS VARCHAR(512 OCTETS)) AS PHYSICAL_ADDRESS_COUNTRY_CODE,
			CAST(COALESCE(c_b_addr_ses.STORE_ADDR_FLG, ''N'') AS VARCHAR(1 OCTETS)) AS PHYSICAL_ADDRESS_STORE_ADDRESS_FLAG,
			CAST(c_b_addr_ses.VFYD_FLG AS VARCHAR(1 OCTETS)) AS PHYSICAL_ADDRESS_VERIFICATION_FLAG,
			CAST(COALESCE(c_b_addr_ses.ANON_FLG, ''N'') AS VARCHAR(1 OCTETS)) AS PHYSICAL_ADDRESS_ANONIMZATION_FLAG,
			CAST(COALESCE(hub_load_DIM_GEOGRAPHY_POSTAL.GEOGRAPHY_ID, -2) AS BIGINT) AS GEOGRAPHY_ID,
			CAST(COALESCE(c_b_addr_ses.LATITUDE,0.0) AS DECIMAL(13,7)) AS PHYSICAL_ADDRESS_LATITUDE,
			CAST(COALESCE(c_b_addr_ses.LONGITUDE,0.0) AS DECIMAL(13,7)) AS PHYSICAL_ADDRESS_LONGITUDE,
			CAST(''N'' AS VARCHAR(1 OCTETS)) AS ETL_SOURCE_DATA_DELETED_FLAG,
			CAST(' || quote_literal(v_etl_source_table_1) || ' AS VARCHAR(256 OCTETS)) AS ETL_SOURCE_TABLE_NAME,
			CAST(CURRENT_TIMESTAMP - CURRENT_TIMEZONE AS TIMESTAMP) AS ETL_CREATE_TIMESTAMP,
			CAST(CURRENT_TIMESTAMP - CURRENT_TIMEZONE AS TIMESTAMP) AS ETL_UPDATE_TIMESTAMP,
			CAST(' || v_stored_procedure_execution_id || ' AS BIGINT) AS ETL_MODIFIED_BY_JOB_ID,
			CAST(' || quote_literal(v_hub_procedure_name) || ' AS VARCHAR(128 OCTETS)) AS ETL_MODIFIED_BY_PROCESS
		FROM {{ ref('C_B_ADDR_SESSION') }} c_b_addr_ses
		LEFT JOIN {{ source('hub_load', 'dim_geography') }} hub_load_DIM_GEOGRAPHY_POSTAL
			/*Alternative to union all, which broke sequence generation*/
			ON CASE
					WHEN c_b_addr_ses.CNTRY_CD = ''US'' THEN TRIM(''USA'' || ''-'' || TRIM(c_b_addr_ses.ST_PROV_CD) || ''-'' || TRIM(c_b_addr_ses.CITY) || ''-'' || LEFT(TRIM(c_b_addr_ses.PSTL_CD) || ''00000'', 5))
					WHEN c_b_addr_ses.CNTRY_CD = ''CA'' THEN TRIM(''CAN'' || ''-'' || TRIM(c_b_addr_ses.ST_PROV_CD))
				END = hub_load_DIM_GEOGRAPHY_POSTAL.GEOGRAPHY_HIERARCHY_LEVEL_MEMBER_CODE
                                    /*	LEFT JOIN ' || v_staging_schema_read_write || '.' || v_source_table_name || '  hub_load
                                            ON c_b_addr_ses.LOOKUP_HASH_ADDRESS_PHYSICAL = hub_load.LOOKUP_HASH_ADDRESS_PHYSICAL
                                        WHERE
                                            hub_load.LOOKUP_HASH_ADDRESS_PHYSICAL IS NULL */

            {% if is_incremental() %}
                -- This filter will only be applied on an incremental run.
                -- It will include records whose LOOKUP_HASH_ADDRESS_PHYSICAL does not exist in the current model.
                WHERE c_b_addr_ses.LOOKUP_HASH_ADDRESS_PHYSICAL NOT IN (SELECT LOOKUP_HASH_ADDRESS_PHYSICAL FROM {{ this }})
            {% endif %}
		GROUP BY
			c_b_addr_ses.LOOKUP_HASH_ADDRESS_PHYSICAL,
			c_b_addr_ses.ADDR_LN_1,
			COALESCE(c_b_addr_ses.ADDR_LN_2, ''''),
			COALESCE(c_b_addr_ses.SUITE, ''''),
			c_b_addr_ses.CITY,
			c_b_addr_ses.PSTL_CD,
			c_b_addr_ses.PSTL_CD_EXT,
			c_b_addr_ses.ST_PROV_CD,
			c_b_addr_ses.CNTRY_CD,
			COALESCE(c_b_addr_ses.STORE_ADDR_FLG, ''N''),
			c_b_addr_ses.VFYD_FLG,
			COALESCE(c_b_addr_ses.ANON_FLG, ''N''),
			COALESCE(hub_load_DIM_GEOGRAPHY_POSTAL.GEOGRAPHY_ID, -2),
			COALESCE(c_b_addr_ses.LATITUDE,0.0),
			COALESCE(c_b_addr_ses.LONGITUDE,0.0)