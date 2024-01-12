CREATE OR REPLACE PROCEDURE EDW_PROCESS.P_LOAD_DIM_ADDRESS_PHYSICAL (
	 IN i_job_execution_id  BIGINT,
     IN i_load_type INT)
MODIFIES SQL DATA
NOT DETERMINISTIC
LANGUAGE SQL

BEGIN
	/*Local Variable Declaration*/
	DECLARE v_timestamp timestamp;--
	DECLARE v_bignint bigint;--
	DECLARE v_varchar varchar(128);--
	DECLARE p_load_type bigint;--
	DECLARE p_parent_job_execution_id BIGINT;--
	DECLARE v_stored_procedure_execution_id BIGINT;--
	DECLARE v_job_execution_name VARCHAR(128);--
	DECLARE v_job_phase_id BIGINT;--
	DECLARE v_source_database_name VARCHAR(128);--
	DECLARE v_source_table_name VARCHAR(128);--
	DECLARE p_source_table_id  BIGINT;--
	DECLARE v_target_database_name VARCHAR(128);--
	DECLARE v_target_table_name VARCHAR(128);--
	DECLARE v_hub_procedure_name VARCHAR(128);--
	DECLARE v_validation_flag BIGINT;--
	DECLARE v_etl_source_database_name VARCHAR(128);--
	DECLARE v_hub_database_name VARCHAR(128);--
	DECLARE v_process_database_name VARCHAR(128);--
	DECLARE v_landing_database_name VARCHAR(128);--
	DECLARE v_staging_database_name VARCHAR(128);--
	DECLARE v_str_sql CLOB(2097152);--
	DECLARE v_str_sql_debug VARCHAR(32000);--
	DECLARE v_str_sql_cursor VARCHAR(32000);--
	DECLARE v_str_sql_log_job VARCHAR(32000);--
	DECLARE v_error_message VARCHAR(16000);--
	DECLARE p_processed_rows BIGINT;--
	DECLARE p_failed_rows BIGINT;--
	DECLARE p_processed_insert_rows BIGINT;--
	DECLARE p_processed_update_rows BIGINT;--
	DECLARE p_processed_delete_rows BIGINT;--
	DECLARE v_job_execution_starttime TIMESTAMP;--
	DECLARE o_return_value INT;--
	DECLARE o_str_debug VARCHAR(32000);--
	DECLARE v_etl_source_table_1 VARCHAR(128);--
	DECLARE v_etl_source_table_2 VARCHAR(128);--
	DECLARE v_etl_source_table_3 VARCHAR(128);--
	DECLARE v_etl_source_table_4 VARCHAR(128);--
	DECLARE v_etl_source_table_5 VARCHAR(128);--
	DECLARE v_staging_schema_read_write VARCHAR(128);--
	DECLARE c_table_cursor CURSOR FOR c_table;--



	/*Local Variable Initialization*/
	SET v_timestamp = CURRENT_TIMESTAMP;--
	SET v_bignint = CAST(0 AS BIGINT);--
	SET v_varchar = CAST('' AS varchar(128));--
	SET p_load_type = i_load_type;--
	SET p_source_table_id = -1;--
	SET p_parent_job_execution_id = i_job_execution_id;--
	SET v_stored_procedure_execution_id = 0;--
	SET v_hub_procedure_name = TRIM('P_LOAD_DIM_ADDRESS_PHYSICAL');--
	SET v_job_execution_name = TRIM(v_hub_procedure_name);--
	SET v_process_database_name = TRIM('EDW_PROCESS');--
	SET v_etl_source_database_name = TRIM('EDW_STAGING');--
	SET v_staging_database_name = TRIM('EDW_STAGING');--
	SET v_source_database_name = TRIM(v_staging_database_name);--
	SET v_hub_database_name = TRIM('EDW_HUB');--
	SET v_target_database_name = TRIM(v_hub_database_name);--
	SET v_landing_database_name = TRIM('EDW_LANDING');--
	SET v_source_table_name = TRIM('HUB_LOAD_DIM_ADDRESS_PHYSICAL');--
	SET v_staging_schema_read_write = TRIM('EDW_STAGING');--
	SET v_job_phase_id = 1;--
	SET v_target_table_name = TRIM('DIM_ADDRESS_PHYSICAL');--
	SET v_validation_flag = 0;--
	SET v_error_message = TRIM('');--
	SET p_processed_rows = 0;--
	SET p_failed_rows = 0;--
	SET p_processed_insert_rows = 0;--
	SET p_processed_update_rows = 0;--
	SET p_processed_delete_rows = 0;--
	SET v_str_sql = TRIM('');--
	SET v_str_sql_cursor = TRIM('');--
	SET v_str_sql_log_job = TRIM('');--
	SET v_str_sql_debug = TRIM('');--
	SET v_job_execution_starttime = CAST('1900-01-01' AS TIMESTAMP);--
	SET o_return_value = 0;--
	SET o_str_debug = TRIM('');--
	SET v_etl_source_table_1 = 'C_B_ADDR';--
	SET v_etl_source_table_2 = 'GPSTORE';--
	SET v_etl_source_table_3 = 'DBARCUD';--
	SET v_etl_source_table_4 = 'BLC_ADDRESS';--
	SET v_etl_source_table_5 = 'ORW_CUSTOMER_ADDRESS';--

	/*
	README:
	Call the sequence SEQ_DATAPIPELINE_PROCESS_H0.NEXTVAL which is used to populate v_stored_procedure_execution_id when logging to
	EDW_PROCESS.AUDIT_LOG_JOB_EXECUTION_H0 table within this stored procedure
	*/

	/*Get v_stored_procedure_execution_id*/
	SET v_str_sql_cursor = 'SELECT ' || v_process_database_name || '.SEQ_DATAPIPELINE_PROCESS_H0.NEXTVAL FROM ' || v_process_database_name || '.DUMMY;';--
	SET o_str_debug = v_str_sql_cursor;--

	PREPARE c_table
	FROM v_str_sql_cursor;--
	OPEN c_table_cursor USING v_process_database_name;--
	FETCH c_table_cursor INTO v_stored_procedure_execution_id;--
	CLOSE c_table_cursor;--

	/*Debugging Logging*/
	SET v_str_sql_debug =  'INSERT INTO ' || v_process_database_name || '.AUDIT_LOG_JOB_EXECUTION_SQL_COMMANDS_H0 (JOB_EXECUTION_ID, SQL_SMNT) VALUES (' || v_stored_procedure_execution_id ||','|| quote_literal(v_str_sql_cursor) || ');';  EXECUTE IMMEDIATE v_str_sql_debug;--

	/*
	README:
	Call the stored prcedure EDW_PROCESS.P_LOG_JOB_H0 to populate EDW_PROCESS.AUDIT_LOG_JOB_EXECUTION table with STARTED aka IN-PROGESS.
	NOTE: This release of the stored procedure P_LOG_JOB_H0 only allows for fixed error message values in v_error_message.
	*/

	/*Populate EDW_PROCESS.AUDIT_LOG_JOB_EXECUTION*/
	/*TODO: Implement inter-process logging and pass PARAM values and dynamic sql called via EXECUTE IMMEDIATE*/
	SET v_job_phase_id = 2;--
	SET v_error_message = 'Stored Procedure Message: RUNNING/STARTED > ' || v_hub_procedure_name || ' > JOB EXECUTION ID: ' || v_stored_procedure_execution_id || ' > Total Rows Processed: ' || p_processed_rows || '.';--
	SET o_str_debug = v_error_message;--
	SET v_str_sql = 'CALL ' || v_process_database_name || '.P_LOG_JOB_H0( ' || quote_literal(v_job_execution_name) || ',' || v_stored_procedure_execution_id ||','|| quote_literal(v_error_message) || ',' || v_job_phase_id || ',' || p_parent_job_execution_id || ',' || p_source_table_id || ',' || quote_literal(v_target_table_name) || ',' || p_processed_rows || ',' || p_failed_rows || ',' || p_processed_insert_rows || ',' || p_processed_update_rows || ',' || p_processed_delete_rows || ');';--
	SET o_str_debug =  v_str_sql;--

	/*Debugging Logging*/
	SET v_str_sql_debug =  'INSERT INTO ' || v_process_database_name || '.AUDIT_LOG_JOB_EXECUTION_SQL_COMMANDS_H0 (JOB_EXECUTION_ID, SQL_SMNT) VALUES (' || v_stored_procedure_execution_id ||','|| quote_literal(v_str_sql) || ');';  EXECUTE IMMEDIATE v_str_sql_debug;--
	EXECUTE IMMEDIATE v_str_sql;--

	/*
	README:
	Use dynamic SQL run via EXECUTE IMMEDIATE and dynamic cursors to find metadata for dynamic SQL used later on in the stored procedure
	*/

	/*Get v_job_execution_starttime*/
	SET v_str_sql_cursor = 'SELECT ADD_HOURS(JOB_EXECUTION_STARTTIME, -3) AS JOB_EXECUTION_STARTTIME
							FROM ' || v_process_database_name || '.AUDIT_LOG_JOB_EXECUTION_H0
							WHERE JOB_EXECUTION_STATUS_CODE = 0 AND JOB_EXECUTION_NAME = ' || quote_literal(v_job_execution_name) || '
							ORDER BY JOB_EXECUTION_STARTTIME DESC ;';--

	PREPARE c_table
	FROM v_str_sql_cursor;--
	OPEN c_table_cursor USING v_process_database_name;--
	FETCH c_table_cursor INTO v_job_execution_starttime;--
	CLOSE c_table_cursor;--

	/*Debugging Logging*/
	SET v_str_sql_debug =  'INSERT INTO ' || v_process_database_name || '.AUDIT_LOG_JOB_EXECUTION_SQL_COMMANDS_H0 (JOB_EXECUTION_ID, SQL_SMNT) VALUES (' || v_stored_procedure_execution_id ||','|| quote_literal(v_str_sql_cursor) || ');';  EXECUTE IMMEDIATE v_str_sql_debug;--
	SET v_str_sql_debug =  'INSERT INTO ' || v_process_database_name || '.AUDIT_LOG_JOB_EXECUTION_SQL_COMMANDS_H0 (JOB_EXECUTION_ID, SQL_SMNT) VALUES (' || v_stored_procedure_execution_id ||','|| quote_literal(v_job_execution_starttime) || ');';  EXECUTE IMMEDIATE v_str_sql_debug;--

	/*
	README:
	Clear and load the Hub Load table with IF/THEN/END logic if p_load_type (i_load_type) was input as 1 by user.
	*/

	IF p_load_type = 1 THEN

		/*Delete Hub Table*/
		SET v_str_sql = 'DELETE FROM ' || v_target_database_name ||'.'|| v_target_table_name || ';';--

		SET o_str_debug =  v_str_sql;--

		/*Debugging Logging*/
		SET v_str_sql_debug =  'INSERT INTO ' || v_process_database_name || '.AUDIT_LOG_JOB_EXECUTION_SQL_COMMANDS_H0 (JOB_EXECUTION_ID, SQL_SMNT) VALUES (' || v_stored_procedure_execution_id ||','|| quote_literal(v_str_sql) || ');';  EXECUTE IMMEDIATE v_str_sql_debug;--

		EXECUTE IMMEDIATE  v_str_sql;--

	END IF;--

	/*
	README:
	Data is inserted with an insert statement and updated with a merge statement.
	This clears up logging, enhances readability and potentially improves performance
	when compared to inserting and updating from one merge statement.
	*/

	/*
	 Declaring TMP TABLE SESSION for C_B_ADDR
	 */
	DECLARE GLOBAL TEMPORARY TABLE SESSION.C_B_ADDR_SESSION AS (
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
            	FROM (
				SELECT
				    COALESCE(TRIM(ADDR_LN_1), ' ') ADDR_LN_1,
				    COALESCE(TRIM(ADDR_LN_2), ' ') ADDR_LN_2,
				    COALESCE(TRIM(SUITE), ' ') SUITE,
				    COALESCE(TRIM(CITY), ' ') CITY,
				    TRIM(PSTL_CD) PSTL_CD,
				    TRIM(PSTL_CD_EXT) PSTL_CD_EXT,
				    COALESCE(TRIM(ST_PROV_CD), ' ') ST_PROV_CD,
				    COALESCE(TRIM(CNTRY_CD), ' ') CNTRY_CD,
				    LAST_UPDATE_DATE,
				    LOOKUP_HASH_ADDRESS_PHYSICAL,
				    STORE_ADDR_FLG,
				  	VFYD_FLG,
				  	ANON_FLG,
				  	LATITUDE,
				  	LONGITUDE,
				    ROWID_OBJECT,
				    ROW_NUMBER() OVER (PARTITION BY LOOKUP_HASH_ADDRESS_PHYSICAL ORDER BY LAST_UPDATE_DATE DESC) rn
				 FROM
					(SELECT
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
							COALESCE(COALESCE(TRIM(c_b_addr_i.ADDR_LN_1), ''), '#') || '&' ||
							COALESCE(COALESCE(TRIM(c_b_addr_i.ADDR_LN_2), ''), '$') || '&' ||
							COALESCE(COALESCE(TRIM(c_b_addr_i.SUITE), ''), '@') || '&' ||
							COALESCE(COALESCE(TRIM(c_b_addr_i.CITY), ''), 'D') || '&' ||
							COALESCE(COALESCE(LEFT(TRIM(c_b_addr_i.PSTL_CD) || '00000', 5), ''), 'E') || '&' ||
							COALESCE(COALESCE(TRIM(c_b_addr_i.PSTL_CD_EXT), ''), 'F') || '&' ||
							COALESCE(COALESCE(TRIM(c_b_addr_i.ST_PROV_CD), ''), 'G') || '&' ||
							COALESCE(COALESCE(TRIM(c_b_addr_i.CNTRY_CD), ''), 'H') AS VARCHAR), 3) LOOKUP_HASH_ADDRESS_PHYSICAL
					    FROM EDW_STAGING.CUR_MDM_C_B_ADDR c_b_addr_i
					) a
				) WHERE rn =1
			)
            WITH DATA
            ON COMMIT PRESERVE ROWS
            NOT LOGGED
            DISTRIBUTE BY HASH(
                ROWID_OBJECT
                )
            ORGANIZE BY COLUMN;

           CALL SYSPROC.ADMIN_CMD('RUNSTATS ON TABLE SESSION.C_B_ADDR_SESSION ON ALL COLUMNS AND COLUMNS ((ROWID_OBJECT,LOOKUP_HASH_ADDRESS_PHYSICAL)) AND DETAILED INDEXES ALL SET PROFILE ONLY');
		   CALL SYSPROC.ADMIN_CMD('RUNSTATS ON TABLE SESSION.C_B_ADDR_SESSION USE PROFILE');


			/*
			 Declaring TMP TABLE SESSION for GPSTORE
	 		*/
			DECLARE GLOBAL TEMPORARY TABLE SESSION.GPSTORE_SESSION AS (
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
				  			STORE_NUMBER,
					        rn
            	FROM (
				SELECT
				    COALESCE(TRIM(ADDR2), ' ') ADDR2,
				    COALESCE(TRIM(ADDR1), ' ') ADDR1,
				    COALESCE(TRIM(CITY), ' ') CITY,
				    TRIM(ZIP) ZIP,
				    TRIM(STATE) STATE,
				    LOAD_TIMESTAMP,
				    LATITUDE,
				    LONGITUDE,
				    LOOKUP_HASH_ADDRESS_PHYSICAL,
				    STORE_NUMBER,
				    ROW_NUMBER() OVER (PARTITION BY LOOKUP_HASH_ADDRESS_PHYSICAL ORDER BY LOAD_TIMESTAMP DESC) rn
				 FROM
					(SELECT
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
							COALESCE(COALESCE(TRIM(gpstore.ADDR2), ''), '#') || '&' ||
							COALESCE(COALESCE(TRIM(gpstore.ADDR1), ''), '$') || '&' || '@' || '&' ||
							COALESCE(COALESCE(TRIM(gpstore.CITY), ''), 'D') || '&' ||
							COALESCE(LEFT(LEFT(REPLACE(TRIM(REPLACE(TRIM(gpstore.ZIP), '-', ' ')), ' ', '') || '000000000', 9), 5), 'E') || '&' ||
							COALESCE(RIGHT(LEFT(REPLACE(TRIM(REPLACE(TRIM(gpstore.ZIP), '-', ' ')), ' ', '') || '000000000', 9), 4), 'F') || '&' ||
							COALESCE(COALESCE(TRIM(gpstore.STATE), ''), 'G') || '&' || 'US' AS VARCHAR), 3) LOOKUP_HASH_ADDRESS_PHYSICAL
					    FROM EDW_STAGING.CUR_LOC_GPSTORE gpstore
					) a
				) WHERE rn =1
			)
            WITH DATA
            ON COMMIT PRESERVE ROWS
            NOT LOGGED
            DISTRIBUTE BY HASH(
                STORE_NUMBER
                )
            ORGANIZE BY COLUMN;



           CALL SYSPROC.ADMIN_CMD('RUNSTATS ON TABLE SESSION.GPSTORE_SESSION ON ALL COLUMNS AND COLUMNS ((STORE_NUMBER,LOOKUP_HASH_ADDRESS_PHYSICAL)) AND DETAILED INDEXES ALL SET PROFILE ONLY');
           CALL SYSPROC.ADMIN_CMD('RUNSTATS ON TABLE SESSION.GPSTORE_SESSION USE PROFILE');

/*Populate Hub Load Table - 1st Insert: C_B_ADDR -> DIM_ADDRESS_PHYSICAL*/
	SET v_str_sql = 'INSERT INTO ' || v_staging_schema_read_write || '.' || v_source_table_name || '
		(ADDRESS_PHYSICAL_ID, ACTIVE_FLAG, EFFECTIVE_START_DATETIME, EFFECTIVE_END_DATETIME, LOOKUP_HASH_ADDRESS_PHYSICAL, PHYSICAL_ADDRESS_LINE_1, PHYSICAL_ADDRESS_LINE_2, PHYSICAL_ADDRESS_SUITE, PHYSICAL_ADDRESS_CITY, PHYSICAL_ADDRESS_POSTAL_CODE, PHYSICAL_ADDRESS_POSTAL_CODE_EXTENSION, PHYSICAL_ADDRESS_STATE_PROVINCE_CODE, PHYSICAL_ADDRESS_COUNTRY_CODE, PHYSICAL_ADDRESS_STORE_ADDRESS_FLAG, PHYSICAL_ADDRESS_VERIFICATION_FLAG,PHYSICAL_ADDRESS_ANONIMZATION_FLAG, GEOGRAPHY_ID, PHYSICAL_ADDRESS_LATITUDE, PHYSICAL_ADDRESS_LONGITUDE, ETL_SOURCE_DATA_DELETED_FLAG, ETL_SOURCE_TABLE_NAME, ETL_CREATE_TIMESTAMP, ETL_UPDATE_TIMESTAMP, ETL_MODIFIED_BY_JOB_ID, ETL_MODIFIED_BY_PROCESS)
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
		FROM SESSION.C_B_ADDR_SESSION c_b_addr_ses
		LEFT JOIN ' || v_staging_database_name || '.HUB_LOAD_DIM_GEOGRAPHY hub_load_DIM_GEOGRAPHY_POSTAL
			/*Alternative to union all, which broke sequence generation*/
			ON CASE
					WHEN c_b_addr_ses.CNTRY_CD = ''US'' THEN TRIM(''USA'' || ''-'' || TRIM(c_b_addr_ses.ST_PROV_CD) || ''-'' || TRIM(c_b_addr_ses.CITY) || ''-'' || LEFT(TRIM(c_b_addr_ses.PSTL_CD) || ''00000'', 5))
					WHEN c_b_addr_ses.CNTRY_CD = ''CA'' THEN TRIM(''CAN'' || ''-'' || TRIM(c_b_addr_ses.ST_PROV_CD))
				END = hub_load_DIM_GEOGRAPHY_POSTAL.GEOGRAPHY_HIERARCHY_LEVEL_MEMBER_CODE
		LEFT JOIN ' || v_staging_schema_read_write || '.' || v_source_table_name || '  hub_load
			ON c_b_addr_ses.LOOKUP_HASH_ADDRESS_PHYSICAL = hub_load.LOOKUP_HASH_ADDRESS_PHYSICAL
		WHERE
			hub_load.LOOKUP_HASH_ADDRESS_PHYSICAL IS NULL
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

	WITH UR;';--

	SET o_str_debug =  v_str_sql;--

	/*Debugging Logging*/
	SET v_str_sql_debug =  'INSERT INTO ' || v_process_database_name || '.AUDIT_LOG_JOB_EXECUTION_SQL_COMMANDS_H0 (JOB_EXECUTION_ID, SQL_SMNT) VALUES (' || v_stored_procedure_execution_id ||','|| quote_literal(v_str_sql) || ');';  EXECUTE IMMEDIATE v_str_sql_debug;--

	EXECUTE IMMEDIATE  v_str_sql;--

/*Update Hub Load Table - 1st Merge: C_B_ADDR -> DIM_ADDRESS_PHYSICAL*/
	SET v_str_sql = 'MERGE INTO ' || v_staging_schema_read_write || '.' || v_source_table_name || ' AS tgt
		USING (
			SELECT DISTINCT
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
			FROM SESSION.C_B_ADDR_SESSION c_b_addr_ses
			LEFT JOIN ' || v_staging_database_name || '.HUB_LOAD_DIM_GEOGRAPHY hub_load_DIM_GEOGRAPHY_POSTAL
			/*Alternative to union all, which broke sequence generation*/
			ON CASE
					WHEN c_b_addr_ses.CNTRY_CD = ''US'' THEN TRIM(''USA'' || ''-'' || TRIM(c_b_addr_ses.ST_PROV_CD) || ''-'' || TRIM(c_b_addr_ses.CITY) || ''-'' || LEFT(TRIM(c_b_addr_ses.PSTL_CD) || ''00000'', 5))
					WHEN c_b_addr_ses.CNTRY_CD = ''CA'' THEN TRIM(''CAN'' || ''-'' || TRIM(c_b_addr_ses.ST_PROV_CD))
				END = hub_load_DIM_GEOGRAPHY_POSTAL.GEOGRAPHY_HIERARCHY_LEVEL_MEMBER_CODE
			LEFT JOIN ' || v_staging_schema_read_write || '.' || v_source_table_name || ' hub_load
				ON c_b_addr_ses.LOOKUP_HASH_ADDRESS_PHYSICAL = hub_load.LOOKUP_HASH_ADDRESS_PHYSICAL
			WHERE
				hub_load.LOOKUP_HASH_ADDRESS_PHYSICAL IS NOT NULL) AS src
			ON (tgt.LOOKUP_HASH_ADDRESS_PHYSICAL = src.LOOKUP_HASH_ADDRESS_PHYSICAL)
		WHEN MATCHED AND(
			COALESCE(tgt.PHYSICAL_ADDRESS_STORE_ADDRESS_FLAG, ''A'') <> COALESCE(src.PHYSICAL_ADDRESS_STORE_ADDRESS_FLAG, ''A'') OR
			COALESCE(tgt.PHYSICAL_ADDRESS_VERIFICATION_FLAG, ''N'') <> COALESCE(src.PHYSICAL_ADDRESS_VERIFICATION_FLAG, ''N'') OR
			COALESCE(tgt.PHYSICAL_ADDRESS_ANONIMZATION_FLAG, ''N'') <> COALESCE(src.PHYSICAL_ADDRESS_ANONIMZATION_FLAG, ''N'') OR
			COALESCE(tgt.GEOGRAPHY_ID, -2) <> COALESCE(src.GEOGRAPHY_ID, -2) OR
			COALESCE(tgt.PHYSICAL_ADDRESS_LATITUDE, 0.0) <> COALESCE(src.PHYSICAL_ADDRESS_LATITUDE, 0.0) OR
			COALESCE(tgt.PHYSICAL_ADDRESS_LONGITUDE, 0.0) <> COALESCE(src.PHYSICAL_ADDRESS_LONGITUDE, 0.0) OR
			COALESCE(tgt.ETL_SOURCE_DATA_DELETED_FLAG, ''N'') <> COALESCE(src.ETL_SOURCE_DATA_DELETED_FLAG, ''N'') OR
			COALESCE(tgt.ETL_SOURCE_TABLE_NAME, ''A'') <> COALESCE(src.ETL_SOURCE_TABLE_NAME, ''A''))
		THEN UPDATE SET
			tgt.PHYSICAL_ADDRESS_STORE_ADDRESS_FLAG=src.PHYSICAL_ADDRESS_STORE_ADDRESS_FLAG,
			tgt.PHYSICAL_ADDRESS_VERIFICATION_FLAG=src.PHYSICAL_ADDRESS_VERIFICATION_FLAG,
			tgt.PHYSICAL_ADDRESS_ANONIMZATION_FLAG=src.PHYSICAL_ADDRESS_ANONIMZATION_FLAG,
			tgt.GEOGRAPHY_ID=src.GEOGRAPHY_ID,
			tgt.PHYSICAL_ADDRESS_LATITUDE=src.PHYSICAL_ADDRESS_LATITUDE,
			tgt.PHYSICAL_ADDRESS_LONGITUDE=src.PHYSICAL_ADDRESS_LONGITUDE,
			tgt.ETL_SOURCE_DATA_DELETED_FLAG=src.ETL_SOURCE_DATA_DELETED_FLAG,
			tgt.ETL_SOURCE_TABLE_NAME=src.ETL_SOURCE_TABLE_NAME,
			tgt.ETL_UPDATE_TIMESTAMP=src.ETL_UPDATE_TIMESTAMP,
			tgt.ETL_MODIFIED_BY_JOB_ID=src.ETL_MODIFIED_BY_JOB_ID,
			tgt.ETL_MODIFIED_BY_PROCESS=src.ETL_MODIFIED_BY_PROCESS
		WITH UR;';--

	SET o_str_debug = v_str_sql;--
	call EDW_PROCESS.P_EXECUTE_LOG_H0(v_str_sql_debug);

	/*Debugging Logging*/
	SET v_str_sql_debug =  'INSERT INTO ' || v_process_database_name || '.AUDIT_LOG_JOB_EXECUTION_SQL_COMMANDS_H0 (JOB_EXECUTION_ID, SQL_SMNT) VALUES (' || v_stored_procedure_execution_id ||','|| quote_literal(v_str_sql) || ');';  EXECUTE IMMEDIATE v_str_sql_debug;--

	EXECUTE IMMEDIATE v_str_sql;--

	/*Update Hub Load Table - 1st Update Deletes: C_B_ADDR*/
	SET v_str_sql = 'MERGE INTO ' || v_staging_schema_read_write || '.' || v_source_table_name || ' AS tgt
		USING (
			SELECT tgt.LOOKUP_HASH_ADDRESS_PHYSICAL
			FROM ' || v_staging_schema_read_write || '.' || v_source_table_name || ' AS tgt
			LEFT JOIN (
				SELECT DISTINCT
					c_b_addr_ses.LOOKUP_HASH_ADDRESS_PHYSICAL
			FROM SESSION.C_B_ADDR_SESSION c_b_addr_ses) AS c_b_addr
				ON tgt.LOOKUP_HASH_ADDRESS_PHYSICAL = c_b_addr.LOOKUP_HASH_ADDRESS_PHYSICAL
			WHERE
				tgt.ETL_SOURCE_TABLE_NAME = ' || quote_literal(v_etl_source_table_1) || '
				AND c_b_addr.LOOKUP_HASH_ADDRESS_PHYSICAL IS NULL) AS src
			ON (tgt.LOOKUP_HASH_ADDRESS_PHYSICAL = src.LOOKUP_HASH_ADDRESS_PHYSICAL)
		WHEN MATCHED THEN UPDATE SET
			tgt.ETL_SOURCE_DATA_DELETED_FLAG = CAST(''Y'' AS VARCHAR(1 OCTETS)),
			tgt.ETL_UPDATE_TIMESTAMP = CAST(CURRENT_TIMESTAMP - CURRENT_TIMEZONE AS TIMESTAMP),
			tgt.ETL_MODIFIED_BY_JOB_ID = CAST(' || v_stored_procedure_execution_id || ' AS BIGINT)
		WITH UR;';--

	SET o_str_debug =  v_str_sql;--

	/*Debugging Logging*/
	SET v_str_sql_debug =  'INSERT INTO ' || v_process_database_name || '.AUDIT_LOG_JOB_EXECUTION_SQL_COMMANDS_H0 (JOB_EXECUTION_ID, SQL_SMNT) VALUES (' || v_stored_procedure_execution_id ||','|| quote_literal(v_str_sql) || ');';  EXECUTE IMMEDIATE v_str_sql_debug;--

	EXECUTE IMMEDIATE  v_str_sql;--

	/*Populate Hub Load Table - 2nd Insert: GPSTORE -> DIM_ADDRESS_PHYSICAL*/
	SET v_str_sql = 'INSERT INTO ' || v_staging_schema_read_write || '.' || v_source_table_name || '
		(ADDRESS_PHYSICAL_ID, ACTIVE_FLAG, EFFECTIVE_START_DATETIME, EFFECTIVE_END_DATETIME, LOOKUP_HASH_ADDRESS_PHYSICAL, PHYSICAL_ADDRESS_LINE_1, PHYSICAL_ADDRESS_LINE_2, PHYSICAL_ADDRESS_SUITE, PHYSICAL_ADDRESS_CITY, PHYSICAL_ADDRESS_POSTAL_CODE, PHYSICAL_ADDRESS_POSTAL_CODE_EXTENSION, PHYSICAL_ADDRESS_STATE_PROVINCE_CODE, PHYSICAL_ADDRESS_COUNTRY_CODE, PHYSICAL_ADDRESS_STORE_ADDRESS_FLAG, PHYSICAL_ADDRESS_VERIFICATION_FLAG, PHYSICAL_ADDRESS_ANONIMZATION_FLAG, GEOGRAPHY_ID, PHYSICAL_ADDRESS_LATITUDE, PHYSICAL_ADDRESS_LONGITUDE, ETL_SOURCE_DATA_DELETED_FLAG, ETL_SOURCE_TABLE_NAME, ETL_CREATE_TIMESTAMP, ETL_UPDATE_TIMESTAMP, ETL_MODIFIED_BY_JOB_ID, ETL_MODIFIED_BY_PROCESS)
		SELECT
		CAST(EDW_PROCESS.SEQ_MASTER_MEMBER_ID_H0.NEXTVAL AS BIGINT) AS ADDRESS_PHYSICAL_ID,
			CAST(''Y'' AS VARCHAR(1 OCTETS)) AS ACTIVE_FLAG,
			CAST(''1900-01-01-00.00.00'' AS TIMESTAMP) AS EFFECTIVE_START_DATETIME,
			CAST(NULL AS TIMESTAMP) AS EFFECTIVE_END_DATETIME,
			gpstore_ses.LOOKUP_HASH_ADDRESS_PHYSICAL AS LOOKUP_HASH_ADDRESS_PHYSICAL,
			CAST(TRIM(gpstore_ses.ADDR2) AS VARCHAR(128 OCTETS)) AS PHYSICAL_ADDRESS_LINE_1,
			CAST(COALESCE(TRIM(gpstore_ses.ADDR1), '''') AS VARCHAR(128 OCTETS)) AS PHYSICAL_ADDRESS_LINE_2,
			CAST('''' AS VARCHAR(128 OCTETS)) AS PHYSICAL_ADDRESS_SUITE,
			CAST(TRIM(gpstore_ses.CITY) AS VARCHAR(128 OCTETS)) AS PHYSICAL_ADDRESS_CITY,
			CAST(LEFT(LEFT(REPLACE(TRIM(REPLACE(TRIM(gpstore_ses.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 5) AS VARCHAR(64 OCTETS)) AS PHYSICAL_ADDRESS_POSTAL_CODE,
			CAST(RIGHT(LEFT(REPLACE(TRIM(REPLACE(TRIM(gpstore_ses.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 4) AS VARCHAR(64 OCTETS)) AS PHYSICAL_ADDRESS_POSTAL_CODE_EXTENSION,
			CAST(TRIM(gpstore_ses.STATE) AS VARCHAR(64 OCTETS)) AS PHYSICAL_ADDRESS_STATE_PROVINCE_CODE,
			CAST(''US'' AS VARCHAR(512 OCTETS)) AS PHYSICAL_ADDRESS_COUNTRY_CODE,
			CAST(''Y'' AS VARCHAR(1 OCTETS)) AS PHYSICAL_ADDRESS_STORE_ADDRESS_FLAG,
			CAST(''N'' AS VARCHAR(1 OCTETS)) AS PHYSICAL_ADDRESS_VERIFICATION_FLAG,
			CAST(''N'' AS VARCHAR(1 OCTETS)) AS PHYSICAL_ADDRESS_ANONIMZATION_FLAG,
			CAST(COALESCE(hub_load_DIM_GEOGRAPHY_POSTAL.GEOGRAPHY_ID, -2) AS BIGINT) AS GEOGRAPHY_ID,
			CAST(COALESCE(gpstore_ses.LATITUDE,0.0) AS DECIMAL(13,7)) AS PHYSICAL_ADDRESS_LATITUDE,
			CAST(COALESCE(gpstore_ses.LONGITUDE,0.0) AS DECIMAL(13,7)) AS PHYSICAL_ADDRESS_LONGITUDE,
			CAST(''N'' AS VARCHAR(1 OCTETS)) AS ETL_SOURCE_DATA_DELETED_FLAG,
			CAST(' || quote_literal(v_etl_source_table_2) || ' AS VARCHAR(256 OCTETS)) AS ETL_SOURCE_TABLE_NAME,
			CAST(CURRENT_TIMESTAMP - CURRENT_TIMEZONE AS TIMESTAMP) AS ETL_CREATE_TIMESTAMP,
			CAST(CURRENT_TIMESTAMP - CURRENT_TIMEZONE AS TIMESTAMP) AS ETL_UPDATE_TIMESTAMP,
			CAST(' || v_stored_procedure_execution_id || ' AS BIGINT) AS ETL_MODIFIED_BY_JOB_ID,
			CAST(' || quote_literal(v_hub_procedure_name) || ' AS VARCHAR(128 OCTETS)) AS ETL_MODIFIED_BY_PROCESS
		FROM SESSION.GPSTORE_SESSION gpstore_ses
		LEFT JOIN ' || v_staging_schema_read_write || '.' || v_source_table_name || ' hub_load
			ON hub_load.LOOKUP_HASH_ADDRESS_PHYSICAL = gpstore_ses.LOOKUP_HASH_ADDRESS_PHYSICAL
		LEFT JOIN SESSION.C_B_ADDR_SESSION c_b_addr_ses
			ON c_b_addr_ses.LOOKUP_HASH_ADDRESS_PHYSICAL = gpstore_ses.LOOKUP_HASH_ADDRESS_PHYSICAL
		LEFT JOIN ' || v_staging_database_name || '.HUB_LOAD_DIM_GEOGRAPHY hub_load_DIM_GEOGRAPHY_POSTAL
			ON hub_load_DIM_GEOGRAPHY_POSTAL.GEOGRAPHY_HIERARCHY_LEVEL_MEMBER_CODE = TRIM(''USA'' || ''-'' || TRIM(gpstore_ses.STATE) || ''-'' || TRIM(gpstore_ses.CITY) || ''-'' || LEFT(LEFT(REPLACE(TRIM(REPLACE(TRIM(gpstore_ses.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 5))
		WHERE
			hub_load.LOOKUP_HASH_ADDRESS_PHYSICAL IS NULL
			AND c_b_addr_ses.LOOKUP_HASH_ADDRESS_PHYSICAL IS NULL
		GROUP BY
			gpstore_ses.LOOKUP_HASH_ADDRESS_PHYSICAL,
			TRIM(gpstore_ses.ADDR2),
			TRIM(gpstore_ses.ADDR1),
			TRIM(gpstore_ses.CITY),
			LEFT(LEFT(REPLACE(TRIM(REPLACE(TRIM(gpstore_ses.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 5),
			RIGHT(LEFT(REPLACE(TRIM(REPLACE(TRIM(gpstore_ses.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 4),
			TRIM(gpstore_ses.STATE),
			COALESCE(hub_load_DIM_GEOGRAPHY_POSTAL.GEOGRAPHY_ID, -2),
			gpstore_ses.LATITUDE,
			gpstore_ses.LONGITUDE
		WITH UR;';--

	SET o_str_debug =  v_str_sql;--

   /*Debugging Logging*/
	SET v_str_sql_debug =  'INSERT INTO ' || v_process_database_name || '.AUDIT_LOG_JOB_EXECUTION_SQL_COMMANDS_H0 (JOB_EXECUTION_ID, SQL_SMNT) VALUES (' || v_stored_procedure_execution_id ||','|| quote_literal(v_str_sql) || ');';  EXECUTE IMMEDIATE v_str_sql_debug;--

	EXECUTE IMMEDIATE  v_str_sql;--

	/*Update Hub Load Table - 2nd Merge: GPSTORE -> DIM_ADDRESS_PHYSCIAL*/
	SET v_str_sql = 'MERGE INTO ' || v_staging_schema_read_write || '.' || v_source_table_name || ' AS tgt
		USING (
			SELECT DISTINCT
				CAST(''Y'' AS VARCHAR(1 OCTETS)) AS ACTIVE_FLAG,
				CAST(''1900-01-01-00.00.00'' AS TIMESTAMP) AS EFFECTIVE_START_DATETIME,
				CAST(NULL AS TIMESTAMP) AS EFFECTIVE_END_DATETIME,
				gpstore_ses.LOOKUP_HASH_ADDRESS_PHYSICAL AS LOOKUP_HASH_ADDRESS_PHYSICAL,
				CAST(TRIM(gpstore_ses.ADDR2) AS VARCHAR(128 OCTETS)) AS PHYSICAL_ADDRESS_LINE_1,
				CAST(COALESCE(TRIM(gpstore_ses.ADDR1), '''') AS VARCHAR(128 OCTETS)) AS PHYSICAL_ADDRESS_LINE_2,
				CAST('''' AS VARCHAR(128 OCTETS)) AS PHYSICAL_ADDRESS_SUITE,
				CAST(TRIM(gpstore_ses.CITY) AS VARCHAR(128 OCTETS)) AS PHYSICAL_ADDRESS_CITY,
				CAST(LEFT(LEFT(REPLACE(TRIM(REPLACE(TRIM(gpstore_ses.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 5) AS VARCHAR(64 OCTETS)) AS PHYSICAL_ADDRESS_POSTAL_CODE,
				CAST(RIGHT(LEFT(REPLACE(TRIM(REPLACE(TRIM(gpstore_ses.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 4) AS VARCHAR(64 OCTETS)) AS PHYSICAL_ADDRESS_POSTAL_CODE_EXTENSION,
				CAST(TRIM(gpstore_ses.STATE) AS VARCHAR(64 OCTETS)) AS PHYSICAL_ADDRESS_STATE_PROVINCE_CODE,
				CAST(''US'' AS VARCHAR(512 OCTETS)) AS PHYSICAL_ADDRESS_COUNTRY_CODE,
				CAST(''Y'' AS VARCHAR(1 OCTETS)) AS PHYSICAL_ADDRESS_STORE_ADDRESS_FLAG,
				CAST(''N'' AS VARCHAR(1 OCTETS)) AS PHYSICAL_ADDRESS_VERIFICATION_FLAG,
				CAST(''N'' AS VARCHAR(1 OCTETS)) AS PHYSICAL_ADDRESS_ANONIMZATION_FLAG,
				CAST(COALESCE(hub_load_DIM_GEOGRAPHY_POSTAL.GEOGRAPHY_ID, -2) AS BIGINT) AS GEOGRAPHY_ID,
				CAST(COALESCE(gpstore_ses.LATITUDE,0.0) AS DECIMAL(13,7)) AS PHYSICAL_ADDRESS_LATITUDE,
				CAST(COALESCE(gpstore_ses.LONGITUDE,0.0) AS DECIMAL(13,7)) AS PHYSICAL_ADDRESS_LONGITUDE,
				CAST(''N'' AS VARCHAR(1 OCTETS)) AS ETL_SOURCE_DATA_DELETED_FLAG,
				CAST(' || quote_literal(v_etl_source_table_2) || ' AS VARCHAR(256 OCTETS)) AS ETL_SOURCE_TABLE_NAME,
				CAST(CURRENT_TIMESTAMP - CURRENT_TIMEZONE AS TIMESTAMP) AS ETL_CREATE_TIMESTAMP,
				CAST(CURRENT_TIMESTAMP - CURRENT_TIMEZONE AS TIMESTAMP) AS ETL_UPDATE_TIMESTAMP,
				CAST(' || v_stored_procedure_execution_id || ' AS BIGINT) AS ETL_MODIFIED_BY_JOB_ID,
				CAST(' || quote_literal(v_hub_procedure_name) || ' AS VARCHAR(128 OCTETS)) AS ETL_MODIFIED_BY_PROCESS
			FROM SESSION.GPSTORE_SESSION gpstore_ses
			LEFT JOIN ' || v_staging_schema_read_write || '.' || v_source_table_name || ' hub_load
				ON hub_load.LOOKUP_HASH_ADDRESS_PHYSICAL = gpstore_ses.LOOKUP_HASH_ADDRESS_PHYSICAL
			LEFT JOIN SESSION.C_B_ADDR_SESSION c_b_addr_ses
				ON c_b_addr_ses.LOOKUP_HASH_ADDRESS_PHYSICAL = gpstore_ses.LOOKUP_HASH_ADDRESS_PHYSICAL
			LEFT JOIN ' || v_staging_database_name || '.HUB_LOAD_DIM_GEOGRAPHY hub_load_DIM_GEOGRAPHY_POSTAL
				ON hub_load_DIM_GEOGRAPHY_POSTAL.GEOGRAPHY_HIERARCHY_LEVEL_MEMBER_CODE = TRIM(''USA'' || ''-'' || TRIM(gpstore_ses.STATE) || ''-'' || TRIM(gpstore_ses.CITY) || ''-'' || LEFT(LEFT(REPLACE(TRIM(REPLACE(TRIM(gpstore_ses.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 5))
			WHERE
				hub_load.LOOKUP_HASH_ADDRESS_PHYSICAL IS NOT NULL
				AND c_b_addr_ses.LOOKUP_HASH_ADDRESS_PHYSICAL IS NULL) AS src
			ON (tgt.LOOKUP_HASH_ADDRESS_PHYSICAL = src.LOOKUP_HASH_ADDRESS_PHYSICAL)
		WHEN MATCHED AND(
			COALESCE(tgt.PHYSICAL_ADDRESS_STORE_ADDRESS_FLAG, ''A'') <> COALESCE(src.PHYSICAL_ADDRESS_STORE_ADDRESS_FLAG, ''A'') OR
			COALESCE(tgt.PHYSICAL_ADDRESS_VERIFICATION_FLAG, ''N'') <> COALESCE(src.PHYSICAL_ADDRESS_VERIFICATION_FLAG, ''N'') OR
			COALESCE(tgt.PHYSICAL_ADDRESS_ANONIMZATION_FLAG, ''N'') <> COALESCE(src.PHYSICAL_ADDRESS_ANONIMZATION_FLAG, ''N'') OR
			COALESCE(tgt.GEOGRAPHY_ID, -2) <> COALESCE(src.GEOGRAPHY_ID, -2) OR
			COALESCE(tgt.PHYSICAL_ADDRESS_LATITUDE, 0.0) <> COALESCE(src.PHYSICAL_ADDRESS_LATITUDE, 0.0) OR
			COALESCE(tgt.PHYSICAL_ADDRESS_LONGITUDE, 0.0) <> COALESCE(src.PHYSICAL_ADDRESS_LONGITUDE, 0.0) OR
			COALESCE(tgt.ETL_SOURCE_DATA_DELETED_FLAG, ''N'') <> COALESCE(src.ETL_SOURCE_DATA_DELETED_FLAG, ''N'') OR
			COALESCE(tgt.ETL_SOURCE_TABLE_NAME, ''A'') <> COALESCE(src.ETL_SOURCE_TABLE_NAME, ''A''))
		THEN UPDATE SET
			tgt.PHYSICAL_ADDRESS_STORE_ADDRESS_FLAG=src.PHYSICAL_ADDRESS_STORE_ADDRESS_FLAG,
			tgt.PHYSICAL_ADDRESS_VERIFICATION_FLAG=src.PHYSICAL_ADDRESS_VERIFICATION_FLAG,
			tgt.PHYSICAL_ADDRESS_ANONIMZATION_FLAG=src.PHYSICAL_ADDRESS_ANONIMZATION_FLAG,
			tgt.GEOGRAPHY_ID=src.GEOGRAPHY_ID,
			tgt.PHYSICAL_ADDRESS_LATITUDE=src.PHYSICAL_ADDRESS_LATITUDE,
			tgt.PHYSICAL_ADDRESS_LONGITUDE=src.PHYSICAL_ADDRESS_LONGITUDE,
			tgt.ETL_SOURCE_DATA_DELETED_FLAG=src.ETL_SOURCE_DATA_DELETED_FLAG,
			tgt.ETL_SOURCE_TABLE_NAME=src.ETL_SOURCE_TABLE_NAME,
			tgt.ETL_UPDATE_TIMESTAMP=src.ETL_UPDATE_TIMESTAMP,
			tgt.ETL_MODIFIED_BY_JOB_ID=src.ETL_MODIFIED_BY_JOB_ID,
			tgt.ETL_MODIFIED_BY_PROCESS=src.ETL_MODIFIED_BY_PROCESS
		WITH UR;';--

	SET o_str_debug = v_str_sql;--

	/*Debugging Logging*/
	SET v_str_sql_debug =  'INSERT INTO ' || v_process_database_name || '.AUDIT_LOG_JOB_EXECUTION_SQL_COMMANDS_H0 (JOB_EXECUTION_ID, SQL_SMNT) VALUES (' || v_stored_procedure_execution_id ||','|| quote_literal(v_str_sql) || ');';  EXECUTE IMMEDIATE v_str_sql_debug;--

	EXECUTE IMMEDIATE v_str_sql;--

	/*Update Hub Load Table - 2nd Update Deletes: GPSTORE*/
	SET v_str_sql = 'MERGE INTO ' || v_staging_schema_read_write || '.' || v_source_table_name || ' AS tgt
		USING (
			SELECT tgt.LOOKUP_HASH_ADDRESS_PHYSICAL
			FROM ' || v_staging_schema_read_write || '.' || v_source_table_name || ' tgt
			LEFT JOIN (
				SELECT DISTINCT
					gpstore_ses.LOOKUP_HASH_ADDRESS_PHYSICAL
				FROM SESSION.GPSTORE_SESSION gpstore_ses) gpstore
				ON tgt.LOOKUP_HASH_ADDRESS_PHYSICAL = gpstore.LOOKUP_HASH_ADDRESS_PHYSICAL
			WHERE
				tgt.ETL_SOURCE_TABLE_NAME = ' || quote_literal(v_etl_source_table_2) || '
				AND gpstore.LOOKUP_HASH_ADDRESS_PHYSICAL IS NULL) AS src
		ON (tgt.LOOKUP_HASH_ADDRESS_PHYSICAL = src.LOOKUP_HASH_ADDRESS_PHYSICAL)
		WHEN MATCHED THEN UPDATE SET
			tgt.ETL_SOURCE_DATA_DELETED_FLAG = CAST(''Y'' AS VARCHAR(1 OCTETS)),
			tgt.ETL_UPDATE_TIMESTAMP = CAST(CURRENT_TIMESTAMP - CURRENT_TIMEZONE AS TIMESTAMP),
			tgt.ETL_MODIFIED_BY_JOB_ID = CAST(' || v_stored_procedure_execution_id || ' AS BIGINT)
		WITH UR;';--

	SET o_str_debug =  v_str_sql;--

	/*Debugging Logging*/
	SET v_str_sql_debug =  'INSERT INTO ' || v_process_database_name || '.AUDIT_LOG_JOB_EXECUTION_SQL_COMMANDS_H0 (JOB_EXECUTION_ID, SQL_SMNT) VALUES (' || v_stored_procedure_execution_id ||','|| quote_literal(v_str_sql) || ');';  EXECUTE IMMEDIATE v_str_sql_debug;--

	EXECUTE IMMEDIATE  v_str_sql;--

	/*Populate Hub Load Table - 3rd Insert: DBARCUD -> DIM_ADDRESS_PHYSICAL*/
	SET v_str_sql = 'INSERT INTO ' || v_staging_schema_read_write || '.' || v_source_table_name || '
		(ADDRESS_PHYSICAL_ID, ACTIVE_FLAG, EFFECTIVE_START_DATETIME, EFFECTIVE_END_DATETIME, LOOKUP_HASH_ADDRESS_PHYSICAL, PHYSICAL_ADDRESS_LINE_1, PHYSICAL_ADDRESS_LINE_2, PHYSICAL_ADDRESS_SUITE, PHYSICAL_ADDRESS_CITY, PHYSICAL_ADDRESS_POSTAL_CODE, PHYSICAL_ADDRESS_POSTAL_CODE_EXTENSION, PHYSICAL_ADDRESS_STATE_PROVINCE_CODE, PHYSICAL_ADDRESS_COUNTRY_CODE, PHYSICAL_ADDRESS_STORE_ADDRESS_FLAG, PHYSICAL_ADDRESS_VERIFICATION_FLAG, PHYSICAL_ADDRESS_ANONIMZATION_FLAG, GEOGRAPHY_ID, PHYSICAL_ADDRESS_LATITUDE, PHYSICAL_ADDRESS_LONGITUDE, ETL_SOURCE_DATA_DELETED_FLAG, ETL_SOURCE_TABLE_NAME, ETL_CREATE_TIMESTAMP, ETL_UPDATE_TIMESTAMP, ETL_MODIFIED_BY_JOB_ID, ETL_MODIFIED_BY_PROCESS)
		SELECT
			CAST(EDW_PROCESS.SEQ_MASTER_MEMBER_ID_H0.NEXTVAL AS BIGINT) AS ADDRESS_PHYSICAL_ID,
			CAST(''Y'' AS VARCHAR(1 OCTETS)) AS ACTIVE_FLAG,
			CAST(''1900-01-01-00.00.00'' AS TIMESTAMP) AS EFFECTIVE_START_DATETIME,
			CAST(NULL AS TIMESTAMP) AS EFFECTIVE_END_DATETIME,
			CAST(HASH(CAST(
				COALESCE(COALESCE(TRIM(dbarcud.ADDR2), ''''), ''#'') || ''&'' ||
				COALESCE(COALESCE(TRIM(dbarcud.ADDR1), ''''), ''$'') || ''&'' ||
				''@'' || ''&'' ||
				COALESCE(COALESCE(TRIM(dbarcud.CITY), ''''), ''D'') || ''&'' ||
				COALESCE(LEFT(LEFT(REPLACE(TRIM(REPLACE(TRIM(dbarcud.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 5), ''E'') || ''&'' ||
				COALESCE(RIGHT(LEFT(REPLACE(TRIM(REPLACE(TRIM(dbarcud.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 4), ''F'') || ''&'' ||
				COALESCE(COALESCE(TRIM(dbarcud.STATE), ''''), ''G'') || ''&'' ||
				COALESCE(CASE
					WHEN TRIM(dbarcud.COUNTRY_CODE) = '''' THEN ''US''
					ELSE TRIM(dbarcud.COUNTRY_CODE)
				END, ''H'') AS VARCHAR), 3) AS VARBINARY(64)) AS LOOKUP_HASH_ADDRESS_PHYSICAL,
			CAST(TRIM(dbarcud.ADDR2) AS VARCHAR(128 OCTETS)) AS PHYSICAL_ADDRESS_LINE_1,
			CAST(COALESCE(TRIM(dbarcud.ADDR1), '''') AS VARCHAR(128 OCTETS)) AS PHYSICAL_ADDRESS_LINE_2,
			CAST('''' AS VARCHAR(128 OCTETS)) AS PHYSICAL_ADDRESS_SUITE,
			CAST(TRIM(dbarcud.CITY) AS VARCHAR(128 OCTETS)) AS PHYSICAL_ADDRESS_CITY,
			CAST(LEFT(LEFT(REPLACE(TRIM(REPLACE(TRIM(dbarcud.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 5) AS VARCHAR(5)) AS PHYSICAL_ADDRESS_POSTAL_CODE,
			CAST(RIGHT(LEFT(REPLACE(TRIM(REPLACE(TRIM(dbarcud.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 4) AS VARCHAR(64 OCTETS)) AS PHYSICAL_ADDRESS_POSTAL_CODE_EXTENSION,
			CAST(TRIM(dbarcud.STATE) AS VARCHAR(64 OCTETS)) AS PHYSICAL_ADDRESS_STATE_PROVINCE_CODE,
			CAST(CASE
				WHEN TRIM(dbarcud.COUNTRY_CODE) = '''' THEN ''US''
				ELSE TRIM(dbarcud.COUNTRY_CODE)
			END AS VARCHAR(512 OCTETS)) AS PHYSICAL_ADDRESS_COUNTRY_CODE,
			CAST(''N'' AS VARCHAR(1 OCTETS)) AS PHYSICAL_ADDRESS_STORE_ADDRESS_FLAG,
			CAST(''N'' AS VARCHAR(1 OCTETS)) AS PHYSICAL_ADDRESS_VERIFICATION_FLAG,
			CAST(''N'' AS VARCHAR(1 OCTETS)) AS PHYSICAL_ADDRESS_ANONIMZATION_FLAG,
			CAST(COALESCE(hub_load_DIM_GEOGRAPHY_POSTAL.GEOGRAPHY_ID, -2) AS BIGINT) AS GEOGRAPHY_ID,
			CAST(0.0 AS DECIMAL(13,7)) AS PHYSICAL_ADDRESS_LATITUDE,
			CAST(0.0 AS DECIMAL(13,7)) AS PHYSICAL_ADDRESS_LONGITUDE,
			CAST(''N'' AS VARCHAR(1 OCTETS)) AS ETL_SOURCE_DATA_DELETED_FLAG,
			CAST(' || quote_literal(v_etl_source_table_3) || ' AS VARCHAR(256 OCTETS)) AS ETL_SOURCE_TABLE_NAME,
			CAST(CURRENT_TIMESTAMP - CURRENT_TIMEZONE AS TIMESTAMP) AS ETL_CREATE_TIMESTAMP,
			CAST(CURRENT_TIMESTAMP - CURRENT_TIMEZONE AS TIMESTAMP) AS ETL_UPDATE_TIMESTAMP,
			CAST(' || v_stored_procedure_execution_id || ' AS BIGINT) AS ETL_MODIFIED_BY_JOB_ID,
			CAST(' || quote_literal(v_hub_procedure_name) || ' AS VARCHAR(128 OCTETS)) AS ETL_MODIFIED_BY_PROCESS
		FROM ' || v_etl_source_database_name || '.CUR_CUS_DBARCUD dbarcud
		INNER JOIN ' || v_etl_source_database_name || '.CUR_CUS_CMASTER cmaster
			ON CAST(cmaster.CCUST# AS VARCHAR) = TRIM(dbarcud.CUSTOMER)
		LEFT JOIN ' || v_staging_schema_read_write || '.' || v_source_table_name || ' hub_load
			ON hub_load.LOOKUP_HASH_ADDRESS_PHYSICAL = HASH(CAST(
				COALESCE(COALESCE(TRIM(dbarcud.ADDR2), ''''), ''#'') || ''&'' ||
				COALESCE(COALESCE(TRIM(dbarcud.ADDR1), ''''), ''$'') || ''&'' ||
				''@'' || ''&'' ||
				COALESCE(COALESCE(TRIM(dbarcud.CITY), ''''), ''D'') || ''&'' ||
				COALESCE(LEFT(LEFT(REPLACE(TRIM(REPLACE(TRIM(dbarcud.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 5), ''E'') || ''&'' ||
				COALESCE(RIGHT(LEFT(REPLACE(TRIM(REPLACE(TRIM(dbarcud.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 4), ''F'') || ''&'' ||
				COALESCE(COALESCE(TRIM(dbarcud.STATE), ''''), ''G'') || ''&'' ||
				COALESCE(CASE
					WHEN TRIM(dbarcud.COUNTRY_CODE) = '''' THEN ''US''
					ELSE TRIM(dbarcud.COUNTRY_CODE)
				END, ''H'') AS VARCHAR), 3)
		LEFT JOIN SESSION.C_B_ADDR_SESSION c_b_addr_ses
				ON c_b_addr_ses.LOOKUP_HASH_ADDRESS_PHYSICAL = HASH(CAST(
					COALESCE(COALESCE(TRIM(dbarcud.ADDR2), ''''), ''#'') || ''&'' ||
					COALESCE(COALESCE(TRIM(dbarcud.ADDR1), ''''), ''$'') || ''&'' ||
					''@'' || ''&'' ||
					COALESCE(COALESCE(TRIM(dbarcud.CITY), ''''), ''D'') || ''&'' ||
					COALESCE(LEFT(LEFT(REPLACE(TRIM(REPLACE(TRIM(dbarcud.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 5), ''E'') || ''&'' ||
					COALESCE(RIGHT(LEFT(REPLACE(TRIM(REPLACE(TRIM(dbarcud.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 4), ''F'') || ''&'' ||
					COALESCE(COALESCE(TRIM(dbarcud.STATE), ''''), ''G'') || ''&'' ||
					COALESCE(CASE
						WHEN TRIM(dbarcud.COUNTRY_CODE) = '''' THEN ''US''
						ELSE TRIM(dbarcud.COUNTRY_CODE)
					END, ''H'') AS VARCHAR), 3)
		LEFT JOIN SESSION.GPSTORE_SESSION gpstore_ses
			ON gpstore_ses.LOOKUP_HASH_ADDRESS_PHYSICAL = HASH(CAST(
				COALESCE(COALESCE(TRIM(dbarcud.ADDR2), ''''), ''#'') || ''&'' ||
				COALESCE(COALESCE(TRIM(dbarcud.ADDR1), ''''), ''$'') || ''&'' ||
				''@'' || ''&'' ||
				COALESCE(COALESCE(TRIM(dbarcud.CITY), ''''), ''D'') || ''&'' ||
				COALESCE(LEFT(LEFT(REPLACE(TRIM(REPLACE(TRIM(dbarcud.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 5), ''E'') || ''&'' ||
				COALESCE(RIGHT(LEFT(REPLACE(TRIM(REPLACE(TRIM(dbarcud.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 4), ''F'') || ''&'' ||
				COALESCE(COALESCE(TRIM(dbarcud.STATE), ''''), ''G'') || ''&'' ||
				COALESCE(CASE
					WHEN TRIM(dbarcud.COUNTRY_CODE) = '''' THEN ''US''
					ELSE TRIM(dbarcud.COUNTRY_CODE)
				END, ''H'') AS VARCHAR), 3)
		LEFT JOIN ' || v_staging_database_name || '.HUB_LOAD_DIM_GEOGRAPHY hub_load_DIM_GEOGRAPHY_POSTAL
			ON CASE
				WHEN dbarcud.COUNTRY_CODE IN (''MX'') THEN TRIM(''MEX'' || ''-'' || TRIM(dbarcud.STATE))
				WHEN dbarcud.COUNTRY_CODE IN (''CA'') THEN TRIM(''CAN'' || ''-'' || TRIM(dbarcud.STATE) || ''-'' || TRIM(dbarcud.CITY) || ''-'' || TRIM(LEFT(REPLACE(TRIM(REPLACE(TRIM(dbarcud.ZIP), ''-'', '' '')), '' '', '''') || ''0000'', 9)/1000))
				ELSE TRIM(''USA'' || ''-'' || TRIM(dbarcud.STATE) || ''-'' || TRIM(dbarcud.CITY) || ''-'' || LEFT(LEFT(REPLACE(TRIM(REPLACE(TRIM(dbarcud.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 5))
			END = hub_load_DIM_GEOGRAPHY_POSTAL.GEOGRAPHY_HIERARCHY_LEVEL_MEMBER_CODE
		WHERE
			LENGTH(TRIM(TRANSLATE(REPLACE(TRIM(dbarcud.CUSTOMER), '' '', ''X''), '''', ''0123456789''))) = 0
			AND LENGTH(TRIM(TRANSLATE(REPLACE(TRIM(REPLACE(TRIM(dbarcud.ZIP), ''-'', '' '')), '' '', ''''), '''', ''0123456789''))) = 0
			AND hub_load.LOOKUP_HASH_ADDRESS_PHYSICAL IS NULL
			AND c_b_addr_ses.LOOKUP_HASH_ADDRESS_PHYSICAL IS NULL
			AND gpstore_ses.LOOKUP_HASH_ADDRESS_PHYSICAL IS NULL
		GROUP BY
			HASH(CAST(
				COALESCE(COALESCE(TRIM(dbarcud.ADDR2), ''''), ''#'') || ''&'' ||
				COALESCE(COALESCE(TRIM(dbarcud.ADDR1), ''''), ''$'') || ''&'' ||
				''@'' || ''&'' ||
				COALESCE(COALESCE(TRIM(dbarcud.CITY), ''''), ''D'') || ''&'' ||
				COALESCE(LEFT(LEFT(REPLACE(TRIM(REPLACE(TRIM(dbarcud.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 5), ''E'') || ''&'' ||
				COALESCE(RIGHT(LEFT(REPLACE(TRIM(REPLACE(TRIM(dbarcud.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 4), ''F'') || ''&'' ||
				COALESCE(COALESCE(TRIM(dbarcud.STATE), ''''), ''G'') || ''&'' ||
				COALESCE(CASE
					WHEN TRIM(dbarcud.COUNTRY_CODE) = '''' THEN ''US''
					ELSE TRIM(dbarcud.COUNTRY_CODE)
				END, ''H'') AS VARCHAR), 3),
			TRIM(dbarcud.ADDR2),
			COALESCE(TRIM(dbarcud.ADDR1), ''''),
			TRIM(dbarcud.CITY),
			LEFT(LEFT(REPLACE(TRIM(REPLACE(TRIM(dbarcud.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 5),
			RIGHT(LEFT(REPLACE(TRIM(REPLACE(TRIM(dbarcud.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 4),
			TRIM(dbarcud.STATE),
			CASE
				WHEN TRIM(dbarcud.COUNTRY_CODE) = '''' THEN ''US''
				ELSE TRIM(dbarcud.COUNTRY_CODE)
			END,
			COALESCE(hub_load_DIM_GEOGRAPHY_POSTAL.GEOGRAPHY_ID, -2)
		WITH UR;';--

	SET o_str_debug =  v_str_sql;--

	/*Debugging Logging*/
	SET v_str_sql_debug =  'INSERT INTO ' || v_process_database_name || '.AUDIT_LOG_JOB_EXECUTION_SQL_COMMANDS_H0 (JOB_EXECUTION_ID, SQL_SMNT) VALUES (' || v_stored_procedure_execution_id ||','|| quote_literal(v_str_sql) || ');';  EXECUTE IMMEDIATE v_str_sql_debug;--

	EXECUTE IMMEDIATE  v_str_sql;--

	/*Update Hub Load Table - 3rd Merge: DBARCUD -> DIM_ADDRESS_PHYSICAL*/
	SET v_str_sql = 'MERGE INTO ' || v_staging_schema_read_write || '.' || v_source_table_name || ' AS tgt
		USING (
			SELECT DISTINCT
				CAST(''Y'' AS VARCHAR(1 OCTETS)) AS ACTIVE_FLAG,
				CAST(''1900-01-01-00.00.00'' AS TIMESTAMP) AS EFFECTIVE_START_DATETIME,
				CAST(NULL AS TIMESTAMP) AS EFFECTIVE_END_DATETIME,
				CAST(HASH(CAST(
					COALESCE(COALESCE(TRIM(dbarcud.ADDR2), ''''), ''#'') || ''&'' ||
					COALESCE(COALESCE(TRIM(dbarcud.ADDR1), ''''), ''$'') || ''&'' ||
					''@'' || ''&'' ||
					COALESCE(COALESCE(TRIM(dbarcud.CITY), ''''), ''D'') || ''&'' ||
					COALESCE(LEFT(LEFT(REPLACE(TRIM(REPLACE(TRIM(dbarcud.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 5), ''E'') || ''&'' ||
					COALESCE(RIGHT(LEFT(REPLACE(TRIM(REPLACE(TRIM(dbarcud.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 4), ''F'') || ''&'' ||
					COALESCE(COALESCE(TRIM(dbarcud.STATE), ''''), ''G'') || ''&'' ||
					COALESCE(CASE
						WHEN TRIM(dbarcud.COUNTRY_CODE) = '''' THEN ''US''
						ELSE TRIM(dbarcud.COUNTRY_CODE)
					END, ''H'') AS VARCHAR), 3) AS VARBINARY(64)) AS LOOKUP_HASH_ADDRESS_PHYSICAL,
				CAST(TRIM(dbarcud.ADDR2) AS VARCHAR(128 OCTETS)) AS PHYSICAL_ADDRESS_LINE_1,
				CAST(COALESCE(TRIM(dbarcud.ADDR1), '''') AS VARCHAR(128 OCTETS)) AS PHYSICAL_ADDRESS_LINE_2,
				CAST('''' AS VARCHAR(128 OCTETS)) AS PHYSICAL_ADDRESS_SUITE,
				CAST(TRIM(dbarcud.CITY) AS VARCHAR(128 OCTETS)) AS PHYSICAL_ADDRESS_CITY,
				CAST(LEFT(LEFT(REPLACE(TRIM(REPLACE(TRIM(dbarcud.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 5) AS VARCHAR(5)) AS PHYSICAL_ADDRESS_POSTAL_CODE,
				CAST(RIGHT(LEFT(REPLACE(TRIM(REPLACE(TRIM(dbarcud.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 4) AS VARCHAR(64 OCTETS)) AS PHYSICAL_ADDRESS_POSTAL_CODE_EXTENSION,
				CAST(TRIM(dbarcud.STATE) AS VARCHAR(64 OCTETS)) AS PHYSICAL_ADDRESS_STATE_PROVINCE_CODE,
				CAST(CASE
					WHEN TRIM(dbarcud.COUNTRY_CODE) = '''' THEN ''US''
					ELSE TRIM(dbarcud.COUNTRY_CODE)
				END AS VARCHAR(512 OCTETS)) AS PHYSICAL_ADDRESS_COUNTRY_CODE,
				CAST(''N'' AS VARCHAR(1 OCTETS)) AS PHYSICAL_ADDRESS_STORE_ADDRESS_FLAG,
				CAST(''N'' AS VARCHAR(1 OCTETS)) AS PHYSICAL_ADDRESS_VERIFICATION_FLAG,
				CAST(''N'' AS VARCHAR(1 OCTETS)) AS PHYSICAL_ADDRESS_ANONIMZATION_FLAG,
				CAST(COALESCE(hub_load_DIM_GEOGRAPHY_POSTAL.GEOGRAPHY_ID, -2) AS BIGINT) AS GEOGRAPHY_ID,
				CAST(0.0 AS DECIMAL(13,7)) AS PHYSICAL_ADDRESS_LATITUDE,
				CAST(0.0 AS DECIMAL(13,7)) AS PHYSICAL_ADDRESS_LONGITUDE,
				CAST(''N'' AS VARCHAR(1 OCTETS)) AS ETL_SOURCE_DATA_DELETED_FLAG,
				CAST(' || quote_literal(v_etl_source_table_3) || ' AS VARCHAR(256 OCTETS)) AS ETL_SOURCE_TABLE_NAME,
				CAST(CURRENT_TIMESTAMP - CURRENT_TIMEZONE AS TIMESTAMP) AS ETL_CREATE_TIMESTAMP,
				CAST(CURRENT_TIMESTAMP - CURRENT_TIMEZONE AS TIMESTAMP) AS ETL_UPDATE_TIMESTAMP,
				CAST(' || v_stored_procedure_execution_id || ' AS BIGINT) AS ETL_MODIFIED_BY_JOB_ID,
				CAST(' || quote_literal(v_hub_procedure_name) || ' AS VARCHAR(128 OCTETS)) AS ETL_MODIFIED_BY_PROCESS
			FROM ' || v_etl_source_database_name || '.CUR_CUS_DBARCUD dbarcud
			JOIN ' || v_etl_source_database_name || '.CUR_CUS_CMASTER cmaster
				ON CAST(cmaster.CCUST# AS VARCHAR) = TRIM(dbarcud.CUSTOMER)
			LEFT JOIN ' || v_staging_schema_read_write || '.' || v_source_table_name || ' hub_load
				ON hub_load.LOOKUP_HASH_ADDRESS_PHYSICAL = HASH(CAST(
					COALESCE(COALESCE(TRIM(dbarcud.ADDR2), ''''), ''#'') || ''&'' ||
					COALESCE(COALESCE(TRIM(dbarcud.ADDR1), ''''), ''$'') || ''&'' ||
					''@'' || ''&'' ||
					COALESCE(COALESCE(TRIM(dbarcud.CITY), ''''), ''D'') || ''&'' ||
					COALESCE(LEFT(LEFT(REPLACE(TRIM(REPLACE(TRIM(dbarcud.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 5), ''E'') || ''&'' ||
					COALESCE(RIGHT(LEFT(REPLACE(TRIM(REPLACE(TRIM(dbarcud.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 4), ''F'') || ''&'' ||
					COALESCE(COALESCE(TRIM(dbarcud.STATE), ''''), ''G'') || ''&'' ||
					COALESCE(CASE
						WHEN TRIM(dbarcud.COUNTRY_CODE) = '''' THEN ''US''
						ELSE TRIM(dbarcud.COUNTRY_CODE)
					END, ''H'') AS VARCHAR), 3)
			LEFT JOIN SESSION.C_B_ADDR_SESSION c_b_addr_ses
					ON c_b_addr_ses.LOOKUP_HASH_ADDRESS_PHYSICAL = HASH(CAST(
						COALESCE(COALESCE(TRIM(dbarcud.ADDR2), ''''), ''#'') || ''&'' ||
						COALESCE(COALESCE(TRIM(dbarcud.ADDR1), ''''), ''$'') || ''&'' ||
						''@'' || ''&'' ||
						COALESCE(COALESCE(TRIM(dbarcud.CITY), ''''), ''D'') || ''&'' ||
						COALESCE(LEFT(LEFT(REPLACE(TRIM(REPLACE(TRIM(dbarcud.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 5), ''E'') || ''&'' ||
						COALESCE(RIGHT(LEFT(REPLACE(TRIM(REPLACE(TRIM(dbarcud.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 4), ''F'') || ''&'' ||
						COALESCE(COALESCE(TRIM(dbarcud.STATE), ''''), ''G'') || ''&'' ||
						COALESCE(CASE
							WHEN TRIM(dbarcud.COUNTRY_CODE) = '''' THEN ''US''
							ELSE TRIM(dbarcud.COUNTRY_CODE)
						END, ''H'') AS VARCHAR), 3)
			LEFT JOIN SESSION.GPSTORE_SESSION gpstore_ses
				ON gpstore_ses.LOOKUP_HASH_ADDRESS_PHYSICAL = HASH(CAST(
						COALESCE(COALESCE(TRIM(dbarcud.ADDR2), ''''), ''#'') || ''&'' ||
						COALESCE(COALESCE(TRIM(dbarcud.ADDR1), ''''), ''$'') || ''&'' ||
						''@'' || ''&'' ||
						COALESCE(COALESCE(TRIM(dbarcud.CITY), ''''), ''D'') || ''&'' ||
						COALESCE(LEFT(LEFT(REPLACE(TRIM(REPLACE(TRIM(dbarcud.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 5), ''E'') || ''&'' ||
						COALESCE(RIGHT(LEFT(REPLACE(TRIM(REPLACE(TRIM(dbarcud.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 4), ''F'') || ''&'' ||
						COALESCE(COALESCE(TRIM(dbarcud.STATE), ''''), ''G'') || ''&'' ||
						COALESCE(CASE
							WHEN TRIM(dbarcud.COUNTRY_CODE) = '''' THEN ''US''
							ELSE TRIM(dbarcud.COUNTRY_CODE)
						END, ''H'') AS VARCHAR), 3)
			LEFT JOIN ' || v_staging_database_name || '.HUB_LOAD_DIM_GEOGRAPHY hub_load_DIM_GEOGRAPHY_POSTAL
				ON CASE
						WHEN dbarcud.COUNTRY_CODE IN (''MX'') THEN TRIM(''MEX'' || ''-'' || TRIM(dbarcud.STATE))
						WHEN dbarcud.COUNTRY_CODE IN (''CA'') THEN TRIM(''CAN'' || ''-'' || TRIM(dbarcud.STATE) || ''-'' || TRIM(dbarcud.CITY) || ''-'' || TRIM(LEFT(REPLACE(TRIM(REPLACE(TRIM(dbarcud.ZIP), ''-'', '' '')), '' '', '''') || ''0000'', 9)/1000))
						ELSE TRIM(''USA'' || ''-'' || TRIM(dbarcud.STATE) || ''-'' || TRIM(dbarcud.CITY) || ''-'' || LEFT(LEFT(REPLACE(TRIM(REPLACE(TRIM(dbarcud.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 5))
					END = hub_load_DIM_GEOGRAPHY_POSTAL.GEOGRAPHY_HIERARCHY_LEVEL_MEMBER_CODE
			WHERE
				LENGTH(TRIM(TRANSLATE(REPLACE(TRIM(dbarcud.CUSTOMER), '' '', ''X''), '''', ''0123456789''))) = 0
				AND LENGTH(TRIM(TRANSLATE(REPLACE(TRIM(REPLACE(TRIM(dbarcud.ZIP), ''-'', '' '')), '' '', ''''), '''', ''0123456789''))) = 0
				AND hub_load.LOOKUP_HASH_ADDRESS_PHYSICAL IS NOT NULL
				AND c_b_addr_ses.LOOKUP_HASH_ADDRESS_PHYSICAL IS NULL
				AND gpstore_ses.LOOKUP_HASH_ADDRESS_PHYSICAL IS NULL) AS src
			ON (tgt.LOOKUP_HASH_ADDRESS_PHYSICAL = src.LOOKUP_HASH_ADDRESS_PHYSICAL)
		WHEN MATCHED AND(
			COALESCE(tgt.PHYSICAL_ADDRESS_STORE_ADDRESS_FLAG, ''A'') <> COALESCE(src.PHYSICAL_ADDRESS_STORE_ADDRESS_FLAG, ''A'') OR
			COALESCE(tgt.PHYSICAL_ADDRESS_VERIFICATION_FLAG, ''N'') <> COALESCE(src.PHYSICAL_ADDRESS_VERIFICATION_FLAG, ''N'') OR
			COALESCE(tgt.PHYSICAL_ADDRESS_ANONIMZATION_FLAG, ''N'') <> COALESCE(src.PHYSICAL_ADDRESS_ANONIMZATION_FLAG, ''N'') OR
			COALESCE(tgt.GEOGRAPHY_ID, -2) <> COALESCE(src.GEOGRAPHY_ID, -2) OR

			COALESCE(tgt.PHYSICAL_ADDRESS_LATITUDE, 0.0) <> COALESCE(src.PHYSICAL_ADDRESS_LATITUDE, 0.0) OR
			COALESCE(tgt.PHYSICAL_ADDRESS_LONGITUDE, 0.0) <> COALESCE(src.PHYSICAL_ADDRESS_LONGITUDE, 0.0) OR
			COALESCE(tgt.ETL_SOURCE_DATA_DELETED_FLAG, ''N'') <> COALESCE(src.ETL_SOURCE_DATA_DELETED_FLAG, ''N'') OR
			COALESCE(tgt.ETL_SOURCE_TABLE_NAME, ''A'') <> COALESCE(src.ETL_SOURCE_TABLE_NAME, ''A''))
		THEN UPDATE SET
			tgt.PHYSICAL_ADDRESS_STORE_ADDRESS_FLAG=src.PHYSICAL_ADDRESS_STORE_ADDRESS_FLAG,
			tgt.PHYSICAL_ADDRESS_VERIFICATION_FLAG=src.PHYSICAL_ADDRESS_VERIFICATION_FLAG,
			tgt.PHYSICAL_ADDRESS_ANONIMZATION_FLAG=src.PHYSICAL_ADDRESS_ANONIMZATION_FLAG,
			tgt.GEOGRAPHY_ID=src.GEOGRAPHY_ID,
			tgt.PHYSICAL_ADDRESS_LATITUDE=src.PHYSICAL_ADDRESS_LATITUDE,
			tgt.PHYSICAL_ADDRESS_LONGITUDE=src.PHYSICAL_ADDRESS_LONGITUDE,
			tgt.ETL_SOURCE_DATA_DELETED_FLAG=src.ETL_SOURCE_DATA_DELETED_FLAG,
			tgt.ETL_SOURCE_TABLE_NAME=src.ETL_SOURCE_TABLE_NAME,
			tgt.ETL_UPDATE_TIMESTAMP=src.ETL_UPDATE_TIMESTAMP,
			tgt.ETL_MODIFIED_BY_JOB_ID=src.ETL_MODIFIED_BY_JOB_ID,
			tgt.ETL_MODIFIED_BY_PROCESS=src.ETL_MODIFIED_BY_PROCESS
		WITH UR;';--

	SET o_str_debug = v_str_sql;--

	/*Debugging Logging*/
	SET v_str_sql_debug =  'INSERT INTO ' || v_process_database_name || '.AUDIT_LOG_JOB_EXECUTION_SQL_COMMANDS_H0 (JOB_EXECUTION_ID, SQL_SMNT) VALUES (' || v_stored_procedure_execution_id ||','|| quote_literal(v_str_sql) || ');';  EXECUTE IMMEDIATE v_str_sql_debug;--

	EXECUTE IMMEDIATE v_str_sql;--

	/*Update Hub Load Table - 3rd Update Deletes: DBARCUD*/
	SET v_str_sql = 'MERGE INTO ' || v_staging_schema_read_write || '.' || v_source_table_name || ' AS tgt
		USING (
			SELECT tgt.LOOKUP_HASH_ADDRESS_PHYSICAL
			FROM ' || v_staging_schema_read_write || '.' || v_source_table_name || ' AS tgt
			LEFT JOIN (
				SELECT DISTINCT
					HASH(CAST(
						COALESCE(COALESCE(TRIM(dbarcud.ADDR2), ''''), ''#'') || ''&'' ||
						COALESCE(COALESCE(TRIM(dbarcud.ADDR1), ''''), ''$'') || ''&'' ||
						''@'' || ''&'' ||
						COALESCE(COALESCE(TRIM(dbarcud.CITY), ''''), ''D'') || ''&'' ||
						COALESCE(LEFT(LEFT(REPLACE(TRIM(REPLACE(TRIM(dbarcud.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 5), ''E'') || ''&'' ||
						COALESCE(RIGHT(LEFT(REPLACE(TRIM(REPLACE(TRIM(dbarcud.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 4), ''F'') || ''&'' ||
						COALESCE(COALESCE(TRIM(dbarcud.STATE), ''''), ''G'') || ''&'' ||
						COALESCE(CASE
							WHEN TRIM(dbarcud.COUNTRY_CODE) = '''' THEN ''US''
							ELSE TRIM(dbarcud.COUNTRY_CODE)
						END, ''H'') AS VARCHAR), 3) AS LOOKUP_HASH_ADDRESS_PHYSICAL
				FROM ' || v_etl_source_database_name || '.CUR_CUS_DBARCUD dbarcud
				JOIN ' || v_etl_source_database_name || '.CUR_CUS_CMASTER cmaster
					ON CAST(cmaster.CCUST# AS VARCHAR) = TRIM(dbarcud.CUSTOMER)
				WHERE
					LENGTH(TRIM(TRANSLATE(REPLACE(TRIM(dbarcud.CUSTOMER), '' '', ''X''), '''', ''0123456789''))) = 0
					AND LENGTH(TRIM(TRANSLATE(REPLACE(TRIM(REPLACE(TRIM(dbarcud.ZIP), ''-'', '' '')), '' '', ''''), '''', ''0123456789''))) = 0) AS dbarcud
				ON tgt.LOOKUP_HASH_ADDRESS_PHYSICAL = dbarcud.LOOKUP_HASH_ADDRESS_PHYSICAL
			WHERE
				tgt.ETL_SOURCE_TABLE_NAME = ' || quote_literal(v_etl_source_table_3) || '
				AND dbarcud.LOOKUP_HASH_ADDRESS_PHYSICAL IS NULL) AS src
			ON (tgt.LOOKUP_HASH_ADDRESS_PHYSICAL = src.LOOKUP_HASH_ADDRESS_PHYSICAL)
		WHEN MATCHED THEN UPDATE SET
			tgt.ETL_SOURCE_DATA_DELETED_FLAG = CAST(''Y'' AS VARCHAR(1 OCTETS)),
			tgt.ETL_UPDATE_TIMESTAMP = CAST(CURRENT_TIMESTAMP - CURRENT_TIMEZONE AS TIMESTAMP),
			tgt.ETL_MODIFIED_BY_JOB_ID = CAST(' || v_stored_procedure_execution_id || ' AS BIGINT)
		WITH UR;';--

	SET o_str_debug =  v_str_sql;--

	/*Debugging Logging*/
	SET v_str_sql_debug =  'INSERT INTO ' || v_process_database_name || '.AUDIT_LOG_JOB_EXECUTION_SQL_COMMANDS_H0 (JOB_EXECUTION_ID, SQL_SMNT) VALUES (' || v_stored_procedure_execution_id ||','|| quote_literal(v_str_sql) || ');';  EXECUTE IMMEDIATE v_str_sql_debug;--

	EXECUTE IMMEDIATE  v_str_sql;--

	/*Populate Hub Load Table - 4th Insert: BLC_ADDRESS -> DIM_ADDRESS_PHYSCIAL*/
	SET v_str_sql = 'INSERT INTO ' || v_staging_schema_read_write || '.' || v_source_table_name || '
		(ADDRESS_PHYSICAL_ID, ACTIVE_FLAG, EFFECTIVE_START_DATETIME, EFFECTIVE_END_DATETIME, LOOKUP_HASH_ADDRESS_PHYSICAL, PHYSICAL_ADDRESS_LINE_1, PHYSICAL_ADDRESS_LINE_2, PHYSICAL_ADDRESS_SUITE, PHYSICAL_ADDRESS_CITY, PHYSICAL_ADDRESS_POSTAL_CODE, PHYSICAL_ADDRESS_POSTAL_CODE_EXTENSION, PHYSICAL_ADDRESS_STATE_PROVINCE_CODE, PHYSICAL_ADDRESS_COUNTRY_CODE, PHYSICAL_ADDRESS_STORE_ADDRESS_FLAG, PHYSICAL_ADDRESS_VERIFICATION_FLAG, PHYSICAL_ADDRESS_ANONIMZATION_FLAG, GEOGRAPHY_ID, PHYSICAL_ADDRESS_LATITUDE, PHYSICAL_ADDRESS_LONGITUDE, ETL_SOURCE_DATA_DELETED_FLAG, ETL_SOURCE_TABLE_NAME, ETL_CREATE_TIMESTAMP, ETL_UPDATE_TIMESTAMP, ETL_MODIFIED_BY_JOB_ID, ETL_MODIFIED_BY_PROCESS)
		SELECT
			CAST(EDW_PROCESS.SEQ_MASTER_MEMBER_ID_H0.NEXTVAL AS BIGINT) AS ADDRESS_PHYSICAL_ID,
			CAST(''Y'' AS VARCHAR(1 OCTETS)) AS ACTIVE_FLAG,
			CAST(''1900-01-01-00.00.00'' AS TIMESTAMP) AS EFFECTIVE_START_DATETIME,
			CAST(NULL AS TIMESTAMP) AS EFFECTIVE_END_DATETIME,
			CAST(HASH(CAST(
				COALESCE(COALESCE(TRIM(blc_address.ADDRESS_LINE1), ''''), ''#'') || ''&'' ||
				COALESCE(COALESCE(TRIM(blc_address.ADDRESS_LINE2), ''''), ''$'') || ''&'' ||
				''@'' || ''&'' ||
				COALESCE(COALESCE(TRIM(blc_address.CITY), ''''), ''D'') || ''&'' ||
				COALESCE(LEFT(LEFT(REPLACE(TRIM(REPLACE(TRIM(blc_address.POSTAL_CODE), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 5), ''E'') || ''&'' ||
				COALESCE(RIGHT(RIGHT(''000000000'' || REPLACE(TRIM(REPLACE(TRIM(COALESCE(blc_address.ZIP_FOUR, '''')), ''-'', '' '')), '' '', '''') , 9), 4), ''F'')  || ''&'' ||
				COALESCE(COALESCE(TRIM(blc_address.SUB_STATE_PROV_REG), ''''), ''G'') || ''&'' ||
				''US'' AS VARCHAR), 3) AS VARBINARY(64)) AS LOOKUP_HASH_ADDRESS_PHYSICAL,
			CAST(COALESCE(TRIM(blc_address.ADDRESS_LINE1), '''') AS VARCHAR(128 OCTETS)) AS PHYSICAL_ADDRESS_LINE_1,
			CAST(COALESCE(TRIM(blc_address.ADDRESS_LINE2), '''') AS VARCHAR(128 OCTETS)) AS PHYSICAL_ADDRESS_LINE_2,
			CAST('''' AS VARCHAR(128 OCTETS)) AS PHYSICAL_ADDRESS_SUITE,
			CAST(TRIM(blc_address.CITY) AS VARCHAR(128 OCTETS)) AS PHYSICAL_ADDRESS_CITY,
			CAST(LEFT(LEFT(REPLACE(TRIM(REPLACE(TRIM(blc_address.POSTAL_CODE), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 5) AS VARCHAR(64 OCTETS)) AS PHYSICAL_ADDRESS_POSTAL_CODE,
			RIGHT(RIGHT(''000000000'' || REPLACE(TRIM(REPLACE(TRIM(COALESCE(blc_address.ZIP_FOUR, '''')), ''-'', '' '')), '' '', '''') , 9), 4) AS PHYSICAL_ADDRESS_POSTAL_CODE_EXTENSION,
			CAST(COALESCE(TRIM(blc_address.SUB_STATE_PROV_REG), '''') AS VARCHAR(64 OCTETS)) AS PHYSICAL_ADDRESS_STATE_PROVINCE_CODE,
			CAST(''US'' AS VARCHAR(512 OCTETS)) AS PHYSICAL_ADDRESS_COUNTRY_CODE,
			CAST(''N'' AS VARCHAR(1 OCTETS)) AS PHYSICAL_ADDRESS_STORE_ADDRESS_FLAG,
			CAST(''N'' AS VARCHAR(1 OCTETS)) AS PHYSICAL_ADDRESS_VERIFICATION_FLAG,
			CAST(''N'' AS VARCHAR(1 OCTETS)) AS PHYSICAL_ADDRESS_ANONIMZATION_FLAG,
			CAST(COALESCE(hub_load_DIM_GEOGRAPHY_POSTAL.GEOGRAPHY_ID, -2) AS BIGINT) AS GEOGRAPHY_ID,
			CAST(0.0 AS DECIMAL(13,7)) AS PHYSICAL_ADDRESS_LATITUDE,
			CAST(0.0 AS DECIMAL(13,7)) AS PHYSICAL_ADDRESS_LONGITUDE,
			CAST(''N'' AS VARCHAR(1 OCTETS)) AS ETL_SOURCE_DATA_DELETED_FLAG,
			CAST(' || quote_literal(v_etl_source_table_4) || ' AS VARCHAR(256 OCTETS)) AS ETL_SOURCE_TABLE_NAME,
			CAST(CURRENT_TIMESTAMP - CURRENT_TIMEZONE AS TIMESTAMP) AS ETL_CREATE_TIMESTAMP,
			CAST(CURRENT_TIMESTAMP - CURRENT_TIMEZONE AS TIMESTAMP) AS ETL_UPDATE_TIMESTAMP,
			CAST(' || v_stored_procedure_execution_id || ' AS BIGINT) AS ETL_MODIFIED_BY_JOB_ID,
			CAST(' || quote_literal(v_hub_procedure_name) || ' AS VARCHAR(128 OCTETS)) AS ETL_MODIFIED_BY_PROCESS
		FROM ' || v_etl_source_database_name || '.CUR_CUS_BLC_ADDRESS blc_address
		LEFT JOIN ' || v_staging_schema_read_write || '.' || v_source_table_name || ' hub_load
			ON hub_load.LOOKUP_HASH_ADDRESS_PHYSICAL = HASH(CAST(
				COALESCE(COALESCE(TRIM(blc_address.ADDRESS_LINE1), ''''), ''#'') || ''&'' ||
				COALESCE(COALESCE(TRIM(blc_address.ADDRESS_LINE2), ''''), ''$'') || ''&'' ||
				''@'' || ''&'' ||
				COALESCE(COALESCE(TRIM(blc_address.CITY), ''''), ''D'') || ''&'' ||
				COALESCE(LEFT(LEFT(REPLACE(TRIM(REPLACE(TRIM(blc_address.POSTAL_CODE), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 5), ''E'') || ''&'' ||
				COALESCE(RIGHT(RIGHT(''000000000'' || REPLACE(TRIM(REPLACE(TRIM(COALESCE(blc_address.ZIP_FOUR, '''')), ''-'', '' '')), '' '', '''') , 9), 4), ''F'')  || ''&'' ||
				COALESCE(COALESCE(TRIM(blc_address.SUB_STATE_PROV_REG), ''''), ''G'') || ''&'' ||
				''US'' AS VARCHAR), 3)
		LEFT JOIN SESSION.C_B_ADDR_SESSION c_b_addr_ses
			ON c_b_addr_ses.LOOKUP_HASH_ADDRESS_PHYSICAL = HASH(CAST(
				COALESCE(COALESCE(TRIM(blc_address.ADDRESS_LINE1), ''''), ''#'') || ''&'' ||
				COALESCE(COALESCE(TRIM(blc_address.ADDRESS_LINE2), ''''), ''$'') || ''&'' ||
				''@'' || ''&'' ||
				COALESCE(COALESCE(TRIM(blc_address.CITY), ''''), ''D'') || ''&'' ||
				COALESCE(LEFT(LEFT(REPLACE(TRIM(REPLACE(TRIM(blc_address.POSTAL_CODE), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 5), ''E'') || ''&'' ||
				COALESCE(RIGHT(RIGHT(''000000000'' || REPLACE(TRIM(REPLACE(TRIM(COALESCE(blc_address.ZIP_FOUR, '''')), ''-'', '' '')), '' '', '''') , 9), 4), ''F'')  || ''&'' ||
				COALESCE(COALESCE(TRIM(blc_address.SUB_STATE_PROV_REG), ''''), ''G'') || ''&'' ||
				''US'' AS VARCHAR), 3)
		LEFT JOIN (
			SELECT DISTINCT
				HASH(CAST(
					COALESCE(COALESCE(TRIM(dbarcud.ADDR2), ''''), ''#'') || ''&'' ||
					COALESCE(COALESCE(TRIM(dbarcud.ADDR1), ''''), ''$'') || ''&'' ||
					''@'' || ''&'' ||
					COALESCE(COALESCE(TRIM(dbarcud.CITY), ''''), ''D'') || ''&'' ||
					COALESCE(LEFT(LEFT(REPLACE(TRIM(REPLACE(TRIM(dbarcud.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 5), ''E'') || ''&'' ||
					COALESCE(RIGHT(LEFT(REPLACE(TRIM(REPLACE(TRIM(dbarcud.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 4), ''F'') || ''&'' ||
					COALESCE(COALESCE(TRIM(dbarcud.STATE), ''''), ''G'') || ''&'' ||
					COALESCE(CASE
						WHEN TRIM(dbarcud.COUNTRY_CODE) = '''' THEN ''US''
						ELSE TRIM(dbarcud.COUNTRY_CODE)
					END, ''H'') AS VARCHAR), 3) AS LOOKUP_HASH_ADDRESS_PHYSICAL
			FROM ' || v_etl_source_database_name || '.CUR_CUS_DBARCUD dbarcud
			JOIN ' || v_etl_source_database_name || '.CUR_CUS_CMASTER cmaster
				ON CAST(cmaster.CCUST# AS VARCHAR) = TRIM(dbarcud.CUSTOMER)
			WHERE
				LENGTH(TRIM(TRANSLATE(REPLACE(TRIM(dbarcud.CUSTOMER), '' '', ''X''), '''', ''0123456789''))) = 0
				AND LENGTH(TRIM(TRANSLATE(REPLACE(TRIM(REPLACE(TRIM(dbarcud.ZIP), ''-'', '' '')), '' '', ''''), '''', ''0123456789''))) = 0) AS dbarcud
			ON dbarcud.LOOKUP_HASH_ADDRESS_PHYSICAL = HASH(CAST(
				COALESCE(COALESCE(TRIM(blc_address.ADDRESS_LINE1), ''''), ''#'') || ''&'' ||
				COALESCE(COALESCE(TRIM(blc_address.ADDRESS_LINE2), ''''), ''$'') || ''&'' ||
				''@'' || ''&'' ||
				COALESCE(COALESCE(TRIM(blc_address.CITY), ''''), ''D'') || ''&'' ||
				COALESCE(LEFT(LEFT(REPLACE(TRIM(REPLACE(TRIM(blc_address.POSTAL_CODE), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 5), ''E'') || ''&'' ||
				COALESCE(RIGHT(RIGHT(''000000000'' || REPLACE(TRIM(REPLACE(TRIM(COALESCE(blc_address.ZIP_FOUR, '''')), ''-'', '' '')), '' '', '''') , 9), 4), ''F'')  || ''&'' ||
				COALESCE(COALESCE(TRIM(blc_address.SUB_STATE_PROV_REG), ''''), ''G'') || ''&'' ||
				''US'' AS VARCHAR), 3)
		LEFT JOIN SESSION.GPSTORE_SESSION gpstore_ses
			ON gpstore_ses.LOOKUP_HASH_ADDRESS_PHYSICAL = HASH(CAST(
				COALESCE(COALESCE(TRIM(blc_address.ADDRESS_LINE1), ''''), ''#'') || ''&'' ||
				COALESCE(COALESCE(TRIM(blc_address.ADDRESS_LINE2), ''''), ''$'') || ''&'' ||
				''@'' || ''&'' ||
				COALESCE(COALESCE(TRIM(blc_address.CITY), ''''), ''D'') || ''&'' ||
				COALESCE(LEFT(LEFT(REPLACE(TRIM(REPLACE(TRIM(blc_address.POSTAL_CODE), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 5), ''E'') || ''&'' ||
				COALESCE(RIGHT(RIGHT(''000000000'' || REPLACE(TRIM(REPLACE(TRIM(COALESCE(blc_address.ZIP_FOUR, '''')), ''-'', '' '')), '' '', '''') , 9), 4), ''F'')  || ''&'' ||
				COALESCE(COALESCE(TRIM(blc_address.SUB_STATE_PROV_REG), ''''), ''G'') || ''&'' ||
				''US'' AS VARCHAR), 3)
		LEFT JOIN ' || v_staging_database_name || '.HUB_LOAD_DIM_GEOGRAPHY hub_load_DIM_GEOGRAPHY_POSTAL
			ON hub_load_DIM_GEOGRAPHY_POSTAL.GEOGRAPHY_HIERARCHY_LEVEL_MEMBER_CODE = TRIM(''USA'' || ''-'' || TRIM(blc_address.SUB_STATE_PROV_REG) || ''-'' || TRIM(blc_address.CITY) || ''-'' || LEFT(LEFT(REPLACE(TRIM(REPLACE(TRIM(blc_address.POSTAL_CODE), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 5))
		WHERE LENGTH(TRIM(TRANSLATE(REPLACE(TRIM(REPLACE(TRIM(blc_address.POSTAL_CODE), ''-'', '' '')), '' '', ''''), '''', ''0123456789''))) = 0
			AND hub_load.LOOKUP_HASH_ADDRESS_PHYSICAL IS NULL
			AND c_b_addr_ses.LOOKUP_HASH_ADDRESS_PHYSICAL IS NULL
			AND dbarcud.LOOKUP_HASH_ADDRESS_PHYSICAL IS NULL
			AND gpstore_ses.LOOKUP_HASH_ADDRESS_PHYSICAL IS NULL
		GROUP BY
			HASH(CAST(
				COALESCE(COALESCE(TRIM(blc_address.ADDRESS_LINE1), ''''), ''#'') || ''&'' ||
				COALESCE(COALESCE(TRIM(blc_address.ADDRESS_LINE2), ''''), ''$'') || ''&'' ||
				''@'' || ''&'' ||
				COALESCE(COALESCE(TRIM(blc_address.CITY), ''''), ''D'') || ''&'' ||
				COALESCE(LEFT(LEFT(REPLACE(TRIM(REPLACE(TRIM(blc_address.POSTAL_CODE), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 5), ''E'') || ''&'' ||
				COALESCE(RIGHT(RIGHT(''000000000'' || REPLACE(TRIM(REPLACE(TRIM(COALESCE(blc_address.ZIP_FOUR, '''')), ''-'', '' '')), '' '', '''') , 9), 4), ''F'')  || ''&'' ||
				COALESCE(COALESCE(TRIM(blc_address.SUB_STATE_PROV_REG), ''''), ''G'') || ''&'' ||
				''US'' AS VARCHAR), 3),
			COALESCE(TRIM(blc_address.ADDRESS_LINE1), ''''),
			COALESCE(TRIM(blc_address.ADDRESS_LINE2), ''''),
			TRIM(blc_address.CITY),
			LEFT(LEFT(REPLACE(TRIM(REPLACE(TRIM(blc_address.POSTAL_CODE), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 5),
			RIGHT(RIGHT(''000000000'' || REPLACE(TRIM(REPLACE(TRIM(COALESCE(blc_address.ZIP_FOUR, '''')), ''-'', '' '')), '' '', '''') , 9), 4),
			COALESCE(TRIM(blc_address.SUB_STATE_PROV_REG), ''''),
			COALESCE(hub_load_DIM_GEOGRAPHY_POSTAL.GEOGRAPHY_ID, -2)
		WITH UR;';--

	SET o_str_debug =  v_str_sql;--

	/*Debugging Logging*/
	SET v_str_sql_debug =  'INSERT INTO ' || v_process_database_name || '.AUDIT_LOG_JOB_EXECUTION_SQL_COMMANDS_H0 (JOB_EXECUTION_ID, SQL_SMNT) VALUES (' || v_stored_procedure_execution_id ||','|| quote_literal(v_str_sql) || ');';  EXECUTE IMMEDIATE v_str_sql_debug;--

	EXECUTE IMMEDIATE  v_str_sql;--

	/*Update Hub Load Table - 4th Merge: BLC_ADDRESS -> DIM_ADDRESS_PHYSICAL*/
	SET v_str_sql = 'MERGE INTO ' || v_staging_schema_read_write || '.' || v_source_table_name || ' AS tgt
		USING (
			SELECT DISTINCT
				CAST(''Y'' AS VARCHAR(1 OCTETS)) AS ACTIVE_FLAG,
				CAST(''1900-01-01-00.00.00'' AS TIMESTAMP) AS EFFECTIVE_START_DATETIME,
				CAST(NULL AS TIMESTAMP) AS EFFECTIVE_END_DATETIME,
				CAST(HASH(CAST(
					COALESCE(COALESCE(TRIM(blc_address.ADDRESS_LINE1), ''''), ''#'') || ''&'' ||
					COALESCE(COALESCE(TRIM(blc_address.ADDRESS_LINE2), ''''), ''$'') || ''&'' ||
					''@'' || ''&'' ||
					COALESCE(COALESCE(TRIM(blc_address.CITY), ''''), ''D'') || ''&'' ||
					COALESCE(LEFT(LEFT(REPLACE(TRIM(REPLACE(TRIM(blc_address.POSTAL_CODE), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 5), ''E'') || ''&'' ||
					COALESCE(RIGHT(RIGHT(''000000000'' || REPLACE(TRIM(REPLACE(TRIM(COALESCE(blc_address.ZIP_FOUR, '''')), ''-'', '' '')), '' '', '''') , 9), 4), ''F'')  || ''&'' ||
					COALESCE(COALESCE(TRIM(blc_address.SUB_STATE_PROV_REG), ''''), ''G'') || ''&'' ||
					''US'' AS VARCHAR), 3) AS VARBINARY(64)) AS LOOKUP_HASH_ADDRESS_PHYSICAL,
				CAST(COALESCE(TRIM(blc_address.ADDRESS_LINE1), '''') AS VARCHAR(128 OCTETS)) AS PHYSICAL_ADDRESS_LINE_1,
				CAST(COALESCE(TRIM(blc_address.ADDRESS_LINE2), '''') AS VARCHAR(128 OCTETS)) AS PHYSICAL_ADDRESS_LINE_2,
				CAST('''' AS VARCHAR(128 OCTETS)) AS PHYSICAL_ADDRESS_SUITE,
				CAST(TRIM(blc_address.CITY) AS VARCHAR(128 OCTETS)) AS PHYSICAL_ADDRESS_CITY,
				CAST(LEFT(LEFT(REPLACE(TRIM(REPLACE(TRIM(blc_address.POSTAL_CODE), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 5) AS VARCHAR(64 OCTETS)) AS PHYSICAL_ADDRESS_POSTAL_CODE,
				RIGHT(RIGHT(''000000000'' || REPLACE(TRIM(REPLACE(TRIM(COALESCE(blc_address.ZIP_FOUR, '''')), ''-'', '' '')), '' '', '''') , 9), 4) AS PHYSICAL_ADDRESS_POSTAL_CODE_EXTENSION,
				CAST(COALESCE(TRIM(blc_address.SUB_STATE_PROV_REG), '''') AS VARCHAR(64 OCTETS)) AS PHYSICAL_ADDRESS_STATE_PROVINCE_CODE,
				CAST(''US'' AS VARCHAR(512 OCTETS)) AS PHYSICAL_ADDRESS_COUNTRY_CODE,
				CAST(''N'' AS VARCHAR(1 OCTETS)) AS PHYSICAL_ADDRESS_STORE_ADDRESS_FLAG,
				CAST(''N'' AS VARCHAR(1 OCTETS)) AS PHYSICAL_ADDRESS_VERIFICATION_FLAG,
				CAST(''N'' AS VARCHAR(1 OCTETS)) AS PHYSICAL_ADDRESS_ANONIMZATION_FLAG,
				CAST(COALESCE(hub_load_DIM_GEOGRAPHY_POSTAL.GEOGRAPHY_ID, -2) AS BIGINT) AS GEOGRAPHY_ID,
				CAST(0.0 AS DECIMAL(13,7)) AS PHYSICAL_ADDRESS_LATITUDE,
				CAST(0.0 AS DECIMAL(13,7)) AS PHYSICAL_ADDRESS_LONGITUDE,
				CAST(''N'' AS VARCHAR(1 OCTETS)) AS ETL_SOURCE_DATA_DELETED_FLAG,
				CAST(' || quote_literal(v_etl_source_table_4) || ' AS VARCHAR(256 OCTETS)) AS ETL_SOURCE_TABLE_NAME,
				CAST(CURRENT_TIMESTAMP - CURRENT_TIMEZONE AS TIMESTAMP) AS ETL_CREATE_TIMESTAMP,
				CAST(CURRENT_TIMESTAMP - CURRENT_TIMEZONE AS TIMESTAMP) AS ETL_UPDATE_TIMESTAMP,
				CAST(' || v_stored_procedure_execution_id || ' AS BIGINT) AS ETL_MODIFIED_BY_JOB_ID,
				CAST(' || quote_literal(v_hub_procedure_name) || ' AS VARCHAR(128 OCTETS)) AS ETL_MODIFIED_BY_PROCESS
			FROM ' || v_etl_source_database_name || '.CUR_CUS_BLC_ADDRESS blc_address
			LEFT JOIN ' || v_staging_schema_read_write || '.' || v_source_table_name || ' hub_load
				ON hub_load.LOOKUP_HASH_ADDRESS_PHYSICAL = HASH(CAST(
					COALESCE(COALESCE(TRIM(blc_address.ADDRESS_LINE1), ''''), ''#'') || ''&'' ||
					COALESCE(COALESCE(TRIM(blc_address.ADDRESS_LINE2), ''''), ''$'') || ''&'' ||
					''@'' || ''&'' ||
					COALESCE(COALESCE(TRIM(blc_address.CITY), ''''), ''D'') || ''&'' ||
					COALESCE(LEFT(LEFT(REPLACE(TRIM(REPLACE(TRIM(blc_address.POSTAL_CODE), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 5), ''E'') || ''&'' ||
					COALESCE(RIGHT(RIGHT(''000000000'' || REPLACE(TRIM(REPLACE(TRIM(COALESCE(blc_address.ZIP_FOUR, '''')), ''-'', '' '')), '' '', '''') , 9), 4), ''F'')  || ''&'' ||
					COALESCE(COALESCE(TRIM(blc_address.SUB_STATE_PROV_REG), ''''), ''G'') || ''&'' ||
					''US'' AS VARCHAR), 3)
			LEFT JOIN SESSION.C_B_ADDR_SESSION c_b_addr_ses
				ON c_b_addr_ses.LOOKUP_HASH_ADDRESS_PHYSICAL = HASH(CAST(
					COALESCE(COALESCE(TRIM(blc_address.ADDRESS_LINE1), ''''), ''#'') || ''&'' ||
					COALESCE(COALESCE(TRIM(blc_address.ADDRESS_LINE2), ''''), ''$'') || ''&'' ||
					''@'' || ''&'' ||
					COALESCE(COALESCE(TRIM(blc_address.CITY), ''''), ''D'') || ''&'' ||
					COALESCE(LEFT(LEFT(REPLACE(TRIM(REPLACE(TRIM(blc_address.POSTAL_CODE), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 5), ''E'') || ''&'' ||
					COALESCE(RIGHT(RIGHT(''000000000'' || REPLACE(TRIM(REPLACE(TRIM(COALESCE(blc_address.ZIP_FOUR, '''')), ''-'', '' '')), '' '', '''') , 9), 4), ''F'')  || ''&'' ||
					COALESCE(COALESCE(TRIM(blc_address.SUB_STATE_PROV_REG), ''''), ''G'') || ''&'' ||
					''US'' AS VARCHAR), 3)
			LEFT JOIN (
				SELECT DISTINCT
					HASH(CAST(
						COALESCE(COALESCE(TRIM(dbarcud.ADDR2), ''''), ''#'') || ''&'' ||
						COALESCE(COALESCE(TRIM(dbarcud.ADDR1), ''''), ''$'') || ''&'' ||
						''@'' || ''&'' ||
						COALESCE(COALESCE(TRIM(dbarcud.CITY), ''''), ''D'') || ''&'' ||
						COALESCE(LEFT(LEFT(REPLACE(TRIM(REPLACE(TRIM(dbarcud.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 5), ''E'') || ''&'' ||
						COALESCE(RIGHT(LEFT(REPLACE(TRIM(REPLACE(TRIM(dbarcud.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 4), ''F'') || ''&'' ||
						COALESCE(COALESCE(TRIM(dbarcud.STATE), ''''), ''G'') || ''&'' ||
						COALESCE(CASE
							WHEN TRIM(dbarcud.COUNTRY_CODE) = '''' THEN ''US''
							ELSE TRIM(dbarcud.COUNTRY_CODE)
						END, ''H'') AS VARCHAR), 3) AS LOOKUP_HASH_ADDRESS_PHYSICAL
				FROM ' || v_etl_source_database_name || '.CUR_CUS_DBARCUD dbarcud
				JOIN ' || v_etl_source_database_name || '.CUR_CUS_CMASTER cmaster
					ON CAST(cmaster.CCUST# AS VARCHAR) = TRIM(dbarcud.CUSTOMER)
				WHERE
					LENGTH(TRIM(TRANSLATE(REPLACE(TRIM(dbarcud.CUSTOMER), '' '', ''X''), '''', ''0123456789''))) = 0
					AND LENGTH(TRIM(TRANSLATE(REPLACE(TRIM(REPLACE(TRIM(dbarcud.ZIP), ''-'', '' '')), '' '', ''''), '''', ''0123456789''))) = 0) AS dbarcud
				ON dbarcud.LOOKUP_HASH_ADDRESS_PHYSICAL = HASH(CAST(
					COALESCE(COALESCE(TRIM(blc_address.ADDRESS_LINE1), ''''), ''#'') || ''&'' ||
					COALESCE(COALESCE(TRIM(blc_address.ADDRESS_LINE2), ''''), ''$'') || ''&'' ||
					''@'' || ''&'' ||
					COALESCE(COALESCE(TRIM(blc_address.CITY), ''''), ''D'') || ''&'' ||
					COALESCE(LEFT(LEFT(REPLACE(TRIM(REPLACE(TRIM(blc_address.POSTAL_CODE), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 5), ''E'') || ''&'' ||
					COALESCE(RIGHT(RIGHT(''000000000'' || REPLACE(TRIM(REPLACE(TRIM(COALESCE(blc_address.ZIP_FOUR, '''')), ''-'', '' '')), '' '', '''') , 9), 4), ''F'')  || ''&'' ||
					COALESCE(COALESCE(TRIM(blc_address.SUB_STATE_PROV_REG), ''''), ''G'') || ''&'' ||
					''US'' AS VARCHAR), 3)
			LEFT JOIN SESSION.GPSTORE_SESSION gpstore_ses
				ON gpstore_ses.LOOKUP_HASH_ADDRESS_PHYSICAL = HASH(CAST(
					COALESCE(COALESCE(TRIM(blc_address.ADDRESS_LINE1), ''''), ''#'') || ''&'' ||
					COALESCE(COALESCE(TRIM(blc_address.ADDRESS_LINE2), ''''), ''$'') || ''&'' ||
					''@'' || ''&'' ||
					COALESCE(COALESCE(TRIM(blc_address.CITY), ''''), ''D'') || ''&'' ||
					COALESCE(LEFT(LEFT(REPLACE(TRIM(REPLACE(TRIM(blc_address.POSTAL_CODE), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 5), ''E'') || ''&'' ||
					COALESCE(RIGHT(RIGHT(''000000000'' || REPLACE(TRIM(REPLACE(TRIM(COALESCE(blc_address.ZIP_FOUR, '''')), ''-'', '' '')), '' '', '''') , 9), 4), ''F'')  || ''&'' ||
					COALESCE(COALESCE(TRIM(blc_address.SUB_STATE_PROV_REG), ''''), ''G'') || ''&'' ||
					''US'' AS VARCHAR), 3)
			LEFT JOIN ' || v_staging_database_name || '.HUB_LOAD_DIM_GEOGRAPHY hub_load_DIM_GEOGRAPHY_POSTAL
				ON hub_load_DIM_GEOGRAPHY_POSTAL.GEOGRAPHY_HIERARCHY_LEVEL_MEMBER_CODE = TRIM(''USA'' || ''-'' || TRIM(blc_address.SUB_STATE_PROV_REG) || ''-'' || TRIM(blc_address.CITY) || ''-'' || LEFT(LEFT(REPLACE(TRIM(REPLACE(TRIM(blc_address.POSTAL_CODE), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 5))
			WHERE
				LENGTH(TRIM(TRANSLATE(REPLACE(TRIM(REPLACE(TRIM(blc_address.POSTAL_CODE), ''-'', '' '')), '' '', ''''), '''', ''0123456789''))) = 0
				AND hub_load.LOOKUP_HASH_ADDRESS_PHYSICAL IS NOT NULL
				AND c_b_addr_ses.LOOKUP_HASH_ADDRESS_PHYSICAL IS NULL
				AND dbarcud.LOOKUP_HASH_ADDRESS_PHYSICAL IS NULL
				AND gpstore_ses.LOOKUP_HASH_ADDRESS_PHYSICAL IS NULL) AS src
			ON (tgt.LOOKUP_HASH_ADDRESS_PHYSICAL = src.LOOKUP_HASH_ADDRESS_PHYSICAL)
		WHEN MATCHED AND(
			COALESCE(tgt.PHYSICAL_ADDRESS_STORE_ADDRESS_FLAG, ''A'') <> COALESCE(src.PHYSICAL_ADDRESS_STORE_ADDRESS_FLAG, ''A'') OR
			COALESCE(tgt.PHYSICAL_ADDRESS_VERIFICATION_FLAG, ''N'') <> COALESCE(src.PHYSICAL_ADDRESS_VERIFICATION_FLAG, ''N'') OR
			COALESCE(tgt.PHYSICAL_ADDRESS_ANONIMZATION_FLAG, ''N'') <> COALESCE(src.PHYSICAL_ADDRESS_ANONIMZATION_FLAG, ''N'') OR
			COALESCE(tgt.GEOGRAPHY_ID, -2) <> COALESCE(src.GEOGRAPHY_ID, -2) OR
			COALESCE(tgt.PHYSICAL_ADDRESS_LATITUDE, 0.0) <> COALESCE(src.PHYSICAL_ADDRESS_LATITUDE, 0.0) OR
			COALESCE(tgt.PHYSICAL_ADDRESS_LONGITUDE, 0.0) <> COALESCE(src.PHYSICAL_ADDRESS_LONGITUDE, 0.0) OR
			COALESCE(tgt.ETL_SOURCE_DATA_DELETED_FLAG, ''N'') <> COALESCE(src.ETL_SOURCE_DATA_DELETED_FLAG, ''N'') OR
			COALESCE(tgt.ETL_SOURCE_TABLE_NAME, ''A'') <> COALESCE(src.ETL_SOURCE_TABLE_NAME, ''A''))
		THEN UPDATE SET
			tgt.PHYSICAL_ADDRESS_STORE_ADDRESS_FLAG=src.PHYSICAL_ADDRESS_STORE_ADDRESS_FLAG,
			tgt.PHYSICAL_ADDRESS_VERIFICATION_FLAG=src.PHYSICAL_ADDRESS_VERIFICATION_FLAG,
			tgt.PHYSICAL_ADDRESS_ANONIMZATION_FLAG=src.PHYSICAL_ADDRESS_ANONIMZATION_FLAG,
			tgt.GEOGRAPHY_ID=src.GEOGRAPHY_ID,
			tgt.PHYSICAL_ADDRESS_LATITUDE=src.PHYSICAL_ADDRESS_LATITUDE,
			tgt.PHYSICAL_ADDRESS_LONGITUDE=src.PHYSICAL_ADDRESS_LONGITUDE,
			tgt.ETL_SOURCE_DATA_DELETED_FLAG=src.ETL_SOURCE_DATA_DELETED_FLAG,
			tgt.ETL_SOURCE_TABLE_NAME=src.ETL_SOURCE_TABLE_NAME,
			tgt.ETL_UPDATE_TIMESTAMP=src.ETL_UPDATE_TIMESTAMP,
			tgt.ETL_MODIFIED_BY_JOB_ID=src.ETL_MODIFIED_BY_JOB_ID,
			tgt.ETL_MODIFIED_BY_PROCESS=src.ETL_MODIFIED_BY_PROCESS
		WITH UR;';--

	SET o_str_debug =  v_str_sql;--

	/*Debugging Logging*/
	SET v_str_sql_debug =  'INSERT INTO ' || v_process_database_name || '.AUDIT_LOG_JOB_EXECUTION_SQL_COMMANDS_H0 (JOB_EXECUTION_ID, SQL_SMNT) VALUES (' || v_stored_procedure_execution_id ||','|| quote_literal(v_str_sql) || ');';  EXECUTE IMMEDIATE v_str_sql_debug;--

	EXECUTE IMMEDIATE  v_str_sql;--

	/*Update Hub Load Table - 4th Update Deletes: BLC_ADDRESS*/
	SET v_str_sql = 'MERGE INTO ' || v_staging_schema_read_write  || '.' || v_source_table_name || ' AS tgt
		USING (
			SELECT tgt.LOOKUP_HASH_ADDRESS_PHYSICAL
			FROM ' || v_staging_schema_read_write || '.' || v_source_table_name || ' AS tgt
			LEFT JOIN (
				SELECT DISTINCT
					HASH(CAST(
						COALESCE(COALESCE(TRIM(blc_address.ADDRESS_LINE1), ''''), ''#'') || ''&'' ||
						COALESCE(COALESCE(TRIM(blc_address.ADDRESS_LINE2), ''''), ''$'') || ''&'' ||
						''@'' || ''&'' ||
						COALESCE(COALESCE(TRIM(blc_address.CITY), ''''), ''D'') || ''&'' ||
						COALESCE(LEFT(LEFT(REPLACE(TRIM(REPLACE(TRIM(blc_address.POSTAL_CODE), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 5), ''E'') || ''&'' ||
						COALESCE(RIGHT(RIGHT(''000000000'' || REPLACE(TRIM(REPLACE(TRIM(COALESCE(blc_address.ZIP_FOUR, '''')), ''-'', '' '')), '' '', '''') , 9), 4), ''F'')  || ''&'' ||
						COALESCE(COALESCE(TRIM(blc_address.SUB_STATE_PROV_REG), ''''), ''G'') || ''&'' ||
						''US'' AS VARCHAR), 3) AS LOOKUP_HASH_ADDRESS_PHYSICAL
				FROM ' || v_etl_source_database_name || '.CUR_CUS_BLC_ADDRESS blc_address
				WHERE LENGTH(TRIM(TRANSLATE(REPLACE(TRIM(REPLACE(TRIM(blc_address.POSTAL_CODE), ''-'', '' '')), '' '', ''''), '''', ''0123456789''))) = 0) AS blc_address
				ON tgt.LOOKUP_HASH_ADDRESS_PHYSICAL = blc_address.LOOKUP_HASH_ADDRESS_PHYSICAL
			WHERE
				tgt.ETL_SOURCE_TABLE_NAME = ' || quote_literal(v_etl_source_table_4) || '
				AND blc_address.LOOKUP_HASH_ADDRESS_PHYSICAL IS NULL) AS src
		ON (tgt.LOOKUP_HASH_ADDRESS_PHYSICAL = src.LOOKUP_HASH_ADDRESS_PHYSICAL)
		WHEN MATCHED THEN UPDATE SET
			tgt.ETL_SOURCE_DATA_DELETED_FLAG = CAST(''Y'' AS VARCHAR(1 OCTETS)),
			tgt.ETL_UPDATE_TIMESTAMP = CAST(CURRENT_TIMESTAMP - CURRENT_TIMEZONE AS TIMESTAMP),
			tgt.ETL_MODIFIED_BY_JOB_ID = CAST(' || v_stored_procedure_execution_id || ' AS BIGINT)
		WITH UR;';--

	SET o_str_debug =  v_str_sql;--

	/*Debugging Logging*/
	SET v_str_sql_debug =  'INSERT INTO ' || v_process_database_name || '.AUDIT_LOG_JOB_EXECUTION_SQL_COMMANDS_H0 (JOB_EXECUTION_ID, SQL_SMNT) VALUES (' || v_stored_procedure_execution_id ||','|| quote_literal(v_str_sql) || ');';  EXECUTE IMMEDIATE v_str_sql_debug;--

	EXECUTE IMMEDIATE  v_str_sql;--

	/*Populate Hub Load Table - 5th Insert: ORW_CUSTOMER_ADDRESS -> DIM_ADDRESS_PHYSICAL*/
	SET v_str_sql = 'INSERT INTO ' || v_staging_schema_read_write || '.' || v_source_table_name || '
		(ADDRESS_PHYSICAL_ID, ACTIVE_FLAG, EFFECTIVE_START_DATETIME, EFFECTIVE_END_DATETIME, LOOKUP_HASH_ADDRESS_PHYSICAL, PHYSICAL_ADDRESS_LINE_1, PHYSICAL_ADDRESS_LINE_2, PHYSICAL_ADDRESS_SUITE, PHYSICAL_ADDRESS_CITY, PHYSICAL_ADDRESS_POSTAL_CODE, PHYSICAL_ADDRESS_POSTAL_CODE_EXTENSION, PHYSICAL_ADDRESS_STATE_PROVINCE_CODE, PHYSICAL_ADDRESS_COUNTRY_CODE, PHYSICAL_ADDRESS_STORE_ADDRESS_FLAG, PHYSICAL_ADDRESS_VERIFICATION_FLAG, PHYSICAL_ADDRESS_ANONIMZATION_FLAG, GEOGRAPHY_ID, PHYSICAL_ADDRESS_LATITUDE, PHYSICAL_ADDRESS_LONGITUDE, ETL_SOURCE_DATA_DELETED_FLAG, ETL_SOURCE_TABLE_NAME, ETL_CREATE_TIMESTAMP, ETL_UPDATE_TIMESTAMP, ETL_MODIFIED_BY_JOB_ID, ETL_MODIFIED_BY_PROCESS)
		SELECT
			CAST(EDW_PROCESS.SEQ_MASTER_MEMBER_ID_H0.NEXTVAL AS BIGINT) AS ADDRESS_PHYSICAL_ID,
			CAST(''Y'' AS VARCHAR(1 OCTETS)) AS ACTIVE_FLAG,
			CAST(''1900-01-01-00.00.00'' AS TIMESTAMP) AS EFFECTIVE_START_DATETIME,
			CAST(NULL AS TIMESTAMP) AS EFFECTIVE_END_DATETIME,
			CAST(HASH(CAST(
				COALESCE(COALESCE(TRIM(orw_customer_address.ADDRESS1), ''''), ''#'') || ''&'' ||
				COALESCE(COALESCE(TRIM(orw_customer_address.ADDRESS2), ''''), ''$'') || ''&'' ||
				''@'' || ''&'' ||
				COALESCE(COALESCE(TRIM(orw_customer_address.CITY), ''''), ''D'') || ''&'' ||
				COALESCE(LEFT(LEFT(REPLACE(TRIM(REPLACE(TRIM(orw_customer_address.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 5), ''E'') || ''&'' ||
				COALESCE(RIGHT(LEFT(REPLACE(TRIM(REPLACE(TRIM(orw_customer_address.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 4), ''F'') || ''&'' ||
				COALESCE(COALESCE(TRIM(orw_customer_address.STATE), ''''), ''G'') || ''&'' ||
				''US'' AS VARCHAR), 3) AS VARBINARY(64)) AS LOOKUP_HASH_ADDRESS_PHYSICAL,
			CAST(COALESCE(TRIM(orw_customer_address.ADDRESS1), '''') AS VARCHAR(128 OCTETS)) AS PHYSICAL_ADDRESS_LINE_1,
			CAST(COALESCE(TRIM(orw_customer_address.ADDRESS2), '''') AS VARCHAR(128 OCTETS)) AS PHYSICAL_ADDRESS_LINE_2,
			CAST('''' AS VARCHAR(128 OCTETS)) AS PHYSICAL_ADDRESS_SUITE,
			CAST(COALESCE(TRIM(orw_customer_address.CITY), '''') AS VARCHAR(128 OCTETS)) AS PHYSICAL_ADDRESS_CITY,
			CAST(LEFT(LEFT(REPLACE(TRIM(REPLACE(TRIM(orw_customer_address.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 5) AS VARCHAR(64 OCTETS)) AS PHYSICAL_ADDRESS_POSTAL_CODE,
			CAST(RIGHT(LEFT(REPLACE(TRIM(REPLACE(TRIM(orw_customer_address.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 4) AS VARCHAR(64 OCTETS)) AS PHYSICAL_ADDRESS_POSTAL_CODE_EXTENSION,
			CAST(COALESCE(TRIM(orw_customer_address.STATE), '''') AS VARCHAR(64 OCTETS)) AS PHYSICAL_ADDRESS_STATE_PROVINCE_CODE,
			CAST(''US'' AS VARCHAR(512 OCTETS)) AS PHYSICAL_ADDRESS_COUNTRY_CODE,
			CAST(''N'' AS VARCHAR(1 OCTETS)) AS PHYSICAL_ADDRESS_STORE_ADDRESS_FLAG,
			CAST(''N'' AS VARCHAR(1 OCTETS)) AS PHYSICAL_ADDRESS_VERIFICATION_FLAG,
			CAST(''N'' AS VARCHAR(1 OCTETS)) AS PHYSICAL_ADDRESS_ANONIMZATION_FLAG,
			CAST(COALESCE(hub_load_DIM_GEOGRAPHY_POSTAL.GEOGRAPHY_ID, -2) AS BIGINT) AS GEOGRAPHY_ID,
			CAST(0.0 AS DECIMAL(13,7)) AS PHYSICAL_ADDRESS_LATITUDE,
			CAST(0.0 AS DECIMAL(13,7)) AS PHYSICAL_ADDRESS_LONGITUDE,
			CAST(''N'' AS VARCHAR(1 OCTETS)) AS ETL_SOURCE_DATA_DELETED_FLAG,
			CAST(' || quote_literal(v_etl_source_table_5) || ' AS VARCHAR(256 OCTETS)) AS ETL_SOURCE_TABLE_NAME,
			CAST(CURRENT_TIMESTAMP - CURRENT_TIMEZONE AS TIMESTAMP) AS ETL_CREATE_TIMESTAMP,
			CAST(CURRENT_TIMESTAMP - CURRENT_TIMEZONE AS TIMESTAMP) AS ETL_UPDATE_TIMESTAMP,
			CAST(' || v_stored_procedure_execution_id || ' AS BIGINT) AS ETL_MODIFIED_BY_JOB_ID,
			CAST(' || quote_literal(v_hub_procedure_name) || ' AS VARCHAR(128 OCTETS)) AS ETL_MODIFIED_BY_PROCESS
		FROM ' || v_etl_source_database_name || '.CUR_CUS_ORW_CUSTOMER_ADDRESS orw_customer_address
		LEFT JOIN ' || v_staging_schema_read_write || '.' || v_source_table_name || ' hub_load
			ON hub_load.LOOKUP_HASH_ADDRESS_PHYSICAL = HASH(CAST(
				COALESCE(COALESCE(TRIM(orw_customer_address.ADDRESS1), ''''), ''#'') || ''&'' ||
				COALESCE(COALESCE(TRIM(orw_customer_address.ADDRESS2), ''''), ''$'') || ''&'' ||
				''@'' || ''&'' ||
				COALESCE(COALESCE(TRIM(orw_customer_address.CITY), ''''), ''D'') || ''&'' ||
				COALESCE(LEFT(LEFT(REPLACE(TRIM(REPLACE(TRIM(orw_customer_address.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 5), ''E'') || ''&'' ||
				COALESCE(RIGHT(LEFT(REPLACE(TRIM(REPLACE(TRIM(orw_customer_address.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 4), ''F'') || ''&'' ||
				COALESCE(COALESCE(TRIM(orw_customer_address.STATE), ''''), ''G'') || ''&'' ||
				''US'' AS VARCHAR), 3)
		LEFT JOIN SESSION.C_B_ADDR_SESSION c_b_addr_ses
			ON c_b_addr_ses.LOOKUP_HASH_ADDRESS_PHYSICAL = HASH(CAST(
				COALESCE(COALESCE(TRIM(orw_customer_address.ADDRESS1), ''''), ''#'') || ''&'' ||
				COALESCE(COALESCE(TRIM(orw_customer_address.ADDRESS2), ''''), ''$'') || ''&'' ||
				''@'' || ''&'' ||
				COALESCE(COALESCE(TRIM(orw_customer_address.CITY), ''''), ''D'') || ''&'' ||
				COALESCE(LEFT(LEFT(REPLACE(TRIM(REPLACE(TRIM(orw_customer_address.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 5), ''E'') || ''&'' ||
				COALESCE(RIGHT(LEFT(REPLACE(TRIM(REPLACE(TRIM(orw_customer_address.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 4), ''F'') || ''&'' ||
				COALESCE(COALESCE(TRIM(orw_customer_address.STATE), ''''), ''G'') || ''&'' ||
				''US'' AS VARCHAR), 3)
		LEFT JOIN (
			SELECT DISTINCT
				HASH(CAST(
					COALESCE(COALESCE(TRIM(dbarcud.ADDR2), ''''), ''#'') || ''&'' ||
					COALESCE(COALESCE(TRIM(dbarcud.ADDR1), ''''), ''$'') || ''&'' ||
					''@'' || ''&'' ||
					COALESCE(COALESCE(TRIM(dbarcud.CITY), ''''), ''D'') || ''&'' ||
					COALESCE(LEFT(LEFT(REPLACE(TRIM(REPLACE(TRIM(dbarcud.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 5), ''E'') || ''&'' ||
					COALESCE(RIGHT(LEFT(REPLACE(TRIM(REPLACE(TRIM(dbarcud.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 4), ''F'') || ''&'' ||
					COALESCE(COALESCE(TRIM(dbarcud.STATE), ''''), ''G'') || ''&'' ||
					COALESCE(CASE
						WHEN TRIM(dbarcud.COUNTRY_CODE) = '''' THEN ''US''
						ELSE TRIM(dbarcud.COUNTRY_CODE)
					END, ''H'') AS VARCHAR), 3) AS LOOKUP_HASH_ADDRESS_PHYSICAL
			FROM ' || v_etl_source_database_name || '.CUR_CUS_DBARCUD dbarcud
			JOIN ' || v_etl_source_database_name || '.CUR_CUS_CMASTER cmaster
				ON CAST(cmaster.CCUST# AS VARCHAR) = TRIM(dbarcud.CUSTOMER)
			WHERE
				LENGTH(TRIM(TRANSLATE(REPLACE(TRIM(dbarcud.CUSTOMER), '' '', ''X''), '''', ''0123456789''))) = 0
				AND LENGTH(TRIM(TRANSLATE(REPLACE(TRIM(REPLACE(TRIM(dbarcud.ZIP), ''-'', '' '')), '' '', ''''), '''', ''0123456789''))) = 0) AS dbarcud
			ON dbarcud.LOOKUP_HASH_ADDRESS_PHYSICAL = HASH(CAST(
				COALESCE(COALESCE(TRIM(orw_customer_address.ADDRESS1), ''''), ''#'') || ''&'' ||
				COALESCE(COALESCE(TRIM(orw_customer_address.ADDRESS2), ''''), ''$'') || ''&'' ||
				''@'' || ''&'' ||
				COALESCE(COALESCE(TRIM(orw_customer_address.CITY), ''''), ''D'') || ''&'' ||
				COALESCE(LEFT(LEFT(REPLACE(TRIM(REPLACE(TRIM(orw_customer_address.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 5), ''E'') || ''&'' ||
				COALESCE(RIGHT(LEFT(REPLACE(TRIM(REPLACE(TRIM(orw_customer_address.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 4), ''F'') || ''&'' ||
				COALESCE(COALESCE(TRIM(orw_customer_address.STATE), ''''), ''G'') || ''&'' ||
				''US'' AS VARCHAR), 3)
		LEFT JOIN SESSION.GPSTORE_SESSION gpstore_ses
			ON gpstore_ses.LOOKUP_HASH_ADDRESS_PHYSICAL = HASH(CAST(
				COALESCE(COALESCE(TRIM(orw_customer_address.ADDRESS1), ''''), ''#'') || ''&'' ||
				COALESCE(COALESCE(TRIM(orw_customer_address.ADDRESS2), ''''), ''$'') || ''&'' ||
				''@'' || ''&'' ||
				COALESCE(COALESCE(TRIM(orw_customer_address.CITY), ''''), ''D'') || ''&'' ||
				COALESCE(LEFT(LEFT(REPLACE(TRIM(REPLACE(TRIM(orw_customer_address.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 5), ''E'') || ''&'' ||
				COALESCE(RIGHT(LEFT(REPLACE(TRIM(REPLACE(TRIM(orw_customer_address.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 4), ''F'') || ''&'' ||
				COALESCE(COALESCE(TRIM(orw_customer_address.STATE), ''''), ''G'') || ''&'' ||
				''US'' AS VARCHAR), 3)
		LEFT JOIN (
			SELECT DISTINCT
				HASH(CAST(
					COALESCE(COALESCE(TRIM(ADDRESS_LINE1), ''''), ''#'') || ''&'' ||
					COALESCE(COALESCE(TRIM(ADDRESS_LINE2), ''''), ''$'') || ''&'' ||
					''@'' || ''&'' ||
					COALESCE(COALESCE(TRIM(CITY), ''''), ''D'') || ''&'' ||
					COALESCE(LEFT(LEFT(REPLACE(TRIM(REPLACE(TRIM(POSTAL_CODE), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 5), ''E'') || ''&'' ||
					COALESCE(RIGHT(RIGHT(''000000000'' || REPLACE(TRIM(REPLACE(TRIM(COALESCE(ZIP_FOUR, '''')), ''-'', '' '')), '' '', '''') , 9), 4), ''F'')  || ''&'' ||
					COALESCE(COALESCE(TRIM(SUB_STATE_PROV_REG), ''''), ''G'') || ''&'' ||
					''US'' AS VARCHAR), 3) AS LOOKUP_HASH_ADDRESS_PHYSICAL
			FROM ' || v_etl_source_database_name || '.CUR_CUS_BLC_ADDRESS
			WHERE
			LENGTH(TRIM(TRANSLATE(REPLACE(TRIM(REPLACE(TRIM(POSTAL_CODE), ''-'', '' '')), '' '', ''''), '''', ''0123456789''))) = 0) blc_address
			ON blc_address.LOOKUP_HASH_ADDRESS_PHYSICAL = HASH(CAST(
				COALESCE(COALESCE(TRIM(orw_customer_address.ADDRESS1), ''''), ''#'') || ''&'' ||
				COALESCE(COALESCE(TRIM(orw_customer_address.ADDRESS2), ''''), ''$'') || ''&'' ||
				''@'' || ''&'' ||
				COALESCE(COALESCE(TRIM(orw_customer_address.CITY), ''''), ''D'') || ''&'' ||
				COALESCE(LEFT(LEFT(REPLACE(TRIM(REPLACE(TRIM(orw_customer_address.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 5), ''E'') || ''&'' ||
				COALESCE(RIGHT(LEFT(REPLACE(TRIM(REPLACE(TRIM(orw_customer_address.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 4), ''F'') || ''&'' ||
				COALESCE(COALESCE(TRIM(orw_customer_address.STATE), ''''), ''G'') || ''&'' ||
				''US'' AS VARCHAR), 3)
		LEFT JOIN ' || v_staging_database_name || '.HUB_LOAD_DIM_GEOGRAPHY hub_load_DIM_GEOGRAPHY_POSTAL
			ON hub_load_DIM_GEOGRAPHY_POSTAL.GEOGRAPHY_HIERARCHY_LEVEL_MEMBER_CODE = TRIM(''USA'' || ''-'' || TRIM(orw_customer_address.STATE) || ''-'' || TRIM(orw_customer_address.CITY) || ''-'' || LEFT(LEFT(REPLACE(TRIM(REPLACE(TRIM(orw_customer_address.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 5))
		WHERE
			TRIM(orw_customer_address.ADDRESS1) <> ''#''
			AND TRIM(orw_customer_address.CITY) <> ''D''
			AND LENGTH(TRIM(TRANSLATE(REPLACE(TRIM(REPLACE(TRIM(orw_customer_address.ZIP), ''-'', '' '')), '' '', ''''), '''', ''0123456789''))) = 0
			AND hub_load.LOOKUP_HASH_ADDRESS_PHYSICAL IS NULL
			AND c_b_addr_ses.LOOKUP_HASH_ADDRESS_PHYSICAL IS NULL
			AND dbarcud.LOOKUP_HASH_ADDRESS_PHYSICAL IS NULL
			AND gpstore_ses.LOOKUP_HASH_ADDRESS_PHYSICAL IS NULL
			AND blc_address.LOOKUP_HASH_ADDRESS_PHYSICAL IS NULL
		GROUP BY
			HASH(CAST(
				COALESCE(COALESCE(TRIM(orw_customer_address.ADDRESS1), ''''), ''#'') || ''&'' ||
				COALESCE(COALESCE(TRIM(orw_customer_address.ADDRESS2), ''''), ''$'') || ''&'' ||
				''@'' || ''&'' ||
				COALESCE(COALESCE(TRIM(orw_customer_address.CITY), ''''), ''D'') || ''&'' ||
				COALESCE(LEFT(LEFT(REPLACE(TRIM(REPLACE(TRIM(orw_customer_address.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 5), ''E'') || ''&'' ||
				COALESCE(RIGHT(LEFT(REPLACE(TRIM(REPLACE(TRIM(orw_customer_address.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 4), ''F'') || ''&'' ||
				COALESCE(COALESCE(TRIM(orw_customer_address.STATE), ''''), ''G'') || ''&'' ||
				''US'' AS VARCHAR), 3),
			COALESCE(TRIM(orw_customer_address.ADDRESS1), ''''),
			COALESCE(TRIM(orw_customer_address.ADDRESS2), ''''),
			COALESCE(TRIM(orw_customer_address.CITY), ''''),
			LEFT(LEFT(REPLACE(TRIM(REPLACE(TRIM(orw_customer_address.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 5),
			RIGHT(LEFT(REPLACE(TRIM(REPLACE(TRIM(orw_customer_address.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 4),
			COALESCE(TRIM(orw_customer_address.STATE), ''''),
			COALESCE(hub_load_DIM_GEOGRAPHY_POSTAL.GEOGRAPHY_ID, -2)
		WITH UR;';--

	SET o_str_debug =  v_str_sql;--

	/*Debugging Logging*/
	SET v_str_sql_debug =  'INSERT INTO ' || v_process_database_name || '.AUDIT_LOG_JOB_EXECUTION_SQL_COMMANDS_H0 (JOB_EXECUTION_ID, SQL_SMNT) VALUES (' || v_stored_procedure_execution_id ||','|| quote_literal(v_str_sql) || ');';  EXECUTE IMMEDIATE v_str_sql_debug;--

	EXECUTE IMMEDIATE  v_str_sql;--

	/*Update Hub Load Table - 5th Merge: ORW_CUSTOMER_ADDRESS -> DIM_ADDRESS_PHYSICAL*/
	SET v_str_sql = 'MERGE INTO ' || v_staging_schema_read_write || '.' || v_source_table_name || ' AS tgt
		USING (
			SELECT DISTINCT
				CAST(''Y'' AS VARCHAR(1 OCTETS)) AS ACTIVE_FLAG,
				CAST(''1900-01-01-00.00.00'' AS TIMESTAMP) AS EFFECTIVE_START_DATETIME,
				CAST(NULL AS TIMESTAMP) AS EFFECTIVE_END_DATETIME,
				CAST(HASH(CAST(
					COALESCE(COALESCE(TRIM(orw_customer_address.ADDRESS1), ''''), ''#'') || ''&'' ||
					COALESCE(COALESCE(TRIM(orw_customer_address.ADDRESS2), ''''), ''$'') || ''&'' ||
					''@'' || ''&'' ||
					COALESCE(COALESCE(TRIM(orw_customer_address.CITY), ''''), ''D'') || ''&'' ||
					COALESCE(LEFT(LEFT(REPLACE(TRIM(REPLACE(TRIM(orw_customer_address.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 5), ''E'') || ''&'' ||
					COALESCE(RIGHT(LEFT(REPLACE(TRIM(REPLACE(TRIM(orw_customer_address.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 4), ''F'') || ''&'' ||
					COALESCE(COALESCE(TRIM(orw_customer_address.STATE), ''''), ''G'') || ''&'' ||
					''US'' AS VARCHAR), 3) AS VARBINARY(64)) AS LOOKUP_HASH_ADDRESS_PHYSICAL,
				CAST(COALESCE(TRIM(orw_customer_address.ADDRESS1), '''') AS VARCHAR(128 OCTETS)) AS PHYSICAL_ADDRESS_LINE_1,
				CAST(COALESCE(TRIM(orw_customer_address.ADDRESS2), '''') AS VARCHAR(128 OCTETS)) AS PHYSICAL_ADDRESS_LINE_2,
				CAST('''' AS VARCHAR(128 OCTETS)) AS PHYSICAL_ADDRESS_SUITE,
				CAST(COALESCE(TRIM(orw_customer_address.CITY), '''') AS VARCHAR(128 OCTETS)) AS PHYSICAL_ADDRESS_CITY,
				CAST(LEFT(LEFT(REPLACE(TRIM(REPLACE(TRIM(orw_customer_address.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 5) AS VARCHAR(64 OCTETS)) AS PHYSICAL_ADDRESS_POSTAL_CODE,
				CAST(RIGHT(LEFT(REPLACE(TRIM(REPLACE(TRIM(orw_customer_address.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 4) AS VARCHAR(64 OCTETS)) AS PHYSICAL_ADDRESS_POSTAL_CODE_EXTENSION,
				CAST(COALESCE(TRIM(orw_customer_address.STATE), '''') AS VARCHAR(64 OCTETS)) AS PHYSICAL_ADDRESS_STATE_PROVINCE_CODE,
				CAST(''US'' AS VARCHAR(512 OCTETS)) AS PHYSICAL_ADDRESS_COUNTRY_CODE,
				CAST(''N'' AS VARCHAR(1 OCTETS)) AS PHYSICAL_ADDRESS_STORE_ADDRESS_FLAG,
				CAST(''N'' AS VARCHAR(1 OCTETS)) AS PHYSICAL_ADDRESS_VERIFICATION_FLAG,
				CAST(''N'' AS VARCHAR(1 OCTETS)) AS PHYSICAL_ADDRESS_ANONIMZATION_FLAG,
				CAST(COALESCE(hub_load_DIM_GEOGRAPHY_POSTAL.GEOGRAPHY_ID, -2) AS BIGINT) AS GEOGRAPHY_ID,
				CAST(0.0 AS DECIMAL(13,7)) AS PHYSICAL_ADDRESS_LATITUDE,
				CAST(0.0 AS DECIMAL(13,7)) AS PHYSICAL_ADDRESS_LONGITUDE,
				CAST(''N'' AS VARCHAR(1 OCTETS)) AS ETL_SOURCE_DATA_DELETED_FLAG,
				CAST(' || quote_literal(v_etl_source_table_5) || ' AS VARCHAR(256 OCTETS)) AS ETL_SOURCE_TABLE_NAME,
				CAST(CURRENT_TIMESTAMP - CURRENT_TIMEZONE AS TIMESTAMP) AS ETL_CREATE_TIMESTAMP,
				CAST(CURRENT_TIMESTAMP - CURRENT_TIMEZONE AS TIMESTAMP) AS ETL_UPDATE_TIMESTAMP,
				CAST(' || v_stored_procedure_execution_id || ' AS BIGINT) AS ETL_MODIFIED_BY_JOB_ID,
				CAST(' || quote_literal(v_hub_procedure_name) || ' AS VARCHAR(128 OCTETS)) AS ETL_MODIFIED_BY_PROCESS
				FROM ' || v_etl_source_database_name || '.CUR_CUS_ORW_CUSTOMER_ADDRESS orw_customer_address
			LEFT JOIN ' || v_staging_schema_read_write || '.' || v_source_table_name || ' hub_load
				ON hub_load.LOOKUP_HASH_ADDRESS_PHYSICAL = HASH(CAST(
					COALESCE(COALESCE(TRIM(orw_customer_address.ADDRESS1), ''''), ''#'') || ''&'' ||
					COALESCE(COALESCE(TRIM(orw_customer_address.ADDRESS2), ''''), ''$'') || ''&'' ||
					''@'' || ''&'' ||
					COALESCE(COALESCE(TRIM(orw_customer_address.CITY), ''''), ''D'') || ''&'' ||
					COALESCE(LEFT(LEFT(REPLACE(TRIM(REPLACE(TRIM(orw_customer_address.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 5), ''E'') || ''&'' ||
					COALESCE(RIGHT(LEFT(REPLACE(TRIM(REPLACE(TRIM(orw_customer_address.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 4), ''F'') || ''&'' ||
					COALESCE(COALESCE(TRIM(orw_customer_address.STATE), ''''), ''G'') || ''&'' ||
					''US'' AS VARCHAR), 3)
			LEFT JOIN SESSION.C_B_ADDR_SESSION c_b_addr_ses
				ON c_b_addr_ses.LOOKUP_HASH_ADDRESS_PHYSICAL = HASH(CAST(
					COALESCE(COALESCE(TRIM(orw_customer_address.ADDRESS1), ''''), ''#'') || ''&'' ||
					COALESCE(COALESCE(TRIM(orw_customer_address.ADDRESS2), ''''), ''$'') || ''&'' ||
					''@'' || ''&'' ||
					COALESCE(COALESCE(TRIM(orw_customer_address.CITY), ''''), ''D'') || ''&'' ||
					COALESCE(LEFT(LEFT(REPLACE(TRIM(REPLACE(TRIM(orw_customer_address.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 5), ''E'') || ''&'' ||
					COALESCE(RIGHT(LEFT(REPLACE(TRIM(REPLACE(TRIM(orw_customer_address.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 4), ''F'') || ''&'' ||
					COALESCE(COALESCE(TRIM(orw_customer_address.STATE), ''''), ''G'') || ''&'' ||
					''US'' AS VARCHAR), 3)
			LEFT JOIN (
				SELECT DISTINCT
					HASH(CAST(
						COALESCE(COALESCE(TRIM(dbarcud.ADDR2), ''''), ''#'') || ''&'' ||
						COALESCE(COALESCE(TRIM(dbarcud.ADDR1), ''''), ''$'') || ''&'' ||
						''@'' || ''&'' ||
						COALESCE(COALESCE(TRIM(dbarcud.CITY), ''''), ''D'') || ''&'' ||
						COALESCE(LEFT(LEFT(REPLACE(TRIM(REPLACE(TRIM(dbarcud.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 5), ''E'') || ''&'' ||
						COALESCE(RIGHT(LEFT(REPLACE(TRIM(REPLACE(TRIM(dbarcud.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 4), ''F'') || ''&'' ||
						COALESCE(COALESCE(TRIM(dbarcud.STATE), ''''), ''G'') || ''&'' ||
						COALESCE(CASE
							WHEN TRIM(dbarcud.COUNTRY_CODE) = '''' THEN ''US''
							ELSE TRIM(dbarcud.COUNTRY_CODE)
						END, ''H'') AS VARCHAR), 3) AS LOOKUP_HASH_ADDRESS_PHYSICAL
				FROM ' || v_etl_source_database_name || '.CUR_CUS_DBARCUD dbarcud
				JOIN ' || v_etl_source_database_name || '.CUR_CUS_CMASTER cmaster
					ON CAST(cmaster.CCUST# AS VARCHAR) = TRIM(dbarcud.CUSTOMER)
				WHERE
					LENGTH(TRIM(TRANSLATE(REPLACE(TRIM(dbarcud.CUSTOMER), '' '', ''X''), '''', ''0123456789''))) = 0
					AND LENGTH(TRIM(TRANSLATE(REPLACE(TRIM(REPLACE(TRIM(dbarcud.ZIP), ''-'', '' '')), '' '', ''''), '''', ''0123456789''))) = 0) AS dbarcud
				ON dbarcud.LOOKUP_HASH_ADDRESS_PHYSICAL = HASH(CAST(
					COALESCE(COALESCE(TRIM(orw_customer_address.ADDRESS1), ''''), ''#'') || ''&'' ||
					COALESCE(COALESCE(TRIM(orw_customer_address.ADDRESS2), ''''), ''$'') || ''&'' ||
					''@'' || ''&'' ||
					COALESCE(COALESCE(TRIM(orw_customer_address.CITY), ''''), ''D'') || ''&'' ||
					COALESCE(LEFT(LEFT(REPLACE(TRIM(REPLACE(TRIM(orw_customer_address.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 5), ''E'') || ''&'' ||
					COALESCE(RIGHT(LEFT(REPLACE(TRIM(REPLACE(TRIM(orw_customer_address.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 4), ''F'') || ''&'' ||
					COALESCE(COALESCE(TRIM(orw_customer_address.STATE), ''''), ''G'') || ''&'' ||
					''US'' AS VARCHAR), 3)
			LEFT JOIN SESSION.GPSTORE_SESSION gpstore_ses
				ON gpstore_ses.LOOKUP_HASH_ADDRESS_PHYSICAL = HASH(CAST(
					COALESCE(COALESCE(TRIM(orw_customer_address.ADDRESS1), ''''), ''#'') || ''&'' ||
					COALESCE(COALESCE(TRIM(orw_customer_address.ADDRESS2), ''''), ''$'') || ''&'' ||
					''@'' || ''&'' ||
					COALESCE(COALESCE(TRIM(orw_customer_address.CITY), ''''), ''D'') || ''&'' ||
					COALESCE(LEFT(LEFT(REPLACE(TRIM(REPLACE(TRIM(orw_customer_address.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 5), ''E'') || ''&'' ||
					COALESCE(RIGHT(LEFT(REPLACE(TRIM(REPLACE(TRIM(orw_customer_address.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 4), ''F'') || ''&'' ||
					COALESCE(COALESCE(TRIM(orw_customer_address.STATE), ''''), ''G'') || ''&'' ||
					''US'' AS VARCHAR), 3)
			LEFT JOIN (
				SELECT DISTINCT
					HASH(CAST(
						COALESCE(COALESCE(TRIM(ADDRESS_LINE1), ''''), ''#'') || ''&'' ||
						COALESCE(COALESCE(TRIM(ADDRESS_LINE2), ''''), ''$'') || ''&'' ||
						''@'' || ''&'' ||
						COALESCE(COALESCE(TRIM(CITY), ''''), ''D'') || ''&'' ||
						COALESCE(LEFT(LEFT(REPLACE(TRIM(REPLACE(TRIM(POSTAL_CODE), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 5), ''E'') || ''&'' ||
						COALESCE(RIGHT(RIGHT(''000000000'' || REPLACE(TRIM(REPLACE(TRIM(COALESCE(ZIP_FOUR, '''')), ''-'', '' '')), '' '', '''') , 9), 4), ''F'')  || ''&'' ||
						COALESCE(COALESCE(TRIM(SUB_STATE_PROV_REG), ''''), ''G'') || ''&'' ||
						''US'' AS VARCHAR), 3) AS LOOKUP_HASH_ADDRESS_PHYSICAL
				FROM ' || v_etl_source_database_name || '.CUR_CUS_BLC_ADDRESS
				WHERE
				LENGTH(TRIM(TRANSLATE(REPLACE(TRIM(REPLACE(TRIM(POSTAL_CODE), ''-'', '' '')), '' '', ''''), '''', ''0123456789''))) = 0) blc_address
				ON blc_address.LOOKUP_HASH_ADDRESS_PHYSICAL = HASH(CAST(
					COALESCE(COALESCE(TRIM(orw_customer_address.ADDRESS1), ''''), ''#'') || ''&'' ||
					COALESCE(COALESCE(TRIM(orw_customer_address.ADDRESS2), ''''), ''$'') || ''&'' ||
					''@'' || ''&'' ||
					COALESCE(COALESCE(TRIM(orw_customer_address.CITY), ''''), ''D'') || ''&'' ||
					COALESCE(LEFT(LEFT(REPLACE(TRIM(REPLACE(TRIM(orw_customer_address.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 5), ''E'') || ''&'' ||
					COALESCE(RIGHT(LEFT(REPLACE(TRIM(REPLACE(TRIM(orw_customer_address.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 4), ''F'') || ''&'' ||
					COALESCE(COALESCE(TRIM(orw_customer_address.STATE), ''''), ''G'') || ''&'' ||
					''US'' AS VARCHAR), 3)
			LEFT JOIN ' || v_staging_database_name || '.HUB_LOAD_DIM_GEOGRAPHY hub_load_DIM_GEOGRAPHY_POSTAL
				ON hub_load_DIM_GEOGRAPHY_POSTAL.GEOGRAPHY_HIERARCHY_LEVEL_MEMBER_CODE = TRIM(''USA'' || ''-'' || TRIM(orw_customer_address.STATE) || ''-'' || TRIM(orw_customer_address.CITY) || ''-'' || LEFT(LEFT(REPLACE(TRIM(REPLACE(TRIM(orw_customer_address.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 5))
			WHERE
				TRIM(orw_customer_address.ADDRESS1) <> ''#''
				AND TRIM(orw_customer_address.CITY) <> ''D''
				AND LENGTH(TRIM(TRANSLATE(REPLACE(TRIM(REPLACE(TRIM(orw_customer_address.ZIP), ''-'', '' '')), '' '', ''''), '''', ''0123456789''))) = 0
				AND hub_load.LOOKUP_HASH_ADDRESS_PHYSICAL IS NOT NULL
				AND c_b_addr_ses.LOOKUP_HASH_ADDRESS_PHYSICAL IS NULL
				AND dbarcud.LOOKUP_HASH_ADDRESS_PHYSICAL IS NULL
				AND gpstore_ses.LOOKUP_HASH_ADDRESS_PHYSICAL IS NULL
				AND blc_address.LOOKUP_HASH_ADDRESS_PHYSICAL IS NULL) AS src
			ON (tgt.LOOKUP_HASH_ADDRESS_PHYSICAL = src.LOOKUP_HASH_ADDRESS_PHYSICAL)
		WHEN MATCHED AND(
			COALESCE(tgt.PHYSICAL_ADDRESS_STORE_ADDRESS_FLAG, ''A'') <> COALESCE(src.PHYSICAL_ADDRESS_STORE_ADDRESS_FLAG, ''A'') OR
			COALESCE(tgt.PHYSICAL_ADDRESS_VERIFICATION_FLAG, ''N'') <> COALESCE(src.PHYSICAL_ADDRESS_VERIFICATION_FLAG, ''N'') OR
			COALESCE(tgt.PHYSICAL_ADDRESS_ANONIMZATION_FLAG, ''N'') <> COALESCE(src.PHYSICAL_ADDRESS_ANONIMZATION_FLAG, ''N'') OR
			COALESCE(tgt.GEOGRAPHY_ID, -2) <> COALESCE(src.GEOGRAPHY_ID, -2) OR
			COALESCE(tgt.PHYSICAL_ADDRESS_LATITUDE, 0.0) <> COALESCE(src.PHYSICAL_ADDRESS_LATITUDE, 0.0) OR
			COALESCE(tgt.PHYSICAL_ADDRESS_LONGITUDE, 0.0) <> COALESCE(src.PHYSICAL_ADDRESS_LONGITUDE, 0.0) OR
			COALESCE(tgt.ETL_SOURCE_DATA_DELETED_FLAG, ''N'') <> COALESCE(src.ETL_SOURCE_DATA_DELETED_FLAG, ''N'') OR
			COALESCE(tgt.ETL_SOURCE_TABLE_NAME, ''A'') <> COALESCE(src.ETL_SOURCE_TABLE_NAME, ''A''))
		THEN UPDATE SET
			tgt.PHYSICAL_ADDRESS_STORE_ADDRESS_FLAG=src.PHYSICAL_ADDRESS_STORE_ADDRESS_FLAG,
			tgt.PHYSICAL_ADDRESS_VERIFICATION_FLAG=src.PHYSICAL_ADDRESS_VERIFICATION_FLAG,
			tgt.PHYSICAL_ADDRESS_ANONIMZATION_FLAG=src.PHYSICAL_ADDRESS_ANONIMZATION_FLAG,
			tgt.GEOGRAPHY_ID=src.GEOGRAPHY_ID,
			tgt.PHYSICAL_ADDRESS_LATITUDE=src.PHYSICAL_ADDRESS_LATITUDE,
			tgt.PHYSICAL_ADDRESS_LONGITUDE=src.PHYSICAL_ADDRESS_LONGITUDE,
			tgt.ETL_SOURCE_DATA_DELETED_FLAG=src.ETL_SOURCE_DATA_DELETED_FLAG,
			tgt.ETL_SOURCE_TABLE_NAME=src.ETL_SOURCE_TABLE_NAME,
			tgt.ETL_UPDATE_TIMESTAMP=src.ETL_UPDATE_TIMESTAMP,
			tgt.ETL_MODIFIED_BY_JOB_ID=src.ETL_MODIFIED_BY_JOB_ID,
			tgt.ETL_MODIFIED_BY_PROCESS=src.ETL_MODIFIED_BY_PROCESS
		WITH UR;';--

	SET o_str_debug =  v_str_sql;--

	/*Debugging Logging*/
	SET v_str_sql_debug =  'INSERT INTO ' || v_process_database_name || '.AUDIT_LOG_JOB_EXECUTION_SQL_COMMANDS_H0 (JOB_EXECUTION_ID, SQL_SMNT) VALUES (' || v_stored_procedure_execution_id ||','|| quote_literal(v_str_sql) || ');';  EXECUTE IMMEDIATE v_str_sql_debug;--

	EXECUTE IMMEDIATE  v_str_sql;--

	/*Update Hub Load Table - 5th Update Deletes: ORW_CUSTOMER_ADDRESS*/
	SET v_str_sql = 'MERGE INTO ' || v_staging_schema_read_write || '.' || v_source_table_name || ' AS tgt
		USING (
			SELECT tgt.LOOKUP_HASH_ADDRESS_PHYSICAL
			FROM ' || v_staging_schema_read_write || '.' || v_source_table_name || ' AS tgt
			LEFT JOIN (
				SELECT DISTINCT
					HASH(CAST(
						COALESCE(COALESCE(TRIM(orw_customer_address.ADDRESS1), ''''), ''#'') || ''&'' ||
						COALESCE(COALESCE(TRIM(orw_customer_address.ADDRESS2), ''''), ''$'') || ''&'' ||
						''@'' || ''&'' ||
						COALESCE(COALESCE(TRIM(orw_customer_address.CITY), ''''), ''D'') || ''&'' ||
						COALESCE(LEFT(LEFT(REPLACE(TRIM(REPLACE(TRIM(orw_customer_address.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 5), ''E'') || ''&'' ||
						COALESCE(RIGHT(LEFT(REPLACE(TRIM(REPLACE(TRIM(orw_customer_address.ZIP), ''-'', '' '')), '' '', '''') || ''000000000'', 9), 4), ''F'') || ''&'' ||
						COALESCE(COALESCE(TRIM(orw_customer_address.STATE), ''''), ''G'') || ''&'' ||
						''US'' AS VARCHAR), 3) AS LOOKUP_HASH_ADDRESS_PHYSICAL
				FROM ' || v_etl_source_database_name || '.CUR_CUS_ORW_CUSTOMER_ADDRESS orw_customer_address
				WHERE
					TRIM(orw_customer_address.ADDRESS1) <> ''#''
					AND TRIM(orw_customer_address.CITY) <> ''D''
					AND LENGTH(TRIM(TRANSLATE(REPLACE(TRIM(REPLACE(TRIM(orw_customer_address.ZIP), ''-'', '' '')), '' '', ''''), '''', ''0123456789''))) = 0) orw_customer_address
				ON tgt.LOOKUP_HASH_ADDRESS_PHYSICAL = orw_customer_address.LOOKUP_HASH_ADDRESS_PHYSICAL
			WHERE
				tgt.ETL_SOURCE_TABLE_NAME = ' || quote_literal(v_etl_source_table_5) || '
				AND orw_customer_address.LOOKUP_HASH_ADDRESS_PHYSICAL IS NULL) AS src
		ON (tgt.LOOKUP_HASH_ADDRESS_PHYSICAL = src.LOOKUP_HASH_ADDRESS_PHYSICAL)
		WHEN MATCHED THEN UPDATE SET
			tgt.ETL_SOURCE_DATA_DELETED_FLAG = CAST(''Y'' AS VARCHAR(1 OCTETS)),
			tgt.ETL_UPDATE_TIMESTAMP = CAST(CURRENT_TIMESTAMP - CURRENT_TIMEZONE AS TIMESTAMP),
			tgt.ETL_MODIFIED_BY_JOB_ID = CAST(' || v_stored_procedure_execution_id || ' AS BIGINT)
		WITH UR;';--

	SET o_str_debug =  v_str_sql;--

	/*Debugging Logging*/
	SET v_str_sql_debug =  'INSERT INTO ' || v_process_database_name || '.AUDIT_LOG_JOB_EXECUTION_SQL_COMMANDS_H0 (JOB_EXECUTION_ID, SQL_SMNT) VALUES (' || v_stored_procedure_execution_id ||','|| quote_literal(v_str_sql) || ');';  EXECUTE IMMEDIATE v_str_sql_debug;--

	EXECUTE IMMEDIATE  v_str_sql;--

	/*Populate Hub Table - Insert: HUB_LOAD_DIM_ADDRESS_PHYSICAL -> DIM_ADDRESS_PHYSICAL*/
	SET v_str_sql = 'INSERT INTO ' || v_target_database_name || '.' || v_target_table_name || '
		(ADDRESS_PHYSICAL_ID, ACTIVE_FLAG, EFFECTIVE_START_DATETIME, EFFECTIVE_END_DATETIME, LOOKUP_HASH_ADDRESS_PHYSICAL, PHYSICAL_ADDRESS_LINE_1, PHYSICAL_ADDRESS_LINE_2, PHYSICAL_ADDRESS_SUITE, PHYSICAL_ADDRESS_CITY, PHYSICAL_ADDRESS_POSTAL_CODE, PHYSICAL_ADDRESS_POSTAL_CODE_EXTENSION, PHYSICAL_ADDRESS_STATE_PROVINCE_CODE, PHYSICAL_ADDRESS_COUNTRY_CODE, PHYSICAL_ADDRESS_STORE_ADDRESS_FLAG, PHYSICAL_ADDRESS_VERIFICATION_FLAG, PHYSICAL_ADDRESS_ANONIMZATION_FLAG, GEOGRAPHY_ID, PHYSICAL_ADDRESS_LATITUDE, PHYSICAL_ADDRESS_LONGITUDE, ETL_SOURCE_DATA_DELETED_FLAG, ETL_SOURCE_TABLE_NAME, ETL_CREATE_TIMESTAMP, ETL_UPDATE_TIMESTAMP, ETL_MODIFIED_BY_JOB_ID, ETL_MODIFIED_BY_PROCESS)
		SELECT
			src.ADDRESS_PHYSICAL_ID,
			src.ACTIVE_FLAG,
			src.EFFECTIVE_START_DATETIME,
			src.EFFECTIVE_END_DATETIME,
			src.LOOKUP_HASH_ADDRESS_PHYSICAL,
			src.PHYSICAL_ADDRESS_LINE_1,
			src.PHYSICAL_ADDRESS_LINE_2,
			src.PHYSICAL_ADDRESS_SUITE,
			src.PHYSICAL_ADDRESS_CITY,
			src.PHYSICAL_ADDRESS_POSTAL_CODE,
			src.PHYSICAL_ADDRESS_POSTAL_CODE_EXTENSION,
			src.PHYSICAL_ADDRESS_STATE_PROVINCE_CODE,
			src.PHYSICAL_ADDRESS_COUNTRY_CODE,
			src.PHYSICAL_ADDRESS_STORE_ADDRESS_FLAG,
			src.PHYSICAL_ADDRESS_VERIFICATION_FLAG,
			src.PHYSICAL_ADDRESS_ANONIMZATION_FLAG,
			src.GEOGRAPHY_ID,
			src.PHYSICAL_ADDRESS_LATITUDE,
			src.PHYSICAL_ADDRESS_LONGITUDE,
			src.ETL_SOURCE_DATA_DELETED_FLAG,
			src.ETL_SOURCE_TABLE_NAME,
			src.ETL_CREATE_TIMESTAMP,
			src.ETL_UPDATE_TIMESTAMP,
			src.ETL_MODIFIED_BY_JOB_ID,
			src.ETL_MODIFIED_BY_PROCESS
		FROM ' || v_staging_schema_read_write || '.' || v_source_table_name || ' src
		LEFT JOIN ' || v_target_database_name || '.' || v_target_table_name || ' tgt
			ON src.LOOKUP_HASH_ADDRESS_PHYSICAL = tgt.LOOKUP_HASH_ADDRESS_PHYSICAL
		WHERE
			tgt.LOOKUP_HASH_ADDRESS_PHYSICAL IS NULL;';--

	SET o_str_debug =  v_str_sql;--

	/*Debugging Logging*/
	SET v_str_sql_debug =  'INSERT INTO ' || v_process_database_name || '.AUDIT_LOG_JOB_EXECUTION_SQL_COMMANDS_H0 (JOB_EXECUTION_ID, SQL_SMNT) VALUES (' || v_stored_procedure_execution_id ||','|| quote_literal(v_str_sql) || ');';  EXECUTE IMMEDIATE v_str_sql_debug;--

	EXECUTE IMMEDIATE  v_str_sql;--

	/*Update Hub Table - Merge: HUB_LOAD_DIM_ADDRESS_PHYSICAL -> DIM_ADDRESS_PHYSICAL*/
	SET v_str_sql =  'MERGE INTO ' || v_target_database_name || '.' || v_target_table_name || ' AS tgt
		USING ' || v_staging_schema_read_write || '.' || v_source_table_name || ' AS src
			ON (tgt.LOOKUP_HASH_ADDRESS_PHYSICAL=src.LOOKUP_HASH_ADDRESS_PHYSICAL)
		WHEN MATCHED AND(
			COALESCE(tgt.PHYSICAL_ADDRESS_STORE_ADDRESS_FLAG, ''A'') <> COALESCE(src.PHYSICAL_ADDRESS_STORE_ADDRESS_FLAG, ''A'') OR
			COALESCE(tgt.PHYSICAL_ADDRESS_VERIFICATION_FLAG, ''N'') <> COALESCE(src.PHYSICAL_ADDRESS_VERIFICATION_FLAG, ''N'') OR
			COALESCE(tgt.PHYSICAL_ADDRESS_ANONIMZATION_FLAG, ''N'') <> COALESCE(src.PHYSICAL_ADDRESS_ANONIMZATION_FLAG, ''N'') OR
			COALESCE(tgt.GEOGRAPHY_ID, -2) <> COALESCE(src.GEOGRAPHY_ID, -2) OR
			COALESCE(tgt.PHYSICAL_ADDRESS_LATITUDE, 0.0) <> COALESCE(src.PHYSICAL_ADDRESS_LATITUDE, 0.0) OR
			COALESCE(tgt.PHYSICAL_ADDRESS_LONGITUDE, 0.0) <> COALESCE(src.PHYSICAL_ADDRESS_LONGITUDE, 0.0) OR
			COALESCE(tgt.ETL_SOURCE_DATA_DELETED_FLAG, ''N'') <> COALESCE(src.ETL_SOURCE_DATA_DELETED_FLAG, ''N'') OR
			COALESCE(tgt.ETL_SOURCE_TABLE_NAME, ''A'') <> COALESCE(src.ETL_SOURCE_TABLE_NAME, ''A''))
		THEN UPDATE SET
			tgt.PHYSICAL_ADDRESS_STORE_ADDRESS_FLAG=src.PHYSICAL_ADDRESS_STORE_ADDRESS_FLAG,
			tgt.PHYSICAL_ADDRESS_VERIFICATION_FLAG=src.PHYSICAL_ADDRESS_VERIFICATION_FLAG,
			tgt.PHYSICAL_ADDRESS_ANONIMZATION_FLAG=src.PHYSICAL_ADDRESS_ANONIMZATION_FLAG,
			tgt.GEOGRAPHY_ID=src.GEOGRAPHY_ID,
			tgt.PHYSICAL_ADDRESS_LATITUDE=src.PHYSICAL_ADDRESS_LATITUDE,
			tgt.PHYSICAL_ADDRESS_LONGITUDE=src.PHYSICAL_ADDRESS_LONGITUDE,
			tgt.ETL_SOURCE_DATA_DELETED_FLAG=src.ETL_SOURCE_DATA_DELETED_FLAG,
			tgt.ETL_SOURCE_TABLE_NAME=src.ETL_SOURCE_TABLE_NAME,
			tgt.ETL_UPDATE_TIMESTAMP=src.ETL_UPDATE_TIMESTAMP,
			tgt.ETL_MODIFIED_BY_JOB_ID=src.ETL_MODIFIED_BY_JOB_ID,
			tgt.ETL_MODIFIED_BY_PROCESS=src.ETL_MODIFIED_BY_PROCESS
		WITH UR;';--

	SET o_str_debug =  v_str_sql;--

	/*Debugging Logging*/
	SET v_str_sql_debug =  'INSERT INTO ' || v_process_database_name || '.AUDIT_LOG_JOB_EXECUTION_SQL_COMMANDS_H0 (JOB_EXECUTION_ID, SQL_SMNT) VALUES (' || v_stored_procedure_execution_id ||','|| quote_literal(v_str_sql) || ');';  EXECUTE IMMEDIATE v_str_sql_debug;--

	EXECUTE IMMEDIATE  v_str_sql;--

	/*
	README:
	Get ROWS PROCESSED row count for Hub Load job via row count of Hub Load table.
	*/

	--Target Table Row Count (TOTAL ROWS)
	SET v_str_sql_cursor = 'SELECT COUNT(1) FROM ' ||  v_target_database_name ||'.'|| v_target_table_name || '
							WHERE ETL_MODIFIED_BY_JOB_ID = ' || v_stored_procedure_execution_id || ';';

	PREPARE c_table
	FROM v_str_sql_cursor;
	OPEN c_table_cursor;
	FETCH c_table_cursor INTO p_processed_rows;
	CLOSE c_table_cursor;

   --Debugging Logging
	SET v_str_sql_debug =  'INSERT INTO ' || v_process_database_name || '.AUDIT_LOG_JOB_EXECUTION_SQL_COMMANDS_H0 (JOB_EXECUTION_ID, SQL_SMNT) VALUES (' || v_stored_procedure_execution_id ||','|| quote_literal(v_str_sql_cursor) || ');';
	EXECUTE IMMEDIATE v_str_sql_debug;

	--Target Table Row Count (NEW INSERT ROWS)
	SET v_str_sql_cursor = 'SELECT COUNT(1) FROM ' ||  v_target_database_name ||'.'|| v_target_table_name || '
							WHERE ETL_MODIFIED_BY_JOB_ID = ' || v_stored_procedure_execution_id || '
							AND ETL_CREATE_TIMESTAMP >= ' || quote_literal(v_job_execution_starttime) || '
							;';

	PREPARE c_table
	FROM v_str_sql_cursor;
	OPEN c_table_cursor;
	FETCH c_table_cursor INTO p_processed_insert_rows;
	CLOSE c_table_cursor;

   --Debugging Logging	-- p_processed_insert_rows, p_processed_update_rows
	SET v_str_sql_debug =  'INSERT INTO ' || v_process_database_name || '.AUDIT_LOG_JOB_EXECUTION_SQL_COMMANDS_H0 (JOB_EXECUTION_ID, SQL_SMNT) VALUES (' || v_stored_procedure_execution_id ||','|| quote_literal(v_str_sql_cursor) || ');';
	EXECUTE IMMEDIATE v_str_sql_debug;

	--Target Table Row Count (UPDATED ROWS)
	SET v_str_sql_cursor = 'SELECT COUNT(1) FROM ' ||  v_target_database_name ||'.'|| v_target_table_name || '
							WHERE ETL_MODIFIED_BY_JOB_ID = ' || v_stored_procedure_execution_id || '
							AND ETL_UPDATE_TIMESTAMP >= ' || quote_literal(v_job_execution_starttime) || '
							;';

	PREPARE c_table
	FROM v_str_sql_cursor;
	OPEN c_table_cursor;
	FETCH c_table_cursor INTO p_processed_update_rows;
	CLOSE c_table_cursor;

   --Debugging Logging
	SET v_str_sql_debug =  'INSERT INTO ' || v_process_database_name || '.AUDIT_LOG_JOB_EXECUTION_SQL_COMMANDS_H0 (JOB_EXECUTION_ID, SQL_SMNT) VALUES (' || v_stored_procedure_execution_id ||','|| quote_literal(v_str_sql_cursor) || ');';
	EXECUTE IMMEDIATE v_str_sql_debug;

	--Procedure Completed
	SET v_error_message =  'Stored Procedure Message: COMPLETED/SUCCESS > ' || v_hub_procedure_name || ' > JOB EXECUTION ID: ' || v_stored_procedure_execution_id || ' > Total Rows Processed into HUB table: ' || p_processed_rows || '.';
	--Populate EDWD_PROCESS.AUDIT_LOG_JOB_EXECUTION
	SET v_job_phase_id = 0;
	SET v_str_sql = 'CALL ' || v_process_database_name || '.P_LOG_JOB_H0( ' || quote_literal(v_job_execution_name) || ',' || v_stored_procedure_execution_id ||','|| quote_literal(v_error_message) || ',' || v_job_phase_id || ',' || p_parent_job_execution_id || ',' || p_source_table_id || ',' || quote_literal(v_target_table_name) || ',' || p_processed_rows || ',' || p_failed_rows || ',' || p_processed_insert_rows || ',' || p_processed_update_rows || ',' || p_processed_delete_rows || ');';
	SET o_str_debug =  v_str_sql;
	EXECUTE IMMEDIATE  v_str_sql;

/*End Stored Procedure*/
END