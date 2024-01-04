{{
    config(
        materialized='incremental',
        unique_key='LOOKUP_HASH_ADDRESS_PHYSICAL',
        on_schema_change='ignore'
    )
}}

Select * 
From 
    {{ ref('addr_session_join_dim_geography') }}
-- this is the is_incremental() block
{% if is_incremental() %}
  -- this will only include records that have a timestamp later than the latest one in the existing data
  WHERE ETL_UPDATE_TIMESTAMP > (SELECT MAX(ETL_UPDATE_TIMESTAMP) FROM {{ this }})
{% endif %}

union all

Select * 
From 
    {{ ref('GPSTORE_SESSION') }}
{% if is_incremental() %}
  -- this will only include records that have a timestamp later than the latest one in the existing data
  WHERE ETL_UPDATE_TIMESTAMP > (SELECT MAX(ETL_UPDATE_TIMESTAMP) FROM {{ this }})
{% endif %}

union all

Select * 
From 
    {{ ref('DBARCUD_join_CMASTER') }}
{% if is_incremental() %}
  -- this will only include records that have a timestamp later than the latest one in the existing data
  WHERE ETL_UPDATE_TIMESTAMP > (SELECT MAX(ETL_UPDATE_TIMESTAMP) FROM {{ this }})
{% endif %}

union all

Select * 
From 
    {{ source('edw_staging', 'CUR_CUS_BLC_ADDRESS') }}
{% if is_incremental() %}
  -- this will only include records that have a timestamp later than the latest one in the existing data
  WHERE ETL_UPDATE_TIMESTAMP > (SELECT MAX(ETL_UPDATE_TIMESTAMP) FROM {{ this }})
{% endif %}

union all

Select * 
From 
    {{ source('edw_staging', 'CUR_CUS_ORW_CUSTOMER_ADDRESS') }}
{% if is_incremental() %}
  -- this will only include records that have a timestamp later than the latest one in the existing data
  WHERE ETL_UPDATE_TIMESTAMP > (SELECT MAX(ETL_UPDATE_TIMESTAMP) FROM {{ this }})
{% endif %}