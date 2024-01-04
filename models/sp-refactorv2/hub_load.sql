{{
    config(
        materialized='incremental',
        unique_key='LOOKUP_HASH_ADDRESS_PHYSICAL'
    )
}}

Select * 
From 
    {{ ref('addr_session_join_dim_geography') }}
{% if is_incremental() %}
    -- Include only new records or records to be updated.
    Where
        C_B_ADDR_SES.LOOKUP_HASH_ADDRESS_PHYSICAL Not In (
            Select LOOKUP_HASH_ADDRESS_PHYSICAL From {{ this }}
        )
{% endif %}

union all

Select * 
From 
    {{ ref('GPSTORE_SESSION') }}
{% if is_incremental() %}
    -- Include only new records or records to be updated.
    Where
        C_B_ADDR_SES.LOOKUP_HASH_ADDRESS_PHYSICAL Not In (
            Select LOOKUP_HASH_ADDRESS_PHYSICAL From {{ this }}
        )
{% endif %}

union all

Select * 
From 
    {{ ref('DBARCUD_join_CMASTER') }}
{% if is_incremental() %}
    -- Include only new records or records to be updated.
    Where
        C_B_ADDR_SES.LOOKUP_HASH_ADDRESS_PHYSICAL Not In (
            Select LOOKUP_HASH_ADDRESS_PHYSICAL From {{ this }}
        )
{% endif %}

union all

Select * 
From 
    {{ source('edw_staging', 'CUR_CUS_BLC_ADDRESS') }}
{% if is_incremental() %}
    -- Include only new records or records to be updated.
    Where
        C_B_ADDR_SES.LOOKUP_HASH_ADDRESS_PHYSICAL Not In (
            Select LOOKUP_HASH_ADDRESS_PHYSICAL From {{ this }}
        )
{% endif %}

union all

Select * 
From 
    {{ source('edw_staging', 'CUR_CUS_ORW_CUSTOMER_ADDRESS') }}
{% if is_incremental() %}
    -- Include only new records or records to be updated.
    Where
        C_B_ADDR_SES.LOOKUP_HASH_ADDRESS_PHYSICAL Not In (
            Select LOOKUP_HASH_ADDRESS_PHYSICAL From {{ this }}
        )
{% endif %}