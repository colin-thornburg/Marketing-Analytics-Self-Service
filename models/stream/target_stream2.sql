{{
    config(
        materialized='incremental'
    )
}}

Select * from {{ source('customer_stream', 'stream_customer_data_raw') }}