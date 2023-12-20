{{-
    config(
        materialized='incremental_stream'
    )
-}}
SELECT * FROM incr_stream.stream_source('customer_stream', 'stream_customer_data_raw')