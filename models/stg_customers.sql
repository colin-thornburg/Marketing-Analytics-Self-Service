{{
    config(
        materialized='table'
    )
}}

Select * from {{ ref('customers') }}
