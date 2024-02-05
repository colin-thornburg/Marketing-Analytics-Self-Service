{{
    config(
        materialized='table'
    )
}}

Select * from {{ ref('orders') }}