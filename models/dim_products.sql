-- models/marts/dim_products.sql

WITH source_data AS (
    SELECT
        product_name,
        product_category
    FROM
        {{ ref('stg_products') }}
),

surrogate_key_assignment AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY product_name) AS surrogate_key,
        product_name,
        product_category
    FROM
        source_data
),

final_output AS (
    SELECT
        s.surrogate_key,
        s.product_name,
        s.product_category
    FROM
        surrogate_key_assignment s
    LEFT JOIN
        {{ this }} d
    ON
        s.product_name = d.product_name
    WHERE
        d.product_name IS NULL

    UNION ALL

    SELECT
        d.surrogate_key,
        d.product_name,
        d.product_category
    FROM
        {{ this }} d
)

SELECT * FROM final_output