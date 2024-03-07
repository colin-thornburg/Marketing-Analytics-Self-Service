-- dim_products.sql (Place this under your dbt `models` directory)

WITH source_data AS (
    SELECT
        product_name,
        product_category
    FROM
        {{ ref('stg_products') }}
),

surrogate_key_generation AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY product_name) AS surrogate_key, -- Example surrogate key generation
        product_name,
        product_category
    FROM
        source_data
)

SELECT
    s.surrogate_key,
    s.product_name,
    s.product_category
FROM
    surrogate_key_generation s
LEFT JOIN
    {{ ref('dim_products') }} d
ON
    s.product_name = d.product_name
WHERE
    d.product_name IS NULL  -- This ensures only new products get added
