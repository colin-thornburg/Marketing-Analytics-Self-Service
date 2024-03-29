SELECT
    product_name,
    product_category
FROM
   {{ ref('stg_products_raw') }} 
