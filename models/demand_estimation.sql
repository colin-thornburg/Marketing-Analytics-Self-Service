-- oos_probability_and_demand_estimation.sql

WITH zero_sales_blocks AS (
    SELECT *
    FROM {{ ref('base_zero_sale') }}
    -- Add necessary WHERE, GROUP BY, or additional logic here
),

oos_blocks_last_x_weeks AS (
    SELECT *
    FROM {{ ref('oos_last_x_weeks') }}
    -- Additional logic if needed
),

trailing_oos_days AS (
    SELECT *
    FROM {{ ref('trailing_oos') }}
    -- Additional logic if needed
),

joined_data AS (
    SELECT 
        zsb.*,
        olxw.*,
        tod.*
    FROM zero_sales_blocks zsb
    LEFT JOIN oos_blocks_last_x_weeks olxw ON zsb.id = olxw.id
    LEFT JOIN trailing_oos_days tod ON zsb.id = tod.id
    -- Add more JOINs if necessary
    -- Replace * with specific columns as needed
)

SELECT *
FROM joined_data
-- Add WHERE, GROUP BY, or ORDER BY clauses if needed
