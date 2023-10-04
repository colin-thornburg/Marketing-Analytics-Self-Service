
--Demo with Experts
--Campaigns_orders.sql
WITH order_analytics AS (
    SELECT
        ORDER_ID,
        CUSTOMER_ID,
        FIRST_NAME,
        LAST_NAME,
        ORDER_DATE,
        STATE,
        PAYMENT_ID,
        PAYMENT_METHOD,
        AMOUNT,
        CREATED_AT,
        FIRST_ORDER_DATE,
        MOST_RECENT_ORDER_DATE
    FROM {{ ref('abc_company','order_analytics') }}
),

campaigns AS (
    SELECT
        CAMPAIGN_ID,
        CUSTOMER_ID,
        CAMPAIGN_NAME,
        CAMPAIGN_START_DATE,
        CAMPAIGN_END_DATE,
        PROMO_CODE,
        PROMO_DISCOUNT
    FROM {{ ref('campaigns') }}
)

SELECT
    o.ORDER_ID,
    o.CUSTOMER_ID,
    o.FIRST_NAME,
    o.LAST_NAME,
    o.ORDER_DATE,
    o.STATE,
    o.PAYMENT_ID,
    o.PAYMENT_METHOD,
    o.AMOUNT,
    o.CREATED_AT,
    o.FIRST_ORDER_DATE,
    o.MOST_RECENT_ORDER_DATE,
    c.CAMPAIGN_ID,
    c.CAMPAIGN_NAME,
    c.CAMPAIGN_START_DATE,
    c.CAMPAIGN_END_DATE,
    c.PROMO_CODE,
    c.PROMO_DISCOUNT
FROM order_analytics o
JOIN campaigns c ON o.CUSTOMER_ID = c.CUSTOMER_ID
