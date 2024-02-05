
with joined_data as (
    select
        o.order_id,
        o.customer_id,
        c.name as customer_name,
        c.email as customer_email,
        o.order_date
    from {{ ref('orders') }} o
    join {{ ref('customers') }} c
    on o.customer_id = c.id
)

-- Select data from the common table expression (CTE)
select
    *
from joined_data;