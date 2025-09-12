-- Collect some basic profile information about customers

select
    customers.customer_id,
    min(orders.order_date) as first_order_date,
    max(orders.order_date) as most_recent_order_date,
    count(orders.order_id) as number_of_orders
from {{ ref("stg_customers") }} customers
left join {{ ref("stg_orders") }} as orders on orders.customer_id = customers.customer_id
group by customers.customer_id