-- collect data about orders and customers for paid orders

select
    orders.order_id,
    orders.customer_id,
    orders.order_date,
    orders.order_status,
    successful_payments.total_amount_paid,
    successful_payments.payment_finalized_date,
    customers.first_name as customer_first_name,
    customers.last_name as customer_last_name
from {{ ref("stg_orders") }} as orders
left join {{ ref("int_successful_payments") }} as successful_payments 
    on orders.order_id = successful_payments.order_id
left join {{ ref("stg_customers") }} as customers 
    on orders.customer_id = customers.customer_id