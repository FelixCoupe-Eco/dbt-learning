select 
    order_id, 
    customer_id,
    order_status,
    order_amount as amount
from {{ ref('stg_stripe_payments') }}
