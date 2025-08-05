select 
    id as customer_id,
    orderid as order_id, 
    paymentmethod as payment_method, 
    status as order_status, 
    amount / 100 as order_amount, 
    created as created_at
from {{ source('stripe', 'payment') }}
