with customers as (
    select * from {{ ref('stg_customers') }}
),
order_payments as (
    select
        order_id,
        customer_id,
        payment_status,
        order_status,
        payment_method,
        sum (case when payment_status = 'success' then amount end) as amount
    from  {{ ref ('silver_orders') }}
    group by order_id, customer_id, payment_status, order_status, payment_method
),
final as (
    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        order_payments.order_id,
        order_payments.order_status,
        order_payments.payment_status,
        coalesce (order_payments.amount, 0) as amount
    from customers
    left join order_payments using (customer_id)
)
select * from final