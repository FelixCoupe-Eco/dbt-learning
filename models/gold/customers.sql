with customers as (
    select * from {{ ref('stg_customers') }}
),
orders as  (
    select * from {{ ref ('stg_orders' )}}
),
order_payments as (
    select
        order_id,
        sum (case when order_status = 'success' then amount end) as amount
    from  {{ ref ('silver_orders') }}
    group by order_id
),
final as (
    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        orders.order_id,
        orders.order_date,
        coalesce (order_payments.amount, 0) as amount
    from customers
    left join orders using (customer_id)
    left join order_payments using (order_id)
)
select * from final