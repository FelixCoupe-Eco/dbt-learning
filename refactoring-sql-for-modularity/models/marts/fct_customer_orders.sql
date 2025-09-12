-- refactoring of customer_orders_legacy
with
    -- identify order ids with successful payments
    successful_payments as (
        select
            order_id,
            max(created) as payment_finalized_date,
            sum(payment_amount) / 100.0 as total_amount_paid
        from {{ ref("stg_payments") }}
        where payment_status <> 'fail'
        group by order_id
    ),
    -- collect order/customer information for successful orders
    paid_orders as (
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
        left join successful_payments on orders.order_id = successful_payments.order_id
        left join {{ ref("stg_customers") }} customers on orders.customer_id = customers.customer_id
    ),
    -- Information about customer orders
    customer_order_info as (
        select
            customers.customer_id,
            min(orders.order_date) as first_order_date,
            max(orders.order_date) as most_recent_order_date,
            count(orders.order_id) as number_of_orders
        from {{ ref("stg_customers") }} customers
        left join {{ ref("stg_orders") }} as orders on orders.customer_id = customers.customer_id
        group by 1
    ),
    -- Total value of customer orders to date
    customer_value as (
        select 
            order_id, 
            customer_id,
            total_amount_paid,
            sum(total_amount_paid) over (
                    partition by customer_id 
                    order by order_id
                ) as customer_lifetime_value
        from paid_orders 
    ),
final as (
    select
        paid_orders.*,
        row_number() over (order by paid_orders.order_id) as transaction_seq, -- to understand the sequence of orders
        case
            when customer_order_info.first_order_date = paid_orders.order_date then 'new' 
            else 'returning'
        end as new_or_returning_customer,
        customer_value.customer_lifetime_value,
        customer_order_info.first_order_date as customer_first_order_date
    from paid_orders
    left join customer_order_info using (customer_id)
    left outer join customer_value on customer_value.order_id = paid_orders.order_id
    order by order_id
)
select * from final