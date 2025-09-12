-- refactoring of customer_orders_legacy
with
    -- Total value of each customer based on successful orders to date
    customer_value as (
        select 
            order_id, 
            customer_id,
            total_amount_paid,
            sum(total_amount_paid) over (
                    partition by customer_id 
                    order by order_id
                ) as customer_lifetime_value
        from {{ ref("int_paid_orders") }} paid_orders
    ),
final as (
    select
        paid_orders.order_id,
        paid_orders.customer_id,
        paid_orders.order_date,
        paid_orders.order_status,
        paid_orders.total_amount_paid,
        paid_orders.payment_finalized_date,
        paid_orders.customer_first_name,
        paid_orders.customer_last_name,
        row_number() over (order by paid_orders.order_id) as transaction_seq,
        case
            when customer_profile.first_order_date = paid_orders.order_date then 'new' 
            else 'returning'
        end as new_or_returning_customer,
        customer_value.customer_lifetime_value,
        customer_profile.first_order_date as customer_first_order_date
    from {{ ref("int_paid_orders") }} paid_orders
    left join {{ ref("int_customer_profile") }} customer_profile using (customer_id)
    left outer join customer_value on customer_value.order_id = paid_orders.order_id
    order by order_id
)
select * from final