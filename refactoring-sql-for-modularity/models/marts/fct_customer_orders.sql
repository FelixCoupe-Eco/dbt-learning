-- Original customer_orders model (non-refactored)
with
    paid_orders as (
        select
            orders.order_id,
            orders.customer_id,
            orders.order_date,
            orders.order_status,
            payments.total_amount_paid,
            payments.payment_finalized_date,
            customers.first_name as customer_first_name,
            customers.last_name as customer_last_name
        from {{ ref("stg_orders") }} as orders
        left join
            (
                select
                    order_id,
                    max(created) as payment_finalized_date,
                    sum(payment_amount) / 100.0 as total_amount_paid
                from {{ ref("stg_payments") }}
                where payment_status <> 'fail'
                group by 1
            ) payments
            on orders.order_id = payments.order_id
        left join {{ ref("stg_customers") }} customers on orders.customer_id = customers.customer_id
    ),

    customer_orders as (
        select
            customers.customer_id,
            min(order_date) as first_order_date,
            max(order_date) as most_recent_order_date,
            count(orders.order_id) as number_of_orders
        from {{ ref("stg_customers") }} customers
        left join {{ ref("stg_orders") }} as orders on orders.customer_id = customers.customer_id
        group by 1
    )

select
    p.*,
    row_number() over (order by p.order_id) as transaction_seq,
    row_number() over (
        partition by customer_id order by p.order_id
    ) as customer_sales_seq,
    case
        when c.first_order_date = p.order_date then 'new' else 'return'
    end as nvsr,
    x.clv_bad as customer_lifetime_value,
    c.first_order_date as fdos
from paid_orders p
left join customer_orders as c using (customer_id)
left outer join
    (
        select p.order_id, sum(t2.total_amount_paid) as clv_bad
        from paid_orders p
        left join
            paid_orders t2
            on p.customer_id = t2.customer_id
            and p.order_id >= t2.order_id
        group by 1
        order by p.order_id
    ) x
    on x.order_id = p.order_id
order by order_id
