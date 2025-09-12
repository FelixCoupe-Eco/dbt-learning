-- PROGRESS got to min 5 in vid here: https://learn.getdbt.com/learn/course/refactoring-sql-for-modularity/part-2-practice-refactoring-90min/practice-refactoring?page=5

-- refactoring of customer_orders_legacy
with
    -- Import CTEs
    paid_orders as (
        select * from {{ ref("int_paid_orders") }}
    ),
    customer_profile as (
        select * from {{ ref("int_customer_profile") }}
    ), 
    -- Logical CTEs
    -- Final CTE
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
            ROW_NUMBER() OVER (PARTITION BY paid_orders.customer_id ORDER BY paid_orders.order_id) as customer_sales_seq,
            case
                when customer_profile.first_order_date = paid_orders.order_date then 'new' 
                else 'returning'
            end as nvsr, -- Binary for new or returning customer
            sum(total_amount_paid) over (
                    partition by paid_orders.customer_id 
                    order by paid_orders.order_date
                ) as customer_lifetime_value,
            customer_profile.first_order_date as fdos -- customer_first_order_date
        from paid_orders
        left join customer_profile on customer_profile.customer_id = paid_orders.customer_id
        order by order_id
    )
select * from final