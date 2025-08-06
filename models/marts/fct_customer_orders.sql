-- Import CTEs
with

orders as (
    select * from {{ ref('int_orders') }}
),

customers as (
    select * from {{ ref('stg_jaffle_shop__customers') }}
),

-- Precompute array of distinct order_ids per customer
customer_order_arrays as (
    select 
        customer_id,
        array_agg(distinct order_id) as customer_order_ids
    from orders
    group by customer_id
),

-- Customer-level aggregates using window functions
customer_orders as (
    select 
        o.*,
        c.full_name,
        c.surname,
        c.givenname,

        min(o.order_date) over(partition by o.customer_id) as customer_first_order_date,
        min(o.valid_order_date) over(partition by o.customer_id) as customer_first_non_returned_order_date,
        max(o.valid_order_date) over(partition by o.customer_id) as customer_most_recent_non_returned_order_date,

        count(*) over(partition by o.customer_id) as customer_order_count,

        sum(case when o.valid_order_date is not null then 1 else 0 end) over(partition by o.customer_id) as customer_non_returned_order_count,

        sum(case when o.valid_order_date is not null then o.order_value_dollars else 0 end) over(partition by o.customer_id) as customer_total_lifetime_value

    from orders o
    inner join customers c on o.customer_id = c.customer_id
),

-- Join with customer_order_ids array
add_customer_order_ids as (
    select 
        co.*,
        coa.customer_order_ids
    from customer_orders co
    left join customer_order_arrays coa
        on co.customer_id = coa.customer_id
),

-- Add average order value
add_avg_order_values as (
    select 
        *,
        case 
            when customer_total_lifetime_value = 0 or customer_non_returned_order_count = 0
            then 0
            else customer_total_lifetime_value / customer_non_returned_order_count 
        end as customer_avg_non_returned_order_value
    from add_customer_order_ids
),

-- Final CTE
final as (
    select
        order_id,
        customer_id,
        surname,
        givenname,
        customer_first_order_date as first_order_date,
        customer_order_count as order_count,
        customer_total_lifetime_value as total_lifetime_value,
        order_value_dollars,
        order_status,
        payment_status,
        customer_avg_non_returned_order_value,
        customer_order_ids
    from add_avg_order_values
)

-- Final output
select * from final
