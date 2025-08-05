with shop_orders as (
    select * from {{ ref('stg_orders') }}
),
stripe_payments as (
    select * from {{ ref('stg_stripe_payments') }}
),
final as (
    select 
        spp.order_id, 
        spp.customer_id,
        spp.payment_method,
        spp.order_status as payment_status,
        so.status as order_status,
        spp.order_amount as amount
    from stripe_payments spp
    left join shop_orders so using (order_id, customer_id)
)
select * from final
