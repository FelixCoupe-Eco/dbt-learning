-- filter payments to successul only

select
    order_id,
    max(created) as payment_finalized_date,
    sum(payment_amount) / 100.0 as total_amount_paid
from {{ ref("stg_payments") }}
where payment_status <> 'fail'
group by order_id