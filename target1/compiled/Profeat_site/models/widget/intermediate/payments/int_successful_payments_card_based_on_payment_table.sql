with refunded_payments as (
    select *
    from `dwh-wazzup`.`dbt_nbespalov`.`stg_payment`
    where status = 'refundSucceeded'
)
select
        concat(created_at,user_id) as ID,
        created_at,
        user_id,
        'payment' as payment_type,
        subscription_plan_id,
        null as duration,
        null as duration_period,
        order_id as order_id
from  `dwh-wazzup`.`dbt_nbespalov`.`stg_payment` payments
where status =  'succeeded'
        and not exists (
                select refunded_payments.order_Id 
                from refunded_payments
                where payments.order_Id = refunded_payments.order_Id
)