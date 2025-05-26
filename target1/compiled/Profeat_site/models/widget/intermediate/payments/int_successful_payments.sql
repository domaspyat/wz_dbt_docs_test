with subscription_plan_info as (
    select * except(duration),
    concat(duration,' ', unit) as duration
    from `dwh-wazzup`.`dbt_nbespalov`.`stg_subscription_plan`
 ),filtered_users as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`stg_users_filtered_from_test`
 ),inner_event as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`stg_inner_event`
 ), 
    successful_payments_cleared_from_refunded as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_successful_payments_card_based_on_payment_table`
 ),
    all_payments_union as (
 select
       concat(date_trunc(inner_event.created_at,minute),inner_event.user_id) as id,
        date_trunc(inner_event.created_at,minute) as paid_at,
        cast(inner_event.created_at as date) paid_date,
        inner_event.user_id,
        case when name = 'bills_completed_partner' then 'cashless_partner'
             when name = 'bills_completed_client' then 'cashless_client'
             when name = 'payment_success_client' then 'payment_success_client'
             when name = 'payment_success_partner' then 'payment_success_partner'
             when name like '%payment_success%' then 'payment_success_old_event'
        end as payment_type,
        case when name like '%bills%' then 'cashlessPlan'
             else subscrtiption_info.id
        end  as subscription_plan_id,

        inner_event.duration,
        REGEXP_REPLACE(inner_event.duration,'[^0-9 ]','') duration_period,
        inner_event.order_id,
        inner_event.sum
from  inner_event 
join filtered_users on inner_event.user_id = filtered_users.id
left join subscription_plan_info  subscrtiption_info on inner_event.duration = subscrtiption_info.duration
 where (name like '%bills%' ) 
                         and (sum is null or sum is distinct from 0)
union all

select  distinct 
        concat(date_trunc(inner_event.created_at,minute),inner_event.user_id) as id,
        date_trunc(inner_event.created_at,minute) as paid_at,
        cast(inner_event.created_at as date) paid_date,
        inner_event.user_id,        
        case when name = 'bills_completed_partner' then 'cashless_partner'
             when name = 'bills_completed_client' then 'cashless_client'
             when name = 'payment_success_client' then 'payment_success_client'
             when name = 'payment_success_partner' then 'payment_success_partner'
             when name like '%payment_success%' then 'payment_success_old_event'
        end as payment_type,
         subscription_plan_id,
        inner_event.duration,
        REGEXP_REPLACE(inner_event.duration,'[^0-9 ]','') duration_period,
        inner_event.order_id,
        inner_event.sum
from inner_event 
join filtered_users on inner_event.user_id = filtered_users.id
left join subscription_plan_info  subscrtiption_info on inner_event.duration = subscrtiption_info.duration
left join successful_payments_cleared_from_refunded  on inner_event.order_id = successful_payments_cleared_from_refunded .order_id 
                                                        and inner_event.user_id = successful_payments_cleared_from_refunded .user_id

 where (name like '%payment_success%') 
                         and (sum is null or sum is distinct from 0)
        and cast(inner_event.created_at as date) >= '2023-09-12' -- начиная примерно с этой даты начали падать события об успешной оплате в разбивке на партнеров/клиентов
      and not exists (
             select order_id
             from `dwh-wazzup`.`dbt_nbespalov`.`stg_payment` payment
             where payment.order_id = inner_event.order_Id
             and status = 'refundSucceeded'
           )

union all

select
        concat(date_trunc(payment.created_at,minute),user_id) as id,
        date_trunc(payment.created_at,minute) as paid_at,
        cast(payment.created_at as date) paid_date,
        payment.user_id,
        'payment_success_old_event' as payment_type,
         subscription_plan_id as subscription_plan_id,
        subscrtiption_info.duration,
        REGEXP_REPLACE(subscrtiption_info.duration,'[^0-9 ]','') duration_period,
        payment.order_id,
        subscrtiption_info.amount as sum
from successful_payments_cleared_from_refunded payment
join filtered_users on payment.user_id = filtered_users.id
join subscription_plan_info  subscrtiption_info on payment.subscription_plan_id = subscrtiption_info.id
 where  cast(payment.created_at as date) < '2023-09-12')
,   defining_first_payment_date as (
 select user_id,
        min(paid_date) first_paid_date,
        min(paid_at) as first_paid_at
 from all_payments_union
 where sum is distinct from 1
 group by user_id
)
select all_payments_union.*,
        first_paid_date,
        first_paid_at
from all_payments_union
left join defining_first_payment_date using(user_id)