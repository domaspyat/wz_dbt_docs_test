with segments as (
    select subscription_start as segment_start,
    subscription_end_with_last_payment_date as segment_end,
    account_id,
    segment from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_who_paid_in_dynamics_combined_intervals_all_segments`

), 

client_types as (
    select *
    from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_defining_clients_types`

),

defining_clients_with_segments as (
    select coalesce(client_types.account_id, segments.account_id) as account_id,                        -- ID аккаунта
    coalesce((case when subscription_start>=segment_start then subscription_start end), segment_start
    ) as subscription_start,                    -- Дата начала активности
    subscription_start as active_period_start,  -- Дата начала активности/дата начала сегмента
    coalesce((case when subscription_end<=segment_end then subscription_end end),
    segment_end)  as subscription_end,          -- Дата завершения активности
    subscription_end as active_period_end,      -- Дата завершения активности/дата завершения сегмента
    segment_start,                              -- Дата начала сегмента
    segment_end,                                -- Дата завершения сегмента
    segment,                                    -- Сегмент
    has_paid                                    -- True - если пользователь когда-либо оплачива подписку
     from client_types
    full outer join  segments
    on segments.account_id=client_types.account_id
    and segments.segment_start<=client_types.subscription_end
    where segment is not null and subscription_start is not null),


defining_clients_with_segments_to_deduplicate as (
    select *, row_number() over (partition by account_id, subscription_start order by segment_start desc) as rn -- Внутреннее поле - для дедупликации
    from defining_clients_with_segments
),

segments_final as (
    select * from defining_clients_with_segments_to_deduplicate
    where rn=1
),

segments_final_with_date as (
    select segments_final.*,
    days.date                   -- Дата - появяется, если пользователь был активен в этот день
    from segments_final
    inner join  `dwh-wazzup`.`analytics_tech`.`days` days
    on days.date>=segments_final.subscription_start and days.date<=subscription_end),

segments_with_if_new_payments as (
    select *, 
    first_value(date_trunc(subscription_start,month)) over (partition by account_id order by subscription_start ) as first_subscription_start_month,    -- Месяц начала активности
    first_value(subscription_start) over (partition by account_id order by subscription_start ) as first_subscription_start_date                        -- Дата начала активности
    from segments_final_with_date),

segment_with_currency as (
select segments_with_if_new_payments.*, 
currency            -- Валюта пользователя на сегодняшний день
from segments_with_if_new_payments
inner join `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info` profile_info
on profile_info.account_id=segments_with_if_new_payments.account_id)
    -- Показывает активность пользователей по дням. Запись появляется в таблице, если пользователь в этот день был активен. Подробнее об активных аккаунтах https://www.notion.so/687832f855e84aefbb3b5b65c89b8923?pvs=4
select * from segment_with_currency