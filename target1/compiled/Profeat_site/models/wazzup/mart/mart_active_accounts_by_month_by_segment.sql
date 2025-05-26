with segments as (
    SELECT  account_id,                 -- ID аккаунта
            segment,                    -- Сегмент
            subscription_end_fixed,     -- Дата завершения активности (всегда максимально указывается текущий день (current_date), даже если подписка завершается позже)
            subscription_start,         -- Дата начала активности
            lag(subscription_end_fixed, 1) over (partition by account_id order by subscription_start asc) as last_subscription_end_with_segment -- Завершение предыдущего периода активности по аккаунту
    FROM  `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_who_paid_in_dynamics_defining_clients_with_segments`
),
segments_with_if_return_payments as (
    select segments.*, 
        month, -- Месяц активности. Пользователь считается активным в этом месяце, если он был активен хотя бы день
            (case
                when
                    date_diff(subscription_start, last_subscription_end_with_segment, day) > 20
                    and date_trunc(last_subscription_end_with_segment, month) != month
                    and date_trunc(subscription_start, month) = month
                then 'return_payment_monthly'
                else 'other_payments'
            end) as payment_type_monthly    -- return_payment_monthly - если пользователь вернулся в отчетном месяце после 20 дней неактивности , other - в других случаях
    from segments
    inner join `dwh-wazzup`.`dbt_nbespalov`.`stg_months` months
    on months.month>=date_trunc(segments.subscription_start,month) and months.month<=date_trunc(segments.subscription_end_fixed,month)),

segments_with_if_new_payments as (
    select *, 
    first_value(date_trunc(subscription_start,month)) over (partition by account_id order by subscription_start ) as first_subscription_start_month -- Месяц первой активности
    from segments_with_if_return_payments),

segment_with_currency as (
select segments_with_if_new_payments.*, 
        region_international,       -- Регион
        russian_country_name,       -- Название страны на русском языке
        currency,                   -- валюта
        account_language
from segments_with_if_new_payments
inner join `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info` profile_info
            on profile_info.account_id=segments_with_if_new_payments.account_id
where profile_info.is_employee is false            
            ),

last_segment_in_month_to_deduplicate as (
    select *, 
    row_number() over (partition by account_id,month order by subscription_start desc) as rn -- внутренний параметр для дедупликации
    from segment_with_currency
),
subscriptions_history_with_dates as (
select *,
          1 cnt_in_a_month
from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_combined_by_type_all_subscriptions_free_trial_paid`
join `dwh-wazzup`.`dbt_nbespalov`.`stg_days` days on days.date >= subscription_start and days.date <= subscription_end
),count_of_days_for_each_type as (
select  account_id,
        date_trunc(date,month) month,
        sum(case when type = 'trial' then cnt_in_a_month else 0 end)                                               as trial_sum_in_a_month,
        sum(case when type = 'paid' then cnt_in_a_month else 0 end)                                                as paid_sum_in_a_month,
        sum(case when type in ('free_subscriptions','free_subscriptions_partners') then cnt_in_a_month else 0 end) as free_sum_in_a_month
from subscriptions_history_with_dates
group by 1,2
),defining_client_type as (
select *,
      case when paid_sum_in_a_month  > 0 then 'paid'
           when free_sum_in_a_month  > 0 then 'free'
           when trial_sum_in_a_month > 0 then 'trial'
           else 'unknown'
           end client_type 
from count_of_days_for_each_type)
  -- Показывает активность пользователей по месяцам. Запись появляется в таблице, если пользователь в этот месяце был активен хотя бы один день. Подробнее об активных аккаунтах https://www.notion.so/687832f855e84aefbb3b5b65c89b8923?pvs=4
select last_segment_in_month_to_deduplicate.*,
       client_type
from last_segment_in_month_to_deduplicate
join defining_client_type using(account_id, month)
where rn=1 --TODO удалить rn из готовой модели