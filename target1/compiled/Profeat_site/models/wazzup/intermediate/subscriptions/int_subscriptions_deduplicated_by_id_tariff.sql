with combined_intervals as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_subscription_deduplicated_by_id_combined_intervals`
),

subscription_with_months  as (  
    select distinct
    month,              -- Месяц
    subscription_id,    -- ID подписки
    account_id          -- ID аккаунта
    from combined_intervals inner join `dwh-wazzup`.`dbt_nbespalov`.`stg_months` months
    on months.month>=date_trunc(combined_intervals.subscription_start,month) and months.month<=date_trunc(combined_intervals.subscription_end,month)
),

last_value_tarif as (          
    select subscription_id,                                             -- ID подписки
    date_trunc(stg_subscriptionUpdates.paid_date, month) as paid_month, -- Месяц покупки подписки
    last_value(tariff_new) over (partition by subscription_id, date_trunc(stg_subscriptionUpdates.paid_date, month) order by created_at asc rows between unbounded preceding and unbounded following) as tariff_new
    from `dwh-wazzup`.`dbt_nbespalov`.`int_subscription_updates__corect_date_and_filledna_tarif_period_quantity` stg_subscriptionUpdates 
    where tariff_new is not null
),

last_value_tarif_by_subscription_id as (
    select * from last_value_tarif
    where tariff_new is not null 
    group by 1,2,3
),

last_value_tarif_next_month as ( 
    select *, coalesce(lag(paid_month) over (partition by subscription_id order by paid_month desc), date_add(current_date, interval 1 month)) as next_month
    from last_value_tarif_by_subscription_id
),

tarif_info as (
    select subscription_with_months.*, 
    tariff_new from subscription_with_months
    left join last_value_tarif_next_month
    on last_value_tarif_next_month.subscription_id=subscription_with_months.subscription_id
    and subscription_with_months.month>=last_value_tarif_next_month.paid_month and 
    subscription_with_months.month<last_value_tarif_next_month.next_month)
        -- Таблица, которая отражает есть ли у аккаунта в определенном месяце тариф PRO или MAX
 select account_id, -- ID аккаунта
 month,             -- Месяц
 max(case when tariff_new='max' then True else False end) as has_max,       -- Есть ли MAX?
  max(case when tariff_new='pro' then True else False end) as has_pro       -- Есть ли PRO?
  from tarif_info
  group by 1,2