with subscription_intervals as (
    select 
    date_trunc(subscription_start, month) as subscription_start_month,
    date_trunc(subscription_end, month) as subscription_end_month,
    account_id
    from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_deduplicated_without_promised_date_combined_intervals`
),

affiliates as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`stg_affiliates` 
),

months as (
    select * from `dwh-wazzup`.`analytics_tech`.`months`
)

    -- Таблица, которая показывает активные аккаунты по месяцам
select distinct months.month, account_id        -- Месяц.  -- ID аккаунта
  from  subscription_intervals
inner join  months
on months.month>=subscription_start_month and months.month<=subscription_end_month
inner join affiliates on affiliates.child_id=subscription_intervals.account_id
where affiliates.name!='Аккаунт для демо'