with stg_accounts as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`stg_accounts`
),
months as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`stg_months`
),
affiliates as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`stg_affiliates`
), 
int_subscriptions_deduplicated_without_promised_date_combined_intervals as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_deduplicated_without_promised_date_combined_intervals`
),
profile_info as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info`
),
accounts_live_time as 
(SELECT distinct
        intervals.account_id,                                                                       -- Идентификатор аккаунта, соответствует id из таблицы stg_accounts
        accounts.country,                                                                           -- Страна
        month as live_month,                                                                        -- Месяц жизни пользователя. Формируется на основе истории подписок, формат 2022-11-29
        first_value(subscription_start) over (partition by intervals.account_id order by intervals.subscription_start) first_subscription_start,    -- Дата начала первой подписки
        date_trunc(subscription_start,month) as start_month,
        date_trunc(subscription_end,month) as end_month,                                            
        profile_info.register_date,                                                                 -- Дата регистрации клиента
        case when dense_rank() over (partition by intervals.account_id order by month) <= 3 then 'new'
        else 'old' end as client_living_type,                                                       -- Тип жизни клиента
        case when accounts.currency in ('RUR','KZT') then 'ru' else 'global' end as market_type,    -- Рынок
         account_segment_type as account_type                                                       -- Тип аккаунта
FROM int_subscriptions_deduplicated_without_promised_date_combined_intervals intervals
inner join stg_accounts accounts
            on accounts.account_id=intervals.account_id
inner join profile_info on intervals.account_id = profile_info.account_id and is_employee is false
left join affiliates affiliates
            on affiliates.child_id=accounts.account_id
inner join months months on date_trunc(subscription_start,month) <= month and month <= date_trunc(subscription_end,month)
      
)   -- Таблица клиентов и их времени жизни
select distinct *except(start_month,end_month),
    first_value(end_month) over (partition by account_id order by end_month desc) last_end_month    -- Месяц окончания последней подписки
from accounts_live_time