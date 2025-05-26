with last_month as (
select *, last_value(subscription_month) over (partition by account_id  order by subscription_month asc rows between unbounded preceding and unbounded following ) as last_value_tarif_month from 
`dwh-wazzup`.`dbt_nbespalov`.`mart_subscription_parameters_and_segment_by_month`
)
    -- Таблица аккаунтов и их каналов с тарифами с последним месяцем активности
select account_id,                                                                          -- ID аккаунта
 string_agg(distinct concat(subscription_type, ' - ', tariff_new))  as type_and_tariff,     -- Каналы и тарифы
 min(subscription_month) as last_active_subscription_month                                  -- Последний месяц активности подписки
  from last_month
where last_value_tarif_month=subscription_month and subscription_type!='equipment'
group by 1