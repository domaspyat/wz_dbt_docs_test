with accounts_who_paid as (
    select account_Id from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_paid_subscription_with_type_and_tariff`   -- ID аккаунта
),
  registration_source_who_paid as (
    select attribution_data.* 
    from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_attribution_data` attribution_data
    inner join accounts_who_paid who_paid on attribution_data.account_id = who_paid.account_id
    ),

first_subscription_type as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_first_subscription_date_and_type`
    ),

profile_info as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info`
),

accounts_full_info as (
    select 
            distinct
            profile_info.account_id,                                            -- ID аккаунта
            profile_info.russian_country_name,                                  -- Название страны на русском языке
            profile_info.currency as account_currency,                          -- Валюта
            profile_info.type as account_type,                                  -- Тип аккаунта
            registration_source_who_paid.registration_date,                     -- Дата регистрации
            registration_source_who_paid.utm_source,                            -- Конечный источник откуда пришел клиент
            registration_source_who_paid.utm_medium,                            -- Тип источника откуда пришел клиент
            registration_source_who_paid.utm_campaign,                          -- Название или идентификатор рекламы
            registration_source_who_paid.utm_term,                              -- Ключевые слова, по которым пришли клиенты
            registration_source_who_paid.utm_content,                           -- Параметр для различия похожих объявлений или элементов внутри одной кампании (баннеров, кнопок, текстов)
            registration_source_who_paid.registration_source_agg_current,       -- Тип источника регистрации
            registration_source_who_paid.registration_source_current,           -- Конкретный источник регистрации
            registration_source_who_paid.account_registration_type_current,     -- Тип аккаунта, присвоенный по итогу кампании
            tariff,                                                             -- Тариф купленной подписки
            period,                                                             -- Период купленной подписки
            profile_info.is_employee                                            -- Это аккаунт сотрудника?
    from registration_source_who_paid
    left join first_subscription_type
    on registration_source_who_paid.account_id=first_subscription_type.account_id
    left join profile_info
    on registration_source_who_paid.account_id=profile_info.account_id
),
all_subscriptions as (
    select int_subscriptions_deduplicated_without_promised_date_combined_intervals.* from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_deduplicated_without_promised_date_combined_intervals` int_subscriptions_deduplicated_without_promised_date_combined_intervals
    inner join accounts_who_paid who_paid on int_subscriptions_deduplicated_without_promised_date_combined_intervals.account_id = who_paid.account_id

),
subscriptions_data as (
  select all_subscriptions.*,
         accounts_full_info.* except(account_id),
         first_value(subscription_start) over(partition by all_subscriptions.account_id order by subscription_start) as first_subscription_start,
         first_value(subscription_end) over(partition by all_subscriptions.account_id order by subscription_end desc) as last_subscription_end,
  from all_subscriptions 
  left join accounts_full_info on all_subscriptions.account_id = accounts_full_info.account_id 
)                   -- Таблица покупок подписок с детальной информацией о том, откуда пришел клиент (без обещанных платежей)
select *except(is_employee)
from subscriptions_data
where is_employee is false