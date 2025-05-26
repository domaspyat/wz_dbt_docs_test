with payments_with_min_date as (
    select *,
    min(paid_month) over (partition by account_id) as first_payment_month,                 -- Месяц первой оплаты, формат 2022-11-29
    dense_rank() over (partition by account_id order by paid_month) as month_payment_order -- Порядковый номер оплаты, в порядке возрастания
    from `dwh-wazzup`.`dbt_nbespalov`.`int_payments_with_account_and_partner_type` 
    where account_type!='employee'),

registration_source as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_attribution_data`
    ),

first_subscription_type as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_first_subscription_date_and_type`
    ),

profile_info as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info`
),

payments_with_registration_source as (
    select payments_with_min_date.*,
    profile_info.russian_country_name,                          -- Название страны на русском языке
    profile_info.currency as account_currency,                  -- Валюта
    registration_source.registration_date,                      -- Дата регистрации
    registration_source.utm_source,                             -- Извлечение UTM source из URL, по которому зарегистрировался клиент
    registration_source.utm_medium,                             -- Извлечение UTM medium из URL, по которому зарегистрировался клиент
    registration_source.utm_campaign,                           -- Извлечение UTM campaign из URL, по которому зарегистрировался клиент
    registration_source.utm_term,                               -- Извлечение UTM term из URL, по которому зарегистрировался клиент
    registration_source.utm_content,                            -- Извлечение UTM content из URL, по которому зарегистрировался клиент
    registration_source.registration_source_agg_current,        -- Агрегированная версия registration_source_current и account_type_current. Если аккаунт как-то связан с партнёрами, то текущий тип аккаунта, иначе источник регистрации. Приставка current используется так как тут показывается текущий тип аккаунта, а не в момент регистрации
    registration_source.registration_source_current,            -- Детальное указание источника регистрации (для обычных аккаунтов) и текущего типа аккаунта для партнёрских. Приставка current используется так как тут показывается текущий тип аккаунта, а не в момент регистрации
    registration_source.account_registration_type_current,      -- То же самое, что и registration_source_agg_current, но здесь выделяется всего две категории, либо обычный аккаунт, либо связан с партнёрами. Приставка current используется так как тут показывается текущий тип аккаунта, а не в момент регистрации
    profile_info.region_international,                          -- Регион
    tariff,                                                     -- Тариф первой купленной подписки
    period,                                                     -- Период первой купленной подписки
    profile_info.is_employee                                    -- Это аккаунт сотрудника?
    from payments_with_min_date
    left join registration_source
    on payments_with_min_date.account_id=registration_source.account_id
    left join first_subscription_type
    on payments_with_min_date.account_id=first_subscription_type.account_id
    left join profile_info
    on payments_with_min_date.account_id=profile_info.account_id
)   -- Таблица используется для когоротного анализа по оплатам.Она нужна, чтобы отслеживать конверсию в повторную покупку, сравнивать когорты между собой, смотреть LTV
select * 
from payments_with_registration_source
where not exists (
    select account_id
    from profile_info
    where is_employee
    and profile_info.account_id = payments_with_registration_source.account_id
)