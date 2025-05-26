with
    original as (
        select * from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_last_end_date_without_trials_and_promised_payments`
    ),
    clients_type as (
        select * from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_defining_clients_types_without_trials`
    ), 
    last_subscription as (
        select * from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_last_end_date_with_months_weeks_without_trials`
    )
select  -- Таблица, которая показывает динамику активности клиента с информацией об отвалах (без триалов)
    data_otvala,                                    -- Дата отвала клиента
    account_id,                                     -- ID аккаунта
    week,                                           -- Неделя активности клиента
    month,                                          -- Месяц активности клиента
    currency,                                       -- Валюта
    --segment,
    clients_type,                                   -- Тип клиента
    payment_type_weekly,                            -- Тип оплаты недельный
    payment_type_monthly,                           -- Тип оплаты месячный
    date_trunc(data_otvala, month) as month_date    -- Месяц отвала клиента
from last_subscription
order by account_id asc