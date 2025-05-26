with payments_all as (
    select (case when partner_account_id is not null then partner_account_Id else account_id end) as account_id,
    wapi_original_sum,                              -- Оригинальная сумма пополнения WABA
    wapi_transactions_in_rubles,                    -- Транзакции WABA в рублях
    wapi_sum_in_USD,                                -- Сумма пополнения WABA в долларах
    paid_at_billing_date,                           -- Дата и время оплаты
    currency                                        -- Валюта
    from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_with_sum_and_converted_currency_billing_date`
    where wapi_original_sum!=0
), -- Таблица платежей по WABA c суммой != 0

payments_all_groupped_by_account_and_payment_date as (
    select account_id,                              -- ID аккаунта
    paid_at_billing_date,                           -- Дата и время оплаты
    currency,                                       -- Валюта
    sum(wapi_original_sum) as wapi_original_sum,    -- Оригинальная сумма пополнения WABA
    sum(wapi_transactions_in_rubles) as wapi_transactions_in_rubles,    -- Транзакции WABA в рублях
    sum(wapi_sum_in_USD) as wapi_sum_in_USD         -- Сумма пополнения WABA в долларах
    from payments_all
    group by 1,2,3
), -- Таблица платежей по WABA с группировкой по аккаунтам, дате платежа и валюте

partner_type_and_account_type as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_type_and_partner_change_with_partner_type_deduplicated`
),  -- Таблица с информацией о партнере и его смене

profile_info as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info`
), -- Таблица с информацией об аккаунте

payments as (select payments_all.account_id,
    wapi_original_sum as original_sum,              -- Оригинальная сумма пополнения WABA
    wapi_sum_in_USD as wapi_sum_in_USD,             -- Сумма пополнения WABA в долларах
    payments_all.paid_at_billing_date,              -- Дата оплаты
    wapi_transactions_in_rubles	as sum_in_rubles,   -- Транзакции WABA в рублях
    payments_all.currency,                          -- Валюта
    partner_type_and_account_type.partner_id,       -- ID аккаунта партнера
    partner_type_and_account_type.refparent_id,     -- ID аккаунта реф. папы
    partner_type,                                   -- Тип партнера: partner, tech-partner, standart, tech-partner-postpay или null
    account_type,                                   -- Тип аккаунта: child-postpay, tech-partner, standart, tech-partner-postpay, employee, partner или null
    start_occured_at,                               -- Дата и время закрепления аккаунта за партнером
    start_date,                                     -- Дата закрепления за партнером
    partner_register_date                           -- Дата регистрации партнера
    from payments_all_groupped_by_account_and_payment_date payments_all
    left join  partner_type_and_account_type 
    on payments_all.account_id=partner_type_and_account_type.account_id
    and payments_all.paid_at_billing_date>=partner_type_and_account_type.start_date
    and payments_all.paid_at_billing_date<=partner_type_and_account_type.end_date
    left join profile_info
    on profile_info.account_id=partner_type_and_account_type.partner_id),

payments_to_deduplicate as (

select *, 
rank() over (partition by account_id, paid_at_billing_date, currency order by start_date desc) as rn  -- Ранг партиации
from payments
),

payments_deduplicated as (
  select *, date_trunc(paid_at_billing_date, month) as paid_month from payments_to_deduplicate        -- Месяц оплаты
  where rn=1
)
-- Таблица подписок WABA с суммой, валютой и данными о партнере
select * from payments_deduplicated