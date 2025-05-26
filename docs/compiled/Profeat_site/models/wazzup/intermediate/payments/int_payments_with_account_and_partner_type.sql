with payments_all as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_payments_all_payments_union`
),

partner_type_and_account_type as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_type_and_partner_change_with_partner_type_deduplicated`
),

profile_info as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info`
),

payments as (select payments_all.account_id,    -- ID аккаунта
    payments_all.sum_in_rubles, -- Сумма оплаты в рублях
    payments_all.sum_in_USD,    -- Сумма оплаты в долларах
    original_sum,               -- Сумма оплаты
    payments_all.paid_date,     -- Дата оплаты
    payments_all.currency,      -- Валюта
    payments_all.data_source,   -- Источник оплаты
    partner_type_and_account_type.partner_id,   -- ID аккаунта партнера
    partner_type_and_account_type.refparent_id, -- ID аккаунта реф. партнера
    partner_type,               -- Тип аккаунта партнера
    account_type,               -- Тип аккаунта
    start_occured_at,           -- Дата и время начала действия изменения
    start_date,                 -- Дата начала действия изменения
    partner_register_date       -- Дата регистрации партнера
    from payments_all
    left join  partner_type_and_account_type 
    on payments_all.account_id=partner_type_and_account_type.account_id
    and payments_all.paid_date>=partner_type_and_account_type.start_date
    and payments_all.paid_date<=partner_type_and_account_type.end_date
    left join profile_info
    on profile_info.account_id=partner_type_and_account_type.partner_id
    where original_sum!=0),

payments_to_deduplicate as (

select *, row_number() over (partition by account_id, paid_date, currency, data_source order by start_date desc) as rn -- Самые поздние платежи по account_id, paid_date, currency, data_source
from payments
),

payments_deduplicated as (
  select *, date_trunc(paid_date, month) as paid_month -- Месяц оплаты
  from payments_to_deduplicate
  where rn=1
)
    -- Таблица платежей с номерами аккантов и типом партнера
select * from payments_deduplicated