with payer_segment_with_payer_account as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_who_paid__payer_account`
),

accounts as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`stg_accounts`
),

currency_with_payments as (
    select payer_segment_with_payer_account.account_id, -- ID аккаунта
    partner_type,                                       -- Тип аккаунта партнера
    segment_type,                                       -- Сегмент клиента
    payer_account,                                      -- ID аккаунта плательщика
    accounts.currency as payer_account_currency         -- Валюта плательщика
    from payer_segment_with_payer_account 
    left join accounts on payer_segment_with_payer_account.payer_account=accounts.account_id)
    -- Таблица с аккаунтами, их сегментами и плательщиком
select * from currency_with_payments