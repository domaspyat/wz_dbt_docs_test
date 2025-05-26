with partner_type_account_type_refparent_partner as (
    select account_id,
    refparent_id,
    partner_id,
    account_type,
    partner_type,
    start_date from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_type_and_partner_change_with_partner_type_deduplicated`
),

partner_type_account_type_refparent_partner_with_row_number as (
    select *,
    row_number() over (partition by account_id order by start_date asc) as rn
     from partner_type_account_type_refparent_partner
),
partner_type_account_type_refparent_partner_on_registration_date as (
    select account_id,  -- ID аккаунта
    start_date,         -- Дата начала действия изменения
    partner_id,         -- ID партнера
    refparent_id,       -- ID реф. партнера
    account_type,       -- Тип аккаунта
    partner_type        -- Тип аккаунта партнера
     from partner_type_account_type_refparent_partner_with_row_number  
    where rn=1
)   -- Таблица с аккаунтами и их взаимосвязями с партнерами, а также типами этих аккаунтов и датой начала действия изменения взаимосвязи
select * from partner_type_account_type_refparent_partner_on_registration_date