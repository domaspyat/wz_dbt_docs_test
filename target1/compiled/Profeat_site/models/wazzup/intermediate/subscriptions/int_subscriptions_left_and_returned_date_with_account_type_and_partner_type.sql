with subscription_by_days as (
SELECT *
 FROM `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_defining_clients_types_who_paid_without_trials_and_promised_payments`),

partner_type_and_account_type as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_type_and_partner_change_with_partner_type_deduplicated`
),
subscription_with_account_and_partner_type as (

select subscription_by_days.*,
 account_type,                                  -- Тип аккаунта
  partner_type,                                 -- Тип аккаунта партнера
  partner_type_and_account_type.refparent_id,   -- ID аккаунта реф. партнера
  start_date,                                   -- Дата регистрации
affiliates.reflink_code                         -- Код реферала
 from subscription_by_days 
 left join  partner_type_and_account_type 
    on subscription_by_days.account_id=partner_type_and_account_type.account_id
    and subscription_by_days.subscription_start>=partner_type_and_account_type.start_date
    and subscription_by_days.subscription_start<=partner_type_and_account_type.end_date
left join `dwh-wazzup`.`dbt_nbespalov`.`stg_affiliates` affiliates on affiliates.child_id=partner_type_and_account_type.account_id ),

subscription_duplicates as (

select *, 
(case when account_type='standart' and partner_type='partner' and reflink_code='manual_registration' then 'of-partner-client_manual'
when account_type='standart' and partner_type='partner' and reflink_code is distinct from 'manual_registration' then 'of-partner-client_ref_link'
when account_type='standart' and partner_type='tech-partner' then 'tech-partner-client'
when account_type='standart' and partner_type='tech-partner-postpay' then 'tech-partner-client'
when account_type='standart' and refparent_id is not null and partner_type is null then 'referal'
when account_type='standart' and partner_type is null then 'standart'
when account_type='partner' then 'of-partner'
when account_type='tech-partner' then 'tech-partner'
when account_type='tech-partner-postpay' then 'tech-partner'
when account_type='standart' and partner_type='standart' then 'standart'
end
) as account_type_partner_type, -- Тип аккаунта с зависимостью от партнера
row_number() over (partition by account_id, subscription_start order by start_date desc) rn from -- Партиация по номеру аккаунта и дате начала подписки
subscription_with_account_and_partner_type)

select * from subscription_duplicates
where rn=1