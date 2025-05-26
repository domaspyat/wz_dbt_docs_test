with last_payment_guid_not_promised_payment as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_who_paid__last_payment_guid_not_promised_payment`
),

payments as (
    select 
    guid,
    cast(partner_account_id as integer) as partner_account_id
    from `dwh-wazzup`.`dbt_nbespalov`.`int_payments_card`
),

old_payments_last_guid as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_who_paid__old_payments_last_guid`
),

current_account_type_and_parents as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_partner_refparent_account_type_partner_type__current`
    where account_type='standart'
),

last_sub_with_payments as (select 
         last_payment.account_id,                           -- ID аккаунта
         last_payment.activation_reason_id,                 -- тут док акт ризон айди
         coalesce(payments.partner_account_id,old_payments_last_guid.partner_account_id) as payer_partner_account_id,   -- ID аккаунта партнера, если он плательщик
         current_account_type_and_parents.partner_Id,       -- ID партнера
         current_account_type_and_parents.refparent_id,     -- ID реф. партнера
         current_account_type_and_parents.partner_type      -- Тип аккаунта партнера
         from last_payment_guid_not_promised_payment last_payment
         left join payments on last_payment.activation_reason_id=payments.guid
         left join old_payments_last_guid on last_payment.account_id=old_payments_last_guid.account_id 
         left join current_account_type_and_parents on last_payment.account_id=current_account_type_and_parents.account_id
         )
    -- Таблица с аккаунтами и плательщиками, стары и новый биллинг совмещен
select * from last_sub_with_payments