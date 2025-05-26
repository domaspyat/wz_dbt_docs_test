with free_subscriptions as (
    select account_Id,              -- ID аккаунта
    created_date as paid_date,      -- Дата создания бесплатной подписки
    full_tarif_sum_in_rubles        -- Полная сумма оплаты в рублях
     from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_full_sum__free_subscripions`
),

paid_subscrtiptions as (
    select  account_Id,                -- ID аккаунта
    paid_at_billing_date as paid_date, -- Дата оплаты подписки
    full_tarif_sum_in_rubles           -- Полная сумма оплаты в рублях
     from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_full_sum__paid_subscriptions`
),

subscriptions_all as (

select * from free_subscriptions
union all
select * from paid_subscrtiptions),

subscritions_sum as (

select subscriptions_all.account_id,    -- ID аккаунта
sum(full_tarif_sum_in_rubles) as full_tarif_sum_in_rubles   -- Полная сумма оплаты в рублях
 from subscriptions_all inner join `dwh-wazzup`.`dbt_nbespalov`.`stg_accounts` accounts 
on accounts.account_id=subscriptions_all.account_id 
where subscriptions_all.paid_date<=date_add(accounts.register_date, interval 1 month)
and accounts.type='standart'
group by 1)

select * from subscritions_sum  -- Таблица с полной суммой первой оплаты, если она была в течение месяца после регистрации