with billing_affiliate as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`stg_billingAffiliate`
),

exchange_rates as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`stg_exchangeRates`
),

accounts as (
    select account_id, 
    type from `dwh-wazzup`.`dbt_nbespalov`.`stg_accounts`
),

billing_packages as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`stg_billingPackages`
),

subscription_deleted as (
    select occured_date as unsubscribe_date,                                            -- Дата удаления подписки
    billing_packages.type as subscription_type,                                         -- Тип подписки
    billing_affiliate.guid,                                                             -- ID подписки
    cast(billing_affiliate.account_id as string) as account_id,                         -- ID аккаунта
    coalesce(billing_affiliate.sum*cor_rate, billing_affiliate.sum) as sum_in_rubles    -- Сумма возврата на бонусный счёт
    from billing_affiliate
    inner join accounts on billing_affiliate.account_id = accounts.account_id
    left join exchange_rates on billing_affiliate.occured_date = exchange_rates._ibk
                                                            and billing_affiliate.currency = exchange_rates.currency
                                                            and nominal = 'RUR'
    inner join billing_packages on billing_packages.guid=billing_affiliate.guid                                                        
    where object = 'unsubscribe' and accounts.type != 'employee'
)

select * from subscription_deleted  -- Таблица удаленных саппортом подписок