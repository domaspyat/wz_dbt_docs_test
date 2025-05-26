with bills_pay as (
    select  
        stg_billingPackages.account_id,
        bills.paid_at,                           -- (05.03.2025)
        paid_date, 
        round(cast(sum_in_rubles as decimal),4) as sum_in_rubles,
        original_sum,
        subscription_id,
        action,
        completed_date,
        'bills' as data_source,
        subscriptionupdates_guid as subscription_update_id ,
        subscription_updates_original_sum,
        wapi_discount_for_partners,
        wapi_original_sum,
        partner_discount,
        account_type,
        round(cast(wapi_transactions_in_rubles as decimal) ,4) as wapi_transactions_in_rubles,
        subscription_sum_in_rubles,
        subscription_sum_original,
        round(cast(sum_in_rubles_full_subscription as decimal),4) as sum_in_rubles_full_subscription,
        currency
    from `dwh-wazzup`.`dbt_nbespalov`.`int_payments_bills_only_valid` bills
    inner join `dwh-wazzup`.`dbt_nbespalov`.`stg_billingPackages` stg_billingPackages 
    on stg_billingPackages.guid=bills.subscription_id
),
card_pay as (
    select  
        account_id,                         -- ID аккаунта
        paid_at,                            -- Дата и время перехода платежа в state = completed (05.03.2025)
        paid_date,                          -- Дата оплаты
        sum_in_rubles,                      -- Сумма оплаты в рублях
        original_sum,                       -- Сумма оплаты
        subscription_id,                    -- ID подписки
        action,                             -- Действие с подпиской
        paid_date as completed_date,        -- Дата завершения оплаты
        data_source,                        -- Источник оплаты
        subscription_update_id ,            -- ID изменения. Соответствует guid из subscriptionUpdates
        subscription_updates_original_sum,  -- Сумма оплаты из subscriptionUpdates
        wapi_discount_for_partners,         -- Комиссия партнера за пополнение баланса WABA
        wapi_original_sum,                  -- Сумма оплаты баланса WABA
        partner_discount,                   -- Скидка партнера
        account_type,                       -- Тип аккаунта
        wapi_transactions_in_rubles,        -- Сумма оплаты баланса WABA в рублях
        subscription_sum_in_rubles,         -- Сумма оплаты подписки в рублях
        subscription_sum_original,          -- Сумма оплаты подписки
        sum_in_rubles_full_subscription,    -- Сумма оплаты изменения
        currency                            -- Валюта
    from `dwh-wazzup`.`dbt_nbespalov`.`int_payments_card_with_account_type_subscription_only`
),
   
revenue_union as (
    select * from bills_pay
    UNION ALL
    select * from card_pay
    )
    -- Таблица платежей через счёт или карту за подписки и баланс WABA
select * from revenue_union