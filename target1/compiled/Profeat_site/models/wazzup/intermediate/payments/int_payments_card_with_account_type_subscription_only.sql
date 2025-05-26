with card_pay as (select  stg_billingPackages.account_id,  -- ID аккаунта
            paid_date,                      -- Дата перехода платежа в state = completed
            payments.paid_at,               -- Дата и время перехода платежа в state = completed (05.03.2025)
            payments.sum_in_rubles,         -- Сумма оплаты в рублях
            payments.subscription_id,       -- ID подписки
            subscription_updates.action,    -- Действие с подпиской
            payments.original_sum,          -- Сумма оплаты
            'card' as data_source,          -- Источник оплаты
            subscription_updates.guid as subscription_update_id,                    -- ID изменения. Соответствует guid из subscriptionUpdates
            subscription_updates.sum as sum_in_rubles_full_subscription,            -- Полная сумма оплаты за действие в рублях
            subscription_updates.original_sum as subscription_updates_original_sum, -- Сумма из subscriptionUpdates
            wapi_transactions_in_rubles,    -- Сумма оплаты баланса WABA в рублях
            wapi_original_sum,              -- Сумма оплаты баланса WABA
            partner_discount,               -- Скидка партнера
            account_type,                   -- Тип аккаунта
            payments.currency,              -- Валюта
            (case when partner_discount is not null and ((payments.paid_date>='2022-11-28'  and account_type='partner') or (payments.paid_date>='2023-02-10' and account_type='tech-partner')) then 0.1*wapi_transactions_in_rubles end) as wapi_discount_for_partners, --скидку для оф. партнеров ввели 2022-11-28, для тех. партнеров 2023-02-10
            subscription_updates.sum_in_rubles-coalesce(subscription_updates.wapi_transactions_in_rubles,0) as subscription_sum_in_rubles,  -- Сумма оплаты подписки в рублях
            subscription_updates.original_sum-coalesce(subscription_updates.wapi_original_sum,0) as subscription_sum_original,  -- Сумма оплаты подписки
            row_number() over (partition by  subscription_updates.guid  order by paid_date,start_date desc) as rn   -- Берутся только самые поздние изменения по subscriptionUpdates.guid
    from `dwh-wazzup`.`dbt_nbespalov`.`int_payments_card` payments
    inner join `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_with_sum_and_converted_currency_billing_date` subscription_updates
    on payments.guid=subscription_updates.activation_reason_id
    left join `dwh-wazzup`.`dbt_nbespalov`.`stg_billingPackages` stg_billingPackages 
    on stg_billingPackages.guid=subscription_updates.subscription_id
     inner join `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_type_and_partner_change_with_partner_type_deduplicated` account_type_data 
    on account_type_data.account_id=payments.account_id and  payments.paid_date>=account_type_data.start_date and payments.paid_date<=account_type_data.end_date
    where payments.subscription_id is not null and payments.sum_in_rubles!=0),

card_pay_deduplicated as (
    select * from card_pay
    where rn=1
)
    -- Продвинутая таблица оплат без обещанных платежей
select * from card_pay_deduplicated