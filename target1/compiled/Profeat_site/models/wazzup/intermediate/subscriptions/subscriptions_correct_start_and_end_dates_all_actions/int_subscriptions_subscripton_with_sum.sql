with billing_packages as (
    select 
    account_id, 
    guid,
    type as subscription_type,
    is_free
     from `dwh-wazzup`.`dbt_nbespalov`.`stg_billingPackages`
),

subscription_updates as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_subscription_updates__corect_date_and_filledna_tarif_period_quantity`
),

subscription_updated_info as (
    select subscription_updates.subscription_id,    -- ID подписки
    period_new,                                     -- Новый период подписки
    subscription_updates.paid_at as start_at,       -- Дата и время начала подписки
    subscription_updates.action,                    -- Действие с подпиской
    subscription_updates.new_until_expired_days,    -- Количество дней до окончания подписки при продлении или уменьшении тарифа
    subscription_updates.partner_account_id,        -- ID аккаунта партнера
    subscription_updates.until_expired_days,        -- Количество дней до окончания подписки в случаях отличных от new_until_expired_days
    subscription_updates.active_until,              -- Дата и время окончания обещанного платежа
    subscription_updates.sum,                       -- Сумма оплаты подписки
    subscription_updates.wapi_transactions,         -- Сумма пополнения WABA
    subscription_updates.balance_to_withdraw,       -- Сумма используемых бонусов при оплате
    subscription_updates.guid,                      -- ID изменения подписки
    subscription_updates.currency,                  -- Валюта
    subscription_updates.activation_reason_id,      -- ID причины активации изменения
    subscription_updates.paid_at_billing as paid_at_billing,            -- Дата и время оплаты по биллингу (04.03.2025)
    subscription_updates.paid_at_billing_date as paid_at_billing_date,  -- Дата оплаты по биллингу
    subscription_updates.paid_at_billing_completed_at,                -- Дата и время завершения оплаты по биллингу (04.03.2025)
    subscription_updates.paid_at_billing_completed_date,                -- Дата завершения оплаты по биллингу
    partner_discount,                               -- Скидка партнера
    billing_packages.is_free,                       -- Подписки бесплатная?
    billing_packages.subscription_type,             -- Тип (транспорт) подписки
    billing_packages.account_id,                    -- ID аккаунта
    from subscription_updates
    inner join billing_packages on billing_packages.guid=subscription_updates.subscription_id
),

subscription_with_end_dates as (
    select *, 
    (case when action='setPromisedPayment' then active_until
    when activation_reason_id='7e65a671-4665-4db4-bb59-a73c704f0657' then cast('2022-02-13' as datetime)
    else
    null
    end)
    as end_at
    from subscription_updated_info
)
    -- Таблица с детальной информацией о подписке с суммой используемых бонусов и суммой оплаты
select * from subscription_with_end_dates