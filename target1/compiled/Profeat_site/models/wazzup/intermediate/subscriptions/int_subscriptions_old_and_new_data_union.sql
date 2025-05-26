

with old_data as (
    select 
    account_id,                                 -- ID аккаунта
    cast(start_at as datetime) as start_at,     -- время начала подписки
    cast(end_date as datetime) as end_at,       -- время завершения подписи
    CAST(start_at AS DATE) AS start_date,       -- дата начала подписки
    end_date,                                   -- дата завершения подписки
    cast(NULL as STRING) AS action,             -- Действие с подпиской: addQuantity, setPromisedPayment, pay, subtractQuantity, templateMessages, loweringTariff, raiseTariff, renewal, balanceTopup или null
    guid AS subscription_id,                    -- ID подписки
    partner_account_id,                         -- ID аккаунта партнера. Указывается, если за подписку платил партнер
    cast(null as string) as guid,               -- guid изменения - соответствует guid из subscriptionUpdates
    null as sum,                                -- сумма в валюте платежа
    cast(null as string) as currency,           -- валюта платежа
    cast(null as string) as subscription_type,  -- тип (транспорт) подписки
    null as wapi_transactions                   -- сумма пополнения баланса вабы
    from `dwh-wazzup`.`dbt_nbespalov`.`int_payments_old_billing_fixed_expiration_date`
),


missing_subscription_data_paidat as (
    select 
    account_id,                                                         -- ID аккаунта
    start_at,                                                           -- время начала подписки
    CAST(NULL AS DATETIME) AS end_at,                                   -- время завершения подписи
    start_date,                                                         -- дата начала подписки
    end_date,                                                           -- дата завершения подписки
    coalesce(subscriptionUpdates.action, paid_at.action)  as action,    -- Действие с подпиской: addQuantity, setPromisedPayment, pay, subtractQuantity, templateMessages, loweringTariff, raiseTariff, renewal, balanceTopup или null
    paid_at.subscription_id,                                            -- ID подписки
    partner_account_id,                                                 -- ID аккаунта партнера. Указывается, если за подписку платил партнер
    paid_at.guid,                                                       -- guid изменения - соответствует guid из subscriptionUpdates
    paid_at.sum,                                                        -- сумма в валюте платежа
    paid_at.currency,                                                   -- валюта платежа
    subscription_type,                                                  -- тип (транспорт) подписки
    paid_at.wapi_transactions                                           -- сумма пополнения баланса вабы
    from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptons_with_sum_and_correct_dates_joined_paidat` paid_at
    left join  `dwh-wazzup`.`dbt_nbespalov`.`stg_subscriptionUpdates`  subscriptionUpdates 
    on subscriptionUpdates.guid=paid_at.guid
    WHERE 
    (end_date IS NOT NULL)  and (paid_at.action is distinct from 'addQuantity')
    and (paid_at.action is distinct from 'raiseTariff')
    
),

missing_subscription_data_datetime as (
    select  account_id,                 -- ID аккаунта
    start_at,                           -- время начала подписки
    CAST(NULL AS DATETIME) AS end_at,   -- время завершения подписи
    start_date,                         -- дата начала подписки
    end_date,                           -- дата завершения подписки
    action,                             -- Действие с подпиской: addQuantity, setPromisedPayment, pay, subtractQuantity, templateMessages, loweringTariff, raiseTariff, renewal, balanceTopup или null
    subscription_id,                    -- ID подписки
    partner_account_id,                 -- ID аккаунта партнера. Указывается, если за подписку платил партнер
    guid ,                              -- guid изменения - соответствует guid из subscriptionUpdates
    sum,                                -- сумма в валюте платежа
    currency,                           -- валюта платежа
    subscription_type,                  -- тип (транспорт) подписки
    wapi_transactions                   -- сумма пополнения баланса вабы
    from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_correct_start_and_end_date_joined_datetime_deduplicated`
    where action is distinct from 'addQuantity' and guid is distinct from '5fd23f66-b12a-42a5-9061-b078fbffe8c5' 
    and action is distinct from 'raiseTariff'
),

promised_payments as (
    select 
    account_id,                             -- ID аккаунта
    start_at,                               -- время начала подписки
    end_at,                                 -- время завершения подписи
    CAST(start_at AS DATE) AS start_date,   -- дата начала подписки
    CAST(end_at AS DATE) AS end_date,       -- дата завершения подписки
    action,                                 -- Действие с подпиской: addQuantity, setPromisedPayment, pay, subtractQuantity, templateMessages, loweringTariff, raiseTariff, renewal, balanceTopup или null
    subscription_id,                        -- ID подписки
    partner_account_id,                     -- ID аккаунта партнера. Указывается, если за подписку платил партнер
    guid,                                   -- guid изменения - соответствует guid из subscriptionUpdates
    sum,                                    -- сумма в валюте платежа
    currency,                               -- валюта платежа
    subscription_type,                      -- тип (транспорт) подписки
    wapi_transactions                       -- сумма пополнения баланса вабы
    from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_subscripton_with_sum`
    where action='setPromisedPayment'
    
),


subscription_updates_with_non_pay_actions as (
    select 
    account_id,                             -- ID аккаунта
    start_at,                               -- время начала подписки
    CAST(NULL AS DATETIME) AS end_at,       -- время завершения подписи
    cast(start_at as date) as start_date,   -- дата начала подписки
    CAST(NULL AS date) AS end_at,           -- дата завершения подписки
    action,                                 -- Действие с подпиской: addQuantity, setPromisedPayment, pay, subtractQuantity, templateMessages, loweringTariff, raiseTariff, renewal, balanceTopup или null
    subscription_id,                        -- ID подписки
    partner_account_id,                     -- ID аккаунта партнера. Указывается, если за подписку платил партнер
    guid,                                   -- guid изменения - соответствует guid из subscriptionUpdates
    sum,                                    -- сумма в валюте платежа
    currency,                               -- валюта платежа
    subscription_type,                      -- тип (транспорт) подписки
    wapi_transactions                       -- сумма пополнения баланса вабы
    from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_subscripton_with_sum`
    where action not in ('pay','renewal','setPromisedPayment','subtractQuantity')
),


billing_data_union_all as (
 -- источник - табличка составлена как union из нескольких источников
    select *, False as is_promised_payment, 'old_data' as source from old_data
    UNION ALL
    select *, False as is_promised_payment,  'missing_subscription_data_paidat' as source from missing_subscription_data_paidat
    UNION ALL 
    select *, False as is_promised_payment, 'missing_subscription_data_datetime' as source from missing_subscription_data_datetime
    UNION ALL
    select *, False as is_promised_payment, 'subscription_updates_with_non_pay_actions' as source from subscription_updates_with_non_pay_actions
    UNION ALL
    select *, True as is_promised_payment,'promised_payments' as source from promised_payments
),remove_duplicates as (
select *,                   -- табличка, которая для каждого изменения подписки указывается корректное время изменения и дата завершения подписки
row_number() over (partition by guid) rn
from billing_data_union_all)
select *
from remove_duplicates
where rn = 1