with payments as (
    select * from  `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_with_sum_and_converted_currency` 
),

subscription_sum as  (
    select currency, 
    start_date, 
    sum_in_rubles-coalesce(wapi_transactions_in_rubles,0) as sum_in_rubles, 
    original_sum-coalesce(wapi_original_sum,0) as original_sum,
    sum_in_usd-coalesce(wapi_sum_in_usd,0) as sum_in_usd, 
    guid, 
    partner_account_id,
    subscription_type,
    account_id  from payments 
    where sum_in_rubles>=wapi_transactions_in_rubles 
),

waba_sessions_sum as 
    (select currency,                               -- валюта
    start_date,                                     -- дата оплаты
    wapi_transactions_in_rubles as sum_in_rubles,   -- сумма в рублях
    wapi_original_sum as original_sum,              -- сумма в валюте
    wapi_sum_in_usd as sum_in_usd,                  -- сумма в долларах
    guid,                                           -- guid изменения подписки. Соотвествует guid из subscriptionUpdates
    partner_account_id,                             -- partner_account_id - если подписку оплачивал партнер
    'wapi_sessions' as subscription_type,           -- тип подписки
    account_id                                      -- аккаунт, которому принадлежит подписка
    from payments where wapi_transactions_in_rubles>0
        ),

subscriptions_and_waba_union as (
    select * from subscription_sum
    where sum_in_rubles!=0
    union all 
    select * from waba_sessions_sum
),
profile_info as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info`
)
    -- Траты на подписку в зависимости от типа подписки
select subscriptions_and_waba_union.*
from subscriptions_and_waba_union
where not exists 
        (
            select account_Id
            from profile_info 
            where is_employee
            and subscriptions_and_waba_union.account_id = profile_info.account_id
        )