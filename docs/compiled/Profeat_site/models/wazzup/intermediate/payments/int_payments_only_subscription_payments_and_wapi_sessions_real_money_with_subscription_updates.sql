



with good_and_bad_balance_bills_cards_wapi_discount as (
    select client_id as account_id, 
    paid_date, 
    action, 
    subscription_id, 
    subscription_update_id, 
    sum_in_rubles_spent_on_waba_balance,
    sum_in_rubles_spent_on_subscription,
    bad_balance_spent_on_waba_balance,
    coalesce(wapi_discount_for_partners_sum_in_rubles,0) as wapi_discount_for_partners_sum_in_rubles,
    coalesce(wapi_transactions_in_rubles,0) as wapi_transactions_in_rubles
    from `dwh-wazzup`.`dbt_nbespalov`.`int_payments_billing_affiliate_subscription_payments_real_money_partner_with_client_bonus`
    
    union all 

    select account_id, 
    paid_date, 
    action, 
    subscription_id, 
    subscription_update_id, 
    sum_in_rubles_spent_on_waba_balance,
    sum_in_rubles_spent_on_subscription,
    bad_balance_spent_on_waba_balance_sum_in_rubles as bad_balance_spent_on_waba_balance,
    coalesce(wapi_discount_for_partners_sum_in_rubles,0) as wapi_discount_for_partners_sum_in_rubles,
    coalesce(wapi_transactions_in_rubles,0) as wapi_transactions_in_rubles    
    from `dwh-wazzup`.`dbt_nbespalov`.`int_payments_billing_affiliate_subscription_payments_real_money_bills_and_payments_with_balance`),

good_and_bad_balance_bills_cards_wapi_discount_aggregated as (
 select account_id,     -- ID аккаунта
paid_date,              -- Дата оплаты
action,                 -- Действие с подпиской
subscription_id,        -- ID подписки
subscription_update_id, -- ID изменения. Соответствует guid из subscriptionUpdates
round(sum(sum_in_rubles_spent_on_subscription),2) as sum_in_rubles_spent_on_subscription    -- Сумма оплаты подписки реальными деньгами в рублях
 from good_and_bad_balance_bills_cards_wapi_discount
group by 1,2,3,4,5)
    -- Таблица платежей реальными деньгами за подписки и баланс WABA с subscription_update_id
select * 
from good_and_bad_balance_bills_cards_wapi_discount_aggregated
where action not in ('balanceTopup','templateMessages')