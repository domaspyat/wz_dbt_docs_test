with good_and_bad_balance_bills_cards_wapi_discount AS (
   select * 
   from `dwh-wazzup`.`dbt_nbespalov`.`int_payments_only_subscription_payments_and_wapi_sessions_real_money_with_data_source_and_subscription_update_id`
   ),

good_and_bad_balance_bills_cards_wapi_discount_aggregated AS (
 select account_id, -- ID аккаунта
        paid_date,          -- Дата оплаты
        action,             -- Действие с подпиской
        subscription_id,    -- ID подписки
        currency,           -- Валюта
        round(sum(sum_in_rubles_spent_on_waba_balance),2)                                                               AS sum_in_rubles_spent_on_waba_balance,      -- Сумма оплаты баланса WABA в рублях реальными деньгами
        round(sum(sum_in_rubles_spent_on_subscription),2)                                                               AS sum_in_rubles_spent_on_subscription,      -- Сумма оплаты подписки в рублях
        round(sum((case when bad_balance_spent_on_waba_balance<0 then 0 else bad_balance_spent_on_waba_balance end)),2) AS bad_balance_spent_on_waba_balance,        -- Сумма плохого баланса, потраченного на баланс WABA
        round(sum(wapi_discount_for_partners_sum_in_rubles),2)                                                          AS wapi_discount_for_partners_sum_in_rubles, -- Комиссия партнера за пополнение баланса WABA в рублях
        round(sum(wapi_transactions_in_rubles),2)                                                                       AS wapi_transactions_in_rubles               -- Сумма оплаты баланса WABA в рублях
 from good_and_bad_balance_bills_cards_wapi_discount
 group by 1, 2, 3, 4, 5)
    -- Таблица платежей реальными деньгами за подписки и баланс WABA
select * 
from good_and_bad_balance_bills_cards_wapi_discount_aggregated