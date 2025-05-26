WITH discount AS (
    SELECT 
        partner_id, 
        paid_date,
        subscription_updates_currency_client,
        coalesce(rur,1)*subscription_sum_without_balance_spent_by_client_original-coalesce(rur,1)*subscripion_sum_with_discount_original AS discount_sum_in_rubles, 
        subscription_sum_without_balance_spent_by_client_original-subscripion_sum_with_discount_original AS discount_sum_original   
    FROM `dwh-wazzup`.`dbt_nbespalov`.`int_payments_billing_affiliate_subscription_payments_real_money_partner_with_client_bonus` partner_balance
    LEFT JOIN `dwh-wazzup`.`dbt_nbespalov`.`int_payments_exchange_rates_unpivoted` exchange_rates ON exchange_rates.data=partner_balance.paid_date AND exchange_rates.currency=partner_balance.subscription_updates_currency_client
    WHERE action NOT IN ('balanceTopup','templateMessages')
)

SELECT 
    partner_id                           AS partner_id,             -- аккаунт партнера
    subscription_updates_currency_client AS currency,               -- валюта
    paid_date,                                                      -- день оплаты
    sum(discount_sum_in_rubles)          AS discount_sum_in_rubles, -- сумма, конвертированная в рубли
    sum(discount_sum_original)           AS discount_sum_original   -- сумма в валюте ЛК дочки, за которую была оплата
FROM discount
GROUP BY 1,2,3