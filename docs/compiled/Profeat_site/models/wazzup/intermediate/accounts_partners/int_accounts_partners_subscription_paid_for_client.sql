SELECT partner_id,  -- аккаунт партнера
client_id,          -- аккаунт дочки, которой принадлежит подписка
sum(balance_spent_by_partner) as balance_spent_by_partner , -- баланс, потраченный партнером
sum(good_balance_spent_by_partner_on_subscription+good_balance_spent_by_partner_on_waba_balance) as good_balance_spent_by_partner,   -- 'хороший' баланс, потраченный партнером
sum(good_balance_spent_by_partner_on_subscription) as good_balance_spent_by_partner_on_subscription,
sum(good_balance_spent_by_partner_on_waba_balance) as good_balance_spent_by_partner_on_waba_balance
FROM  `dwh-wazzup`.`dbt_nbespalov`.`int_payments_billing_affiliate_subscription_payments_real_money_partner_with_client_bonus`
group by 1,2
    -- Сколько было списано с баланса партнера на оплаты подписок и пополнение баланса вабы дочек. Подробнее о хорошем балансе можно почитать в notion в глоссарии