SELECT
  sui.account_id,
  ptd.partner_id,
  SUM(sum_in_rubles_spent_on_subscription)                             AS subscription_sum,
  SUM(wapi_transactions_in_rubles - bad_balance_spent_on_waba_balance) AS waba_sum_without_bonus,
  SUM(bad_balance_spent_on_subscription)                               AS bad_balance_spent_on_subscription,
  SUM(bad_balance_spent_on_waba_balance)                               AS bad_balance_spent_on_waba_balance
FROM `dwh-wazzup`.`dbt_nbespalov`.`int_payments_only_subscription_payments_and_wapi_sessions_real_money_with_data_source_and_subscription_update_id` sui
JOIN `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_type_and_partner_change_with_partner_type_deduplicated` ptd ON sui.account_id = ptd.account_id AND sui.paid_date BETWEEN ptd.start_date AND ptd.end_date
WHERE data_source IS DISTINCT FROM 'partner_payment'
GROUP BY 1, 2