WITH transaction_currency AS (
                             SELECT *
                             FROM `dwh-wazzup`.`dbt_nbespalov`.`int_channels_waba_currency_by_each_month`
                             ),

     balance_data AS (
                             SELECT date_sub(date_trunc(billingpackages_waba._ibk, month),
                                             INTERVAL 1 MONTH) AS balance_month
                                  , balance
                                  , balance * coalesce(rur, 1) AS balance_sum_in_rubles
                             FROM `dwh-wazzup`.`wazzup`.`billingPackages_waba` billingPackages_waba
                                 LEFT JOIN `dwh-wazzup`.`dbt_nbespalov`.`int_payments_exchange_rates_unpivoted` exchange_rates
                             ON exchange_rates.currency=billingPackages_waba.account_currency AND exchange_rates.data=last_day(date_sub(date_trunc(billingPackages_waba._ibk, MONTH), INTERVAL 1 MONTH))
                                 INNER JOIN `dwh-wazzup`.`dbt_nbespalov`.`stg_billingPackages` billing_packages
                                 ON billing_packages.guid=billingPackages_waba.guid
                             WHERE billing_packages.guid IS DISTINCT
                             FROM '57bf9315-afcb-4421-a18f-b053097dec27'
                                 AND billingPackages_waba.state!='deleted'
                                 AND balance>0
                                 AND NOT EXISTS
                                 (SELECT account_id
                                 FROM `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_type_and_partner_change_with_partner_type_deduplicated` partner_type_and_account_type
                                 WHERE billing_packages.account_id=partner_type_and_account_type.account_id AND account_type='employee')
                                 AND NOT EXISTS
                                 (SELECT account_id
                                 FROM `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info` profile_info
                                 WHERE is_employee IS TRUE
                                 AND profile_info.account_id = billing_packages.account_id
                                 )
                             )

-- Таблица c общим балансом WABA на конец месяца
SELECT balance_month                                       -- Месяц
     , sum(balance)               AS balance               -- Общий баланс WABA на конец месяца
     , sum(balance_sum_in_rubles) AS balance_sum_in_rubles -- Общий баланс WABA на конец месяца в рублях
FROM balance_data
GROUP BY 1