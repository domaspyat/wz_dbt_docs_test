WITH profile_info AS (
                     SELECT account_id
                          , is_employee
                     FROM `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info`
                     ),
     withdrawals_amount_defining AS (
                                    SELECT date_trunc(occured_date, month) AS withdrawal_month
                                         , currency
                                         , sum(sum_in_rubles)              AS withdrawals_amount
                                    FROM `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_waba_withdrawals` withdrawals
                                    WHERE NOT EXISTS (
                                        SELECT account_Id
                                        FROM profile_info
                                        WHERE is_employee
                                      AND withdrawals.account_id = profile_info.account_id
                                        )
                                    GROUP BY 1, 2
                                    ),
     waba_reward_by_month AS (  -- Вознаграждение за WABA (10% при object = 'rewardWaba')
                     SELECT reward.paid_month
                          , currency
                          , sum(reward.sum_in_rubles) AS waba_reward_sum_in_rubles
                     FROM `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_waba_reward_by_account` reward
                     LEFT JOIN profile_info ON reward.account_id = profile_info.account_id
                     WHERE profile_info.is_employee IS FALSE
                     GROUP BY 1, 2
                     ),

     waba_sessions_and_postpay_data AS (
                     SELECT date_trunc(spendings.spend_date, month)          AS paid_month
                          , spendings.sum_in_rubles
                          , original_sum
                          , currency
                          , state
                     FROM `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_waba_balance_spending_by_currency_and_account` spendings
                     WHERE NOT EXISTS (
                         SELECT account_Id
                         FROM profile_info
                         WHERE is_employee
                       AND spendings.account_id = profile_info.account_id
                         )
                     UNION ALL
                     SELECT CAST(date_trunc(revenue.paid_date, MONTH) AS DATE) AS paid_month
                          , revenue.amount AS sum_in_rubles
                          , revenue.amount AS original_sum
                          , 'RUR'                                              AS currency
                          , 'paid'
                     FROM `dwh-wazzup`.`dbt_nbespalov`.`int_payments_postpay_waba_revenue` revenue

                     WHERE NOT EXISTS (
                         SELECT account_Id
                         FROM profile_info
                         WHERE is_employee
                       AND revenue.account_id = CAST (profile_info.account_id AS string)
                         )
                     ),

     waba_sessions_payments AS (
                     SELECT paid_month
                          , currency
                          , sum(sum_in_rubles)   AS waba_balance_spending_sum_in_rubles
                          , sum(original_sum)    AS waba_balance_spending_original_sum
                          , sum(case when state = 'paid' then sum_in_rubles else 0 end)   AS waba_balance_spending_sum_in_rubles_paid
                          , sum(case when state = 'paid' then original_sum  else 0 end)   AS waba_balance_spending_original_sum_paid
                          , sum(case when state = 'holded' then sum_in_rubles else 0 end) AS waba_balance_spending_sum_in_rubles_holded
                          , sum(case when state = 'holded' then original_sum else 0 end)  AS waba_balance_spending_original_sum_holded
                     FROM waba_sessions_and_postpay_data
                     GROUP BY 1, 2

                     )
    , waba_sessions_revenue_pre AS (
                     SELECT date_trunc(real_money.paid_date, month)       AS paid_month
                          , real_money.currency
                          , sum(bad_balance_spent_on_waba_balance)        AS bad_balance_spent_on_waba_balance
                          , sum(sum_in_rubles_spent_on_subscription)      AS sum_in_rubles_spent_on_subscription_without_bad_balance
                          , sum(sum_in_rubles_spent_on_waba_balance)      AS sum_in_rubles_spent_on_waba_balance_without_bad_balance
                          , sum(wapi_discount_for_partners_sum_in_rubles) AS wapi_discount_for_partners_sum_in_rubles
                          , sum(wapi_transactions_in_rubles)              AS wapi_transactions_in_rubles_with_bad_balance

                          , sum(bad_balance_spent_on_waba_balance/coalesce(rur,1))        AS bad_balance_spent_on_waba_balance_original_sum
                          , sum(sum_in_rubles_spent_on_subscription/coalesce(rur,1))      AS original_sum_spent_on_subscription_without_bad_balance
                          , sum(sum_in_rubles_spent_on_waba_balance/coalesce(rur,1))      AS original_sum_spent_on_waba_balance_without_bad_balance
                          , sum(wapi_discount_for_partners_sum_in_rubles/coalesce(rur,1)) AS wapi_discount_for_partners_original_sum
                          , sum(wapi_transactions_in_rubles/coalesce(rur,1))              AS wapi_transactions_with_bad_balance_original_sum

                     FROM `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_waba_sessions_real_money` real_money
                     LEFT JOIN profile_info ON real_money.account_id = profile_info.account_id

                     LEFT JOIN `dwh-wazzup`.`dbt_nbespalov`.`int_payments_postpay_waba_revenue` revenue ON CAST (real_money.account_id AS INT) = CAST (revenue.account_id AS INT)
                         AND wapi_transactions_in_rubles > 0
                         AND date_trunc(real_money.paid_date, MONTH) = CAST (date_trunc(revenue.paid_date, MONTH) AS DATE)

                     LEFT JOIN `dwh-wazzup`.`dbt_nbespalov`.`int_payments_exchange_rates_unpivoted` exchange_rates_unpivoted
                              ON exchange_rates_unpivoted._ibk = real_money.paid_date AND exchange_rates_unpivoted.currency = real_money.currency
                     WHERE profile_info.is_employee IS FALSE
                       AND revenue.account_id IS NULL
                     GROUP BY 1, 2

                     UNION ALL
                     SELECT CAST (date_trunc(revenue.paid_date, MONTH) AS DATE) AS paid_month
                          , 'RUR'                                               AS currency
                          , NULL                                                AS bad_balance_spent_on_waba_balance
                          , NULL                                                AS sum_in_rubles_spent_on_subscription_without_bad_balance
                          , NULL                                                AS sum_in_rubles_spent_on_waba_balance_without_bad_balance
                          , NULL                                                AS wapi_discount_for_partners_sum_in_rubles
                          , sum(revenue.amount)                                 AS wapi_transactions_in_rubles_with_bad_balance

                          , NULL                                                AS bad_balance_spent_on_waba_balance_original_sum
                          , NULL                                                AS original_sum_spent_on_subscription_without_bad_balance
                          , NULL                                                AS original_sum_spent_on_waba_balance_without_bad_balance
                          , NULL                                                AS wapi_discount_for_partners_original_sum
                          , sum(revenue.amount)                                 AS wapi_transactions_with_bad_balance_original_sum

                     FROM `dwh-wazzup`.`dbt_nbespalov`.`int_payments_postpay_waba_revenue` revenue
                     WHERE NOT EXISTS (
                         SELECT account_Id
                         FROM profile_info
                         WHERE is_employee
                            AND revenue.account_id = CAST (profile_info.account_id AS string)
                         )
                     GROUP BY paid_month
                     ),

     waba_sessions_revenue AS (
                     SELECT paid_month
                          , currency
                          , sum(bad_balance_spent_on_waba_balance)                        AS bad_balance_spent_on_waba_balance
                          , sum(sum_in_rubles_spent_on_subscription_without_bad_balance)  AS sum_in_rubles_spent_on_subscription_without_bad_balance
                          , sum(sum_in_rubles_spent_on_waba_balance_without_bad_balance)  AS sum_in_rubles_spent_on_waba_balance_without_bad_balance
                          , sum(wapi_discount_for_partners_sum_in_rubles)                 AS wapi_discount_for_partners_sum_in_rubles
                          , sum(wapi_transactions_in_rubles_with_bad_balance)             AS wapi_transactions_in_rubles_with_bad_balance
                          
                          , sum(bad_balance_spent_on_waba_balance_original_sum)           AS bad_balance_spent_on_waba_balance_original_sum
                          , sum(original_sum_spent_on_subscription_without_bad_balance)   AS original_sum_spent_on_subscription_without_bad_balance
                          , sum(original_sum_spent_on_waba_balance_without_bad_balance)   AS original_sum_spent_on_waba_balance_without_bad_balance
                          , sum(wapi_discount_for_partners_original_sum)                  AS wapi_discount_for_partners_original_sum
                          , sum(wapi_transactions_with_bad_balance_original_sum)          AS wapi_transactions_with_bad_balance_original_sum
                     FROM waba_sessions_revenue_pre
                     GROUP BY paid_month, 2
                     ),

     key_reply_invoice AS (
                     SELECT paid_month
                          , provider
                          , invoice_subscriptions_original
                          , invoice_subscriptions_in_rubles
                          , invoice_sessions_original
                          , invoice_sessions_in_rubles
                     FROM `dwh-wazzup`.`dbt_nbespalov`.`int_payments_waba_key_reply_invoice`
                     WHERE provider = 'KeyReply'
                     ),

     gupshup_invoice AS (
                     SELECT paid_month
                          , provider
                          , invoice_subscriptions_original
                          , invoice_subscriptions_in_rubles
                          , invoice_sessions_original
                          , invoice_sessions_in_rubles
                     FROM `dwh-wazzup`.`dbt_nbespalov`.`int_payments_waba_key_reply_invoice`
                     WHERE provider = 'Gupshup'
                     ),
     pnl AS (
        SELECT  
                month  
                , uslovno_peremennye_raskhody_sessii                                  
                , uslovno_peremennye_raskhody_sessii_vyplaty_partneram                
                , uslovno_peremennye_raskhody_sessii_komissiya_na_vyplaty_partneram   
                , uslovno_peremennye_raskhody_sessii_ekvairing                        
                , uslovno_peremennye_raskhody_sessii_komissiya_stripe_za_vyvod_usd    
                , uslovno_peremennye_raskhody_sessii_komissiya_za_oplatu_waba         
                , uslovno_peremennye_raskhody_sessii_raskhody_na_konversatsiyu_valyuty
        FROM `dwh-wazzup`.`dbt_nbespalov`.`stg_pnl`
     ),

/*    waba_balance_by_month AS (
                     SELECT *
                     FROM `dwh-wazzup`.`dbt_nbespalov`.`int_channels_waba_balance_end_of_month`
                     ),
*/
     waba_balance_by_month_by_payments_and_spendings_keyreply AS (
                     SELECT month                             AS balance_month,
                            currency,
                            sum(balance)                      AS balance_in_rubles
                     FROM `dwh-wazzup`.`dbt_nbespalov`.`int_channels_waba_balance_by_spending_by_month`
                     WHERE month <= '2024-02-01'
                     GROUP BY 1, 2
                     ),

     waba_balance_by_month_by_payments_and_spendings_gupshup AS (
                     SELECT balance_month,
                            currency,
                            sum(balance_in_rubles)            AS balance_in_rubles
                     FROM `dwh-wazzup`.`dbt_nbespalov`.`int_channels_waba_balance_end_of_the_month_gupshup`
                     GROUP BY 1, 2
                     ),
     joining_tables AS (
SELECT coalesce(waba_sessions_revenue.paid_month, waba_sessions_payments.paid_month, waba_reward_by_month.paid_month,
                key_reply_invoice.paid_month, 
                gupshup_invoice.paid_month,
                waba_balance_by_month_by_payments_and_spendings_keyreply.balance_month,
                waba_balance_by_month_by_payments_and_spendings_gupshup.balance_month)      AS paid_month

     , coalesce(waba_sessions_revenue.currency, waba_sessions_payments.currency, waba_reward_by_month.currency,
                waba_balance_by_month_by_payments_and_spendings_keyreply.currency,
                waba_balance_by_month_by_payments_and_spendings_gupshup.currency)            AS currency         
     , bad_balance_spent_on_waba_balance
     , sum_in_rubles_spent_on_subscription_without_bad_balance
     , sum_in_rubles_spent_on_waba_balance_without_bad_balance
     , wapi_discount_for_partners_sum_in_rubles
     , wapi_transactions_in_rubles_with_bad_balance

     , bad_balance_spent_on_waba_balance_original_sum
     , original_sum_spent_on_subscription_without_bad_balance
     , original_sum_spent_on_waba_balance_without_bad_balance
     , wapi_discount_for_partners_original_sum
     , wapi_transactions_with_bad_balance_original_sum
     , waba_balance_spending_sum_in_rubles
     , waba_balance_spending_original_sum
     , waba_balance_spending_sum_in_rubles_paid
     , waba_balance_spending_original_sum_paid
     , waba_balance_spending_sum_in_rubles_holded
     , waba_balance_spending_original_sum_holded
     , waba_reward_sum_in_rubles
     , key_reply_invoice.invoice_sessions_in_rubles                                 AS key_invoice_sessions_in_rubles
     , gupshup_invoice.invoice_sessions_in_rubles                                   AS gupshup_invoice_sessions_in_rubles
     , waba_balance_by_month_by_payments_and_spendings_gupshup.balance_in_rubles    AS waba_balance_by_month_sum_in_rubles --keyreply balance
     , waba_balance_by_month_by_payments_and_spendings_keyreply.balance_in_rubles   AS balance_by_spending       -- gupshup balance
     , withdrawals_amount_defining.withdrawals_amount                               AS withdrawals_amount
     , uslovno_peremennye_raskhody_sessii_vyplaty_partneram
     , uslovno_peremennye_raskhody_sessii_komissiya_na_vyplaty_partneram
     , uslovno_peremennye_raskhody_sessii_ekvairing
     , uslovno_peremennye_raskhody_sessii_komissiya_stripe_za_vyvod_usd
     , uslovno_peremennye_raskhody_sessii_komissiya_za_oplatu_waba
     , uslovno_peremennye_raskhody_sessii_raskhody_na_konversatsiyu_valyuty
     , uslovno_peremennye_raskhody_sessii
FROM waba_sessions_revenue
    FULL OUTER JOIN waba_sessions_payments
            ON waba_sessions_revenue.paid_month = waba_sessions_payments.paid_month
                AND waba_sessions_payments.currency = waba_sessions_revenue.currency
    FULL OUTER JOIN waba_reward_by_month
            ON waba_reward_by_month.paid_month = waba_sessions_revenue.paid_month
            AND waba_reward_by_month.currency = waba_sessions_revenue.currency
    LEFT JOIN key_reply_invoice
            ON key_reply_invoice.paid_month = waba_sessions_revenue.paid_month
    LEFT JOIN gupshup_invoice
            ON gupshup_invoice.paid_month = waba_sessions_revenue.paid_month

    LEFT JOIN waba_balance_by_month_by_payments_and_spendings_keyreply
            ON waba_balance_by_month_by_payments_and_spendings_keyreply.balance_month = waba_sessions_revenue.paid_month
            AND waba_balance_by_month_by_payments_and_spendings_keyreply.currency = waba_sessions_revenue.currency

    LEFT JOIN waba_balance_by_month_by_payments_and_spendings_gupshup
            ON waba_balance_by_month_by_payments_and_spendings_gupshup.balance_month = waba_sessions_revenue.paid_month
            AND waba_balance_by_month_by_payments_and_spendings_gupshup.currency = waba_sessions_revenue.currency


    LEFT JOIN withdrawals_amount_defining 
            ON waba_sessions_revenue.paid_month = withdrawals_amount_defining.withdrawal_month
            AND waba_sessions_revenue.currency = withdrawals_amount_defining.currency
    LEFT JOIN pnl 
            ON waba_sessions_revenue.paid_month = pnl.month
     ),final AS (
            select paid_month,
            
                   sum(case when currency = 'RUR' then sum_in_rubles_spent_on_subscription_without_bad_balance else 0 end)  AS  sum_in_rubles_spent_on_subscription_without_bad_balance_RUR,
                    sum(case when currency = 'RUR' then sum_in_rubles_spent_on_waba_balance_without_bad_balance else 0 end) AS  sum_in_rubles_spent_on_waba_balance_without_bad_balance_RUR,
                    sum(case when currency = 'RUR' then wapi_transactions_in_rubles_with_bad_balance else 0 end)            AS  wapi_transactions_in_rubles_with_bad_balance_RUR,
                    sum(case when currency = 'RUR'then waba_balance_spending_sum_in_rubles_paid else 0 end)                 AS  waba_balance_spending_sum_in_rubles_paid_RUR,
                    sum(case when currency = 'RUR'then waba_balance_spending_sum_in_rubles_holded else 0 end)               AS  waba_balance_spending_sum_in_rubles_holded_RUR,
                    sum(case when currency = 'RUR'then waba_balance_spending_sum_in_rubles else 0 end)                      AS  waba_balance_spending_sum_in_rubles_RUR,
            
                
                
                   sum(case when currency = 'KZT' then sum_in_rubles_spent_on_subscription_without_bad_balance else 0 end)  AS  sum_in_rubles_spent_on_subscription_without_bad_balance_KZT,
                    sum(case when currency = 'KZT' then sum_in_rubles_spent_on_waba_balance_without_bad_balance else 0 end) AS  sum_in_rubles_spent_on_waba_balance_without_bad_balance_KZT,
                    sum(case when currency = 'KZT' then wapi_transactions_in_rubles_with_bad_balance else 0 end)            AS  wapi_transactions_in_rubles_with_bad_balance_KZT,
                    sum(case when currency = 'KZT'then waba_balance_spending_sum_in_rubles_paid else 0 end)                 AS  waba_balance_spending_sum_in_rubles_paid_KZT,
                    sum(case when currency = 'KZT'then waba_balance_spending_sum_in_rubles_holded else 0 end)               AS  waba_balance_spending_sum_in_rubles_holded_KZT,
                    sum(case when currency = 'KZT'then waba_balance_spending_sum_in_rubles else 0 end)                      AS  waba_balance_spending_sum_in_rubles_KZT,
            
                
                
                   sum(case when currency = 'EUR' then sum_in_rubles_spent_on_subscription_without_bad_balance else 0 end)  AS  sum_in_rubles_spent_on_subscription_without_bad_balance_EUR,
                    sum(case when currency = 'EUR' then sum_in_rubles_spent_on_waba_balance_without_bad_balance else 0 end) AS  sum_in_rubles_spent_on_waba_balance_without_bad_balance_EUR,
                    sum(case when currency = 'EUR' then wapi_transactions_in_rubles_with_bad_balance else 0 end)            AS  wapi_transactions_in_rubles_with_bad_balance_EUR,
                    sum(case when currency = 'EUR'then waba_balance_spending_sum_in_rubles_paid else 0 end)                 AS  waba_balance_spending_sum_in_rubles_paid_EUR,
                    sum(case when currency = 'EUR'then waba_balance_spending_sum_in_rubles_holded else 0 end)               AS  waba_balance_spending_sum_in_rubles_holded_EUR,
                    sum(case when currency = 'EUR'then waba_balance_spending_sum_in_rubles else 0 end)                      AS  waba_balance_spending_sum_in_rubles_EUR,
            
                
                
                   sum(case when currency = 'USD' then sum_in_rubles_spent_on_subscription_without_bad_balance else 0 end)  AS  sum_in_rubles_spent_on_subscription_without_bad_balance_USD,
                    sum(case when currency = 'USD' then sum_in_rubles_spent_on_waba_balance_without_bad_balance else 0 end) AS  sum_in_rubles_spent_on_waba_balance_without_bad_balance_USD,
                    sum(case when currency = 'USD' then wapi_transactions_in_rubles_with_bad_balance else 0 end)            AS  wapi_transactions_in_rubles_with_bad_balance_USD,
                    sum(case when currency = 'USD'then waba_balance_spending_sum_in_rubles_paid else 0 end)                 AS  waba_balance_spending_sum_in_rubles_paid_USD,
                    sum(case when currency = 'USD'then waba_balance_spending_sum_in_rubles_holded else 0 end)               AS  waba_balance_spending_sum_in_rubles_holded_USD,
                    sum(case when currency = 'USD'then waba_balance_spending_sum_in_rubles else 0 end)                      AS  waba_balance_spending_sum_in_rubles_USD,
            
                  

            
                    sum(case when currency = 'RUR' then original_sum_spent_on_subscription_without_bad_balance else 0 end) AS  original_sum_spent_on_subscription_without_bad_balance_RUR,
                    sum(case when currency = 'RUR' then original_sum_spent_on_waba_balance_without_bad_balance else 0 end) AS  original_sum_spent_on_waba_balance_without_bad_balance_RUR,
                    sum(case when currency = 'RUR' then wapi_transactions_with_bad_balance_original_sum else 0 end)        AS  wapi_transactions_with_bad_balance_RUR_original_sum,
                    sum(case when currency = 'RUR'then waba_balance_spending_original_sum_paid else 0 end)                 AS  waba_balance_spendings_paid_RUR_original_sum,
                    sum(case when currency = 'RUR'then waba_balance_spending_original_sum_holded else 0 end)               AS  waba_balance_spendings_holded_RUR_original_sum,
                    sum(case when currency = 'RUR'then waba_balance_spending_original_sum else 0 end)                      AS  waba_balance_spendings_RUR_original_sum,
            
                
                
                    sum(case when currency = 'KZT' then original_sum_spent_on_subscription_without_bad_balance else 0 end) AS  original_sum_spent_on_subscription_without_bad_balance_KZT,
                    sum(case when currency = 'KZT' then original_sum_spent_on_waba_balance_without_bad_balance else 0 end) AS  original_sum_spent_on_waba_balance_without_bad_balance_KZT,
                    sum(case when currency = 'KZT' then wapi_transactions_with_bad_balance_original_sum else 0 end)        AS  wapi_transactions_with_bad_balance_KZT_original_sum,
                    sum(case when currency = 'KZT'then waba_balance_spending_original_sum_paid else 0 end)                 AS  waba_balance_spendings_paid_KZT_original_sum,
                    sum(case when currency = 'KZT'then waba_balance_spending_original_sum_holded else 0 end)               AS  waba_balance_spendings_holded_KZT_original_sum,
                    sum(case when currency = 'KZT'then waba_balance_spending_original_sum else 0 end)                      AS  waba_balance_spendings_KZT_original_sum,
            
                
                
                    sum(case when currency = 'EUR' then original_sum_spent_on_subscription_without_bad_balance else 0 end) AS  original_sum_spent_on_subscription_without_bad_balance_EUR,
                    sum(case when currency = 'EUR' then original_sum_spent_on_waba_balance_without_bad_balance else 0 end) AS  original_sum_spent_on_waba_balance_without_bad_balance_EUR,
                    sum(case when currency = 'EUR' then wapi_transactions_with_bad_balance_original_sum else 0 end)        AS  wapi_transactions_with_bad_balance_EUR_original_sum,
                    sum(case when currency = 'EUR'then waba_balance_spending_original_sum_paid else 0 end)                 AS  waba_balance_spendings_paid_EUR_original_sum,
                    sum(case when currency = 'EUR'then waba_balance_spending_original_sum_holded else 0 end)               AS  waba_balance_spendings_holded_EUR_original_sum,
                    sum(case when currency = 'EUR'then waba_balance_spending_original_sum else 0 end)                      AS  waba_balance_spendings_EUR_original_sum,
            
                
                
                    sum(case when currency = 'USD' then original_sum_spent_on_subscription_without_bad_balance else 0 end) AS  original_sum_spent_on_subscription_without_bad_balance_USD,
                    sum(case when currency = 'USD' then original_sum_spent_on_waba_balance_without_bad_balance else 0 end) AS  original_sum_spent_on_waba_balance_without_bad_balance_USD,
                    sum(case when currency = 'USD' then wapi_transactions_with_bad_balance_original_sum else 0 end)        AS  wapi_transactions_with_bad_balance_USD_original_sum,
                    sum(case when currency = 'USD'then waba_balance_spending_original_sum_paid else 0 end)                 AS  waba_balance_spendings_paid_USD_original_sum,
                    sum(case when currency = 'USD'then waba_balance_spending_original_sum_holded else 0 end)               AS  waba_balance_spendings_holded_USD_original_sum,
                    sum(case when currency = 'USD'then waba_balance_spending_original_sum else 0 end)                      AS  waba_balance_spendings_USD_original_sum,
            
                  

                    sum(sum_in_rubles_spent_on_subscription_without_bad_balance) AS  sum_in_rubles_spent_on_subscription_without_bad_balance,
                    sum(sum_in_rubles_spent_on_waba_balance_without_bad_balance) AS  sum_in_rubles_spent_on_waba_balance_without_bad_balance,
                    sum(wapi_transactions_in_rubles_with_bad_balance) AS  wapi_transactions_in_rubles_with_bad_balance,
                    sum(waba_balance_spending_sum_in_rubles) AS  waba_balance_spending_sum_in_rubles,
                    sum(waba_balance_spending_sum_in_rubles_paid) AS  waba_balance_spending_sum_in_rubles_paid,
                    sum(waba_balance_spending_sum_in_rubles_holded) AS  waba_balance_spending_sum_in_rubles_holded,
                    

                    sum(waba_reward_sum_in_rubles) AS  waba_reward_sum_in_rubles,
                    min(key_invoice_sessions_in_rubles) AS  key_invoice_sessions_in_rubles,
                    min(gupshup_invoice_sessions_in_rubles) AS  gupshup_invoice_sessions_in_rubles,
                    sum(waba_balance_by_month_sum_in_rubles) AS  waba_balance_by_month_sum_in_rubles,
                    sum(balance_by_spending) AS  balance_by_spending,
                    sum(withdrawals_amount) AS  withdrawals_amount,
                    min(cast(uslovno_peremennye_raskhody_sessii_vyplaty_partneram as float64)) AS  uslovno_peremennye_raskhody_sessii_vyplaty_partneram,
                    min(cast(uslovno_peremennye_raskhody_sessii_komissiya_na_vyplaty_partneram as float64)) AS  uslovno_peremennye_raskhody_sessii_komissiya_na_vyplaty_partneram,
                    min(cast(uslovno_peremennye_raskhody_sessii_ekvairing as float64)) AS  uslovno_peremennye_raskhody_sessii_ekvairing,
                    min(cast(uslovno_peremennye_raskhody_sessii_komissiya_stripe_za_vyvod_usd as float64)) AS  uslovno_peremennye_raskhody_sessii_komissiya_stripe_za_vyvod_usd,
                    min(cast(uslovno_peremennye_raskhody_sessii_komissiya_za_oplatu_waba as float64)) AS  uslovno_peremennye_raskhody_sessii_komissiya_za_oplatu_waba,
                    min(cast(uslovno_peremennye_raskhody_sessii_raskhody_na_konversatsiyu_valyuty as float64)) AS  uslovno_peremennye_raskhody_sessii_raskhody_na_konversatsiyu_valyuty,
                    min(cast(uslovno_peremennye_raskhody_sessii as float64)) AS  uslovno_peremennye_raskhody_sessii,

                    -- До января 2024 года не можем провалидировать данные по списаниям с WABA баланса, поэтому для подсчета прибылей используем поле "Выручка от сессий"
                    CASE WHEN paid_month < '2024-01-01' THEN sum(sum_in_rubles_spent_on_waba_balance_without_bad_balance) 
                                                        - coalesce(min(key_invoice_sessions_in_rubles),0) 
                                                        - coalesce(min(gupshup_invoice_sessions_in_rubles),0)
                                                        ELSE sum(waba_balance_spending_sum_in_rubles) 
                                                        - coalesce(min(key_invoice_sessions_in_rubles),0) 
                                                        - coalesce(min(gupshup_invoice_sessions_in_rubles),0)
                    END                                                                                                     AS gross_profit,

                    CASE WHEN paid_month < '2024-01-01' THEN (sum(sum_in_rubles_spent_on_waba_balance_without_bad_balance) 
                                                        - coalesce(min(key_invoice_sessions_in_rubles),0) 
                                                        - coalesce(min(gupshup_invoice_sessions_in_rubles),0))
                                                        / sum(sum_in_rubles_spent_on_waba_balance_without_bad_balance) * 100
                                                        ELSE (sum(waba_balance_spending_sum_in_rubles) 
                                                        - coalesce(min(key_invoice_sessions_in_rubles),0) 
                                                        - coalesce(min(gupshup_invoice_sessions_in_rubles),0))
                                                        /sum(waba_balance_spending_sum_in_rubles)*100 
                    END                                                                                                     AS gross_profit_percent,

                    CASE WHEN paid_month < '2024-01-01' THEN sum(sum_in_rubles_spent_on_waba_balance_without_bad_balance)
                                                        - coalesce(min(key_invoice_sessions_in_rubles),0) 
                                                        - coalesce(min(gupshup_invoice_sessions_in_rubles),0) 
                                                        - coalesce(min(cast(uslovno_peremennye_raskhody_sessii as float64)),0)
                                                        ELSE sum(waba_balance_spending_sum_in_rubles) 
                                                        - coalesce(min(key_invoice_sessions_in_rubles),0) 
                                                        - coalesce(min(gupshup_invoice_sessions_in_rubles),0) 
                                                        - coalesce(min(cast(uslovno_peremennye_raskhody_sessii as float64)),0) 
                    END                                                                                                     AS margin_profit,

                    CASE WHEN paid_month < '2024-01-01' THEN (sum(sum_in_rubles_spent_on_waba_balance_without_bad_balance)
                                                        - coalesce(min(key_invoice_sessions_in_rubles),0) 
                                                        - coalesce(min(gupshup_invoice_sessions_in_rubles),0) 
                                                        - coalesce(min(cast(uslovno_peremennye_raskhody_sessii as float64)),0))
                                                        /sum(sum_in_rubles_spent_on_waba_balance_without_bad_balance)*100
                                                        ELSE (sum(waba_balance_spending_sum_in_rubles) 
                                                        - coalesce(min(key_invoice_sessions_in_rubles),0) 
                                                        - coalesce(min(gupshup_invoice_sessions_in_rubles),0) 
                                                        - coalesce(min(cast(uslovno_peremennye_raskhody_sessii as float64)),0))
                                                        /sum(waba_balance_spending_sum_in_rubles)*100 
                    END                                                                                                     AS margin_percent
            from joining_tables
            GROUP BY 1 
            )
     
SELECT paid_month,
       metric_name,
       metric_value
FROM final,
     UNNEST([
        STRUCT('sum_in_rubles_spent_on_subscription_without_bad_balance_RUR' as metric_name, sum_in_rubles_spent_on_subscription_without_bad_balance_RUR as metric_value),
        STRUCT('sum_in_rubles_spent_on_waba_balance_without_bad_balance_RUR', sum_in_rubles_spent_on_waba_balance_without_bad_balance_RUR),
        STRUCT('wapi_transactions_in_rubles_with_bad_balance_RUR', wapi_transactions_in_rubles_with_bad_balance_RUR),
        STRUCT('waba_balance_spending_sum_in_rubles_RUR', waba_balance_spending_sum_in_rubles_RUR),
        STRUCT('waba_balance_spending_sum_in_rubles_paid_RUR', waba_balance_spending_sum_in_rubles_paid_RUR),
        STRUCT('waba_balance_spending_sum_in_rubles_holded_RUR', waba_balance_spending_sum_in_rubles_holded_RUR),

        STRUCT('sum_in_rubles_spent_on_subscription_without_bad_balance_KZT', sum_in_rubles_spent_on_subscription_without_bad_balance_KZT),
        STRUCT('sum_in_rubles_spent_on_waba_balance_without_bad_balance_KZT', sum_in_rubles_spent_on_waba_balance_without_bad_balance_KZT),
        STRUCT('wapi_transactions_in_rubles_with_bad_balance_KZT', wapi_transactions_in_rubles_with_bad_balance_KZT),
        STRUCT('waba_balance_spending_sum_in_rubles_KZT', waba_balance_spending_sum_in_rubles_KZT),
        STRUCT('waba_balance_spending_sum_in_rubles_paid_KZT', waba_balance_spending_sum_in_rubles_paid_KZT),
        STRUCT('waba_balance_spending_sum_in_rubles_holded_KZT', waba_balance_spending_sum_in_rubles_holded_KZT),

        STRUCT('sum_in_rubles_spent_on_subscription_without_bad_balance_EUR', sum_in_rubles_spent_on_subscription_without_bad_balance_EUR),
        STRUCT('sum_in_rubles_spent_on_waba_balance_without_bad_balance_EUR', sum_in_rubles_spent_on_waba_balance_without_bad_balance_EUR),
        STRUCT('wapi_transactions_in_rubles_with_bad_balance_EUR', wapi_transactions_in_rubles_with_bad_balance_EUR),
        STRUCT('waba_balance_spending_sum_in_rubles_EUR', waba_balance_spending_sum_in_rubles_EUR),
        STRUCT('waba_balance_spending_sum_in_rubles_paid_EUR', waba_balance_spending_sum_in_rubles_paid_EUR),
        STRUCT('waba_balance_spending_sum_in_rubles_holded_EUR', waba_balance_spending_sum_in_rubles_holded_EUR),

        STRUCT('sum_in_rubles_spent_on_subscription_without_bad_balance_USD', sum_in_rubles_spent_on_subscription_without_bad_balance_USD),
        STRUCT('sum_in_rubles_spent_on_waba_balance_without_bad_balance_USD', sum_in_rubles_spent_on_waba_balance_without_bad_balance_USD),
        STRUCT('wapi_transactions_in_rubles_with_bad_balance_USD', wapi_transactions_in_rubles_with_bad_balance_USD),
        STRUCT('waba_balance_spending_sum_in_rubles_USD', waba_balance_spending_sum_in_rubles_USD),
        STRUCT('waba_balance_spending_sum_in_rubles_paid_USD', waba_balance_spending_sum_in_rubles_paid_USD),
        STRUCT('waba_balance_spending_sum_in_rubles_holded_USD', waba_balance_spending_sum_in_rubles_holded_USD),


        STRUCT('original_sum_spent_on_subscription_without_bad_balance_RUR', original_sum_spent_on_subscription_without_bad_balance_RUR),
        STRUCT('original_sum_spent_on_waba_balance_without_bad_balance_RUR', original_sum_spent_on_waba_balance_without_bad_balance_RUR),
        STRUCT('wapi_transactions_with_bad_balance_RUR_original_sum', wapi_transactions_with_bad_balance_RUR_original_sum),
        STRUCT('waba_balance_spendings_RUR_original_sum', waba_balance_spendings_RUR_original_sum),
        STRUCT('waba_balance_spendings_paid_RUR_original_sum', waba_balance_spendings_paid_RUR_original_sum),
        STRUCT('waba_balance_spendings_holded_RUR_original_sum', waba_balance_spendings_holded_RUR_original_sum),

        STRUCT('original_sum_spent_on_subscription_without_bad_balance_KZT', original_sum_spent_on_subscription_without_bad_balance_KZT),
        STRUCT('original_sum_spent_on_waba_balance_without_bad_balance_KZT', original_sum_spent_on_waba_balance_without_bad_balance_KZT),
        STRUCT('wapi_transactions_with_bad_balance_KZT_original_sum', wapi_transactions_with_bad_balance_KZT_original_sum),
        STRUCT('waba_balance_spendings_KZT_original_sum', waba_balance_spendings_KZT_original_sum),
        STRUCT('waba_balance_spendings_paid_KZT_original_sum', waba_balance_spendings_paid_KZT_original_sum),
        STRUCT('waba_balance_spendings_holded_KZT_original_sum', waba_balance_spendings_holded_KZT_original_sum),

        STRUCT('original_sum_spent_on_subscription_without_bad_balance_EUR', original_sum_spent_on_subscription_without_bad_balance_EUR),
        STRUCT('original_sum_spent_on_waba_balance_without_bad_balance_EUR', original_sum_spent_on_waba_balance_without_bad_balance_EUR),
        STRUCT('wapi_transactions_with_bad_balance_EUR_original_sum', wapi_transactions_with_bad_balance_EUR_original_sum),
        STRUCT('waba_balance_spendings_EUR_original_sum', waba_balance_spendings_EUR_original_sum),
        STRUCT('waba_balance_spendings_paid_EUR_original_sum', waba_balance_spendings_paid_EUR_original_sum),
        STRUCT('waba_balance_spendings_holded_EUR_original_sum', waba_balance_spendings_holded_EUR_original_sum),

        STRUCT('original_sum_spent_on_subscription_without_bad_balance_USD', original_sum_spent_on_subscription_without_bad_balance_USD),
        STRUCT('original_sum_spent_on_waba_balance_without_bad_balance_USD', original_sum_spent_on_waba_balance_without_bad_balance_USD),
        STRUCT('wapi_transactions_with_bad_balance_USD_original_sum', wapi_transactions_with_bad_balance_USD_original_sum),
        STRUCT('waba_balance_spendings_USD_original_sum', waba_balance_spendings_USD_original_sum),
        STRUCT('waba_balance_spendings_paid_USD_original_sum', waba_balance_spendings_paid_USD_original_sum),
        STRUCT('waba_balance_spendings_holded_USD_original_sum', waba_balance_spendings_holded_USD_original_sum),

        STRUCT('sum_in_rubles_spent_on_subscription_without_bad_balance', sum_in_rubles_spent_on_subscription_without_bad_balance),
        STRUCT('sum_in_rubles_spent_on_waba_balance_without_bad_balance', sum_in_rubles_spent_on_waba_balance_without_bad_balance),
        STRUCT('wapi_transactions_in_rubles_with_bad_balance', wapi_transactions_in_rubles_with_bad_balance),
        STRUCT('waba_balance_spending_sum_in_rubles', waba_balance_spending_sum_in_rubles),

        STRUCT('waba_reward_sum_in_rubles', waba_reward_sum_in_rubles),
        STRUCT('key_invoice_sessions_in_rubles', key_invoice_sessions_in_rubles),
        STRUCT('gupshup_invoice_sessions_in_rubles', gupshup_invoice_sessions_in_rubles),
        STRUCT('waba_balance_by_month_sum_in_rubles', waba_balance_by_month_sum_in_rubles),
        STRUCT('balance_by_spending', balance_by_spending),
        STRUCT('withdrawals_amount', withdrawals_amount),
        STRUCT('uslovno_peremennye_raskhody_sessii_vyplaty_partneram', uslovno_peremennye_raskhody_sessii_vyplaty_partneram),
        STRUCT('uslovno_peremennye_raskhody_sessii_komissiya_na_vyplaty_partneram', uslovno_peremennye_raskhody_sessii_komissiya_na_vyplaty_partneram),
        STRUCT('uslovno_peremennye_raskhody_sessii_ekvairing', uslovno_peremennye_raskhody_sessii_ekvairing),
        STRUCT('uslovno_peremennye_raskhody_sessii_komissiya_stripe_za_vyvod_usd', uslovno_peremennye_raskhody_sessii_komissiya_stripe_za_vyvod_usd),
        STRUCT('uslovno_peremennye_raskhody_sessii_komissiya_za_oplatu_waba', uslovno_peremennye_raskhody_sessii_komissiya_za_oplatu_waba),
        STRUCT('uslovno_peremennye_raskhody_sessii_raskhody_na_konversatsiyu_valyuty', uslovno_peremennye_raskhody_sessii_raskhody_na_konversatsiyu_valyuty),
        STRUCT('uslovno_peremennye_raskhody_sessii', uslovno_peremennye_raskhody_sessii),
        STRUCT('gross_profit', gross_profit),
        STRUCT('gross_profit_percent', gross_profit_percent),
        STRUCT('margin_profit', margin_profit),
        STRUCT('margin_percent', margin_percent)
     ]) AS metrics