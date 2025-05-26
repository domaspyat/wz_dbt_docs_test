WITH  waba_transactions AS (
                                SELECT date_trunc(waba_transactions.transaction_date, month) AS paid_month
                                     , waba_transactions.subscription_id
                                     , waba_transactions.currency
                                     , deleted_at
                                     , rur                       
                                     , amount
                                     ,  first_value(waba_transactions.currency) over (partition by waba_transactions.subscription_id,date_trunc(waba_transactions.transaction_date, month) order by id desc) AS last_currency_of_the_month
                                FROM `dwh-wazzup`.`dbt_nbespalov`.`stg_channels_waba_transactions` waba_transactions
                                    LEFT JOIN `dwh-wazzup`.`dbt_nbespalov`.`stg_channels_waba_sessions` waba_sessions
                                ON waba_sessions.transaction_id = waba_transactions.id
                                    LEFT JOIN `dwh-wazzup`.`dbt_nbespalov`.`stg_billingPackages` billing_packages ON billing_packages.guid=waba_transactions.subscription_id
                                    LEFT JOIN `dwh-wazzup`.`dbt_nbespalov`.`int_payments_exchange_rates_unpivoted` exchange_rates
                                    ON exchange_rates.currency=waba_transactions.currency AND exchange_rates.data=waba_transactions.transaction_date
                                    LEFT JOIN `dwh-wazzup`.`dbt_nbespalov`.`int_subscription_deleted_from_eventLogs` deleted ON deleted.subscription_id=waba_transactions.subscription_id
                                WHERE waba_sessions.state IS DISTINCT FROM 'canceled'
                                    AND amount!=0
                                    AND waba_transactions.subscription_id IS DISTINCT FROM '57bf9315-afcb-4421-a18f-b053097dec27'
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
    ,amount_balance_by_month AS (
                               SELECT paid_month
                                     , last_currency_of_the_month
                                     , waba_transactions.subscription_id
                                     , waba_transactions.currency
                                     , max(date_trunc(deleted_at, month))                    AS deleted_month
                                     , sum(amount * coalesce(rur, 1))                        AS sum_in_rubles
                                     , sum(amount)                                           AS original_sum
                                FROM  waba_transactions
                                GROUP BY 1, 2, 3, 4
                                )
      , balance_by_month AS (
                             SELECT *
                                     , sum(sum_in_rubles) OVER (PARTITION BY subscription_id, currency ORDER BY paid_month ASC) AS balance
                                     , first_value(paid_month) over (partition by subscription_id,last_currency_of_the_month order by paid_month desc) as  last_month_for_currency
                                     , first_value(last_currency_of_the_month) over (partition by subscription_id order by paid_month desc) as  last_currency
                             FROM amount_balance_by_month
                           )
      , calendar_subscription_id AS (
                                     SELECT DISTINCT subscription_id 
                                                     , month 
                                                     , last_currency_of_the_month
                                     FROM balance_by_month
                                         CROSS JOIN `dwh-wazzup`.`dbt_nbespalov`.`stg_months`
                                     WHERE MONTH >=paid_month
                                       AND MONTH <=date_trunc(CURRENT_DATE, MONTH)
                                       AND (MONTH<deleted_month
                                        OR deleted_month IS NULL)
                                        and (month <= last_month_for_currency or last_currency = last_currency_of_the_month) 
                                )
      , calendar_with_currency_data AS (
                                        SELECT calendar_subscription_id.* 
                                               , balance
                                               , deleted_month
                                               , first_value(coalesce(balance,0)) over (partition by calendar_subscription_id.subscription_id,month order by coalesce(balance,0) desc ) AS correct_balance
                                        FROM calendar_subscription_id
                                            LEFT JOIN balance_by_month
                                                    ON balance_by_month.paid_month = calendar_subscription_id.month
                                                    AND balance_by_month.subscription_id = calendar_subscription_id.subscription_id
                                                    AND balance_by_month.last_currency_of_the_month = calendar_subscription_id.last_currency_of_the_month
                                        WHERE currency = calendar_subscription_id.last_currency_of_the_month or currency is null           
                                )
      , calendar_to_fillna AS (
                                SELECT *
                                     , sum(CASE WHEN balance IS NOT NULL THEN 1 ELSE 0 END) OVER (PARTITION BY subscription_id ORDER BY month ASC) AS r_close
                                FROM calendar_with_currency_data
                                where coalesce(balance,0) = correct_balance
                                )
      , calendar_filled_na AS (
                                SELECT *
                                     , first_value(balance) OVER (PARTITION BY subscription_id, r_close ORDER BY month ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS balance_filled
                                FROM calendar_to_fillna
                                )
                                
-- Таблица c общим балансом WABA по месяцам
SELECT month                                 -- Месяц
     , last_currency_of_the_month AS currency
     , sum(balance_filled)        AS balance -- Баланс на конец месяца
FROM calendar_filled_na
WHERE balance_filled > 0
GROUP BY 1, 2