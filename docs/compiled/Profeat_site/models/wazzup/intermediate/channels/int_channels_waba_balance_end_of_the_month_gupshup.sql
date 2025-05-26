WITH service_subscription_ids as (
    SELECT waba_subscription_id
    FROM `dwh-wazzup`.`dbt_nbespalov`.`stg_channels_waba_transactions_gupshup` wt
    JOIN `dwh-wazzup`.`dbt_nbespalov`.`stg_billingPackages` bp on wt.subscription_id = bp.guid
    JOIN `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info` api on bp.account_id = api.account_id
    AND is_employee
),
    transactions_calculation AS (
                                  SELECT waba_subscription_id
                                        , cast(wtg.date_at AS date)                                                                                          AS transaction_date
                                        , wtg.currency
                                        , sum(amount * coalesce(rur, 1)) OVER (PARTITION BY waba_subscription_id,wtg.currency ORDER BY id)                   AS cum_sum_in_rubles
                                        , sum(amount) OVER (PARTITION BY waba_subscription_id,wtg.currency ORDER BY id)                                      AS cum_original_sum
                                        , first_value(wtg.currency) OVER (PARTITION BY waba_subscription_id,cast(wtg.date_at AS date) ORDER BY id DESC)      AS last_currency_of_the_day
                                        , first_value(wtg.currency) OVER (PARTITION BY waba_subscription_id,date_trunc(wtg.date_at, month) ORDER BY id DESC) AS last_currency_of_the_month
                                        , id
                                  FROM `dwh-wazzup`.`dbt_nbespalov`.`stg_channels_waba_transactions_gupshup` wtg
                                      LEFT JOIN dwh-wazzup.dbt_prod.int_payments_exchange_rates_unpivoted exchange_rates_unpivoted
                                                 ON exchange_rates_unpivoted._ibk = CAST (wtg.date_at AS DATE)
                                                 AND exchange_rates_unpivoted.currency = wtg.currency
                                  WHERE NOT EXISTS (
                                            SELECT 1
                                            FROM service_subscription_ids
                                            WHERE service_subscription_ids.waba_subscription_id = wtg.waba_subscription_id
                                  )
                                  
                                 ),
    transactions_per_day AS (
                              SELECT DISTINCT waba_subscription_id
                                             , transaction_date
                                             , currency
                                             , last_currency_of_the_day
                                             , last_currency_of_the_month
                                             , first_value(cum_sum_in_rubles) OVER (PARTITION BY waba_subscription_id,currency,transaction_date ORDER BY id DESC) AS cum_sum_in_rubles
                                             , first_value(cum_original_sum)  OVER (PARTITION BY waba_subscription_id,currency,transaction_date ORDER BY id DESC) AS cum_original_sum
                              FROM transactions_calculation tc
                            ),
    defining_lead_date AS (
                           SELECT *
                                  , lead(transaction_date, 1, current_date) OVER (PARTITION BY waba_subscription_id,currency ORDER BY transaction_date) AS next_date
                           FROM transactions_per_day
                         ),
    creating_date_intervals AS (
                                SELECT *
                                FROM defining_lead_date
                                JOIN `dwh-wazzup`.`dbt_nbespalov`.`stg_days` sd ON sd.date >= transaction_date AND sd.date < next_date
                               ),
    defining_last_state_of_the_month AS (
                                          SELECT   creating_date_intervals.date AS balance_date
                                                 , cum_sum_in_rubles            AS balance_in_rubles
                                                 , cum_original_sum             AS balance_in_original_currency
                                                 , last_currency_of_the_month   AS currency
                                                 , waba_subscription_id         AS waba_subscription_id 
                                                 , row_number() over (partition by date_trunc(creating_date_intervals.date,month),waba_subscription_id order by date desc) AS rn
                                          FROM creating_date_intervals
                                              JOIN `dwh-wazzup`.`dbt_nbespalov`.`stg_waba_subscription_gupshup` ws ON creating_date_intervals.waba_subscription_id = ws.id
                                          WHERE creating_date_intervals.currency = last_currency_of_the_month
                                                  AND (date < CAST(deleted_at AS DATE)
                                                          OR deleted_at IS NULL)
                                         )
SELECT date_trunc(balance_date,month) AS balance_month
       , balance_in_rubles
       , balance_in_original_currency
       , currency
       , waba_subscription_id 
FROM defining_last_state_of_the_month
WHERE rn = 1