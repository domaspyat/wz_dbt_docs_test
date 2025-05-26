WITH transactions_calculation AS (
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
                               )
SELECT   creating_date_intervals.date AS balance_date
       , cum_sum_in_rubles            AS balance_in_rubles
       , cum_original_sum             AS balance_in_original_currency
       , last_currency_of_the_day     AS currency
       , waba_subscription_id         AS waba_subscription_id 
FROM creating_date_intervals
    JOIN `dwh-wazzup`.`dbt_nbespalov`.`stg_waba_subscription_gupshup` ws ON creating_date_intervals.waba_subscription_id = ws.id
WHERE creating_date_intervals.currency = last_currency_of_the_day
        AND (date < CAST(deleted_at AS DATE)
                OR deleted_at IS NULL)