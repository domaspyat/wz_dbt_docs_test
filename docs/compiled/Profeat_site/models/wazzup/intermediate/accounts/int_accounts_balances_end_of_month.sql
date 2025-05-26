WITH test_accounts AS (
                      SELECT account_id
                      FROM `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_test_accounts`
                      ),
     billing_affiliate AS (
                      SELECT account_id
                           , cast(ba.occured_at AS date)                                                                                  AS occured_date
                           , ba.currency                                                                                                  AS currency
                           , sum(original_sum) OVER (PARTITION BY account_id,ba.currency ORDER BY occured_at)                                  AS cum_original_sum
                           , first_value(ba.currency) OVER (PARTITION BY account_id,cast(ba.occured_at AS date) ORDER BY occured_at DESC)      AS last_currency_of_the_day
                           , first_value(ba.currency) OVER (PARTITION BY account_id,date_trunc(ba.occured_at, month) ORDER BY occured_at DESC) AS last_currency_of_the_month
                           , occured_at
                      FROM `dwh-wazzup`.`dbt_nbespalov`.`stg_billingAffiliate` ba
                          LEFT JOIN `dwh-wazzup`.`dbt_nbespalov`.`int_payments_exchange_rates_unpivoted` exchange_rates_unpivoted 
                               ON exchange_rates_unpivoted._ibk = CAST (ba.occured_at AS DATE)
                               AND exchange_rates_unpivoted.currency = ba.currency
                      WHERE NOT EXISTS (
                          SELECT 1
                          FROM test_accounts
                          WHERE test_accounts.account_id = ba.account_id
                          )

                      ),
     transactions_per_day AS (
                      SELECT DISTINCT account_id
                                    , occured_date
                                    , currency
                                    , last_currency_of_the_day
                                    , last_currency_of_the_month
                                    , first_value(cum_original_sum) OVER (PARTITION BY account_id,currency,occured_date ORDER BY occured_at DESC) AS cum_original_sum
                      FROM billing_affiliate tc
                      ),
     defining_lead_date AS (
                      SELECT *
                           , lead(occured_date, 1, current_date) OVER (PARTITION BY account_id,currency ORDER BY occured_date) AS next_date
                      FROM transactions_per_day
                      ),
     creating_date_intervals AS (
                      SELECT *
                      FROM defining_lead_date
                          JOIN `dwh-wazzup`.`dbt_nbespalov`.`stg_days` sd
                          ON sd.date >= occured_date AND sd.date < next_date
                      ),
     defining_last_state_of_the_month AS (
                      SELECT creating_date_intervals.date                                                                                   AS balance_date
                           , cum_original_sum * coalesce(rur, 1)                                                                            AS balance_in_rubles
                           , cum_original_sum                                                                                               AS balance_in_original_currency
                           , last_currency_of_the_month                                                                                     AS currency
                           , account_id                                                                                                     AS account_id
                           , row_number() OVER (PARTITION BY date_trunc(creating_date_intervals.date, month),account_id ORDER BY date DESC) AS rn
                           , rur
                      FROM creating_date_intervals
                          LEFT JOIN `dwh-wazzup`.`dbt_nbespalov`.`int_payments_exchange_rates_unpivoted` exchange_rates_unpivoted
                          ON exchange_rates_unpivoted._ibk = CAST (creating_date_intervals.date AS DATE)
                          AND exchange_rates_unpivoted.currency = last_currency_of_the_month
                      WHERE creating_date_intervals.currency = last_currency_of_the_month
                      )
                      -- Таблица, которая показывает балансы аккаунтов на конец месяца в рублях и исходной валюте
SELECT date_trunc(balance_date, month)             AS balance_month                 -- Рассматриваемый месяц
     , cast(balance_in_rubles AS int64)            AS balance_in_rubles             -- Баланс в рублях
     , cast(balance_in_original_currency AS int64) AS balance_in_original_currency  -- Баланс в исходной валюте
     , currency                                                                     -- Валюта
     , account_id                                                                   -- ID аккаунта
FROM defining_last_state_of_the_month
WHERE rn = 1