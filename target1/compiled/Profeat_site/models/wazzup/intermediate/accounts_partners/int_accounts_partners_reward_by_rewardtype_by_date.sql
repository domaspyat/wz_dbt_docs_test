WITH reward AS (
  SELECT 
    account_id,
    occured_date,
    billing_affiliate.currency,
    ABS(SUM(SUM*COALESCE(exchange_rates.cor_rate,1))) AS sum_in_rubles,
    ABS(SUM(SUM))                                     AS original_sum
  FROM `dwh-wazzup`.`dbt_nbespalov`.`stg_billingAffiliate` billing_affiliate
  LEFT JOIN `dwh-wazzup`.`dbt_nbespalov`.`stg_exchangeRates` exchange_rates ON exchange_rates.currency=billing_affiliate.currency AND billing_affiliate.occured_date=exchange_rates.data AND nominal = 'RUR'
  WHERE object IN ('reward', 'noReward')
  GROUP BY 1,2,3
),

rewardWaba AS (
  SELECT 
    account_id,
    occured_date,
    billing_affiliate.currency,
    ABS(SUM(SUM*COALESCE(exchange_rates.cor_rate,1))) AS sum_in_rubles_waba,
    ABS(SUM(SUM))                                     AS original_sum_waba
  FROM `dwh-wazzup`.`dbt_nbespalov`.`stg_billingAffiliate` billing_affiliate
  LEFT JOIN `dwh-wazzup`.`dbt_nbespalov`.`stg_exchangeRates` exchange_rates ON exchange_rates.currency=billing_affiliate.currency AND billing_affiliate.occured_date=exchange_rates.data AND nominal = 'RUR'
  WHERE object = 'rewardWaba'
  GROUP BY 1,2,3
)

SELECT
  COALESCE(r.account_id, rw.account_id)                                 AS account_id,
  CASE WHEN sum_in_rubles > 0 OR sum_in_rubles_waba > 0 
  THEN DATE_SUB(COALESCE(r.occured_date, rw.occured_date),interval 1 month)
  ELSE COALESCE(r.occured_date, rw.occured_date) END                    AS occured_date_sub,
  COALESCE(r.currency, rw.currency)                                     AS currency,
  COALESCE(sum_in_rubles, 0)                                            AS sum_in_rubles,
  COALESCE(original_sum, 0)                                             AS original_sum,
  COALESCE(sum_in_rubles_waba, 0)                                       AS sum_in_rubles_waba,
  COALESCE(original_sum_waba, 0)                                        AS original_sum_waba
FROM reward r
FULL OUTER JOIN rewardWaba rw ON r.account_id = rw.account_id AND r.occured_date = rw.occured_date