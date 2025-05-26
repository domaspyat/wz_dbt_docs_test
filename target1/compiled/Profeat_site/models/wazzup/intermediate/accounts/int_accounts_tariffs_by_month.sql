WITH subscription_updates_with_filled_tariff AS (
  SELECT
    su.*,
    IFNULL(su.tariff, FIRST_VALUE(su.tariff IGNORE NULLS) OVER (PARTITION BY subscription_id ORDER BY created_date ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)) AS filled_tariff,
    account_id
  FROM `dwh-wazzup`.`dbt_nbespalov`.`stg_subscriptionUpdates` su
  JOIN `dwh-wazzup`.`dbt_nbespalov`.`stg_billingPackages` bp ON bp.guid = su.subscription_id
),

generating_start_end_ats AS (
  SELECT
    account_id,
    subscription_id,
    created_at AS start_at,
    DATE_ADD(created_at, interval COALESCE(new_until_expired_days, until_expired_days, 30) day) AS end_at,
    filled_tariff
  FROM subscription_updates_with_filled_tariff
  WHERE state = 'activated'
    AND action NOT IN ('setPromisedPayment', 'balanceTopup')
),

subscriptions AS (
  SELECT
    account_id,
    subscription_id,
    start_at,
    CASE WHEN end_at IS NULL THEN LEAD(start_at) OVER (PARTITION BY subscription_id ORDER BY start_at) ELSE end_at END AS end_at,
    filled_tariff
  FROM generating_start_end_ats
),

formatting_end_at AS (
  SELECT
    account_id,
    subscription_id,
    start_at,
    CASE WHEN end_at > LEAD(start_at) OVER (PARTITION BY subscription_id ORDER BY start_at) THEN LEAD(start_at) OVER (PARTITION BY subscription_id ORDER BY start_at)
         ELSE end_at END AS end_at,
    filled_tariff
  FROM subscriptions
),

month_series AS (
  SELECT
    account_id,
    subscription_id,
    filled_tariff,
    DATE_TRUNC(month_date, month) AS active_month
  FROM formatting_end_at,
  UNNEST(
    GENERATE_DATE_ARRAY(
      DATE_TRUNC(DATE(start_at), month),
      DATE_TRUNC(DATE(end_at), month),
      INTERVAL 1 month
    )
  ) AS month_date
)
-- Таблица, которая показывает тарифы, которые активны у пользователя по месяцам
SELECT DISTINCT
  active_month,     -- Месяц активности
  filled_tariff AS tariff,    -- Тариф
  ms.account_id     -- ID аккаунта
FROM month_series ms
JOIN `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info` api ON ms.account_id = api.account_id
WHERE is_employee is false
  AND filled_tariff is not null