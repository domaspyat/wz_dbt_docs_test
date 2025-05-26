WITH last_datetime_client AS (    -- Последнее время оплаты у клиента
  SELECT
    bp.account_id,                        -- ID аккаунта
    MAX(su.created_at) AS last_paid_at,   -- Максимальнов время транзакции
  FROM `dwh-wazzup`.`dbt_nbespalov`.`stg_subscriptionUpdates` su
  JOIN `dwh-wazzup`.`dbt_nbespalov`.`stg_billingPackages` bp ON su.subscription_id = bp.guid
  WHERE su.state = 'activated'            -- Только активированные измененияA
  GROUP BY 1
),

last_pay_month_client AS (   -- CTE с месяцем последней выручки по аккаунту
  SELECT
    account_id,                                           -- ID аккаунта
    DATE_TRUNC(MAX(su.created_at), month) AS max_month    -- Последний месяц оплаты
  FROM `dwh-wazzup`.`dbt_nbespalov`.`stg_subscriptionUpdates` su
  JOIN `dwh-wazzup`.`dbt_nbespalov`.`stg_billingPackages` bp ON su.subscription_id = bp.guid
  WHERE su.state = 'activated'                            -- Только активированные изменения
  GROUP BY 1 
),

revenue_last_month_client AS (
  SELECT
    bp.account_id,                                      -- ID аккаунта
    DATE_TRUNC((su.created_at), month) AS last_month,   -- Месяц оплаты
    SUM(COALESCE(su.sum * cor_rate, su.sum)) AS revenue_last_month                   -- Выручка
  FROM `dwh-wazzup`.`dbt_nbespalov`.`stg_subscriptionUpdates` su
  JOIN `dwh-wazzup`.`dbt_nbespalov`.`stg_billingPackages` bp ON su.subscription_id = bp.guid
  JOIN last_pay_month_client lpm ON lpm.account_id = bp.account_id AND lpm.max_month = DATE_TRUNC((su.created_at), month)
  JOIN `dwh-wazzup`.`dbt_nbespalov`.`mart_revenue_by_segments` rbs ON rbs.account_id = lpm.account_id AND rbs.paid_date = su.created_date
  LEFT JOIN `dwh-wazzup`.`dbt_nbespalov`.`stg_exchangeRates` er ON er.data = su.created_date AND er.currency = su.currency AND nominal = 'RUR'
  WHERE rbs.segment_type IN ('of-partner-client', 'tech-partner-client')  -- Берем только клиентов партнеров
  AND su.state = 'activated'             -- Только активированные изменения
  GROUP BY 1, 2
),

client_paid AS (                  -- Здесь описывается кейс, когда клиент партнера платил сам
  SELECT 
    ldc.*, 
    su.sum        AS amount_paid, -- Сумма оплаты
    su.currency,                  -- Валюта
    'client_paid' AS who_paid     -- Кто платил
  FROM last_datetime_client ldc
  JOIN `dwh-wazzup`.`dbt_nbespalov`.`stg_billingPackages` bp ON bp.account_id = ldc.account_id
  JOIN `dwh-wazzup`.`dbt_nbespalov`.`stg_subscriptionUpdates` su ON su.created_at = ldc.last_paid_at AND bp.account_id = ldc.account_id
  JOIN `dwh-wazzup`.`dbt_nbespalov`.`mart_revenue_by_segments` rbs ON rbs.account_id = ldc.account_id AND rbs.paid_date = su.created_date
  WHERE rbs.segment_type IN ('of-partner-client', 'tech-partner-client')  -- Берем только клиентов партнеров
),

last_datetime_partner AS (  -- Последнее время оплаты у партнера
  SELECT
    subscription_owner        AS account_id,    -- Берем владельца подписки, то есть клиента
    DATETIME(MAX(occured_at)) AS last_paid_at   -- Максимальное время события
  FROM `dwh-wazzup`.`dbt_nbespalov`.`stg_billingAffiliate`
  WHERE account_id != subscription_owner        -- Отделяем кейсы, когда партнер платит за клиента
    AND object = 'subscription'                 -- Только оплата подписок
    AND subscription_update_id is not null      -- Перестраховочка
  GROUP BY 1
),

last_pay_month_partner AS (   -- CTE с месяцем последней выручки по аккаунту
  SELECT
    subscription_owner        AS account_id,           -- Берем владельца подписки, то есть клиента
    DATE_TRUNC(MAX(occured_at), month) AS max_month    -- Последний месяц оплаты
  FROM `dwh-wazzup`.`dbt_nbespalov`.`stg_billingAffiliate`
  WHERE account_id != subscription_owner        -- Отделяем кейсы, когда партнер платит за клиента
    AND object = 'subscription'                 -- Только оплата подписок
    AND subscription_update_id is not null      -- Перестраховочка
  GROUP BY 1 
),

revenue_last_month_partner AS (
  SELECT
    ba.subscription_owner                        AS account_id,         -- Берем владельца подписки, то есть клиента
    DATE_TRUNC((DATETIME(ba.occured_at)), month) AS last_month,         -- Месяц оплаты
    SUM(COALESCE(ba.sum * cor_rate, ba.sum))     AS revenue_last_month  -- Выручка
  FROM `dwh-wazzup`.`dbt_nbespalov`.`stg_billingAffiliate` ba
  JOIN last_pay_month_partner lpp ON lpp.account_id = ba.subscription_owner AND lpp.max_month = DATE_TRUNC((occured_at), month)
  LEFT JOIN `dwh-wazzup`.`dbt_nbespalov`.`stg_exchangeRates` er ON er.data = DATETIME(ba.occured_date) AND er.currency = ba.currency AND nominal = 'RUR'
  WHERE ba.account_id != subscription_owner     -- Отделяем кейсы, когда партнер платит за клиента
    AND object = 'subscription'                 -- Только оплата подписок
    AND subscription_update_id is not null      -- Перестраховочка
  GROUP BY 1, 2
),

partner_paid AS (         -- Здесь описывается кейс, когда партнер платил за клиента
  SELECT 
    ldp.*,
    ba.sum         AS amount_paid,   -- Сумма оплаты
    currency,                        -- Валюта
    'partner_paid' AS who_paid       -- Кто платил
  FROM last_datetime_partner ldp
  JOIN `dwh-wazzup`.`dbt_nbespalov`.`stg_billingAffiliate` ba ON ldp.last_paid_at = DATETIME(ba.occured_at) AND ldp.account_id = ba.subscription_owner
  WHERE ba.account_id != ba.subscription_owner  -- Отделяем кейсы, когда партнер платит за клиента
    AND object = 'subscription'                 -- Только оплата подписок
    AND subscription_update_id is not null      -- Перестраховочка
),

union_data AS (   -- Объединяем данные из двух таблиц
  SELECT *
  FROM client_paid

  UNION ALL

  SELECT *
  FROM partner_paid
),

agg_data AS ( -- Берем только последнюю запись по аккаунту
  SELECT
    account_id,
    MAX(last_paid_at) AS last_paid_at
  FROM union_data
  GROUP BY 1
),

union_revenue_data AS (
  SELECT *
  FROM revenue_last_month_client

  UNION ALL

  SELECT *
  FROM revenue_last_month_partner
),

agg_revenue_data AS (
  SELECT
    account_id,
    MAX(last_month) AS last_month
  FROM union_revenue_data
  GROUP BY 1
)

SELECT DISTINCT
  ad.account_id,                              -- Номер аккаунта
  DATE(ad.last_paid_at) AS last_paid_date,    -- Дата последней оплаты
  amount_paid,                                -- Сумма
  currency,                                   -- Валюта
  who_paid,                                   -- Кто платил?
  COALESCE(rlmc.revenue_last_month, 0) + COALESCE(rlmp.revenue_last_month, 0) AS last_sum_in_month_with_active_subs   -- Выручка за последний месяц активности аккаунта
FROM agg_data ad
JOIN agg_revenue_data ard ON ard.account_id = ad.account_id
JOIN union_data ud ON ud.account_id = ad.account_id AND ud.last_paid_at = ad.last_paid_at
LEFT JOIN revenue_last_month_client rlmc ON rlmc.account_id = ad.account_id AND rlmc.last_month = ard.last_month
LEFT JOIN revenue_last_month_partner rlmp ON rlmp.account_id = ad.account_id AND rlmp.last_month = ard.last_month