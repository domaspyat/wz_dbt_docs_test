WITH first_pay_month AS (   -- CTE с месяцем первой выручки по аккаунту
  SELECT
    account_id,                                           -- ID аккаунта
    DATE_TRUNC(MIN(paid_date), month) AS min_month        -- Первый месяц оплаты
  FROM `dwh-wazzup`.`dbt_nbespalov`.`int_payments_revenue_with_waba`
  WHERE segment_type = 'of-partner'
  GROUP BY 1
),

sum_first_month AS (        -- CTE с суммой выручки по первому месяцу оплаты
  SELECT
    rbs.account_id,                              -- ID аккаунта
    paid_month,                                  -- Месяц оплаты
    SUM(sum_in_rubles) AS revenue_first_month    -- Выручка
  FROM `dwh-wazzup`.`dbt_nbespalov`.`mart_revenue_by_segments` rbs
  JOIN first_pay_month fpm ON fpm.account_id = rbs.account_id AND fpm.min_month = paid_month
  GROUP BY 1, 2
),

child_first_month AS (      -- CTE с количеством дочек в первый месяц оплаты
  SELECT
    partner_id,                                     -- ID аккаунта
    month,                                          -- Месяц
    COUNT(DISTINCT all_account_id) AS all_child_count,   -- Количество дочек
    COUNT(DISTINCT active_account_id) AS active_child_count
  FROM `dwh-wazzup`.`dbt_nbespalov`.`mart_partners_metrics_by_month` mbm
  JOIN first_pay_month fpm ON fpm.account_id = mbm.partner_id AND mbm.month = fpm.min_month
  AND type = 'partner'                              -- Мы берем только партнеров
  GROUP BY 1, 2
)

SELECT DISTINCT
  mbm.partner_id,                 -- Номер аккаунта партнера
  russianName,                    -- Страна
  currency,                       -- Валюта
  partner_register_date,          -- Дата регистрации партнера
  min_month,                      -- Первый месяц оплаты
  revenue_first_month,            -- Выручка в этом месяце
  all_child_count,                     -- Количество дочек в этом месяце
  active_child_count
FROM `dwh-wazzup`.`dbt_nbespalov`.`mart_partners_metrics_by_month` mbm
JOIN first_pay_month fpm ON fpm.account_id = mbm.partner_id
JOIN sum_first_month sfm ON sfm.account_id = mbm.partner_id
JOIN child_first_month cfm ON cfm.partner_id = mbm.partner_id
ORDER BY 5, 1