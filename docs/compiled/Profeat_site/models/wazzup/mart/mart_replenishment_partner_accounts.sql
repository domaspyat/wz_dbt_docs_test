-- Детализация всех пополнений партнерского счета

WITH real_money AS (      -- CTE с данными о том, сколько реальных денег положили на баланс партнеры в RUR сегменте
  SELECT
    ba.account_id     AS partner_id,                  -- ID аккаунта партнера
    ba.currency,                                      -- Валюта на момент транзакции
    ba.occured_date,                                  -- Дата транзакции
    api.type,                                         -- Тип партнера
    SUM(ba.sum)       AS real_money_org               -- Сумма денег
  FROM `dwh-wazzup`.`dbt_nbespalov`.`stg_billingAffiliate` ba
  JOIN `dwh-wazzup`.`dbt_nbespalov`.`int_accounts__type_change_history` api ON api.account_id = ba.account_id AND DATETIME(ba.occured_at) BETWEEN api.start_occured_at AND api.end_occured_at
    WHERE api.type in ('partner', 'tech-partner')     -- Тип аккаунта - партнер или тех.партнер
    AND ba.object = 'payment'                         -- Тип транзакции - платёж
    AND ba.original_sum > 0                           -- Отсекаем траты
    AND ba.method IN ('card', 'bank')                 -- Отсекаем трансферы
    AND is_invalid IS DISTINCT FROM true              -- Корректные счета
  GROUP BY 1, 2, 3, 4
  ),


transfers AS (            -- CTE с данными о том, сколько денег перевели на счёт партнера
  SELECT
    ba.account_id     AS partner_id,                  -- ID аккаунта партнера
    ba.currency,                                      -- Валюта на момент транзакции
    ba.occured_date,                                  -- Дата транзакции
    api.type,                                         -- Тип партнера
    SUM(ba.sum)       AS transfer_org                 -- Сумма переводов
  FROM `dwh-wazzup`.`dbt_nbespalov`.`stg_billingAffiliate` ba
  JOIN `dwh-wazzup`.`dbt_nbespalov`.`int_accounts__type_change_history` api ON api.account_id = ba.account_id AND DATETIME(ba.occured_at) BETWEEN api.start_occured_at AND api.end_occured_at
  -- JOIN currency_date cd ON cd.month = DATE_TRUNC(ba.occured_date, month) AND ba.currency = cd.currency
    WHERE api.type in ('partner', 'tech-partner')     -- Тип аккаунта - партнер или тех.партнер
    AND ba.object = 'payment'                         -- Тип транзакции - платёж
    AND (ba.method = 'transfer'                       -- Метод транзакции - трансфер
    OR ba.method IS NULL)                             -- Несколько кейсов с NULL, но это трансферы
    AND ba.original_sum > 0                           -- Отсекаем исходящие трансферы (траты)
    -- AND ba.currency = cd.currency                     -- Нужный рынок
    -- AND DATE_TRUNC(ba.occured_date, month) = cd.month -- Нужный временной промежуток
  GROUP BY 1, 2, 3, 4
),


bonus_money AS (          -- CTE с данными о том, сколько бонусов получил партнер
  SELECT
    ba.account_id     AS partner_id,                  -- ID аккаунта партнера
    ba.currency,                                      -- Валюта на момент транзакции
    ba.occured_date,                                  -- Дата транзакции
    api.type,                                         -- Тип партнера
    SUM(ba.sum)       AS bonus_money_org              -- Сумма бонусов
  FROM `dwh-wazzup`.`dbt_nbespalov`.`stg_billingAffiliate` ba
  JOIN `dwh-wazzup`.`dbt_nbespalov`.`int_accounts__type_change_history` api ON api.account_id = ba.account_id AND DATETIME(ba.occured_at) BETWEEN api.start_occured_at AND api.end_occured_at
    WHERE api.type in ('partner', 'tech-partner')     -- Тип аккаунта - партнер или тех.партнер
    AND object NOT IN ('payment', 'convertation')     -- Отсекаем платежи и конвертации
    AND ba.original_sum > 0                           -- Отсекаем траты (подписки и т.п)
  GROUP BY 1, 2, 3, 4
),

invalid_bills AS (          -- CTE с данными о том, сколько денег получил партнер с некорректных счетов
  SELECT
    ba.account_id     AS partner_id,                  -- ID аккаунта партнера
    ba.currency,                                      -- Валюта на момент транзакции
    ba.occured_date,                                  -- Дата транзакции
    api.type,                                         -- Тип партнера
    SUM(ba.sum)       AS invalid_bills_org            -- Сумма некорректных счетов
  FROM `dwh-wazzup`.`dbt_nbespalov`.`stg_billingAffiliate` ba
  JOIN `dwh-wazzup`.`dbt_nbespalov`.`int_accounts__type_change_history` api ON api.account_id = ba.account_id AND DATETIME(ba.occured_at) BETWEEN api.start_occured_at AND api.end_occured_at
    WHERE api.type in ('partner', 'tech-partner')     -- Тип аккаунта - партнер или тех.партнер
    AND object = 'payment'                  
    AND is_invalid is true                            -- Некорректные счета
    AND ba.original_sum > 0                           -- Отсекаем траты (подписки и т.п)
  GROUP BY 1, 2, 3, 4
),

convertations AS (         -- CTE с пополнениями за конвертации
  SELECT
    ba.account_id     AS partner_id,                  -- ID аккаунта партнера
    ba.currency,                                      -- Валюта на момент транзакции
    ba.occured_date,                                  -- Дата транзакции
    api.type,                                         -- Тип партнера
    SUM(ba.sum)          AS convertations_org                  -- Сумма пополнений за смену валюты
  FROM `dwh-wazzup`.`dbt_nbespalov`.`stg_billingAffiliate` ba
  JOIN `dwh-wazzup`.`dbt_nbespalov`.`int_accounts__type_change_history` api ON api.account_id = ba.account_id AND DATETIME(ba.occured_at) BETWEEN api.start_occured_at AND api.end_occured_at
    WHERE ba.object = 'convertation'                          -- Смена валюты
    AND api.type in ('partner', 'tech-partner')     -- Тип аккаунта - партнер или тех.партнер
    AND ba.original_sum > 0
  GROUP BY 1, 2, 3, 4
),

all_topups AS (           -- CTE с данными о всех пополнениях
  SELECT
    ba.account_id     AS partner_id,                  -- ID аккаунта партнера
    ba.currency,                                      -- Валюта на момент транзакции
    ba.occured_date,                                  -- Дата транзакции
    api.type,                                         -- Тип партнера
    SUM(ba.sum)       AS combined_org                     -- Общая сумма
  FROM `dwh-wazzup`.`dbt_nbespalov`.`stg_billingAffiliate` ba
  JOIN `dwh-wazzup`.`dbt_nbespalov`.`int_accounts__type_change_history` api ON api.account_id = ba.account_id AND DATETIME(ba.occured_at) BETWEEN api.start_occured_at AND api.end_occured_at
    WHERE api.type in ('partner', 'tech-partner')     -- Тип аккаунта - партнер или тех.партнер
    AND ba.original_sum > 0                               -- Отсекаем траты (подписки и т.п)

  GROUP BY 1, 2, 3, 4
),



original_currency_data as(
  SELECT
    partner_id,                         -- Id партнера
    type,                               -- Тип партнера
    currency,                           -- Валюта транзакции
    occured_date,                       -- Дата транзакции
    real_money_org,                     -- Сумма пополнений реальными деньгами в оригинальной валюте
    transfer_org,                       -- Сумма переводов в оригинальной валюте
    bonus_money_org,                    -- Сумма бонусов в оригинальной валюте
    invalid_bills_org,                  -- Сумма некорректных счетов в оригинальной валюте
    convertations_org,                  -- Сумма пополнений за смену валюты в оригинальной валюте
    combined_org,                       -- Общая сумма в оригинальной валюте
    COALESCE(real_money_org, 0) + COALESCE(invalid_bills_org, 0) as real_money_invalid_bills_org -- Сумма пополнений реальными деньгами и некорректными счетами в оригинальной валюте
  FROM real_money rm
  FULL OUTER JOIN transfers t USING(partner_id, occured_date, currency, type)
  FULL OUTER JOIN bonus_money bm USING(partner_id, occured_date, currency, type)
  FULL OUTER JOIN invalid_bills ib USING(partner_id, occured_date, currency, type)
  FULL OUTER JOIN convertations c USING(partner_id, occured_date, currency, type)
  FULL OUTER JOIN all_topups alt USING(partner_id, occured_date, currency, type)
),

original_currency_and_rur_data as(
  SELECT
    ocd.partner_id,                         -- Id партнера
    ocd.type,                               -- Тип партнера
    ocd.currency,                       -- Валюта транзакции
    occured_date,                       -- Дата транзакции
    real_money_org,                     -- Сумма пополнений реальными деньгами в оригинальной валюте
    transfer_org,                       -- Сумма переводов в оригинальной валюте
    bonus_money_org,                    -- Сумма бонусов в оригинальной валюте
    invalid_bills_org,                  -- Сумма некорректных счетов в оригинальной валюте
    convertations_org,                  -- Сумма пополнений за смену валюты в оригинальной валюте
    combined_org,                       -- Общая сумма в оригинальной валюте
    real_money_invalid_bills_org,       -- Сумма пополнений реальными деньгами и некорректными счетами в оригинальной валюте

    real_money_org * COALESCE(cor_rate, 1) as real_money_rur,                               -- Сумма пополнений реальными деньгами в оригинальной валюте
    transfer_org * COALESCE(cor_rate, 1) as transfer_rur,                                   -- Сумма переводов в оригинальной валюте
    bonus_money_org * COALESCE(cor_rate, 1) as bonus_money_rur,                             -- Сумма бонусов в оригинальной валюте
    invalid_bills_org * COALESCE(cor_rate, 1) as invalid_bills_rur,                         -- Сумма некорректных счетов в оригинальной валюте
    convertations_org * COALESCE(cor_rate, 1) as convertations_rur,                         -- Сумма пополнений за смену валюты в оригинальной валюте
    combined_org * COALESCE(cor_rate, 1) as combined_rur,                                   -- Общая сумма в оригинальной валюте
    real_money_invalid_bills_org * COALESCE(cor_rate, 1) as real_money_invalid_bills_rur    -- Сумма пополнений реальными деньгами и некорректными счетами в оригинальной валюте
    FROM original_currency_data ocd
    LEFT JOIN `dwh-wazzup`.`dbt_nbespalov`.`stg_exchangeRates` er ON ocd.occured_date = er.data AND ocd.currency = er.currency AND nominal = 'RUR'
    JOIN `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info` api ON ocd.partner_id = api.account_Id and is_employee is false
)

SELECT *
FROM original_currency_and_rur_data