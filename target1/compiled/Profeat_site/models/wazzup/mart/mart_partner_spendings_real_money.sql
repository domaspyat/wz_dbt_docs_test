WITH 
--------------------------------------- Партнер платит за клиента

waba_balance_clients_partner_paid AS (
  SELECT    -- Таблица с тратами партнера на баланс WABA клиентов
    ba.account_id                                                                     AS partner_id,                -- ID партнера
    ba.occured_date                                                                   AS occured_date,              -- Дата транзакции
    api.type                                                                          AS account_type,              -- Тип аккаунта
    ba.currency                                                                       AS currency,                  -- Валюта транзакции
    'balanceTopup_client'                                                             AS category,                  -- Категория транзакции
    (ba.sum / (1 - 0.1)) + balance_to_withdraw                                   AS tariff_price_sum,          -- Сумма по тарифу
    balance_to_withdraw                                                          AS bonus_spent_by_client_sum, -- Бонусы клиентов
    0.1                                                                               AS partner_discount,          -- Скидка партнера
    ((((ba.sum / (1 - 0.1)) + balance_to_withdraw) - balance_to_withdraw) * 0.1) AS partner_comission,         -- Комиссия партнера
    ba.sum                                                                       AS partner_price_sum          -- Стоимость для партнера
  FROM `dwh-wazzup`.`dbt_nbespalov`.`stg_billingAffiliate` ba
  JOIN `dwh-wazzup`.`dbt_nbespalov`.`stg_subscriptionUpdates` su ON su.guid = ba.subscription_update_id
  JOIN `dwh-wazzup`.`dbt_nbespalov`.`int_accounts__type_change_history` api ON api.account_id = ba.account_id AND DATETIME(ba.occured_at) BETWEEN api.start_occured_at AND api.end_occured_at
    WHERE su.action = 'balanceTopup'                      -- Берем только WABA
    AND ba.account_id != ba.subscription_owner            -- Оплата подписки клиента
    AND api.type in ('partner', 'tech-partner')                              -- Тип аккаунта - партнер
),

subscriptions_clients_partner_paid AS (
  SELECT  -- Таблица с тратами партнера на подписки клиентов
    ba.account_id                                                                     AS partner_id,                -- ID партнера
    ba.occured_date                                                                   AS occured_date,              -- Дата транзакции
    api.type                                                                          AS account_type,              -- Тип аккаунта
    ba.currency                                                                       AS currency,                  -- Валюта транзакции
    'subscription_client'                                                                                              AS category,                   -- Категория транзакции
    (ba.sum / (1 - partner_discount)) + balance_to_withdraw                                                AS tariff_price_sum,           -- Сумма по тарифу
    balance_to_withdraw                                                                                    AS bonus_spent_by_client_sum,  -- Бонусы клиентов
    partner_discount,                                                                                                                          -- Скидка партнера
    ((((ba.sum / (1 - partner_discount)) + balance_to_withdraw) - balance_to_withdraw) * partner_discount) AS partner_comission,          -- Комиссия партнера
    ba.sum                                                                                                 AS partner_price_sum           -- Стоимость для партнера
  FROM `dwh-wazzup`.`dbt_nbespalov`.`stg_billingAffiliate` ba
  JOIN `dwh-wazzup`.`dbt_nbespalov`.`stg_subscriptionUpdates` su ON su.guid = ba.subscription_update_id
  JOIN `dwh-wazzup`.`dbt_nbespalov`.`int_accounts__type_change_history` api ON api.account_id = ba.account_id AND DATETIME(ba.occured_at) BETWEEN api.start_occured_at AND api.end_occured_at
    WHERE su.action != 'balanceTopup'                         -- Берем всё, кроме WABA
    AND (wapi_transactions IS NULL OR wapi_transactions = 0)  -- Убираем кейсы с одновременной оплатой баланса WABA и подписки WABA
    AND ba.account_id != ba.subscription_owner                -- Оплата подписки клиента
    AND api.type in ('partner', 'tech-partner')                              -- Тип аккаунта - партнер

),

waba_calculations_client AS ( -- Таблица с тратами партнера на баланс WABA в кейсах, когда партнер оплачивал WABA+подписку одним платежом
  SELECT
    ba.account_id                                                                     AS partner_id,                -- ID партнера
    ba.occured_date                                                                   AS occured_date,              -- Дата транзакции
    api.type                                                                          AS account_type,              -- Тип аккаунта
    ba.currency                                                                       AS currency,                  -- Валюта транзакции
    'balanceTopup_client'                                                                                 AS category,                         -- Категория транзакции
    wapi_transactions                                                                         AS tariff_price_sum,                 -- Сумма по тарифу
    CASE WHEN balance_to_withdraw <= (su.sum - wapi_transactions) THEN 0
      ELSE balance_to_withdraw - (su.sum - wapi_transactions) END                                 AS bonus_spent_by_client_sum,        -- Бонусы клиентов
    0.1                                                                                            AS partner_discount,                 -- Скидка партнера
    CASE WHEN balance_to_withdraw < (su.sum - wapi_transactions) THEN (wapi_transactions * 0.1) 
      ELSE ((wapi_transactions - (balance_to_withdraw - (su.sum - wapi_transactions))) * 0.1) END AS partner_comission,                -- Комиссия партнера
    CASE WHEN balance_to_withdraw < (su.sum - wapi_transactions) THEN (wapi_transactions * 0.9) 
      ELSE ((wapi_transactions - (balance_to_withdraw - (su.sum - wapi_transactions))) * 0.9) END AS partner_price_sum,  -- Стоимость для партнера
  FROM `dwh-wazzup`.`dbt_nbespalov`.`stg_billingAffiliate` ba
  JOIN `dwh-wazzup`.`dbt_nbespalov`.`int_accounts__type_change_history` api ON api.account_id = ba.account_id AND DATETIME(ba.occured_at) BETWEEN api.start_occured_at AND api.end_occured_at
  JOIN `dwh-wazzup`.`dbt_nbespalov`.`stg_subscriptionUpdates`su ON su.guid = ba.subscription_update_id
    WHERE ba.object = 'subscription'                       -- Берем всё, кроме WABA баланса
    AND api.type in ('partner', 'tech-partner')                               -- Был партнером на момент транзакции
    AND su.action != 'balanceTopup'                        -- Убираем действия из subscriptionUpdates, которые касаются пополнения баланса WABA
    AND wapi_transactions > 0                              -- Сумма оплаты баланса WABA > 0
    AND ba.account_id != ba.subscription_owner             -- Оплата клиенту
),

subs_calculations_client AS ( -- Таблица с тратами партнера на подписку WABA в кейсах, когда партнер оплачивал WABA+подписку одним платежом
  SELECT
    ba.account_id                                                                     AS partner_id,                -- ID партнера
    ba.occured_date                                                                   AS occured_date,              -- Дата транзакции
    api.type                                                                          AS account_type,              -- Тип аккаунта
    ba.currency                                                                       AS currency,                  -- Валюта транзакции
    'subscription_client'                                                                                              AS category,                   -- Категория транзакции
    su.sum - wapi_transactions                                                           AS tariff_price_sum,          -- Сумма по тарифу
    CASE WHEN balance_to_withdraw >= (su.sum - wapi_transactions) THEN (su.sum - wapi_transactions)
      ELSE balance_to_withdraw  END                                                          AS bonus_spent_by_client_sum, -- Бонусы клиентов
    partner_discount                                                                          AS partner_discount,          -- Скидка партнера
    CASE WHEN balance_to_withdraw >= (su.sum - wapi_transactions) THEN 0 
      ELSE ((su.sum - balance_to_withdraw - wapi_transactions) * partner_discount) END       AS partner_comission,         -- Комиссия партнера
    CASE WHEN balance_to_withdraw >= (su.sum - wapi_transactions) THEN 0 
      ELSE ((su.sum - balance_to_withdraw - wapi_transactions) * (1 - partner_discount)) END AS partner_price_sum    -- Стоимость для партнера
  FROM `dwh-wazzup`.`dbt_nbespalov`.`stg_billingAffiliate` ba
  JOIN `dwh-wazzup`.`dbt_nbespalov`.`int_accounts__type_change_history` api ON api.account_id = ba.account_id AND DATETIME(ba.occured_at) BETWEEN api.start_occured_at AND api.end_occured_at
  JOIN `dwh-wazzup`.`dbt_nbespalov`.`stg_subscriptionUpdates`su ON su.guid = ba.subscription_update_id
    WHERE ba.object = 'subscription'                        -- Берем всё, кроме WABA баланса
    AND api.type in ('partner', 'tech-partner')                                -- Был партнером на момент транзакции
    AND su.action != 'balanceTopup'                         -- Убираем действия из subscriptionUpdates, которые касаются пополнения баланса WABA
    AND wapi_transactions > 0                               -- Сумма оплаты баланса WABA > 0
    AND ba.account_id != ba.subscription_owner              -- Оплата клиенту
),


client_spending as (
  SELECT 
    partner_id,                                                 -- ID партнера
    occured_date,                                               -- Дата транзакции
    account_type,                                               -- Тип аккаунта
    currency,                                                   -- Валюта транзакции
    category,                                                   -- Категория транзакции
    tariff_price_sum AS tariff_price_org,                       -- Сумма по тарифу
    bonus_spent_by_client_sum AS bonus_spent_by_client_org,     -- Бонусы клиентов
    partner_discount,                                           -- Скидка партнера
    partner_comission AS partner_comission_org,                 -- Комиссия партнера
    partner_price_sum AS partner_price_org                      -- Стоимость для партнера
  FROM waba_balance_clients_partner_paid
  FULL OUTER JOIN subscriptions_clients_partner_paid USING (partner_id, occured_date, account_type, currency, category, tariff_price_sum, bonus_spent_by_client_sum, partner_discount, partner_comission, partner_price_sum)
  FULL OUTER JOIN waba_calculations_client USING (partner_id, occured_date, account_type, currency, category, tariff_price_sum, bonus_spent_by_client_sum, partner_discount, partner_comission, partner_price_sum)
  FULL OUTER JOIN subs_calculations_client USING (partner_id, occured_date, account_type, currency, category, tariff_price_sum, bonus_spent_by_client_sum, partner_discount, partner_comission, partner_price_sum)
),

------------------------------------------ Партнер платит за себя

waba_balance_partner AS (
  SELECT    -- Таблица с тратами партнера на баланс WABA себе
    ba.account_id                                                                     AS partner_id,                -- ID партнера
    ba.occured_date                                                                   AS occured_date,              -- Дата транзакции
    api.type                                                                          AS account_type,              -- Тип аккаунта
    ba.currency                                                                       AS currency,                  -- Валюта транзакции
    'balanceTopup_partner'                                                                    AS category,                  -- Категория транзакции
    su.sum                                                                       AS tariff_price_sum,          -- Сумма по тарифу
    0.1                                                                               AS partner_discount,          -- Скидка партнера
    su.sum * 0.1                                                                 AS partner_comission,         -- Комиссия партнера
    ba.sum                                                                       AS partner_price_sum          -- Стоимость для партнера
  FROM `dwh-wazzup`.`dbt_nbespalov`.`stg_billingAffiliate` ba
  JOIN `dwh-wazzup`.`dbt_nbespalov`.`stg_subscriptionUpdates` su ON su.guid = ba.subscription_update_id
  JOIN `dwh-wazzup`.`dbt_nbespalov`.`int_accounts__type_change_history` api ON api.account_id = ba.account_id AND DATETIME(ba.occured_at) BETWEEN api.start_occured_at AND api.end_occured_at
    WHERE su.action = 'balanceTopup'                      -- Берем только WABA
    AND ba.account_id = ba.subscription_owner            -- Оплата подписки себе
    AND api.type in ('partner', 'tech-partner')                              -- Тип аккаунта - партнер
),

subscriptions_partner AS (
  SELECT  -- Таблица с тратами партнера на подписки себе
    ba.account_id                                                                     AS partner_id,                -- ID партнера
    ba.occured_date                                                                   AS occured_date,              -- Дата транзакции
    api.type                                                                          AS account_type,              -- Тип аккаунта
    ba.currency                                                                       AS currency,                  -- Валюта транзакции
    'subscription_partner'                                                                                              AS category,                   -- Категория транзакции
    su.sum                                                                                                 AS tariff_price_sum,           -- Сумма по тарифу
    partner_discount,                                                                                                                          -- Скидка партнера
    su.sum * partner_discount                                                                              AS partner_comission,          -- Комиссия партнера
    ba.sum                                                                                                 AS partner_price_sum           -- Стоимость для партнера
  FROM `dwh-wazzup`.`dbt_nbespalov`.`stg_billingAffiliate` ba
  JOIN `dwh-wazzup`.`dbt_nbespalov`.`stg_subscriptionUpdates` su ON su.guid = ba.subscription_update_id
  JOIN `dwh-wazzup`.`dbt_nbespalov`.`int_accounts__type_change_history` api ON api.account_id = ba.account_id AND DATETIME(ba.occured_at) BETWEEN api.start_occured_at AND api.end_occured_at
    WHERE su.action != 'balanceTopup'                         -- Берем всё, кроме WABA
    AND (wapi_transactions IS NULL OR wapi_transactions = 0)  -- Убираем кейсы с одновременной оплатой баланса WABA и подписки WABA
    AND ba.account_id = ba.subscription_owner                -- Оплата подписки себе
    AND api.type in ('partner', 'tech-partner')                                  -- Тип аккаунта - партнер
),

waba_calculations_partner AS ( -- Таблица с тратами партнера на баланс WABA в кейсах, когда партнер оплачивал WABA+подписку одним платежом
  SELECT
    ba.account_id                                                                     AS partner_id,                -- ID партнера
    ba.occured_date                                                                   AS occured_date,              -- Дата транзакции
    api.type                                                                          AS account_type,              -- Тип аккаунта
    ba.currency                                                                       AS currency,                  -- Валюта транзакции
    'balanceTopup_partner'                                                                                 AS category,                         -- Категория транзакции
    wapi_transactions                                                                         AS tariff_price_sum,                 -- Сумма по тарифу
    0.1                                                                                            AS partner_discount,                 -- Скидка партнера
    CASE WHEN balance_to_withdraw < (su.sum - wapi_transactions) THEN (wapi_transactions * 0.1) 
      ELSE ((wapi_transactions - (balance_to_withdraw - (su.sum - wapi_transactions))) * 0.1) END AS partner_comission,                -- Комиссия партнера
    CASE WHEN balance_to_withdraw < (su.sum - wapi_transactions) THEN (wapi_transactions * 0.9) 
      ELSE ((wapi_transactions - (balance_to_withdraw - (su.sum - wapi_transactions))) * 0.9) END AS partner_price_sum,  -- Стоимость для партнера
  FROM `dwh-wazzup`.`dbt_nbespalov`.`stg_billingAffiliate` ba
  JOIN `dwh-wazzup`.`dbt_nbespalov`.`int_accounts__type_change_history` api ON api.account_id = ba.account_id AND DATETIME(ba.occured_at) BETWEEN api.start_occured_at AND api.end_occured_at
  JOIN `dwh-wazzup`.`dbt_nbespalov`.`stg_subscriptionUpdates`su ON su.guid = ba.subscription_update_id
    WHERE ba.object = 'subscription'                       -- Берем всё, кроме WABA баланса
    AND api.type in ('partner', 'tech-partner')                               -- Был партнером на момент транзакции
    AND su.action != 'balanceTopup'                        -- Убираем действия из subscriptionUpdates, которые касаются пополнения баланса WABA
    AND wapi_transactions > 0                              -- Сумма оплаты баланса WABA > 0
    AND ba.account_id = ba.subscription_owner             -- Оплата себе
),

subs_calculations_partner AS ( -- Таблица с тратами партнера на подписку WABA в кейсах, когда партнер оплачивал WABA+подписку одним платежом
  SELECT
    ba.account_id                                                                     AS partner_id,                -- ID партнера
    ba.occured_date                                                                   AS occured_date,              -- Дата транзакции
    api.type                                                                          AS account_type,              -- Тип аккаунта
    ba.currency                                                                       AS currency,                  -- Валюта транзакции
    'subscription_partner'                                                                            AS category,                  -- Категория транзакции
    su.sum - wapi_transactions                                                           AS tariff_price_sum,          -- Сумма по тарифу
    partner_discount                                                                          AS partner_discount,          -- Скидка партнера
    CASE WHEN balance_to_withdraw >= (su.sum - wapi_transactions) THEN 0 
      ELSE ((su.sum - balance_to_withdraw - wapi_transactions) * partner_discount) END       AS partner_comission,         -- Комиссия партнера
    CASE WHEN balance_to_withdraw >= (su.sum - wapi_transactions) THEN 0 
      ELSE ((su.sum - balance_to_withdraw - wapi_transactions) * (1 - partner_discount)) END AS partner_price_sum    -- Стоимость для партнера
  FROM `dwh-wazzup`.`dbt_nbespalov`.`stg_billingAffiliate` ba
  JOIN `dwh-wazzup`.`dbt_nbespalov`.`int_accounts__type_change_history` api ON api.account_id = ba.account_id AND DATETIME(ba.occured_at) BETWEEN api.start_occured_at AND api.end_occured_at
  JOIN `dwh-wazzup`.`dbt_nbespalov`.`stg_subscriptionUpdates`su ON su.guid = ba.subscription_update_id
    WHERE ba.object = 'subscription'                        -- Берем всё, кроме WABA баланса
    AND api.type in ('partner', 'tech-partner')                                -- Был партнером на момент транзакции
    AND su.action != 'balanceTopup'                         -- Убираем действия из subscriptionUpdates, которые касаются пополнения баланса WABA
    AND wapi_transactions > 0                               -- Сумма оплаты баланса WABA > 0
    AND ba.account_id = ba.subscription_owner              -- Оплата себе
),


partner_spending as (
  SELECT 
    partner_id,                                     -- ID партнера
    occured_date,                                   -- Дата транзакции
    account_type,                                   -- Тип аккаунта
    currency,                                       -- Валюта транзакции
    category,                                       -- Категория транзакции
    tariff_price_sum AS tariff_price_org,           -- Сумма по тарифу
    0 AS bonus_spent_by_client_org,                 -- Бонусы клиентов
    partner_discount,                               -- Скидка партнера
    partner_comission AS partner_comission_org,     -- Комиссия партнера
    partner_price_sum AS partner_price_org          -- Стоимость для партнера
  FROM waba_balance_partner
  FULL OUTER JOIN subscriptions_partner USING (partner_id, occured_date, account_type, currency, category, tariff_price_sum, partner_discount, partner_comission,partner_price_sum)
  FULL OUTER JOIN waba_calculations_partner USING (partner_id, occured_date, account_type, currency, category, tariff_price_sum, partner_discount, partner_comission,partner_price_sum)
  FULL OUTER JOIN subs_calculations_partner USING (partner_id, occured_date, account_type, currency, category, tariff_price_sum, partner_discount, partner_comission,partner_price_sum)
),

original_currency_data as (
  SELECT 
    partner_id,                                     -- ID партнера
    occured_date,                                   -- Дата транзакции
    account_type,                                   -- Тип аккаунта
    currency,                                       -- Валюта транзакции
    category,                                       -- Категория транзакции
    tariff_price_org,                               -- Сумма по тарифу
    bonus_spent_by_client_org,                      -- Бонусы клиентов
    partner_discount,                               -- Скидка партнера
    partner_comission_org,                          -- Комиссия партнера
    partner_price_org                               -- Стоимость для партнера
  FROM partner_spending
  FULL OUTER JOIN client_spending USING (partner_id, occured_date, account_type, currency, category, tariff_price_org, bonus_spent_by_client_org, partner_discount, partner_comission_org, partner_price_org )

),

original_currency_and_rur_data as (
  SELECT 
    partner_id,                                     -- ID партнера
    occured_date,                                   -- Дата транзакции
    account_type,                                   -- Тип аккаунта
    ocd.currency,                                       -- Валюта транзакции
    category,                                       -- Категория транзакции
    tariff_price_org,                               -- Сумма по тарифу
    bonus_spent_by_client_org,                      -- Бонусы клиентов
    partner_discount,                               -- Скидка партнера
    partner_comission_org,                          -- Комиссия партнера
    partner_price_org,                              -- Стоимость для партнера
    tariff_price_org * COALESCE(cor_rate, 1) AS tariff_price_RUR,                               -- Сумма по тарифу
    bonus_spent_by_client_org * COALESCE(cor_rate, 1) AS bonus_spent_by_client_RUR,                      -- Бонусы клиентов
    partner_comission_org * COALESCE(cor_rate, 1) AS partner_comission_RUR,                          -- Комиссия партнера
    partner_price_org * COALESCE(cor_rate, 1) AS partner_price_RUR                               -- Стоимость для партнера
  FROM original_currency_data ocd
  LEFT JOIN `dwh-wazzup`.`dbt_nbespalov`.`stg_exchangeRates`  er ON ocd.occured_date = er.data AND ocd.currency = er.currency AND nominal = 'RUR'

)


SELECT * 
FROM original_currency_and_rur_data