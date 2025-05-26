WITH subs_calculations_client AS (  -- Таблица с тратами клиента на подписку WABA в кейсах, когда клиент оплачивал WABA+подписку одним платежом
  SELECT
  
    tch.account_id AS client_id,                                                                  -- ID клиента
    tch.account_type,                                                                             -- Тип аккаунта
    CASE WHEN ipb.guid IS NOT NULL THEN ipb.currency ELSE su.currency END as currency,            -- Валюта транзакции
    CASE WHEN ipb.guid IS NOT NULL THEN paid_date ELSE su.created_date END as occured_date,       -- Дата транзакции

    SUM(su.sum - wapi_transactions)        AS subs_sum_client,       -- Сумма, потраченная на подписки
    SUM(CASE WHEN ba.sum >= (su.sum - wapi_transactions) THEN (su.sum - wapi_transactions) 
             WHEN activation_object = 'partnerBalance' THEN (su.sum - wapi_transactions)
             WHEN ba.sum > 0 AND activation_object != 'partnerBalance' AND su.state = 'activated' THEN ba.sum 
             ELSE ba.sum END) AS bonuses_spent_on_subs, -- Бонусы, потраченные на подписки
  FROM `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_type_and_partner_change_with_partner_type_deduplicated` tch
  JOIN `dwh-wazzup`.`dbt_nbespalov`.`stg_billingPackages` bp ON tch.account_id = bp.account_id
  JOIN `dwh-wazzup`.`dbt_nbespalov`.`stg_subscriptionUpdates` su ON su.subscription_id = bp.guid AND su.created_at BETWEEN tch.start_occured_at AND tch.end_occured_at
  LEFT JOIN `dwh-wazzup`.`dbt_nbespalov`.`int_payments_bills` ipb ON su.activation_reason_id = ipb.guid
  LEFT JOIN `dwh-wazzup`.`dbt_nbespalov`.`stg_billingAffiliate` ba ON ba.subscription_update_id = su.guid
  
    WHERE tch.account_type = 'standart'                              -- Был конечным клиентом, когда платил
    --AND (tch.partner_type IN ('standart', 'partner') OR tch.partner_type IS NULL)
    AND su.action != 'balanceTopup'                                  -- Исключаем транзакции, которые являются только пополнением WABA
    AND wapi_transactions > 0                                        -- Сумма транзакций WABA больше нуля
    AND su.state = 'activated'                                       -- Только оплаченные изменения
    AND su.partner_discount IS NULL                                  -- Исключаем кейсы, когда платил партнер
  GROUP BY 1, 2, 3, 4
),


waba_calculations_client AS (  -- Таблица с тратами клиента на баланс WABA в кейсах, когда клиент оплачивал WABA+подписку одним платежом
  SELECT
    tch.account_id AS client_id,                                                                  -- ID клиента    
    tch.account_type,                                                                             -- Тип аккаунта
    CASE WHEN ipb.guid IS NOT NULL THEN ipb.currency ELSE su.currency END as currency,            -- Валюта транзакции
    CASE WHEN ipb.guid IS NOT NULL THEN paid_date ELSE su.created_date END as occured_date,       -- Дата транзакции 
    SUM(wapi_transactions)          AS waba_sum_client,           -- Сумма, потраченная на баланс WABA
    SUM(CASE WHEN ba.sum >= (su.sum - wapi_transactions) THEN (ba.sum - (su.sum - wapi_transactions)) 
             WHEN activation_object = 'partnerBalance' THEN wapi_transactions
             WHEN ba.sum > 0 AND activation_object != 'partnerBalance' AND su.state = 'activated' AND ba.sum >= (su.sum - wapi_transactions) THEN (ba.sum - (su.sum - wapi_transactions))
             WHEN ba.sum > 0 AND activation_object != 'partnerBalance' AND su.state = 'activated' AND activation_object = 'partnerBalance' THEN wapi_transactions
             ELSE 0 END)            AS bonuses_spent_on_waba,     -- Бонусы, потраченные на баланс WABA
  FROM `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_type_and_partner_change_with_partner_type_deduplicated` tch
  JOIN `dwh-wazzup`.`dbt_nbespalov`.`stg_billingPackages` bp ON tch.account_id = bp.account_id
  JOIN `dwh-wazzup`.`dbt_nbespalov`.`stg_subscriptionUpdates` su ON su.subscription_id = bp.guid AND su.created_at BETWEEN tch.start_occured_at AND tch.end_occured_at
  LEFT JOIN `dwh-wazzup`.`dbt_nbespalov`.`int_payments_bills` ipb ON su.activation_reason_id = ipb.guid
  LEFT JOIN `dwh-wazzup`.`dbt_nbespalov`.`stg_billingAffiliate` ba ON ba.subscription_update_id = su.guid

    WHERE tch.account_type = 'standart'                                   -- Был конечным клиентом, когда платил
    --AND (tch.partner_type IN ('standart', 'partner') OR tch.partner_type IS NULL)
    AND su.action != 'balanceTopup'                               -- Исключаем транзакции, которые являются только пополнением WABA
    AND wapi_transactions > 0                                     -- Сумма транзакций WABA больше нуля
    AND su.state = 'activated'                                    -- Только оплаченные изменения
    AND su.partner_discount IS NULL                               -- Исключаем кейсы, когда платил партнер
  GROUP BY 1, 2, 3, 4
),

client_subscriptions AS (  -- CTE с оплатой подписок клиентами
  SELECT
    tch.account_id AS client_id,                                                                -- ID клиента     
    tch.account_type,                                                                           -- Тип аккаунта
    CASE WHEN ipb.guid IS NOT NULL THEN ipb.currency ELSE su.currency END as currency,          -- Валюта транзакции
    CASE WHEN ipb.guid IS NOT NULL THEN paid_date ELSE su.created_date END as occured_date,     -- Дата транзакции

    SUM(su.sum)                                                                                   AS client_subscriptions_sum,  -- Сумма, потраченная на подписки
    SUM(CASE WHEN activation_object = 'partnerBalance' THEN su.sum 
             WHEN ba.sum > 0 AND activation_object != 'partnerBalance' AND su.state = 'activated' THEN ba.sum
             ELSE ba.sum END)  AS bonus_money_spent,          -- Бонусы, потраченные на подписки
  FROM `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_type_and_partner_change_with_partner_type_deduplicated` tch
  JOIN `dwh-wazzup`.`dbt_nbespalov`.`stg_billingPackages` bp ON tch.account_id = bp.account_id
  JOIN `dwh-wazzup`.`dbt_nbespalov`.`stg_subscriptionUpdates` su ON su.subscription_id = bp.guid AND su.created_at BETWEEN tch.start_occured_at AND tch.end_occured_at
  LEFT JOIN `dwh-wazzup`.`dbt_nbespalov`.`int_payments_bills` ipb ON su.activation_reason_id = ipb.guid
  LEFT JOIN `dwh-wazzup`.`dbt_nbespalov`.`stg_billingAffiliate` ba ON ba.subscription_update_id = su.guid

    WHERE tch.account_type = 'standart'                                                                                                 -- Был конечным клиентом, когда платил
    AND su.action != 'balanceTopup'                                                                                             -- Берем всё кроме пополнения баланса WWABA
    AND (wapi_transactions IS NULL OR wapi_transactions = 0)                                                                    -- Отсекаем расписанные выше траты
    AND su.state = 'activated'                                                                                                  -- Только оплаченные изменения
    AND su.partner_discount IS NULL                                                                                             -- Исключаем кейсы, когда платил партнер
  GROUP BY 1, 2, 3, 4
),
 

client_waba_balance AS (   -- CTE с оплатой баланса WABA клиентами
  SELECT
    tch.account_id AS client_id,                                                                -- ID клиента
    tch.account_type,                                                                           -- Тип аккаунта
    CASE WHEN ipb.guid IS NOT NULL THEN ipb.currency ELSE su.currency END as currency,          -- Валюта транзакции
    CASE WHEN ipb.guid IS NOT NULL THEN paid_date ELSE su.created_date END as occured_date,     -- Дата транзакции
    SUM(su.sum)                                                                                   AS client_waba_sum,  -- Сумма, потраченная на баланс WABA
    SUM(CASE WHEN activation_object = 'partnerBalance' THEN su.sum 
             WHEN ba.sum > 0 AND activation_object != 'partnerBalance' AND su.state = 'activated' THEN ba.sum
             ELSE ba.sum END)  AS bonus_money_spent,                                                       -- Бонусы, потраченные на баланс WABA
  FROM `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_type_and_partner_change_with_partner_type_deduplicated` tch
  JOIN `dwh-wazzup`.`dbt_nbespalov`.`stg_billingPackages` bp ON tch.account_id = bp.account_id
  JOIN `dwh-wazzup`.`dbt_nbespalov`.`stg_subscriptionUpdates` su ON su.subscription_id = bp.guid AND su.created_at BETWEEN tch.start_occured_at AND tch.end_occured_at
  LEFT JOIN `dwh-wazzup`.`dbt_nbespalov`.`int_payments_bills` ipb ON su.activation_reason_id = ipb.guid
  LEFT JOIN `dwh-wazzup`.`dbt_nbespalov`.`stg_billingAffiliate` ba ON ba.subscription_update_id = su.guid
  
    WHERE tch.account_type = 'standart'                                                                                -- Был конечным клиентом, когда платил
    AND su.action = 'balanceTopup'                                                                                     -- Берем только пополнения баланса WABA
    AND su.state = 'activated'                                                                                         -- Только оплаченные изменения
    AND su.partner_discount IS NULL                                                                                    -- Исключаем кейсы, когда платил партнер
  GROUP BY 1, 2, 3, 4
),

invalid_bills AS (          -- CTE с данными о том, сколько денег получил клиент с некорректных счетов
  SELECT
    ba.account_id     AS client_id,                   -- ID аккаунта
    tch.account_type,                                 -- Тип аккаунта
    currency,                                         -- Валюта транзакции
    occured_date,                                     -- Дата транзакции
    SUM(ba.sum)       AS invalid_bills_sum            -- Сумма некорректных счетов
  FROM `dwh-wazzup`.`dbt_nbespalov`.`stg_billingAffiliate` ba
  JOIN `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_type_and_partner_change_with_partner_type_deduplicated` tch ON DATETIME(ba.occured_at) BETWEEN tch.start_occured_at AND tch.end_occured_at AND tch.account_id = ba.account_id
    WHERE tch.account_type = 'standart'
    AND object = 'payment'                  
    AND is_invalid is true                            -- Некорректные счета
    AND ba.original_sum > 0                           -- Отсекаем траты (подписки и т.п)
  GROUP BY 1, 2, 3, 4
),


original_currency_data as (         -- Итоговые расчеты в оригинальной валюте
  SELECT
    client_id,              -- ID клиента
    account_type,           -- Тип аккаунта
    currency,               -- Валюта транзакции
    occured_date,           -- Дата транзакции

    COALESCE(subs_sum_client, 0) + COALESCE(client_subscriptions_sum, 0) 
    - (COALESCE(bonuses_spent_on_subs, 0) + COALESCE(cs.bonus_money_spent, 0))  AS subscriptions_sum_org,  -- Сумма всех стоимостей оплаченных подписок
    COALESCE(waba_sum_client, 0) + COALESCE(client_waba_sum, 0)
    - (COALESCE(bonuses_spent_on_waba, 0) + COALESCE(cw.bonus_money_spent, 0)) AS waba_sum_org,           -- Сумма всех стоимостей оплаченных WABA пополнений

    COALESCE(invalid_bills_sum, 0) as invalid_bills_sum_org            -- Сумма некорректных счетов

  FROM subs_calculations_client scc
  FULL OUTER JOIN waba_calculations_client wcc USING (client_id,account_type, currency, occured_date)
  FULL OUTER JOIN client_subscriptions cs USING (client_id,account_type, currency, occured_date)
  FULL OUTER JOIN client_waba_balance cw USING (client_id,account_type, currency, occured_date)
  FULL OUTER JOIN invalid_bills ib USING (client_id,account_type, currency, occured_date)

),

original_currency_total_data as (         -- Итоговые расчеты в оригинальной валюте
  SELECT
    client_id,              -- ID клиента
    account_type as type,           -- Тип аккаунта
    currency,               -- Валюта транзакции
    occured_date,           -- Дата транзакции

    subscriptions_sum_org,  -- Сумма всех стоимостей оплаченных подписок
    waba_sum_org,           -- Сумма всех стоимостей оплаченных WABA пополнений
    invalid_bills_sum_org,  -- Сумма некорректных счетов

    COALESCE(subscriptions_sum_org, 0) + COALESCE(waba_sum_org, 0) + COALESCE(invalid_bills_sum_org, 0) as total_org    -- Сумма всех оплат 

  FROM original_currency_data
),

original_currency_and_RUR_data as (         -- Итоговые расчеты в оригинальной валюте и рублях
  SELECT
    client_id,                      -- ID клиента
    ocd.type,                           -- Тип аккаунта
    ocd.currency,                   -- Валюта транзакции
    occured_date,                   -- Дата транзакции

    subscriptions_sum_org,          -- Сумма всех стоимостей оплаченных подписок в оригинальной валюте
    waba_sum_org,                   -- Сумма всех стоимостей оплаченных WABA пополнений в оригинальной валюте
    invalid_bills_sum_org,          -- Сумма некорректных счетов в оригинальной валюте
    total_org,                      -- Сумма всех оплат в оригинальной валюте
    
    subscriptions_sum_org * COALESCE(cor_rate, 1) as subscriptions_sum_RUR,             -- Сумма всех стоимостей оплаченных подписок в рублях
    waba_sum_org * COALESCE(cor_rate, 1) as waba_sum_RUR,                               -- Сумма всех стоимостей оплаченных WABA пополнений в рублях
    invalid_bills_sum_org * COALESCE(cor_rate, 1) as invalid_bills_sum_RUR,             -- Сумма некорректных счетов в рублях
    total_org * COALESCE(cor_rate, 1) as total_RUR                                      -- Сумма всех оплат в рублях

  FROM original_currency_total_data ocd
  LEFT JOIN `dwh-wazzup`.`dbt_nbespalov`.`stg_exchangeRates` er ON ocd.occured_date = er.data AND ocd.currency = er.currency AND nominal = 'RUR'
  JOIN `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info` api ON ocd.client_id = api.account_Id and is_employee is false
)


 
SELECT *
FROM original_currency_and_RUR_data