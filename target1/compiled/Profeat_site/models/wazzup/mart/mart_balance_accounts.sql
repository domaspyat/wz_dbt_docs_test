WITH source_data as (
  SELECT            -- Таблица всех транзакций 
    CASE WHEN ba.account_id = 28266449 AND currency = 'USD' THEN 60569941 ELSE ba.account_id END AS account_id,     -- Id аккаунта
    CASE WHEN ba.account_id = 28266449 THEN 'RUR'
    ELSE first_value(currency) OVER (PARTITION BY ba.account_id, DATE_TRUNC(occured_at, month) ORDER BY occured_at desc) END AS last_currency,     -- Последняя актуальная валюта в рамках аккаунта и месяца
    occured_at,                                     -- Время транзакции
    CAST(occured_at as date) as paid_date,      -- Дата транзакции
    CAST(DATE_TRUNC(occured_at, month) as date) as paid_month,    -- Месяц транзакции
    original_sum AS sum_org,                        -- Сумма транзакции в оригинальной валюте
    currency,                                       -- Валюта транзакции
  FROM `dwh-wazzup`.`dbt_nbespalov`.`stg_billingAffiliate` ba
  LEFT JOIN `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_type_and_partner_change_with_partner_type_deduplicated` tch ON tch.account_id = ba.account_id 
                                                                                          AND CAST(occured_at as date) >= tch.start_date
                                                                                          AND CAST(occured_at as date) < tch.end_date
  WHERE tch.account_type != 'employee'
),

source_data_current as (      -- Поиск общей суммы за месяц в рамках валюты для аккаунта
  SELECT 
    account_id,                                     -- Id аккаунта
    paid_month as current_month,                    -- Месяц для определения балансов
    last_currency as last_currency_current_month,   -- Последняя актуальная валюта в рамках аккаунта и текущего месяца
    FIRST_VALUE(paid_month) OVER (PARTITION BY account_id ORDER BY paid_month ASC) as first_month, -- Первый месяц активности
  FROM source_data
  GROUP BY 1, 2, 3
),

balance_current_agg as (        -- Определение баланса на каждый месяц в последней валюте месяца
  SELECT 
    sdc.account_id,                 -- Id аккаунта
    first_month,                    -- Первый месяц активности
    current_month,                  -- Месяц для определения балансов
    last_currency_current_month,    -- Последняя актуальная валюта в рамках аккаунта и текущего месяца
    COALESCE(lead(current_month) OVER (PARTITION BY sdc.account_id ORDER BY current_month asc), DATE_ADD(DATE_TRUNC(CURRENT_DATE(), month), INTERVAL 1 month)) as next_month,
    -- sd.paid_month,
    sum(sd.sum_org) as sum_current_org     -- Сумма на балансе на начало месяца
  FROM source_data_current sdc
  JOIN source_data sd ON sd.account_id = sdc.account_id AND sd.currency = sdc.last_currency_current_month AND sd.paid_month <= sdc.current_month
  GROUP BY 1, 2, 3, 4
),

balance_all_month as (        -- Добавление месяцев для непрерывности метрики
  SELECT 
    account_id,                           -- Id аккаунта
    last_currency_current_month,   -- Последняя актуальная валюта в рамках аккаунта и текущего месяца
    sum_current_org,     -- Сумма на балансе на начало месяца
    m.month as current_month  -- Месяц для определения балансов
  FROM balance_current_agg
  JOIN `dwh-wazzup`.`dbt_nbespalov`.`stg_months` m ON  m.month >= current_month AND m.month < next_month
),

balance_rur_currency_is_employee as (     -- Исключение раочих аккаунтов и перевод баланса в рубли
  SELECT 
    bam.account_id,                                                         -- Id аккаунта
    -- tch.account_type,                                                       -- Тип аккаунта
    current_month,                                                          -- Месяц для определения балансов
    last_currency_current_month,                                            -- Последняя актуальная валюта в рамках аккаунта и текущего месяца
    sum_current_org,                                                        -- Сумма на балансе на начало месяца в валюте
    sum_current_org * COALESCE(cor_rate, 1) as sum_current_rur,             -- Сумма на балансе на начало месяца в рублях
  FROM balance_all_month bam
  LEFT JOIN `dwh-wazzup`.`dbt_nbespalov`.`stg_exchangeRates` er ON er._ibk = current_month AND last_currency_current_month = er.currency AND nominal = 'RUR' 
  JOIN `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info` api ON api.account_Id = bam.account_id and is_employee is false 

),

balance_previous_month as (
  SELECT 
    account_id,                     -- Id аккаунта
    -- account_type,                   -- Тип аккаунта
    current_month,                  -- Месяц для определения балансов
    last_currency_current_month,    -- Последняя актуальная валюта в рамках аккаунта и текущего месяца
    sum_current_org,                -- Сумма на балансе на начало текущего месяца в валюте
    sum_current_rur,                -- Сумма на балансе на начало текущего месяца в рублях
    LAG(last_currency_current_month) OVER (PARTITION BY account_id ORDER BY current_month) as last_currency_previous_month,    -- Последняя актуальная валюта в рамках аккаунта и предыдущего месяца
    LAG(sum_current_org) OVER (PARTITION BY account_id ORDER BY current_month) as sum_previous_org,     -- Сумма на балансе на начало предыдущего месяца в валюте
    LAG(sum_current_rur) OVER (PARTITION BY account_id ORDER BY current_month) as sum_previous_rur,     -- Сумма на балансе на начало предыдущего месяца в рублях
  FROM balance_rur_currency_is_employee
),

final_table as (
  SELECT 
    bpm.account_id,                         -- Id аккаунта
    -- account_type as ac_type,
    COALESCE(account_type, LEAD(account_type) OVER (PARTITION BY bpm.account_id ORDER BY current_month), 'standart') as account_type,                       -- Тип аккаунта
    -- При использовании модели int_accounts_type_and_partner_change_with_partner_type_deduplicated в некоторых транзакциях не определется тип аккаунта в current_month
    -- При этом на следующий месяц подавляющее количество из этих транзакций определяет тип аккаунта
    -- Для текущего месяца (месяц совпадает с реальным месяцем), чтобы определить тип аккаунта поставлено 'standart', так как почти все эти транзакции потом перетекают в 'standart' 
    current_month,                      -- Месяц для определения балансов
    last_currency_current_month,        -- Последняя актуальная валюта в рамках аккаунта и текущего месяца
    ROUND(sum_current_org) as sum_current_org,                    -- Сумма на балансе на начало текущего месяца в валюте
    ROUND(sum_current_rur) as sum_current_rur,                    -- Сумма на балансе на начало текущего месяца в рублях
    last_currency_previous_month,       -- Последняя актуальная валюта в рамках аккаунта и предыдущего месяца
    ROUND(sum_previous_org) as sum_previous_org,                   -- Сумма на балансе на начало предыдущего месяца в валюте
    ROUND(sum_previous_rur) as sum_previous_rur,                   -- Сумма на балансе на начало предыдущего месяца в рублях
  FROM balance_previous_month bpm
    LEFT JOIN `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_type_and_partner_change_with_partner_type_deduplicated` tch ON tch.account_id = bpm.account_id 
                                                                                          AND current_month >= tch.start_date
                                                                                          AND current_month < tch.end_date
--   WHERE tch.account_type in ('standart', 'partner', 'tech-partner')
  ORDER BY current_month
  
)

SELECT *
FROM final_table