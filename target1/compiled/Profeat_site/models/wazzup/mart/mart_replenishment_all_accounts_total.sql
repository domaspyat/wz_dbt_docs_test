WITH partner_data as (
  SELECT
    partner_id as account_id,                           -- ID аккаунта
    type,                                               -- Тип аккаунта
    currency,                                           -- Валюта
    occured_date,                                       -- Дата оплаты
    real_money_invalid_bills_org as replenishment_org,  -- Сумма оплаты в оригинальной валюте
    real_money_invalid_bills_RUR as replenishment_RUR   -- Сумма оплаты в рублях
  FROM `dwh-wazzup`.`dbt_nbespalov`.`mart_replenishment_partner_accounts` 
),

standrt_data as (
  SELECT
    client_id as account_id,                            -- ID аккаунта
    type,                                               -- Тип аккаунта
    currency,                                           -- Валюта
    occured_date,                                       -- Дата оплаты
    total_org as replenishment_org,                     -- Сумма оплаты в оригинальной валюте
    total_RUR as replenishment_RUR                      -- Сумма оплаты в рублях
  FROM `dwh-wazzup`.`dbt_nbespalov`.`mart_replenishment_standart_accounts` 
),

postpay_data as (
  SELECT 
    Cast(account_id as int64) as account_id,            -- ID аккаунта
    'tech-partner-postpay' as type,                     -- Тип аккаунта
    currency,                                           -- Валюта
    paid_date as occured_date,                          -- Дата оплаты
    original_sum as replenishment_org,                  -- Сумма оплаты в оригинальной валюте
    sum_in_rubles as replenishment_RUR,                 -- Сумма оплаты в рублях
    
  FROM `dwh-wazzup`.`dbt_nbespalov`.`int_payments_postpay_revenue_bills`
  WHERE account_id is not null
),

final_table as (
  SELECT  
    account_id,                                         -- ID аккаунта
    type,                                               -- Тип аккаунта
    currency,                                           -- Валюта
    occured_date,                                       -- Дата оплаты
    replenishment_org,                                  -- Сумма оплаты в оригинальной валюте
    replenishment_RUR                                   -- Сумма оплаты в рублях
  FROM partner_data pd
  FULL OUTER JOIN standrt_data USING(account_id, type, currency, occured_date, replenishment_org, replenishment_RUR)
  FULL OUTER JOIN postpay_data USING(account_id, type, currency, occured_date, replenishment_org, replenishment_RUR)
) 

SELECT *
FROM final_table