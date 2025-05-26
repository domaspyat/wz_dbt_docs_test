select  -- Таблица с движениями средств в ЛК в случае object = 'payment' и method = '('bank','card','paypal')
    occured_date,   -- Дата события
    account_id,     -- ID аккаунта
    currency,       -- Валюта
    sum             -- Сумма события
  from `dwh-wazzup`.`dbt_nbespalov`.`stg_billingAffiliate`
  where object='payment' and method in ('bank','card','paypal')