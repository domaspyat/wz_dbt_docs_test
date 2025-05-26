select  -- Таблица платежей постоплатников
account_id,        -- ID аккаунта
PARSE_DATE('%d.%m.%Y', month) as paid_date, -- Дата оплаты
currency,                       -- Валюта
COALESCE(CAST(REPLACE(bill_sum, ' ', '') AS int), 0) as sum_in_rubles,           -- Сумма оплаты в рублях
COALESCE(CAST(REPLACE(bill_sum, ' ', '') AS int), 0) as original_sum             -- Сумма оплаты
from `dwh-wazzup`.`google_sheets`.`postpay_paying`