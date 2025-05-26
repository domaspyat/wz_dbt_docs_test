select          -- Таблица истории баланса средств в ЛК
    accountId as account_id,            -- Идентификатор аккаунта, соответствует id из таблицы stg_accounts
    _ibk as paid_date,                  -- Дата создания события
    currency,                           -- Валюта
    sumInRubles as sum_in_rubles,       -- Сумма оплаты в рублях. До 12 знаков до запятой, 2 после.
    sum as original_sum,                -- Сумма оплаты. До 12 знаков до запятой, 2 после.
    guid,                               -- Содержит guid или null. Подробнее в wazzup_staging_payments.yml
    object,                             -- Тип транзакции
    method,                             -- Cпособ проведение платежа
    details                             -- JSON с различными полями. Подробнее в wazzup_staging_payments.yml
from `dwh-wazzup`.`wazzup`.`billingAffiliate`