select  -- Таблица платежей в ЛК. Только методы bank, paypal и провайдеры alphabank, planfix
    account_id,                         -- Идентификатор аккаунта, соответствует id из таблицы stg_accounts
    paid_date,                          -- Дата создания события
    currency,                           -- Валюта
    sum_in_rubles,                      -- Сумма оплаты в рублях. До 12 знаков до запятой, 2 после.
    original_sum,                       -- Сумма оплаты. До 12 знаков до запятой, 2 после.
    guid                                -- Содержит guid или null. Подробнее в wazzup_staging_payments.yml
    from `dwh-wazzup`.`dbt_nbespalov`.`stg_payments_bank`
    where object = 'payment'
    and method in ('bank', 'paypal')
    and json_value(details, "$.provider") in ('alphabank','planfix')