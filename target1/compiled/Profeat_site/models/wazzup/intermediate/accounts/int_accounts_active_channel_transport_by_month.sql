-- Таблица показывает какие платные транспорты активны у аккаунта помесячно
SELECT DISTINCT
    DATE_TRUNC(date, month) AS month,     -- Месяц
    transport,                            -- Транспорт
    account_id                            -- ID аккаунта
FROM `dwh-wazzup`.`dbt_nbespalov`.`int_channels_active_channels`
WHERE is_free is distinct from true     -- Берем только платные каналы
    AND package_id is not null
ORDER BY 1, 3