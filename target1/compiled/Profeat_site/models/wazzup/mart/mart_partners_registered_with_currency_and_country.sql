SELECT DISTINCT
  account_id           AS partner_id,                                                 -- Номер аккаунта партнера 
  russian_country_name AS russianName,                                                -- Страна
  currency,                                                                           -- Валюта
  partner_register_date                                                               -- Дата регистрации партнера
FROM `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info`
WHERE type IN ('partner', 'standart')                                                 -- Берем только оф. партнеров
  AND is_employee is false
ORDER BY 4