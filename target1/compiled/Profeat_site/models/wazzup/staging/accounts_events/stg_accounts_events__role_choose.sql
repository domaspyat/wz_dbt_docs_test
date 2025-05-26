SELECT 
    accountId AS account_id     -- Идентификатор аккаунта, соответствует id из таблицы stg_accounts
    , crm                       -- Выбранная CRM на этапе онбординга
    , role                      -- Роль пользователя
FROM 
    `dwh-wazzup`.`wazzup`.`analytic_events`  -- Указываем источник данных
WHERE 
    event_type = 3;          -- Фильтруем события по типу (в данном случае онбординг)