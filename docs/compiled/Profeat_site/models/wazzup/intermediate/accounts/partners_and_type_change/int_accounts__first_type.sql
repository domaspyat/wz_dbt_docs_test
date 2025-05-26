select          -- Табица с первыми изменениями по типу каждого аккаунта
    first_value_occured_at as end_occured_at,       -- Дата и время первого изменения типа аккаунта
    first_value_type,                               -- Первый тип аккаунта
    account_id                                      -- Идентификатор аккаунта, соответствует id из таблицы stg_accounts
from `dwh-wazzup`.`dbt_nbespalov`.`stg_accounts_events__type_change`
group by 1,2,3