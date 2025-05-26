select          -- Таблица с изменениями по типу аккаунта
    accountId as account_id,            -- Идентификатор аккаунта, соответствует id из таблицы stg_accounts
    first_value(occured_at) over  (partition by accountId order by occured_at asc) as first_value_occured_at,       -- Дата и время первого изменения типа аккаунта
    first_value(oldType) over (partition by accountId order by occured_at asc) as first_value_type,                 -- Первый тип аккаунта
    occured_at as start_occured_at,                     -- Дата и время изменения типа аккаунта
    newType as type,                                    -- Новый тип аккаунта
    coalesce(lead(datetime(occured_at)) over (partition by accountId order by occured_at asc), datetime(current_timestamp, 'Europe/Moscow')) as end_occured_at      -- Дата и время , до которого был текущий тип аккаунта
from  `dwh-wazzup`.`wazzup`.`analytic_events`
where event_type=0