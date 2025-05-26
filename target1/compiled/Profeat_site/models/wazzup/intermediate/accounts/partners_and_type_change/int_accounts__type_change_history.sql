-- Таблица историй изменений типов аккаунтов

with type_change as (           -- Берем данные изменений по типу аккаунтов
    select 
        account_id,                         -- Идентификатор аккаунта, соответствует id из таблицы stg_accounts 
        type,                               -- Новый тип аккаунта
        start_occured_at,                   -- Дата и время изменения типа аккаунта
        end_occured_at                      -- Дата и время , до которого был текущий тип аккаунта
    from `dwh-wazzup`.`dbt_nbespalov`.`stg_accounts_events__type_change`
),

type_change_first_values as (           -- Берем данные с датой регистрации аккаунта, первым типом и датой окончания первого типа 
    select 
        account_id,                                 -- Идентификатор аккаунта, соответствует id из таблицы stg_accounts 
        type,                                       -- Первый тип аккаунта
        start_occured_at,                           -- Дата и время регистрации аккаунта, начало действия первого типа
        end_occured_at                              -- Дата и время первого изменения типа аккаунта, если null, то текущее время
    from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_registration_data__first_type`
),

type_change_history as (            -- Таблица историй изменений типов аккаунтов 
    select 
        account_id,                                             -- Идентификатор аккаунта, соответствует id из таблицы stg_accounts 
        type,                                                   -- Тип аккаунта
        datetime(start_occured_at) as start_occured_at,         -- Дата и время регистрации аккаунта, начало действия первого типа
        cast(start_occured_at as date) as start_date,           -- Дата регистрации аккаунта, начало действия первого типа
        datetime(end_occured_at) as end_occured_at,             -- Дата и время первого изменения типа аккаунта, если null, то текущее время
        cast(end_occured_at as date) as end_date                -- Дата первого изменения типа аккаунта, если null, то текущая дата
    from type_change_first_values

    UNION ALL 

    select 
        account_id,                                         -- Идентификатор аккаунта, соответствует id из таблицы stg_accounts 
        type,                                               -- Тип аккаунта
        datetime(start_occured_at) as start_occured_at,     -- Дата и время начала действия данного типа аккаунта
        cast(start_occured_at as date) as start_date,       -- Дата начала действия данного типа аккаунта
        datetime(end_occured_at) as end_occured_at,         -- Дата и время окончания действия данного типа аккаунта
        cast(end_occured_at as date) as end_date            -- Дата окончания действия данного типа аккаунта
    from type_change
)
    
select * from type_change_history