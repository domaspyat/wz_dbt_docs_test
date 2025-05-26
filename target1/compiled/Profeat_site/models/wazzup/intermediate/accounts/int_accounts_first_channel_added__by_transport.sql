select  account_id,                        -- ID аккаунта
    transport,                             -- Транспорт канала
    min(created_at) as channel_created_at  -- Дата и время создания канала
    from `dwh-wazzup`.`dbt_nbespalov`.`int_channels__not_temporary`
    group by 1,2
        -- Таблица с первым добавленным каналом на аккаунте по транспорту