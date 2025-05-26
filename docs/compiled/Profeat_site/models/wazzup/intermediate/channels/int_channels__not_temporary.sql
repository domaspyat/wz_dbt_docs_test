select     -- Таблица c невременными каналами
    account_id,     -- ID аккаунта
    guid,           -- Идентификатор канала
    created_at,     -- Дата и время создания канала
    state,          -- Состояние канала
    created_date,   -- Дата создания канала
    date_trunc(cast(created_at as date),week(monday)) as created_week,  -- Неделя создания канала
    case when transport = 'wapi' then 'waba'
         else transport 
    end as transport,   -- Тип канала, соответствующий мессенджеру
    package_Id,         -- ID подписки
    first_value(guid) over (partition by account_id, transport order by created_at) as first_guid,  -- Первый идентификатор канала по аккаунту и транспорту
    (case
        when guid = first_value(guid) over (partition by account_id, transport order by created_at) then True
        else False
        end) as is_new_channel, -- Это новый канал?
    deleted,    -- Канал удален?
    tariff      -- Тариф подписки у канала
    from `dwh-wazzup`.`dbt_nbespalov`.`stg_channels`
    where temporary=False