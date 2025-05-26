SELECT          -- Таблица с количеством уникальных диалогов
    channelId as channel_id,        -- Идентификатор канала, соответсвует guid из stg_channels
    _ibk as date,                   -- Дата диалогов
    uniqueChats as unique_chats     -- Количество уникальных диалогов
FROM `dwh-wazzup`.`wazzup`.`uniqueChats_aggregated`