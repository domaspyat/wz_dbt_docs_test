select          -- Таблица с отображением истории изменений каналов
    dateTime as occured_at,         -- Дата создания изменения в базе данных
    state,                          -- Статус канала после изменения
    channelId as channel_id,        -- Идентификатор канала, у которого произошло изменение. Соответствует guid из stg_channels.
    packageId as package_id,        -- Идентификатор подписки канала, у которого произошло изменение. Соответствует guid из stg_billingPackages
    id,                             -- Идентификатор изменения.Генерируется Postgress при создании записи
    deleted,                        -- Если канал удалили в рамках текущего изменения,то True, иначе False
    visible                         -- Если канал виден в ЛК в рамках текущего изменения, то True, иначе False
from `dwh-wazzup`.`wazzup`.`channelHistory`