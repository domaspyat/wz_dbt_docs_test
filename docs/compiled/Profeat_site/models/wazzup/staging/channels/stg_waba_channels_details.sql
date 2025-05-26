select          -- Вспомогательная таблица, которая хранит данные специфичные для ваба-каналов
    channelId as channel_id,        -- guid канала, соответствует guid из stg_channels
    wabaid as waba_id,              -- id waba
    fbAppDbHost fb_app_db_host,     -- Служебное поле в котором храним маркер в какой БД хранится информация по приложению ФБ
    disabledAt as disabled_at,      -- Дата и время отключения канала
    tier                            -- Тир канала
from `dwh-wazzup`.`wazzup`.`wabaChannelsDetails`