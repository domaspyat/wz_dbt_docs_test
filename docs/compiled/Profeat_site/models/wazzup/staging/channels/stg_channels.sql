select         -- Таблица каналов
    accountId as account_id,        -- Идентификатор аккаунта, соответствует id из таблицы stg_accounts.
    guid,                           -- Идентификатор канала.Генерируется Postgress при создании записи
    phone,                          -- Номер телефона/username/Идентификатор группы vk
    cast(datetime(createdAt,'Europe/Moscow') as timestamp) as created_at,       -- Дата и время создания канала
    state,                          -- Статус канала
    cast(datetime(createdAt,'Europe/Moscow') as date) as created_date,          -- Дата создания канала
    date_trunc(cast(createdAt as date),week(monday)) created_week,              -- Неделя создания канала (начало недели = понедельник)
    (case when cast(json_value(details,'$.isGupshupWaba') as bool) then True    
    else False
    end) as is_gupshup_waba,                                                    -- Флаг обозначающий что Waba канал относится к провайдеру Gupshup
    concat(guid,'-',(case when cast(json_value(details,'$.isGupshupWaba') as bool) then True
    else False
    end)) as guid_gupshup,                                                      -- Строка объединяющая guid и флаг Gupshup
    transport,                      -- Тип канала, соответствующий мессенджеру
    temporary,                      -- Признак, который говорит о том, что канал временный
    deleted,                        -- Признак, который говорит о том что канал был удален
    packageid as package_Id,        -- Индентификатор подписки
    tariff                          -- Текущий тариф канала
from `dwh-wazzup`.`wazzup`.`channels`