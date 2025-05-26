select          -- Таблица аккаунтов (личных кабинетов)
    id as account_id,               -- Номер аккаунта. Генерируется рандомно при регистрации.
    (case when id=83124875 then  DATETIME('2024-01-16','Europe/Moscow')
    else 
    DATETIME(registerAt,'Europe/Moscow') 
    end)
     as register_at,                -- Дата и время регистрации аккаунта (83124875 - аккаунт исключение)
     (case when id=83124875 then  cast(DATETIME('2024-01-16','Europe/Moscow') as date)
    else 
     cast(DATETIME(registerAt,'Europe/Moscow') as date)
    end)
    as register_date,       -- Дата регистрации аккаунта (у аккаунта 83124875 оплата безналом по банку была До даты регистрации. Волевым решением изменила на день до первого платежа)
    country,                -- Страна клиента определяется при регистрации по локации (ip)
    regEmail,               -- Email, указанный при регистрации аккаунта. Может быть изменен в настройках Личного кабинета
    activated AS is_activated_by_email,
    currency,               -- Валюта аккаунта. Ставится при регистрации в зависимости от локали пользователя. RUR - рубли, USD - доллары, EUR - евро, KZT - тенге
    details,                -- Данные по аккаунту. Описание в "wazzup_staging.yml"
    name as account_name,   -- Имя указанное при регистрации.
    timeZone as time_zone,  -- Часовой пояс Выставляется относительно времени устройства, с которого регистрируется пользователь. Часовой пояс может быть изменен в настройках Личного кабинета
    json_value(details,'$.registrationInfo.ymId') as yandex_id,             -- Метрика Яндекса, заполняемая при регистрации
    
    coalesce((case when JSON_VALUE(details,'$.registrationInfo.url') LIKE '%yclid=%' then split(SPLIT(JSON_VALUE(details,'$.registrationInfo.url'),'yclid=')[OFFSET(1)],'&')[OFFSET(0)] end),
    (case when JSON_VALUE(details,'$.registrationInfo.referer') LIKE '%yclid=%' then split(SPLIT(JSON_VALUE(details,'$.registrationInfo.referer'),'yclid=')[OFFSET(1)],'&')[OFFSET(0)] end))
    as yclid,                  -- Извлечение yclid из URL, по которому зарегистрировался клиент

    json_value(details,'$.registrationInfo.referer') as referrer,           -- Сторонний сайт, с которого пользователь попал на наш сайт. Может быть null
    json_value(details,'$.registrationInfo.url') as ref,                    -- Реферальная ссылка по которой был зарегистрирован аккаунт
    JSON_VALUE(details, '$.discount') as discount,                          -- Скидка для оплаты. 0.35 и 0.5 у партнеров
    json_value(details,'$.registrationInfo.location.city') as city,         -- Название города, в котором находится ip
    json_value(details,'$.registrationInfo.location.region') as region,     -- Название региона, в котором находится ip
    lang as account_language,                                               -- Язык личного кабинета. Меняется в настройках ЛК.
    coalesce((case when JSON_VALUE(details,'$.registrationInfo.url') LIKE '%gclid=%' then split(SPLIT(JSON_VALUE(details,'$.registrationInfo.url'),'gclid=')[OFFSET(1)],'&')[OFFSET(0)] end),
    (case when JSON_VALUE(details,'$.registrationInfo.referer') LIKE '%gclid=%' then split(SPLIT(JSON_VALUE(details,'$.registrationInfo.referer'),'gclid=')[OFFSET(1)],'&')[OFFSET(0)] end))
    as gclid,                  -- Извлечение gclid из URL, по которому зарегистрировался клиент
    coalesce((case when JSON_VALUE(details,'$.registrationInfo.url') LIKE '%utm_source=%' then split(SPLIT(JSON_VALUE(details,'$.registrationInfo.url'),'utm_source=')[OFFSET(1)],'&')[OFFSET(0)] end),
    (case when JSON_VALUE(details,'$.registrationInfo.referer') LIKE '%utm_source=%' then split(SPLIT(JSON_VALUE(details,'$.registrationInfo.referer'),'utm_source=')[OFFSET(1)],'&')[OFFSET(0)] end))
    as utm_source,             -- Извлечение UTM source из URL, по которому зарегистрировался клиент
    coalesce((case when JSON_VALUE(details,'$.registrationInfo.url') LIKE '%utm_medium=%' then split(SPLIT(JSON_VALUE(details,'$.registrationInfo.url'),'utm_medium=')[OFFSET(1)],'&')[OFFSET(0)] end),
    (case when JSON_VALUE(details,'$.registrationInfo.referer') LIKE '%utm_medium=%' then split(SPLIT(JSON_VALUE(details,'$.registrationInfo.referer'),'utm_medium=')[OFFSET(1)],'&')[OFFSET(0)] end))
    as utm_medium,             -- Извлечение UTM medium из URL, по которому зарегистрировался клиент
   
   coalesce( (CASE WHEN json_value(details, '$.registrationInfo.url') LIKE '%utm_campaign=%' AND json_value(details, '$.registrationInfo.url') LIKE '%utm_campaign=%D0%' THEN REGEXP_EXTRACT(split(split(json_value(details, '$.registrationInfo.url'), 'utm_campaign=')[OFFSET(1)], '&')[OFFSET (0)], r'\|(.*)') END),
                 (CASE WHEN json_value(details, '$.registrationInfo.url') LIKE '%utm_campaign=%' THEN split( split(json_value(details, '$.registrationInfo.url'), 'utm_campaign=')[OFFSET(1)], '&')[OFFSET (0)] END),
                 (CASE WHEN json_value(details, '$.registrationInfo.referer') LIKE '%utm_campaign=%' AND json_value(details, '$.registrationInfo.referer') LIKE '%utm_campaign=%D0%'  THEN REGEXP_EXTRACT(split(split(json_value(details, '$.registrationInfo.referer'), 'utm_campaign=')[OFFSET(1)],'&')[OFFSET (0)],r'\|(.*)' ) END),
                 (CASE WHEN json_value(details, '$.registrationInfo.referer') LIKE '%utm_campaign=%' THEN split(split(json_value(details, '$.registrationInfo.referer'), 'utm_campaign=')[OFFSET(1)],'&')[OFFSET (0)] END))
       AS utm_campaign,
       
    coalesce((case when JSON_VALUE(details,'$.registrationInfo.url') LIKE '%utm_term=%' then split(SPLIT(JSON_VALUE(details,'$.registrationInfo.url'),'utm_term=')[OFFSET(1)],'&')[OFFSET(0)] end),
    (case when JSON_VALUE(details,'$.registrationInfo.referer') LIKE '%utm_term=%' then split(SPLIT(JSON_VALUE(details,'$.registrationInfo.referer'),'utm_term=')[OFFSET(1)],'&')[OFFSET(0)] end))
    as utm_term,               -- Извлечение UTM term из URL, по которому зарегистрировался клиент
    coalesce((case when JSON_VALUE(details,'$.registrationInfo.url') LIKE '%utm_content=%' then split(SPLIT(JSON_VALUE(details,'$.registrationInfo.url'),'utm_content=')[OFFSET(1)],'&')[OFFSET(0)] end),
    (case when JSON_VALUE(details,'$.registrationInfo.referer') LIKE '%utm_content=%' then split(SPLIT(JSON_VALUE(details,'$.registrationInfo.referer'),'utm_content=')[OFFSET(1)],'&')[OFFSET(0)] end))
    as utm_content,            -- Извлечение UTM content из URL, по которому зарегистрировался клиент
    cast(json_extract(details, '$.demoAccountId') as INTEGER )as demo_account,          -- Демо-аккаунта у партнера. Создается автоматически
    type,                   -- Тип аккаунта
    case when country in ('AM', 'AZ', 'BY', 'KZ', 'KG', 'MD', 'TJ', 'UZ', 'UA','RU') then 'CIS'     -- CIS - это страны СНГ
         when country is null then 'Неизвестно'
         else 'non-CIS' end as region_type,                     -- Тип региона. Когда null, то 'Неизвестно'.
    DATETIME(TIMESTAMP_SECONDS(cast(cast(JSON_VALUE(features,'$.whatsapp.createdAt') as BIGINT)/1000 as INTEGER)) ,'Europe/Moscow')  as whatsap_trial_start,    -- Дата и время начала триала Whatsapp-канала
    DATETIME(TIMESTAMP_SECONDS(cast(cast(JSON_VALUE(features,'$.whatsapp.endOfTrial') as BIGINT)/1000 as INTEGER)) ,'Europe/Moscow')  as whatsap_trial,         -- Дата и время окончания триала Whatsapp-канала
    DATETIME(TIMESTAMP_SECONDS(cast(cast(JSON_VALUE(features,'$.instagram.createdAt') as BIGINT)/1000 as INTEGER)),'Europe/Moscow') as instagram_trial_start,   -- Дата и время начала триала инстаграм-канала
    DATETIME(TIMESTAMP_SECONDS(cast(cast(JSON_VALUE(features,'$.instagram.endOfTrial') as BIGINT)/1000 as INTEGER)),'Europe/Moscow') as instagram_trial,        -- Дата и время окончания триала инстаграм-канала
    DATETIME(TIMESTAMP_SECONDS(cast(cast(JSON_VALUE(features,'$.tgapi.createdAt') as BIGINT)/1000 as INTEGER))    ,'Europe/Moscow') as tgapi_trial_start,       -- Дата и время начала триала телеграм-канала
    DATETIME(TIMESTAMP_SECONDS(cast(cast(JSON_VALUE(features,'$.tgapi.endOfTrial') as BIGINT)/1000 as INTEGER))    ,'Europe/Moscow') as tgapi_trial,            -- Дата и время окончания триала телеграм-канала
    DATETIME(TIMESTAMP_SECONDS(cast(cast(JSON_VALUE(features,'$.wapi.createdAt') as BIGINT)/1000 as INTEGER))     ,'Europe/Moscow') as wapi_trial_start,        -- Дата и время начала триала ВАБА-канала
    DATETIME(TIMESTAMP_SECONDS(cast(cast(JSON_VALUE(features,'$.wapi.endOfTrial') as BIGINT)/1000 as INTEGER))     ,'Europe/Moscow') as wapi_trial,             -- Дата и время окончания триала ВАБА-канала (Устарело)
    DATETIME(TIMESTAMP_SECONDS(cast(cast(JSON_VALUE(features,'$.avito.createdAt') as BIGINT)/1000 as INTEGER))    ,'Europe/Moscow') as avito_trial_start,       -- Дата и время начала триала авито-канала
    DATETIME(TIMESTAMP_SECONDS(cast(cast(JSON_VALUE(features,'$.avito.endOfTrial') as BIGINT)/1000 as INTEGER))    ,'Europe/Moscow') as avito_trial,            -- Дата и время окончания триала авито-канала
    DATETIME(TIMESTAMP_SECONDS(cast(cast(JSON_VALUE(features,'$.vk.createdAt') as BIGINT)/1000 as INTEGER))       ,'Europe/Moscow') as vk_trial_start,          -- Дата и время начала триала вк-канала
    DATETIME(TIMESTAMP_SECONDS(cast(cast(JSON_VALUE(features,'$.vk.endOfTrial') as BIGINT)/1000 as INTEGER))       ,'Europe/Moscow') as vk_trial,               -- Дата и время окончания триала вк-канала
    DATETIME(TIMESTAMP_SECONDS(cast(cast(JSON_VALUE(features,'$.telegram.createdAt') as BIGINT)/1000 as INTEGER)) ,'Europe/Moscow') as telegram_trial_start,    -- Дата и время начала триала Телеграм-Бот -канала
    DATETIME(TIMESTAMP_SECONDS(cast(cast(JSON_VALUE(features,'$.telegram.endOfTrial') as BIGINT)/1000 as INTEGER)) ,'Europe/Moscow') as telegram_trial,          -- Дата и время окончания триала Телеграм-Бот -канала
    DATETIME(TIMESTAMP_SECONDS(cast(cast(JSON_VALUE(features,'$.viber.createdAt') as BIGINT)/1000 as INTEGER)) ,'Europe/Moscow') as viber_trial_start,          -- Дата и время начала триала viber -канала
    DATETIME(TIMESTAMP_SECONDS(cast(cast(JSON_VALUE(features,'$.viber.endOfTrial') as BIGINT)/1000 as INTEGER)) ,'Europe/Moscow') as viber_trial                -- Дата и время окончания триала viber -канала
from `dwh-wazzup`.`wazzup`.`accounts`