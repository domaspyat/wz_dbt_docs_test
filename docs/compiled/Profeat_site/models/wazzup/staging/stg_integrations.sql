select          -- Таблица с информацией по интеграциям. Храниятся вся история созданных интеграций
    guid as integration_id,         -- Идентификатор интеграции. Генерируется Postgres при создании записи
    accountid as account_id,        -- Идентификатор аккаунта, соответствует id из таблицы stg_accounts
    type as integration_type,       -- Тип интеграции
    state,                          -- Состояние интеграции. Подробнее в wazzup_staging.yml
    domain,                         -- Домен интеграции
    json_value(params,'$.marketplaceType') as marketplace_type,         -- Тип api_v3 интеграции из маркетплейса
    apiKey as api_key,              -- Api ключ для api_v3, megaplan, planfix интеграций, в остальных не используется
    scopeid as scope_id,            -- Старый scopeId для чатов, которые были созданы до раздельных источников, может использоваться старыми интеграциями
    disabledto as disabled_to,      -- Если интеграция в state 'paused', тут будет время, в которое её нужно вернуть в активное состояние
    datetime(cast(createdAt as TIMESTAMP),'Europe/Moscow') as created_at,           -- Дата и время создания интеграции
    cast(datetime(cast(createdAt as TIMESTAMP),'Europe/Moscow') as date)  as created_date,          -- Дата создания интеграции
    cast(deletedAt as TIMESTAMP) as deleted_at,         -- Дата и время удаления интеграции юзером
    cast(deletedAt as date) as deleted_date,            -- Дата удаления интеграции юзером
    cast(activatedAt as TIMESTAMP) as activated_at,     -- Используется в интеграциях с amo. Дата и время когда активировали интеграцию
    cast(activatedAt as date) as activated_date,        -- Используется в интеграциях с amo. Дата когда активировали интеграцию
    crmName as crm_name,                                -- Название CRM. Пишется при установке с маркетплейса интеграций
    coalesce(webhooksUrl, json_value(params,'$.webhooksUrl'),json_value(details,'$.api_v3.webhooksUri')) as web_hooks_url,  -- Адрес для вебхуков для интеграций api_v2
    _ibk,           -- Дата создания интеграции. Необходимо для партицирования данных в BigQuery
    newOptions as new_options,          -- Объект с опциями интеграций. Подробнее в wazzup_staging.yml
    details,                            -- Дополнительная информация об интеграции. Подробнее в wazzup_staging.yml
    
    cast((case 
        when deletedAt is not null then deletedAt
        when lag( createdAt  ,1) over (partition by accountId order by createdAt  DESC) is null then  CURRENT_TIMESTAMP()
        else lag( cast(createdAt as TIMESTAMP)  ,1) over (partition by accountId order by createdAt  DESC) 
        end) 
    as date) as integration_end_date                --Дата окончания интеграции
from `dwh-wazzup`.`wazzup`.`integrations`