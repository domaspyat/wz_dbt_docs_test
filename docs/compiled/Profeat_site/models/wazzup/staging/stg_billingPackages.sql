select          -- Таблица с подписками пользователей
    accountId as account_id,        -- Идентификатор аккаунта, соответствует id из таблицы stg_accounts
    type,                           -- Тип подписки. Соответствует транспорту канала.
    state,                          -- Статус подписки
    isFree as is_free,              -- Признак, который говорит о том что подписка бесплатна (выдана нами безвозмедно)
    guid,                           -- Идентификатор подписки.Генерируется Postgress при создании записи
    tariff,                         -- Тариф подписки
    quantity,                       -- Количество каналов в подписке. Но, для tech-partner-postpay - 10000. 
    paidAt as paid_at,              -- Дата и время оплаты подписки. Дата перезаписывается после каждой успешной оплаты!
    createdAt as created_at,        -- Дата и время создания подписки.
    expiresAt as expires_at,        -- Дата и время истекания подписки
    cast(json_value(autoRenewal,'$.enabled') as bool) as auto_renewal,      -- Признак, указывающий на автоматическое продление подписки
    period                          -- Длительность подписки в месяцах
from `dwh-wazzup`.`wazzup`.`billingPackages`