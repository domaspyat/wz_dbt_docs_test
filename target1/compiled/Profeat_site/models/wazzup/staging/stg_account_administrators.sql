select                                -- Таблица с информацией об администраторах аккаунта Wazzup
    guid,                                                       -- Идентификатор админа
    accountId as account_id,                                    -- Идентификатор аккаунта
    name,                                                       -- Имя админа в Telegram
    telegramid as telegram_id,                                  -- Telegram ID
    integrationsNotifications as integrations_notifications,    -- Включены ли уведы по состоянию интеграции?
    subscriptionsNotifications as subscription_notifications,   -- Включены ли уведы по состоянию подписки?
    username                                                    -- Username Telegram
from `dwh-wazzup`.`wazzup`.`accountAdministrators`