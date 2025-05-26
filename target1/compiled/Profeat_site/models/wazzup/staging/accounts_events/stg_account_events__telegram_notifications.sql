SELECT          -- Таблица с информацией о включении/выключении телеграм-уведомлений
    accountId as account_id,                                -- Идентификатор аккаунта, соответствует id из таблицы stg_accounts
    datetime(occured_at, 'Europe/Moscow') as occured_at,    -- Дата и время совершения события
    adminId as admin_id,                                    -- Идентификатор аккаунта, кому приходят уведомления
    (case when is_channel_notification_on or is_subscription_notification_on or is_integration_notification_on 
    then true
    else false
    end
    ) as is_any_notification_on ,                           -- Включены ли уведомления хотя бы на одну из сущностей: (подписка, канал, интеграция)
    is_channel_notification_on,                             -- Включены ли уведомления от канала
    is_subscription_notification_on,                        -- Включены ли уведомления от подписки
    is_integration_notification_on                          -- Включены ли уведомления от интеграции
FROM  `dwh-wazzup`.`wazzup`.`analytic_events`
where event_type=2