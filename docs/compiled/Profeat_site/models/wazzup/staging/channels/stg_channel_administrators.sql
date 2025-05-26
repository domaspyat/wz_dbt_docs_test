select     -- Таблица, которая показывает включены ли у админов уведомления на работу канала
        channelid as channel_id,                                       -- id канала из таблицы stg_channels
        adminid as admin_id,                                           -- Идентификатор админа из таблицы stg_account_administrators
        phoneUnavailableNotification as phone_unavailable_notification -- Включены уведомления на работу канала?
from `dwh-wazzup`.`wazzup`.`channelAdministrators`