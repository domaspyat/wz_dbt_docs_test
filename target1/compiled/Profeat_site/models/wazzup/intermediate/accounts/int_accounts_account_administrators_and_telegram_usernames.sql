with channel_administrators as (
                select admin_id,
                        max(phone_unavailable_notification) as phone_unavailable_notification
                from `dwh-wazzup`.`dbt_nbespalov`.`stg_channel_administrators`
                group by 1
), --сотрудники, у которых подключено уведомление к какому-нибудь одному каналу

account_administrators as (
    select account_id,                              -- ID аккаунта
    string_agg(username,',') as telegram_username   -- Юзернейм в Telegram
    from `dwh-wazzup`.`dbt_nbespalov`.`stg_account_administrators` account_administrators
    left join channel_administrators
                on channel_administrators.admin_id=account_administrators.guid
    where (subscription_notifications or integrations_notifications or phone_unavailable_notification)
    and username!=''
    group by 1
)   -- Таблица с юзернеймами в Telegram админов аккаунтов
select *
from account_administrators