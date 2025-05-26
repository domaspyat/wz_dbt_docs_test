with notification_events as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`stg_account_events__telegram_notifications`
),

onboarding as (
    select account_id,
    registration_date
    from
        `dwh-wazzup`.`dbt_nbespalov`.`mart_onboarding__accounts_integrations_subscriptions_channels_messages`
),

notifications_with_last_value as (
    select
        notification_events.*,
        last_value(is_any_notification_on)
            over (
                partition by admin_id
                order by
                    occured_at asc
                rows between unbounded preceding and unbounded following
            )
            as is_any_notification_on_last_value
    from notification_events inner join onboarding
        on notification_events.account_id = onboarding.account_id
    where occured_at <= date_add(registration_date, interval 1 month)
)
    -- Метрика по уведомлениям в Telegram
select
    account_id,                                             -- ID аккаунта
    logical_or(is_any_notification_on_last_value)
        as is_any_notification_on_month_after_registration  -- Включены ли уведомления в телеге в месяц после регистрации
from notifications_with_last_value
group by 1