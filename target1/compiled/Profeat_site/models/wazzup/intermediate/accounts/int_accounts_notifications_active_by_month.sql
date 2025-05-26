with
    notification_on as (
        select *
        from  `dwh-wazzup`.`analytics_tech`.`months` month
        inner join
            `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_telegram_notifications_dynamics_combined_intervals` notifications
            on month.month >= date_trunc(notifications.start_date, month)
            and month.month <= date_trunc(notifications.end_date, month)
    ),
    notifications_active as (
        select
            (
                case
                    when
                        date_trunc(start_date, month) = month
                        and date_trunc(end_date, month) = month
                    then date_diff(end_date, start_date, day) + 1
                    when
                        date_trunc(start_date, month) = month
                        and date_trunc(end_date, month) > month
                    then date_diff(last_day(month), start_date, day) + 1
                    when date_trunc(start_date, month) < month
                    then date_diff(end_date, month, day) + 1

                    else date_diff(end_date, start_date, day)
                end

            ) as notification_active_days,
            *
        from notification_on
    )
    -- Таблица с аккаунтами, у которых включены уведомления о работе сервиса
select month,                                   -- Рассматриваемый месяц
account_id,                                     -- ID аккаунта
sum(notification_active_days) as active_days    -- Уведомления включены следующее количество дней
from notifications_active
group by 1, 2