with
    partitions_notifications as (
        select
            account_id,             -- ID аккаунта
            occured_at,             
            admin_id,               -- ID админа аккаунта
            is_any_notification_on, -- Включены ли любые уведомления
            (
                case
                    when
                        lag(is_any_notification_on) over (
                            partition by account_id, admin_id order by occured_at desc
                        )
                        != is_any_notification_on
                    then 1
                    else 0
                end
            ) as partition_number

        from `dwh-wazzup`.`dbt_nbespalov`.`stg_account_events__telegram_notifications`
    ),

    partition_rolling as (

        select
            *,
            sum(partition_number) over (
                partition by account_id, admin_id order by occured_at desc
            ) as partition_number_rolling
        from partitions_notifications
    ),

    partition_groupped as (

        select
            account_id,
            admin_id,
            is_any_notification_on,
            partition_number_rolling,
            min(occured_at) as partition_min    -- Дата и время включения уведомлений
        from partition_rolling

        group by 1, 2, 3, 4
    ),

    partition_end as (

        select
            *,
            coalesce(
                lag(partition_min) over (
                    partition by account_id, admin_id order by partition_min desc
                ),
                current_date
            ) as partition_end_at   -- Дата и время отключения уведомлений. Или текущая дата и время, если не отключены
        from partition_groupped
    )
    -- Таблица, которая показывает периоды активности уведомлений в личном кабинете
select *,
cast(partition_min as date) as start_date,  -- Дата включения уведомлений
cast(partition_end_at as date) as  end_date -- Дата отключения уведомлений. Или текущая дата, если не отключены
from partition_end
where is_any_notification_on