with subscription_all as (
   select * from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_with_deleted_date`
),
    free_subscriptions as(
        select  account_id,
                min_date as start_date,
                max_date as end_date
        from `dwh-wazzup`.`dbt_nbespalov`.`int_channels_agg_with_trials`
        where transport in ('vk','telegram')
                and is_free = true
    ),
check_times AS (
    SELECT account_id, start_date as TIME FROM subscription_all
           UNION DISTINCT
    SELECT account_id, end_date as TIME FROM subscription_all
           UNION DISTINCT
    SELECT account_id, start_date as TIME from free_subscriptions
           UNION DISTINCT
    SELECT account_id, end_date as TIME from free_subscriptions
    )
    -- Таблица, которая показывает даты начала и окончания подписок у аккаунтов в разных записях
select * from check_times