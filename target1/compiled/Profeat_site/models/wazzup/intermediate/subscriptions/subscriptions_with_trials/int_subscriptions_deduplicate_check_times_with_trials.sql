with subscription_all as (
   select * from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_with_deleted_date`
),
    trials as(
        select  account_id,
                min_date as start_date,
                --case when max_date >
                trial_end_date as end_date
                --then trial_end_date 
                --else max_date 
                --end as end_date
        from `dwh-wazzup`.`dbt_nbespalov`.`int_channels_agg_with_trials`
        where min_date <= trial_end_date
    ),
    free_subscriptions as(
        select  account_id,
                min_date as start_date,
                max_date as end_date
        from `dwh-wazzup`.`dbt_nbespalov`.`int_channels_agg_with_trials`
        where transport in ('vk','telegram')
                and is_free = true
    ),
     free_subscriptions_partners as(
        select  channels_agg_with_trials.account_id,
                min_date as start_date,
                max_date as end_date
        from `dwh-wazzup`.`dbt_nbespalov`.`int_channels_agg_with_trials` channels_agg_with_trials
        join `dwh-wazzup`.`dbt_nbespalov`.`stg_accounts` accounts on channels_agg_with_trials.account_Id = accounts.account_Id
        where  is_free = true and  accounts.type = 'partner'
    ),
check_times AS (
    SELECT account_id, start_date as TIME FROM subscription_all
           UNION DISTINCT
    SELECT account_id, end_date as TIME FROM subscription_all
           UNION DISTINCT
    SELECT account_id, start_date as TIME from trials
           UNION DISTINCT
    SELECT account_id, end_date as TIME from trials
           UNION DISTINCT
    SELECT account_id, start_date as TIME from free_subscriptions
           UNION DISTINCT
    SELECT account_id, end_date as TIME from free_subscriptions
            UNION DISTINCT
    SELECT account_id, start_date as TIME from free_subscriptions_partners
           UNION DISTINCT
    SELECT account_id, end_date as TIME from free_subscriptions_partners
    )
        -- Таблица, которая показывает даты начала и окончания триалов у аккаунта
select * from check_times