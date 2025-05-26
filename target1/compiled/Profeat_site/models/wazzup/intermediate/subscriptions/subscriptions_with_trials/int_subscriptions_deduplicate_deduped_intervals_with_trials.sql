with distinct_intervals as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_deduplicate_distinct_intervals_with_trials`
),
 trials as(
        select  account_id,
                min_date as start_date,
                --case when max_date >
                trial_end_date as end_date
                -- then trial_end_date 
                -- else max_date 
                -- end as end_date
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
    free_subscriptions_of_partners as(
        select  int_channels_agg_with_trials.account_id,
                min_date as start_date,
                max_date as end_date
        from `dwh-wazzup`.`dbt_nbespalov`.`int_channels_agg_with_trials` int_channels_agg_with_trials
        join `dwh-wazzup`.`dbt_nbespalov`.`stg_accounts` accounts on int_channels_agg_with_trials.account_Id = accounts.account_Id
        where  is_free = true and accounts.type = 'partner'
    ),

subscription_all_and_trials_with_free_vk_tg as (
    select account_id,
           start_date,
           end_date 
    from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_with_deleted_date`
    union distinct 
    select account_id,
            start_date,
            end_date
    from trials
    union distinct 
    select account_id,
            start_date,
            end_date
    from free_subscriptions
    union distinct 
    select account_id,
            start_date,
            end_date
    from free_subscriptions_of_partners
),
deduped_intervals AS (
          SELECT a.account_id, a.start_date, a.end_date         -- ID аккаунта, -- Дата начала триала, -- Дата окончания триала
          FROM distinct_intervals a
          JOIN subscription_all_and_trials_with_free_vk_tg b
          ON a.account_id = b.account_id 
          AND a.start_date BETWEEN b.start_date AND b.end_date 
          AND a.end_date BETWEEN b.start_date AND b.end_date
          GROUP BY 1,2,3)
        -- Таблица, которая показывает даты начала и окончания триалов в разных столбцах без дублей после аггрегации
select * from deduped_intervals