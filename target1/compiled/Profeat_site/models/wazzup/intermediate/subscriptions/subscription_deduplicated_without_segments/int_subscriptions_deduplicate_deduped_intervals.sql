with distinct_intervals as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_deduplicate_distinct_intervals`
),
subscription_all as (
    select account_id,
           start_date,
           end_date 
    from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_with_deleted_date`
),
deduped_intervals AS (
          SELECT a.account_id, a.start_date, a.end_date
          FROM distinct_intervals a
          JOIN subscription_all b
          ON a.account_id = b.account_id 
          AND a.start_date BETWEEN b.start_date AND b.end_date 
          AND a.end_date BETWEEN b.start_date AND b.end_date
          GROUP BY 1,2,3)
    -- Таблица, которая показывает даты начала и окончания подписок у аккаунтов с недублированными интервалами после группировки
select * from deduped_intervals