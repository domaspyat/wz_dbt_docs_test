with check_times as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_deduplicate_check_times`
),

distinct_intervals AS (
        SELECT account_id, 
        TIME AS start_date,  LEAD(TIME) OVER (PARTITION BY account_id ORDER BY TIME) end_date
        FROM check_times)
    -- Таблица, которая показывает даты начала и окончания подписок у аккаунтов с недублированными интервалами
select * from distinct_intervals