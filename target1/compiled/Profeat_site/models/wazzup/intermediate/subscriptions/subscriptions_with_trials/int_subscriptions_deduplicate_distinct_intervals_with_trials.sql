with check_times as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_deduplicate_check_times_with_trials`
),

distinct_intervals AS (
        SELECT account_id, 
        TIME AS start_date,  LEAD(TIME) OVER (PARTITION BY account_id ORDER BY TIME) end_date
        FROM check_times)
-- Таблица, которая показывает даты начала и окончания триалов в разных столбцах без дублей
select * from distinct_intervals