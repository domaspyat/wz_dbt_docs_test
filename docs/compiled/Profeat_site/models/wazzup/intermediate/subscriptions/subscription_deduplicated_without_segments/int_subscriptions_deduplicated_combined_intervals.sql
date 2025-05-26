

with deduped_intervals as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_deduplicate_deduped_intervals`
),

deduped_intervals_with_flag as (
    SELECT *,
    start_date != IFNULL(LAG(end_date) OVER (PARTITION BY account_id ORDER BY start_date), start_date) as flag
    FROM deduped_intervals),

deduped_intervals_with_group as (
    SELECT *, 
    count(CASE WHEN flag THEN 1 END) OVER (PARTITION BY account_id ORDER BY start_date) grp
    from deduped_intervals_with_flag
),

combined_intervals AS (
  SELECT account_id,  MIN(start_date) subscription_start, MAX(end_date) subscription_end
  FROM deduped_intervals_with_group
  GROUP BY account_id, grp
)
    -- Таблица, которая показывает даты начала и окончания подписок у аккаунтов с комбинированными интервалами
select * from combined_intervals