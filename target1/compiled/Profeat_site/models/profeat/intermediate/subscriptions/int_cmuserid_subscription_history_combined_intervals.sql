with subscription_all as (
    select *
    from `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid_subscription_history`
)
,check_times AS (
    SELECT cmuserid, start_date as TIME FROM subscription_all
           UNION DISTINCT
    SELECT cmuserid, end_date as TIME FROM subscription_all
    )
 , distinct_intervals AS (
        SELECT cmuserid, 
        TIME AS start_date,  LEAD(TIME) OVER (PARTITION BY cmuserid ORDER BY TIME) end_date
        FROM check_times)
,deduped_intervals AS (
          SELECT a.cmuserid, a.start_date, a.end_date
          FROM distinct_intervals a
          JOIN subscription_all b
          ON a.cmuserid = b.cmuserid
          AND a.start_date BETWEEN b.start_date AND b.end_date 
          AND a.end_date BETWEEN b.start_date AND b.end_date
          GROUP BY 1,2,3)
,deduped_intervals_with_flag as (
    SELECT *,
    start_date != IFNULL(LAG(end_date) OVER (PARTITION BY cmuserid ORDER BY start_date), start_date) as flag
    FROM deduped_intervals),

deduped_intervals_with_group as (
    SELECT *, 
    count(CASE WHEN flag THEN 1 END) OVER (PARTITION BY cmuserid ORDER BY start_date) grp
    from deduped_intervals_with_flag
),

combined_intervals AS (
  SELECT cmuserid,  MIN(start_date) subscription_start, MAX(end_date) subscription_end
  FROM deduped_intervals_with_group
  GROUP BY cmuserid, grp
)
select *
from combined_intervals