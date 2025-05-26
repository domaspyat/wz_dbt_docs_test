WITH min_max AS (
  SELECT * FROM `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid__count_distinct_visitkas_visitors_min_max_date` ),
visits_30_day_window AS (
  SELECT
    cmuserid,
    Date AS base_date,
    visitkas_users,
    GENERATE_DATE_ARRAY(Date, DATE_ADD(Date, INTERVAL 30 DAY), INTERVAL 1 DAY) AS rollingwindow
  FROM
    `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid__count_distinct_visitkas_visitors`),
visitors_for_the_last_30_days as (
SELECT
  Date,
  cmuserid,
  COUNT(DISTINCT visitkas_users) AS DistinctVisits
FROM
  visits_30_day_window, UNNEST(rollingwindow) AS Date
WHERE
  Date BETWEEN (SELECT DATE_ADD(min_date, INTERVAL 30 DAY) FROM min_max)
  AND (SELECT max_date FROM min_max)
GROUP BY Date,cmuserid)
select last_days.*,
      case when combined_intervals.cmuserid is not null
            then 'paid'
            else 'free'
      end as client_type,
      case when combined_intervals_paid.cmuserid is not null
            then 'paid'
            else 'free'
      end as client_type_paid,
      case when combined_intervals_posts.cmuserid is not null
            then 'paid'
            else 'free'
      end as client_type_post
from visitors_for_the_last_30_days last_days
left join `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid_subscription_history_combined_intervals` combined_intervals on last_days.cmuserid = combined_intervals.cmuserid 
                                                                                         and last_days.date >= combined_intervals.subscription_start and last_days.date <= combined_intervals.subscription_end
left join `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid_subscription_history_combined_intervals_payments_only` combined_intervals_paid on last_days.cmuserid = combined_intervals_paid.cmuserid 
                                                                                         and last_days.date >= combined_intervals_paid.subscription_start and last_days.date <= combined_intervals_paid.subscription_end  

left join `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid_subscription_history_combined_intervals_posts_only` combined_intervals_posts on last_days.cmuserid = combined_intervals_posts.cmuserid 
                                                                                         and last_days.date >= combined_intervals_posts.subscription_start and last_days.date <= combined_intervals_posts.subscription_end                                                                                                                                                                                  
ORDER BY cmuserid,Date