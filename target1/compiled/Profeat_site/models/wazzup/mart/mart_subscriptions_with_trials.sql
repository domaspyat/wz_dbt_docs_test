with deduped_intervals as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_deduplicate_deduped_intervals_with_trials`
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
  SELECT account_id,                    -- ID аккаунта
  MIN(start_date) subscription_start,   -- Дата начала подписки
  MAX(end_date) subscription_end        -- Дата окончания подписки
  FROM deduped_intervals_with_group
  GROUP BY account_id, grp
)
    -- Таблица с историей подписок пользователей с учетом триалов
select combined_intervals.* 
from combined_intervals combined_intervals
join `dwh-wazzup`.`dbt_nbespalov`.`stg_accounts` accounts on combined_intervals.account_Id = accounts.account_Id
where accounts.type not in ('child-postpay','partner-demo')