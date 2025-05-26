with combined_intervals as (
    select int_subscription_deduplicated.* 
    from `dwh-wazzup`.`dbt_nbespalov`.`int_subscription_deduplicated_by_id_combined_intervals_only_paid` int_subscription_deduplicated
)   -- Таблица с датой первой подписки, у которой длительность более 1 дня
select account_id,                          -- ID аккаунта
min(subscription_start) as min_paid_date    -- Первая дата оплаты по аккаунту
from combined_intervals
group by 1