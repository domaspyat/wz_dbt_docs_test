with combined_intervals as (
    select int_subscription_deduplicated.* 
    from `dwh-wazzup`.`dbt_nbespalov`.`int_subscription_deduplicated_by_id_combined_intervals_only_paid` int_subscription_deduplicated
        inner join `dwh-wazzup`.`dbt_nbespalov`.`stg_billingPackages` billingpackages 
    on billingpackages.guid=int_subscription_deduplicated.subscription_id
    where tariff!='waba'
)   -- Таблица с датой первой подписки, у которой длительность более 1 дня без WABA
select account_id,                       -- ID аккаунта   
min(subscription_start) as min_paid_date -- Первая дата оплаты по аккаунту
from combined_intervals
group by 1