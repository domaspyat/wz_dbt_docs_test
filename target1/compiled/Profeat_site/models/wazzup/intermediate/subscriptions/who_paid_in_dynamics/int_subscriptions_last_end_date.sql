with clients_information as (
 select distinct paid_intervals.*,
        currency,   -- Валюта
        case when packages.account_Id is not null then TRUE else FALSE end as has_paid  -- Клиент нам платил?
from  `dwh-wazzup`.`dbt_nbespalov`.`mart_subscriptions_with_trials` paid_intervals
left join `dwh-wazzup`.`dbt_nbespalov`.`stg_accounts` accounts on paid_intervals.account_Id = accounts.account_Id 
left join `dwh-wazzup`.`dbt_nbespalov`.`stg_billingPackages` packages on paid_intervals.account_Id = packages.account_Id and paid_At is not null
where accounts.type != 'employee'
 )
select *,   -- Таблица, которая показывает последнюю дату окончания подписки у аккаунта
    lag(subscription_end, 1) over ( partition by account_id order by subscription_end asc) as last_subscription_end -- Дата окончания последней подписки
from clients_information