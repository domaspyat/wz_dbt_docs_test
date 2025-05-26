select paid_intervals.*,       -- Таблица, которая показывает последнюю дату окончания подписки у аккаунта (без триалов)
        currency,       -- Валюта
       lag(subscription_end, 1) over ( partition by paid_intervals.account_id order by subscription_end asc) as last_subscription_end   -- Дата окончания последней подписки
from  `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_deduplicated_combined_intervals` paid_intervals
left join `dwh-wazzup`.`dbt_nbespalov`.`stg_accounts` accounts on paid_intervals.account_Id = accounts.account_Id 
where accounts.type != 'employee'