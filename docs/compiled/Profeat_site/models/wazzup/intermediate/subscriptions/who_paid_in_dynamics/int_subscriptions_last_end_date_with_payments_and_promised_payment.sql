select paid_intervals.*,   -- Таблица, которая показывает последнюю дату окончания подписки у аккаунта (реальные и обещанные платежи)
        currency,       -- Валюта
       lag(subscription_end, 1) over ( partition by paid_intervals.account_id order by subscription_end asc) as last_subscription_end
from  `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_deduplicated_with_payments_and_promised_payments_combined_intervals` paid_intervals
left join `dwh-wazzup`.`dbt_nbespalov`.`stg_accounts` accounts on paid_intervals.account_Id = accounts.account_Id 
where accounts.type != 'employee'