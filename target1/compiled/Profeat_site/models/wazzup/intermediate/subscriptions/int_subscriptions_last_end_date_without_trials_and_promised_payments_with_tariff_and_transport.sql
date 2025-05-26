select paid_intervals.*,   -- Таблица, которая показывает последнюю дату окончания подписки у аккаунта (без триалов и обещанных платежей)
        currency, -- Валюта
       lag(subscription_end, 1) over ( partition by paid_intervals.account_id, paid_intervals.tariff, paid_intervals.transport order by subscription_end asc) as last_subscription_end   -- Дата окончания последней подписки
from  `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_deduplicated_without_promised_date_combined_intervals_with_tariff_and_transport` paid_intervals
join `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info` accounts on paid_intervals.account_Id = accounts.account_Id 
where is_employee is false