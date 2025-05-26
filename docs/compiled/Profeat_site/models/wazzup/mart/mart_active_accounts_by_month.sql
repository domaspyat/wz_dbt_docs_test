with subscription_type as (
     select * from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_last_end_date_with_months`
) 
    -- Показывает активность пользователей по месяцам. Запись появляется в таблице, если пользователь в этот месяц был активен (с учетом триалов!!!). Подробнее об активных аккаунтах https://www.notion.so/687832f855e84aefbb3b5b65c89b8923?pvs=4
select *, 
first_value(subscription_start) over (partition by account_id order by subscription_start ) as first_subscription_start -- месяц, когда пользователь в первый раз оплатил подписку или у него начался триал
from subscription_type