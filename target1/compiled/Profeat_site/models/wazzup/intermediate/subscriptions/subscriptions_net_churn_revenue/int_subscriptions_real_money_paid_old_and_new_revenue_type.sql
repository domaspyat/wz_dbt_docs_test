with all_revenue_with_register_month as (
select all_revenue.*, 
date_trunc(start_date,month) as first_subscription_month,   -- Месяц первой подписки
date_trunc(paid_date, month) as paid_month                  -- Месяц оплаты подписки
from `dwh-wazzup`.`dbt_nbespalov`.`int_payments_only_subscription_payments_real_money` all_revenue
 left join `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_first_subscription_date_and_type` profile_info 
on profile_info.account_id=all_revenue.account_id ),

all_revenue_with_revenue_type as (

select *, (case when first_subscription_month=paid_month  then 'new_users_revenue'
when action='pay' then 'old_users_new_subscription'
when action in ('raiseTariff','addQuantity') then 'upsell_revenue'
end) as revenue_type from  all_revenue_with_register_month)
    -- Таблица, которая показывает сколько реальных денег и по какой причине потратили клиенты
select * from all_revenue_with_revenue_type