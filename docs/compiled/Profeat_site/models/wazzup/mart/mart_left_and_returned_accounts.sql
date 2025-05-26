with active_accounts_weekly as (
          select * from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_active_accounts_weekly`
),
active_accounts_monthly as (
          select * from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_active_accounts_monthly`
),

week_left_guys as (
          select * from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_left_accounts_weekly`
),
month_left_guys as (
          select * from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_left_accounts_monthly`
),
returned_guys_weekly as (
            select * from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_returned_accounts_weekly`

),
returned_guys_monthly as (
              select * from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_returned_accounts_monthly`

)

select sum(activeaccs) As active_accs,
       left_guys,
       returned,
       returned_on_left_day,
       active_weekly.week as date,
       active_weekly.currency,
       'weekly' as TYPE
from active_accounts_weekly active_weekly
left join week_left_guys left_weekly on left_weekly.week_of_leave_date = active_weekly.week  and left_weekly.currency = active_weekly.currency 
left join returned_guys_weekly returned_weekly on returned_weekly.week = active_weekly.week  and returned_weekly.currency = active_weekly.currency
--where active_weekly.week <= current_date
group by 2,3,4,5,6,7

union all
    -- Ушедшие и вернувшиеся аккаунты
select sum(activeaccs) As active_accs,  -- Количество активных аккаунтов
       left_guys,                       -- Количество ушедших клиентов
       returned,                        -- Количество вернувшихся клиентов
       returned_on_left_day,            -- Количество клиентов, которые вернулись в день ухода
       active_monthly.month as date,    -- Рассматриваемый временной промежуток
       active_monthly.currency,         -- Валюта
       'monthly' as TYPE                -- Тип временного промежутка
from active_accounts_monthly active_monthly
left join month_left_guys left_monthly on left_monthly.month_of_leave_date = active_monthly.month  and left_monthly.currency = active_monthly.currency 
left join returned_guys_monthly returned_monthly on returned_monthly.month = active_monthly.month  and returned_monthly.currency = active_monthly.currency
group by 2,3,4,5,6,7