with calculating_lt as (
                  select account_id,
                          live_month,       -- Месяц жизни клиента
                          sum(revenue_amount) over (partition by account_id order by live_month) cum_sum_up_to_live_month,  -- Сумма денег в месяц жизни
                          date_diff(live_month,date_trunc(first_subscription_start,month),month) LT         -- lifetime клиента
                  from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_abcx_segmentation_users_living_time_with_revenue_periods` revenue_periods 
                        ),
segments as (
    select   
        account_id,
        date segment_date,
         case    when segment ='standart_without_partner' then 'Конечный клиент'
                 when segment = 'of_partner_child_child_paid' then 'Конечный клиент'

                 when segment = 'tech_partner_child__tech_partner_paid' then 'Тех. партнер'
                 when segment = 'tech_partner_child__child_paid' then 'Тех. партнер'
                 when segment = 'tech-partner' then 'Тех. партнер'

                 when segment = 'partner' then 'Оф. партнер'
                 when segment = 'of_partner_child__of_partner_paid' then 'Оф. партнер'

                 when segment = 'employee' then 'работник'
                 when segment = 'partner-demo' then 'демо-партнёр'
                 when segment = 'tech-partner-postpay' then 'техпартнер-постоплатник'
            else 'unknown' end as account_segment_type,
        row_number() over (partition by account_id,date_trunc(date,month) order by date desc) rn_segments
    from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_who_paid_in_dynamics_defining_clients_with_segments` segments
    join `dwh-wazzup`.`dbt_nbespalov`.`stg_days` days on segments.segment_start <= days.date and days.date < segments.segment_end
),
left_and_active_info as (
select  
      mart_left_and_returned_accounts_with_partner_and_account_type_with_dates.account_id,  -- ID аккаунта
      date,                                         -- Рассматриваемая дата
      currency,                                     -- Валюта
      return_or_left_status_with_churn_period_5,    -- Статус ухода клиента с периодом churn 5
      return_or_left_status_with_churn_period_90,   -- Статус ухода клиента с периодом churn 90
      return_or_left_status,                        -- Статус ухода клиента
      subscription_start,                           -- Дата начала подписки
      subscription_end,                             -- Дата окончания подписки
      segments.account_segment_type,
      row_number() over (partition by segments.account_id,date order by segment_date desc) rn_last_segment_for_date 
from `dwh-wazzup`.`dbt_nbespalov`.`mart_left_and_returned_accounts_with_partner_and_account_type_with_dates` mart_left_and_returned_accounts_with_partner_and_account_type_with_dates
join segments on segments.segment_date <= date
                    and segments.account_id = mart_left_and_returned_accounts_with_partner_and_account_type_with_dates.account_id
                    and rn_segments = 1
where segments.account_segment_type  not in ('unknown','работник','техпартнер-постоплатник','демо-партнёр')
),living_periods as (
select left_and_active_info.*except(rn_last_segment_for_date),
        calculating_lt.*except(account_id),
        row_number() over (partition by left_and_active_info.account_id,date order by live_month desc) rn
from left_and_active_info
left join calculating_lt on left_and_active_info.account_id = calculating_lt.account_id
                                      and live_month <= date_trunc(date,month)
where rn_last_segment_for_date = 1
)   -- Таблица ушедших и вернувшихся клиентов с партнером, датами и LTV
select *except(rn,account_segment_type),
case when subscription_end = date_trunc(subscription_end,month) and --(return_or_left_status in ('left','came_back_after_leaving_period')
                                                                    --or 
                                                                      return_or_left_status_with_churn_period_5 in ('left','came_back_after_leaving_period')
                                                                   -- )  
            then lag(account_segment_type,2) over (partition by account_id order by date) 
        else coalesce(lag(account_segment_type) over (partition by account_id order by date),account_segment_type) end account_segment_type
from living_periods
where rn = 1

union all

select *except(rn,account_segment_type),
'all' as  account_segment_type              -- Сегмент клиента
from living_periods
where rn = 1