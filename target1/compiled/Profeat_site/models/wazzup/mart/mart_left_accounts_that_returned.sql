with finding_next_status_of_account as 
        (select account_id,
                  subscription_start,
                  subscription_end,
                  date,
                  data_otvala,
                  last_subscription_end,
                  client_type_with_churn_period_5,
                  account_type,
                  account_type_partner_type,
                  return_or_left_status_with_churn_period_5,
                  lead(return_or_left_status_with_churn_period_5) over (partition by account_id order by date) next_status,
                  lead(date) over (partition by account_id order by date) next_status_date
from `dwh-wazzup`.`dbt_nbespalov`.`mart_left_and_returned_accounts_with_partner_and_account_type_with_dates`
where return_or_left_status_with_churn_period_5 in ('left','came_back_after_leaving_period','returned')
),
 affiliates as (
    select *
    from `dwh-wazzup`.`dbt_nbespalov`.`stg_affiliates`
),   profile_info as (
        select *
        from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info`
        ),period_numbers as (
select finding_next_status_of_account.account_id,       -- ID аккаунта
       subscription_start,                              -- Дата начала подписки
       subscription_end,                                -- Дата окончания подписки
       data_otvala,                                     -- Дата отвала клиента
       last_subscription_end,                           -- Дата окончания последней подписки
                  account_type,                         -- Тип аккаунта
                  account_type_partner_type,            -- Тип аккаунта с зависимостью от партнера
       return_or_left_status_with_churn_period_5,       -- Статус ухода клиента с периодом churn 5
       case when next_status is distinct from 'returned' then null else next_status end as next_status,             -- Следующий статус клиента
       case when next_status is distinct from 'returned' then null else next_status_date end as next_status_date,   -- Дата следующего статуса
       date_diff(   case when next_status is distinct from 'returned' then null else next_status_date end,
                    data_otvala,
                    day   
                    ) time_period_number,
        profile_info.currency               -- Валюта
from finding_next_status_of_account
join  profile_info on finding_next_status_of_account.account_id = profile_info.account_Id and is_employee is false
where next_status is distinct from 'came_back_after_leaving_period'
                        and return_or_left_status_with_churn_period_5 in ('left','came_back_after_leaving_period')
        and account_type is distinct from 'partner-demo'
        and account_type is distinct from 'employee'
        ),
left_accounts_that_returned as (
select *except(time_period_number),
cast(FLOOR(SAFE_DIVIDE(time_period_number,7)) as int) time_period_number    -- Количество дней между отвалом и возвратом
from period_numbers )
,cum_money as (

                  select account_id,
                        live_month,
                        account_segment_type,
                     segment_type_groupped,
                     Last_value(avg_sum_in_rubles IGNORE NULLS) over (partition by account_id order by live_month rows between unbounded preceding and current row) avg_sum_in_rubles,
                    last_value(abcx_segment IGNORE NULLS) over (partition by account_id order by live_month rows between unbounded preceding and current row) abcx_segment,
                  sum(avg_sum_in_rubles) over (partition by account_id order by live_month) cum_sum_up_to_live_month
                  from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_abcx_segmentation_defining_abcx_segmentation_type_all_segments`
)   -- Таблица ушедших клиентов, которые вернулись
select left_.*,
        coalesce(cum_money.abcx_segment,cum_money_returned.abcx_segment) abcx_segment,                          -- Сегмент по ABCX сегментации
      coalesce(cum_money.segment_type_groupped,cum_money_returned.segment_type_groupped) segment_type_groupped, -- Сегмент клиента после группировки
        cum_money.cum_sum_up_to_live_month as cum_left,                     -- сумма денег ушедших клиентов
       cum_money.avg_sum_in_rubles as avg_sum_in_rubles_left,               -- средняя сумма ушедших
      cum_money_returned.cum_sum_up_to_live_month as cum_returned,          -- сумма денег вернувшихся клиентов
      cum_money_returned.avg_sum_in_rubles as avg_sum_in_rubles_returned    -- средняя сумма вернувшихся

from  left_accounts_that_returned left_ 
left join  cum_money
                                                    on cum_money.account_id = left_.account_id
                                                     and cum_money.live_month = case when data_otvala = date_trunc(data_otvala,month)
                                                                                                then date_trunc(date_add(data_otvala,interval -1 day),month)
                                                                                    when subscription_end = date_trunc(subscription_end,month) 
                                                                                                then  date_trunc(date_add(subscription_end,interval -1 day),month)
                                                                                                      else date_trunc(data_otvala,month)
                                                                                                end
                                                    
left join cum_money as cum_money_returned on cum_money_returned.account_id = left_.account_id
                                                    and cum_money_returned.live_month = date_trunc(next_status_date,month)
where left_.account_id != 58110403
        and coalesce(cum_money.account_segment_type,cum_money_returned.account_segment_type) not in ('оф. партнёр','обычный техпартнер')