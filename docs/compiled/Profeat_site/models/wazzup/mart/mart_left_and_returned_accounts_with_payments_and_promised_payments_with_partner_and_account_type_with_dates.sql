with
    subscriptions_start_and_end_with_dates as (
        select
            *,
            (
                case
                    when
                        date_add(subscriptions.subscription_end, interval 1 day) = date
                        and client_type_with_churn_period_20
                        in ('did_not_come_back')
                    then 'left'
                    when
                        date_add(subscriptions.subscription_end, interval 1 day) = date
                        and client_type_with_churn_period_20
                        in ('came_back_after_leaving_period')
                    then 'came_back_after_leaving_period'
                    when
                        date = subscription_start
                        and date_diff(subscription_start, last_subscription_end, day)
                        > 20
                    then 'returned'
                    when client_type_with_churn_period_20 = 'may_become_active' 
                                     then 'may_become_active'
                    else 'active'
                end
            ) as return_or_left_status, -- Статус клиента на каждый день

          (
                case
                    when
                        date_add(subscriptions.subscription_end, interval 1 day) = date
                        and client_type_with_churn_period_7
                        in ('did_not_come_back')
                    then 'left'
                    when
                        date_add(subscriptions.subscription_end, interval 1 day) = date
                        and client_type_with_churn_period_7
                        in ('came_back_after_leaving_period')
                    then 'came_back_after_leaving_period'
                    when
                        date = subscription_start
                        and date_diff(subscription_start, last_subscription_end, day)
                        > 7
                    then 'returned'
                    when client_type_with_churn_period_7= 'may_become_active' 
                                     then 'may_become_active'
                    else 'active'
                end
            ) as return_or_left_status_with_churn_period_7  -- Статус клиента на каждый день. churn_period определяется как в client_type_with_churn_period_n

        from
            `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_left_and_returned_date_with_payments_and_promised_payments_with_account_type_and_partner_type` subscriptions
        inner join
            `dwh-wazzup`.`analytics_tech`.`days`
            on subscriptions.subscription_start <= days.date
            and subscriptions.subscription_end >= days.date
    )
,int_accounts_who_paid__defining_abcx_segmentation_type as (
    select *
    from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_abcx_segmentation_defining_abcx_segmentation_type`
),
profile_info as (
    select *
    from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info`
)
    -- Таблица с историей подписок без учета триалов и обещанных платежей. Нужна для определения отвалившихся и вернувшихся пользователей в периоде
 select subscriptions_start_and_end_with_dates.*,
       abcx_segment,    -- Сегмент в соответствие нашей abcdx сегментации https://www.notion.so/ABCX-7b8f5f7d3e0b470e83fe632828d64821
       min(subscription_start) over (partition by subscriptions_start_and_end_with_dates.account_id) as min_subscription_start_month -- месяц, когда пользователь в первый раз оплатил подписку
  from subscriptions_start_and_end_with_dates
  left join int_accounts_who_paid__defining_abcx_segmentation_type 
                                                on subscriptions_start_and_end_with_dates.account_id = int_accounts_who_paid__defining_abcx_segmentation_type.account_id
                                                and date_trunc(subscriptions_start_and_end_with_dates.date,month)  =  int_accounts_who_paid__defining_abcx_segmentation_type.live_month
  
where not exists (
    select profile_info.account_id
    from profile_info 
    where   subscriptions_start_and_end_with_dates.account_Id = profile_info.account_Id
            and profile_info.is_employee 
)