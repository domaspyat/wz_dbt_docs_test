select distinct     -- Таблица, которая показывает тип клиента, в зависимости от его оплат
            int_subscriptions_last_end_date.*,
            date_add(subscription_end, interval 1 day) as data_otvala,  -- Дата отвала клиента
            lead(subscription_start) over (partition by int_subscriptions_last_end_date.account_id order by subscription_start) nextsubscriptiondate,   -- Дата покупки следующей подписки
            date_diff(lead(subscription_start) over (partition by int_subscriptions_last_end_date.account_id order by subscription_start),
                subscription_end,
                day
            ) datesbetween, -- Разница между отвалом и датой покупки следующей подписки"
            case
                when (current_date <= subscription_end)
                then 'still_active'
                when
                    date_diff(
                        lead(subscription_start) over (
                            partition by int_subscriptions_last_end_date.account_id order by subscription_start
                        ),
                        subscription_end,
                        day
                    )
                    <= 20
                then 'bought_new_subscription_within_leaving_period'

                when
                    lead(subscription_start) over (
                        partition by int_subscriptions_last_end_date.account_id order by subscription_start
                    )
                    is null
                    and date_diff(current_date, subscription_end, day) <= 20
                then 'may_become_active'

                when
                    date_diff(
                        lead(subscription_start) over (
                            partition by int_subscriptions_last_end_date.account_id order by subscription_start
                        ),
                        subscription_end,
                        day
                    )
                    > 20
                then 'came_back_after_leaving_period'

                when
                    date_diff(
                        lead(subscription_start) over (
                            partition by int_subscriptions_last_end_date.account_id order by subscription_start
                        ),
                        subscription_end,
                        day
                    )
                    is null
                    and current_date > subscription_end
                then 'did_not_come_back'
            end as clients_type,
   case
                when (current_date <= subscription_end
                or int_channels_count_active_by_account.account_Id is not null)
                then 'still_active'
                when
                    date_diff(
                        lead(subscription_start) over (
                            partition by int_subscriptions_last_end_date.account_id order by subscription_start
                        ),
                        subscription_end,
                        day
                    )
                    <= 7
                then 'bought_new_subscription_within_leaving_period'

                when
                    lead(subscription_start) over (
                        partition by int_subscriptions_last_end_date.account_id order by subscription_start
                    )
                    is null
                    and date_diff(current_date, subscription_end, day) <= 7
                then 'may_become_active'

                when
                    date_diff(
                        lead(subscription_start) over (
                            partition by int_subscriptions_last_end_date.account_id order by subscription_start
                        ),
                        subscription_end,
                        day
                    )
                    > 7
                then 'came_back_after_leaving_period'

                when
                    date_diff(
                        lead(subscription_start) over (
                            partition by int_subscriptions_last_end_date.account_id order by subscription_start
                        ),
                        subscription_end,
                        day
                    )
                    is null
                    and current_date > subscription_end
                then 'did_not_come_back'
            end as clients_type_for_communications  -- Тип клиента: still_active, bought_new_subscription_within_leaving_period, came_back_after_leaving_period, did_not_come_back, may_become_active
        from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_last_end_date` int_subscriptions_last_end_date
        left join `dwh-wazzup`.`dbt_nbespalov`.`int_channels_count_active_by_account` int_channels_count_active_by_account on int_subscriptions_last_end_date.account_Id = int_channels_count_active_by_account.account_Id