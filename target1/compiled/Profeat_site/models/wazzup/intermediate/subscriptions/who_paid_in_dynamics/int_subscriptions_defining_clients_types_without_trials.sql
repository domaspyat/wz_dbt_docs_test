select
            *,
            date_add(subscription_end, interval 1 day) as data_otvala,  -- Дата отвала клиента
            lead(subscription_start) over (partition by account_id order by subscription_start) nextsubscriptiondate,   -- Дата покупки следующей подписки
            date_diff(lead(subscription_start) over (partition by account_id order by subscription_start),
                subscription_end,
                day
            ) datesbetween, -- Разница между отвалом и датой покупки следующей подписки
            case
                when date(current_timestamp) <= subscription_end
                then 'still_active'
                when
                    date_diff(
                        lead(subscription_start) over (
                            partition by account_id order by subscription_start
                        ),
                        subscription_end,
                        day
                    )
                    <= 20
                then 'bought_new_subscription_within_leaving_period'

                when
                    lead(subscription_start) over (
                        partition by account_id order by subscription_start
                    )
                    is null
                    and date_diff(date(current_timestamp), subscription_end, day) <= 20
                then 'may_become_active'

                when
                    date_diff(
                        lead(subscription_start) over (
                            partition by account_id order by subscription_start
                        ),
                        subscription_end,
                        day
                    )
                    > 20
                then 'came_back_after_leaving_period'

                when
                    date_diff(
                        lead(subscription_start) over (
                            partition by account_id order by subscription_start
                        ),
                        subscription_end,
                        day
                    )
                    is null
                    and date(current_timestamp) > subscription_end
                then 'did_not_come_back'
            end as clients_type
        from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_last_end_date_without_trials`