select subscriptions_defining_clients_types.*,  -- Таблица, которая показывает последнюю дату окончания подписки у аккаунта, месяц и неделю активности (без триалов)
        month,      -- Месяц между началом подписки и её окончанием
        week,       -- Неделя между началом подписки и её окончанием
            case
                when
                    date_diff(subscription_start, last_subscription_end, day) > 20
                    and date_trunc(last_subscription_end, month) != month
                    and date_trunc(subscription_start, month) = month
                then 'return_payment_monthly'
                else 'other_payments'
            end as payment_type_monthly,    -- Тип оплаты месячный

            case
                when
                    date_diff(subscription_start, last_subscription_end, day) > 20
                    and date_trunc(last_subscription_end, week(monday)) != week
                    and date_trunc(subscription_start, week(monday)) = week
                then 'return_payment_weekly'
                else 'other_payments'
            end as payment_type_weekly      -- Тип оплаты недельный

from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_defining_clients_types_without_trials` subscriptions_defining_clients_types
inner join `dwh-wazzup`.`dbt_nbespalov`.`stg_months` months
            on months.month >= date_trunc(subscription_start, month)
            and months.month <= date_trunc(subscription_end, month)
inner join `dwh-wazzup`.`dbt_nbespalov`.`stg_weeks` weeks
            on weeks.week >= date_trunc(subscription_start, week(monday))
            and weeks.week <= date_trunc(subscription_end, week(monday))
where (date_trunc(week,month) = months.month or week<=months.month)