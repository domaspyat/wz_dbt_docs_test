select subscriptions_defining_clients_types.*,  -- Таблица, которая показывает последнюю дату окончания подписки у аккаунта и месяц активности
        month,  -- Месяц между началом подписки и её окончанием
            case
                when
                    date_diff(subscription_start, last_subscription_end, day) > 20
                    and date_trunc(last_subscription_end, month) != month
                    and date_trunc(subscription_start, month) = month
                then 'return_payment_monthly'
                else 'other_payments'
            end as payment_type_monthly
from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_defining_clients_types` subscriptions_defining_clients_types
inner join `dwh-wazzup`.`dbt_nbespalov`.`stg_months` months
            on months.month >= date_trunc(subscription_start, month)
            and months.month <= date_trunc(subscription_end, month)