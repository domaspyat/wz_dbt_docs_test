with

    difference_last_month as (
        select * from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_deduplicated_by_id_with_previous_and_next_subscription`
    ),

    lost_revenue_last_month_grouped_by_subscripion as (
        select
            subscription_id,
            period_new,
            tariff_new,
            quantity_new,
            partner_discount_new,
            subscription_updates.paid_at,
            currency,
            action
        from
            `dwh-wazzup`.`dbt_nbespalov`.`int_subscription_updates__corect_date_and_filledna_tarif_period_quantity` subscription_updates
        left join
            `dwh-wazzup`.`dbt_nbespalov`.`stg_billingPackages` billingpackages
            on subscription_updates.subscription_id = billingpackages.guid
        where action not in ('balanceTopup', 'setPromisedPayment')
    ),


    lost_revenue as (
        select
            difference_last_month.*,
            (
                case
                    when lost_revenue_last_month_grouped_by_subscripion.period_new = 12
                    then 0.8
                    when lost_revenue_last_month_grouped_by_subscripion.period_new = 6
                    then 0.9
                    else 1
                end
            ) as period_discount,
            coalesce(
                lost_revenue_last_month_grouped_by_subscripion.period_new,
                billingpackages.period
            ) as period_new,
            coalesce(
                lost_revenue_last_month_grouped_by_subscripion.quantity_new,
                billingpackages.quantity
            ) as quantity_new,
            coalesce(
                lost_revenue_last_month_grouped_by_subscripion.tariff_new,
                billingpackages.tariff
            ) as tariff_new,
            partner_discount_new,
            wazzup_tariff_new.sum as tariff_price_new,
            lost_revenue_last_month_grouped_by_subscripion.currency,
            row_number() over (
                partition by
                    lost_revenue_last_month_grouped_by_subscripion.subscription_id,
                    subscription_end
                order by lost_revenue_last_month_grouped_by_subscripion.paid_at desc
            ) rn
        from difference_last_month
        left join
            lost_revenue_last_month_grouped_by_subscripion
            on lost_revenue_last_month_grouped_by_subscripion.subscription_id
            = difference_last_month.subscription_id
            and cast(lost_revenue_last_month_grouped_by_subscripion.paid_at as date)
            <= difference_last_month.subscription_end
        left join
            `dwh-wazzup`.`dbt_nbespalov`.`stg_billingPackages` billingpackages
            on billingpackages.guid = difference_last_month.subscription_id
        left join
            `dwh-wazzup`.`analytics_tech`.`wazzup_tariff` wazzup_tariff_new
            on wazzup_tariff_new.currency
            = lost_revenue_last_month_grouped_by_subscripion.currency
            and wazzup_tariff_new.tariff = coalesce(
                lost_revenue_last_month_grouped_by_subscripion.tariff_new,
                billingpackages.tariff
            )
        where difference_between_days >= 21
    ),
    lost_revenue_sum as (
        select
            account_id,                 -- ID аккаунта
            subscription_id,            -- ID подписки
            subscription_start,         -- Дата начала подписки
            tariff_price_new,           -- Цена нового тарифа
            period_discount,            -- Скидка за период покупки подписки (10% за полгода и 20% за год)
            period_new,                 -- Новый период подписки
            quantity_new,               -- Новое кол-во каналов в подписке
            difference_between_days,    -- Выбираются только клиенты, которые не платили нам более 21го дня
            date_add(subscription_end, interval 1 month) as churn_date, -- Дата откола
            (
                case
                    when partner_discount_new is null
                    then 1
                    else 1 - partner_discount_new
                end
            )* tariff_price_new*1*quantity_new as lost_revenue, -- Потерянная прибыль
            currency    -- Валюта
        from lost_revenue
        where rn = 1
    )   -- Таблица, которая показывает сколько компания потеряла денег из-за откола клиента
select
    lost_revenue_sum.*,
    date_trunc(churn_date, month) as churn_month,   -- Месяц откола
    (
        case
            when lost_revenue_sum.currency = 'RUR'
            then coalesce(abs(lost_revenue), 0)
            when rur is not null
            then coalesce(abs(lost_revenue), 0) * rur
            when lost_revenue_sum.currency = 'EUR' and rur is null
            then coalesce(abs(lost_revenue), 0) * 85
            when lost_revenue_sum.currency = 'USD' and rur is null
            then coalesce(abs(lost_revenue), 0) * 75
            when lost_revenue_sum.currency = 'KZT' and rur is null
            then coalesce(abs(lost_revenue), 0) * 0.24
        end
    ) as lost_sum_in_rubles,    -- Потерянная прибыль в рублях с фикс. курсом

from lost_revenue_sum
left join
    `dwh-wazzup`.`dbt_nbespalov`.`int_payments_exchange_rates_unpivoted`  exchange_rates_unpivoted
    on exchange_rates_unpivoted._ibk = lost_revenue_sum.churn_date
    and exchange_rates_unpivoted.currency = lost_revenue_sum.currency