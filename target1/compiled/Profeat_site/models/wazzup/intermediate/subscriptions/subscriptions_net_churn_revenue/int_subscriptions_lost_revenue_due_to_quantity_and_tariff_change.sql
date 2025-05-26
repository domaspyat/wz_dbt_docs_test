with
    billing_packages_with_tarif_info as (
        select
            subscription_updates.*,
            billingpackages.account_id,
            (
                case
                    when subscription_updates.period_old = 12
                    then 0.8
                    when subscription_updates.period_old = 6
                    then 0.9
                    else 1
                end
            ) as period_discount,
            wazzup_tariff_new.sum as tariff_price_new,  -- Новая цена тарифа
            wazzup_tariff_old.sum as tariff_price_old   -- Старая цена тарифа
        from
            `dwh-wazzup`.`dbt_nbespalov`.`int_subscription_updates__corect_date_and_filledna_tarif_period_quantity` subscription_updates
        left join
            `dwh-wazzup`.`analytics_tech`.`wazzup_tariff` wazzup_tariff_old
            on wazzup_tariff_old.currency = subscription_updates.currency
            and wazzup_tariff_old.tariff = subscription_updates.tariff_old
        left join
            `dwh-wazzup`.`analytics_tech`.`wazzup_tariff` wazzup_tariff_new
            on wazzup_tariff_new.currency = subscription_updates.currency
            and wazzup_tariff_new.tariff = subscription_updates.tariff_new
        left join `dwh-wazzup`.`dbt_nbespalov`.`stg_billingPackages` billingpackages
        on billingpackages.guid=subscription_updates.subscription_id
        where action in ('loweringTariff', 'subtractQuantity')
    ),
    subscription_sum_with_full_price as (
        select
            *,

            (
                case
                    when action = 'subtractQuantity'
                    then
                        tariff_price_new * period_discount * period_old * quantity_old
                        - tariff_price_new * period_discount * period_old * quantity_new
                    when action = 'loweringTariff'
                    then tariff_price_old - tariff_price_new
                end
            ) as lost_sum   -- Потерянная прибыль
        from billing_packages_with_tarif_info
    )
select  -- Таблица, которая показывает сколько компания потеряла денег из-за смены тарифа
    subscription_sum_with_full_price.*,
    date_trunc(paid_date, month) as paid_month, -- Дата оплаты
    (
        case
            when subscription_sum_with_full_price.currency = 'RUR'
            then coalesce(abs(lost_sum), 0)
            when rur is not null
            then coalesce(abs(lost_sum), 0) * rur
            when subscription_sum_with_full_price.currency = 'EUR' and rur is null
            then coalesce(abs(lost_sum), 0) * 85
            when subscription_sum_with_full_price.currency = 'USD' and rur is null
            then coalesce(abs(lost_sum), 0) * 75
            when subscription_sum_with_full_price.currency = 'KZT' and rur is null
            then coalesce(abs(lost_sum), 0) * 0.24
        end
    ) as lost_sum_in_rubles,    -- Потерянная прибыль в рублях с фикс. курсом
from subscription_sum_with_full_price
left join
    `dwh-wazzup`.`dbt_nbespalov`.`int_payments_exchange_rates_unpivoted` exchange_rates_unpivoted
    on exchange_rates_unpivoted._ibk = subscription_sum_with_full_price.paid_date
    and exchange_rates_unpivoted.currency = subscription_sum_with_full_price.currency