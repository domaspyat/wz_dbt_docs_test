with subscription_sum_with_period_and_quantity as (SELECT 
subscription_sum.sum-subscription_sum.wapi_original_sum as sum, -- Сумма оплаты
subscription_sum.currency,                                      -- Валюта
paid_at_billing_date,                                           -- Дата оплаты
billing_packages.account_id,                                    -- ID аккаунта
(case when stg_subscriptionupdates.period='12' then 0.8
when stg_subscriptionupdates.period='6' then 0.9
else 1
end) as period_discount,                                        -- Скидка за длительный период покупки подписки (10% за полгода, 20% за год)
promotion_type,                                                 -- Тип акции
wazzup_tariff.sum as tariff_price,                              -- Цена тарифа
coalesce(cast(stg_subscriptionupdates.quantity as INTEGER), billing_packages.quantity) as quantity, -- Кол-во покупаемых каналов
coalesce(cast(stg_subscriptionupdates.period as INTEGER), billing_packages.period) as period        -- Период покупки подписки (1, 6, 12)
 FROM `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_with_sum_and_converted_currency_billing_date` subscription_sum
left join `dwh-wazzup`.`dbt_nbespalov`.`stg_subscriptionUpdates` stg_subscriptionupdates 
on subscription_sum.guid=stg_subscriptionupdates.guid
left join `dwh-wazzup`.`dbt_nbespalov`.`stg_billingPackages` billing_packages
on billing_packages.guid=subscription_sum.subscription_id
left join `dwh-wazzup`.`analytics_tech`.`wazzup_tariff` wazzup_tariff on wazzup_tariff.currency=subscription_sum.currency 
and coalesce(stg_subscriptionupdates.tariff, billing_packages.tariff)=wazzup_tariff.tariff
where stg_subscriptionupdates.action not in ('balanceTopup','templateMessages')
),

subscription_sum_with_full_price as (

select *, (
  case when promotion_type=751175 then tariff_price*period_discount*period*quantity
  else sum
  end) as full_tarif_sum                        -- Полная сумма оплаты. Формула для расчета скидки за длительный период оплаты
from subscription_sum_with_period_and_quantity),

subscription_sum_with_full_price_with_converted_currency as (

select subscription_sum_with_full_price.*,  (case
        when subscription_sum_with_full_price.currency = 'RUR'  then full_tarif_sum
        when RUR is not null then  full_tarif_sum * RUR
        when subscription_sum_with_full_price.currency = 'EUR'  and RUR is null then  full_tarif_sum * 85 
        when subscription_sum_with_full_price.currency = 'USD'  and RUR is null then  full_tarif_sum * 75
        when subscription_sum_with_full_price.currency = 'KZT' and RUR is null then  full_tarif_sum * 0.24
    end) as full_tarif_sum_in_rubles from subscription_sum_with_full_price -- Полная сумма оплаты в рублях

left join `dwh-wazzup`.`dbt_nbespalov`.`int_payments_exchange_rates_unpivoted` exchange_rates_unpivoted 
    on exchange_rates_unpivoted._ibk = subscription_sum_with_full_price.paid_at_billing_date
    and exchange_rates_unpivoted.currency = subscription_sum_with_full_price.currency )

select * from subscription_sum_with_full_price_with_converted_currency  -- Таблица с полной стоимостью оплаты подписок