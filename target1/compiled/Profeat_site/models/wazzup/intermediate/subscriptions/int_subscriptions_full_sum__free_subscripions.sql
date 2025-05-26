with billing_packages_with_tarif_info as (SELECT billingPackages.*, 
(case when billingPackages.period=12 then 0.8
when billingPackages.period=6 then 0.9
else 1
end) as period_discount,                    -- Скидка за длительный период покупки подписки (10% за полгода, 20% за год)
cast(created_at as date) as created_date,   -- Дата создания бесплатной подписки
wazzup_tariff.sum as tariff_price,          -- Цена тарифа
accounts.currency,                          -- Валюта
billingPackages.type as subscription_type   -- Тип (транспорт) подписки
 FROM `dwh-wazzup`.`dbt_nbespalov`.`stg_billingPackages`  billingPackages 
 left join `dwh-wazzup`.`dbt_nbespalov`.`stg_accounts` accounts on accounts.account_id=billingPackages.account_id
left join `dwh-wazzup`.`analytics_tech`.`wazzup_tariff` wazzup_tariff 
on wazzup_tariff.currency=accounts.currency 
and billingPackages.tariff=wazzup_tariff.tariff
where is_free=True and period<13),

subscription_sum_with_full_price as (

select *, tariff_price*period_discount*period*quantity as full_tarif_sum 
from billing_packages_with_tarif_info),

subscrption_with_full_price_converted as (

select subscription_sum_with_full_price.*,  (case
        when subscription_sum_with_full_price.currency = 'RUR'  then full_tarif_sum
        when RUR is not null then  full_tarif_sum * RUR
        when subscription_sum_with_full_price.currency = 'EUR'  and RUR is null then  full_tarif_sum * 85 
        when subscription_sum_with_full_price.currency = 'USD'  and RUR is null then  full_tarif_sum * 75
        when subscription_sum_with_full_price.currency = 'KZT' and RUR is null then  full_tarif_sum * 0.24
    end) as full_tarif_sum_in_rubles from subscription_sum_with_full_price  -- Цена оплаты в рублях с фикс курсом

left join `dwh-wazzup`.`dbt_nbespalov`.`int_payments_exchange_rates_unpivoted` exchange_rates_unpivoted 
    on exchange_rates_unpivoted._ibk = subscription_sum_with_full_price.created_date
    and exchange_rates_unpivoted.currency = subscription_sum_with_full_price.currency)

select subscrption_with_full_price_converted.*, 
accounts.type as account_type from subscrption_with_full_price_converted 
inner join `dwh-wazzup`.`dbt_nbespalov`.`stg_accounts` accounts 
on subscrption_with_full_price_converted.account_id=accounts.account_id
where full_tarif_sum_in_rubles is not null
and accounts.type not in ('partner-demo','employee','child-postpay')