with discount as (
 select partner_id, 
 paid_date,
 subscription_updates_currency_client,
 coalesce(rur,1)*subscription_sum_without_balance_spent_by_client_original-coalesce(rur,1)*subscripion_sum_with_discount_original as discount_sum_in_rubles, 
subscription_sum_without_balance_spent_by_client_original-subscripion_sum_with_discount_original  
as discount_sum_original   
from `dwh-wazzup`.`dbt_nbespalov`.`int_payments_billing_affiliate_subscription_payments_real_money_partner_with_client_bonus` partner_balance
 left join `dwh-wazzup`.`dbt_nbespalov`.`int_payments_exchange_rates_unpivoted` exchange_rates
 on exchange_rates.data=partner_balance.paid_date
 and exchange_rates.currency=partner_balance.subscription_updates_currency_client
 where action not in ('balanceTopup','templateMessages'))
    -- Сколько партнеры зарабатывают на бонусах при оплате подписки. Учитывается подписочная стоимость подписки минус бонусы клиента
 select partner_id as partner_id,                           -- аккаунт партнера
 subscription_updates_currency_client as currency,          -- валюта
 date_trunc(paid_date,month) as paid_month,                 -- месяц оплаты
 sum(discount_sum_in_rubles) as discount_sum_in_rubles,     -- сумма, конвертированная в рубли
 sum(discount_sum_original) as discount_sum_original        -- сумма в валюте ЛК дочки, за которую была оплата
 from discount
 group by 1,2,3