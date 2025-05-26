select account_id, -- аккаунт партнера
        sum((case when partners_card_and_bank_payments.currency='RUR' then 1 else exchange_rates.RUR end)*sum) as billing_sum,  -- сумма пополнения ЛК
        min(occured_date) as first_billing_date_partner -- дата первого пополнения ЛК
from `dwh-wazzup`.`dbt_nbespalov`.`int_payments_lk_card_and_bank` partners_card_and_bank_payments
left join  `dwh-wazzup`.`dbt_nbespalov`.`int_payments_exchange_rates_unpivoted` exchange_rates 
          on partners_card_and_bank_payments.occured_date=exchange_rates._ibk and partners_card_and_bank_payments.currency=exchange_rates.currency
group by 1
    -- Пополнение ЛК партнеров