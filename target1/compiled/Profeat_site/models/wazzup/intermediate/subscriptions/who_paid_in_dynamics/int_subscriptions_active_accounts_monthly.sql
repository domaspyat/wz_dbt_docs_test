 -- Таблица, которая показывает количество активных аккаунтов с группировкой по месяцам и валютам
 select  
          count(distinct t.account_id) as ActiveAccs,   -- Количество активных аккаунтов
          month,                                        -- Месяц
          currency                                      -- Валюта
from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_last_end_date_with_months_who_paid_without_trials_and_promised_payments` t
group by month,currency