
    -- Таблица, которая показывает количество активных аккаунтов с группировкой по неделям и валютам
select  
          count(distinct t.account_id) as ActiveAccs,           -- Количество активных аккаунтов
          week,                                                 -- Неделя
          currency                                              -- Валюта
from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_last_end_date_with_weeks_who_paid_without_trials_and_promised_payments` t
group by week,currency