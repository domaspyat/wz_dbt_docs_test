

  SELECT    -- Таблица, которая показывает количество вернувшихся клиентов еженедельно
       week,        -- Неделя возврата
       currency,    -- Валюта
       count(distinct account_Id ) as returned, -- Сколько клиентов вернулось за неделю
FROM `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_last_end_date_with_weeks_who_paid_without_trials_and_promised_payments`
where payment_type_weekly = 'return_payment_weekly'
group by 1,2