
  SELECT -- Таблица, которая показывает количество вернувшихся клиентов ежемесячно
       month,       -- Месяц возврата
       currency,    -- Валюта
       count(distinct account_Id ) as returned, -- Сколько клиентов вернулось за месяц
FROM `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_last_end_date_with_months_who_paid_without_trials_and_promised_payments`
where payment_type_monthly = 'return_payment_monthly'
group by 1,2