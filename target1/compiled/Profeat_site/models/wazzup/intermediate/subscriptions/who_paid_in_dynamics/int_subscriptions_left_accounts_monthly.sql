

with counting_left_and_returned as (
    select *,lead(payment_type_monthly) over (partition by account_id order by subscription_start,month) as next_return_payments
    from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_last_end_date_with_months_who_paid_without_trials_and_promised_payments`
    where date_trunc(data_otvala,month) <= current_date
)
SELECT  -- Таблица, которая показывает количество ушедших клиентов ежемесячно
        date_trunc(data_otvala,month) month_of_leave_date,  -- Месяц откола
        currency,                                           -- Валюта
        count(distinct case when client_type_with_churn_period_20 in ('came_back_after_leaving_period','did_not_come_back') then account_Id  end) as left_guys, -- Сколько клиентов ушло за месяц
        count(distinct case when client_type_with_churn_period_20 in ('came_back_after_leaving_period','did_not_come_back') and next_return_payments = 'return_payment_monthly' then account_id end) returned_on_left_day   -- Сколько клиентов купили подписку в день откола
FROM counting_left_and_returned
group by 1,2