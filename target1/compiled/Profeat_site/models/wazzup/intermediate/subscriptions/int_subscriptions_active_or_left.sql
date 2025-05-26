with
    left_or_active as (
        select distinct
            date_trunc(date, month) as month, 
            account_id, 
            return_or_left_status
        from
            `dwh-wazzup`.`dbt_nbespalov`.`mart_left_and_returned_accounts_with_partner_and_account_type_with_dates`
    ),

    left_or_active_wide as (
        
        select month,   -- Месяц
        account_id,     -- ID аккаунта
        max(case when return_or_left_status in ('active','returned') then True else False end) as is_active,    -- Активный аккаунт?
        max(case when return_or_left_status ='left' then True  else False  end) as has_left,                    -- Клиент уходил?
        from left_or_active
        group by 1,2

    ),

    subscription_retention as (
        select account_id,   -- ID аккаунта
        max(case when  type = 'subscriptions' and time_period_number = 2 and cnt >= 7 then True else False end) as is_retained_second_month, -- Клиент сохранился 2 месяца?
        max(first_subscription_start) as first_subscription_start   -- Дата начала первой подписки
        from `dwh-wazzup`.`dbt_nbespalov`.`mart_subscriptions_retention`
        group by 1
    ),


    retained_second_month as (
        select
            left_or_active_wide.*,
            is_retained_second_month,
            first_subscription_start
        from left_or_active_wide
        left join
            subscription_retention
            on left_or_active_wide.account_id = subscription_retention.account_id
    )
    -- Таблица с активными или ушедшими клиентами
select *
from  retained_second_month