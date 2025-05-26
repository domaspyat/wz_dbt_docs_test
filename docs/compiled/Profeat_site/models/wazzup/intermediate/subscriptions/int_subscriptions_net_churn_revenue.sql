

with in_revenue_aggregated as (
    select account_id,  	    -- ID аккаунта
    revenue_type,               -- Тип дохода
    paid_month,                 -- Месяц оплаты
    sum(sum_in_rubles)          -- Сумма оплаты
    from 
    `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_real_money_paid_old_and_new_revenue_type`
    group by 1,2,3
),

lost_revenue_due_to_downsell as ( -- Таблица с понжиением дохода из-за понижения тарифа
    select account_id,                          -- ID аккаунта
    'downsell_loss' as revenue_type,            -- Тип дохода  - понижение тарфиа
    paid_month,                                 -- Месяц оплаты
    -sum(lost_sum_in_rubles) as sum_in_rubles   -- Сумма оплаты в рублях
    from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_lost_revenue_due_to_quantity_and_tariff_change`
    group by 1,2,3
),

lost_revenue_due_to_churn as (  -- Таблица с понижением дохода из-за ухода клиентов
    select account_id,                          -- ID аккаунта
    'churn_loss' as revenue_type,               -- Тип дохода - уход клиента
    churn_month as paid_month,                  -- Месяц оплаты
    -sum(lost_sum_in_rubles) from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_lost_revenue_due_to_churn`  -- Сумма оплаты в рублях
    group by 1 ,2, 3
),

returned_users_revenue as (     -- Таблица с вернувшимися клиентами
    select account_id,                          -- ID аккаунта
    'returned_revenue' as revenue_type,         -- Тип дохода - клиент вернулся
    paid_month,                                 -- Месяц оплаты
    sum(sum_in_rubles) from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_returned_users_revenue`           -- Сумма оплаты в рублях
    group by 1 ,2, 3
),


all_revenue_type_union as (

select  * from lost_revenue_due_to_downsell
union all
select  * from lost_revenue_due_to_churn
union all 
select * from in_revenue_aggregated
union all
select * from returned_users_revenue)

select * from all_revenue_type_union    -- Таблица, которая показывает причину и сумму изменения дохода
pivot(sum(sum_in_rubles) as sum_in_rubles for revenue_type in ('downsell_loss','returned_revenue','churn_loss','new_users_revenue','old_users_new_subscription','upsell_revenue'))