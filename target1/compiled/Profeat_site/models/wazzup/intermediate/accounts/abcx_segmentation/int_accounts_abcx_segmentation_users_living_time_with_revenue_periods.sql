with accounts_live_time as (
select * from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_abcx_segmentation_users_living_time`
),
revenue as (
select month,
        lead(month) over (partition by account_id order by month) as next_payment_month,
        account_id,
        revenue_amount
--sum(sum_in_rubles_all/period) as revenue_amount
from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_abcx_segmentation_revenue_amount_with_real_money_spend_on_subscription`
--group by 1,3
),
revenue_avg as (
select *,
        avg(revenue_amount) over (partition by account_id order by month rows BETWEEN 2 PRECEDING AND 0 FOLLOWING) as avg_sum_in_rubles
from revenue
),
defining_active_months_with_revenue_months as (
select accounts_live_time.account_id,           -- Идентификатор аккаунта, соответствует id из таблицы stg_accounts
        first_subscription_start,               -- Дата начала первой подписки
        country,                                -- Страна
        live_month,                             -- Месяц жизни пользователя. Формируется на основе истории подписок, формат 2022-11-29
        last_end_month,                         -- Месяц окончания последней подписки
        revenue_amount,                         -- Сумма прибыли
        revenue_avg.month as payment_month,     -- Месяц оплаты
        avg_sum_in_rubles ,                     -- Скользящая средняя выручка от пользователя за 3 месяца (2 предыдущих + текущий)
        accounts_live_time.client_living_type,  -- Тип жизни клиента
        row_number() over (partition by accounts_live_time.account_id,live_month order by month desc) rn,
        market_type,                            -- Рынок
        account_type,                           -- Тип аккаунта
        register_date                           -- Дата регистрации клиента
from accounts_live_time
left join revenue_avg  on revenue_avg.account_id = accounts_live_time.account_id
        and accounts_live_time.live_month = revenue_avg.month
)   -- Таблица клиентов и их времени жизни с прибылью
select *
from defining_active_months_with_revenue_months
--where rn = 1
--where account_id = 10082780
--order by live_month
--10082780
--10186725