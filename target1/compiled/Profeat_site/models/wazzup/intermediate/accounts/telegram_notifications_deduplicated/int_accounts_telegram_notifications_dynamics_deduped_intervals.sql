with distinct_intervals as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_telegram_notifications_dynamics_distinct_intervals`
),
subscription_all as (
    select account_id,
           start_date,
           end_date 
    from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_telegram_notifications_dynamics`
),
deduped_intervals AS (
          SELECT a.account_id,  -- ID аккаунта
          a.start_date,         -- Дата начала действия уведомлений
          a.end_date            -- Дата окончания действия уведомлений
          FROM distinct_intervals a
          JOIN subscription_all b
          ON a.account_id = b.account_id 
          AND a.start_date BETWEEN b.start_date AND b.end_date 
          AND a.end_date BETWEEN b.start_date AND b.end_date
          GROUP BY 1,2,3)
    -- Таблица аккаунтов, которая показывает даты начала и окончания действия уведомлений в Telegram с недублированными интервалами после группировки
select * from deduped_intervals