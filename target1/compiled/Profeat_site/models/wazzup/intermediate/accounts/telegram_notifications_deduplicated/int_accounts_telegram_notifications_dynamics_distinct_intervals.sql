with check_times as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_telegram_notifications_dynamics_check_times`
),

distinct_intervals AS (
        SELECT account_id,                                                  -- ID аккаунта
        TIME AS start_date,                                                 -- Дата начала действия уведомлений
        LEAD(TIME) OVER (PARTITION BY account_id ORDER BY TIME) end_date    -- Дата окончания действия уведомлений
        FROM check_times)
    -- Таблица аккаунтов, которая показывает даты начала и окончания действия уведомлений в Telegram с комбинированными интервалами
select * from distinct_intervals