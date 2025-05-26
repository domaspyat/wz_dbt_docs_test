with notifications_all as (
   select * from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_telegram_notifications_dynamics`
),

check_times AS (
    SELECT account_id, start_date as TIME FROM notifications_all
           UNION DISTINCT
    SELECT account_id, end_date as TIME FROM notifications_all
    )
    -- Таблица аккаунтов и датами начала и окончания действия уведомлений в Telegram (в разных записях)
select * from check_times