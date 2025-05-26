with subscriptions as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_payments_only_subscription_payments_and_wapi_sessions_real_money`
    where action not in ('balanceTopup','templateMessages')
)
    -- Таблица платежей реальными деньгами за изменения не 'balanceTopup','templateMessages'
   select *, sum_in_rubles_spent_on_subscription as sum_in_rubles from subscriptions