with difference_last_month as (
        select * from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_deduplicated_by_id_with_previous_and_next_subscription`
    ),
 
 
 returned_revenue as (

 select date_trunc(paid_date,month) as paid_month,  -- Месяц оплаты
 paid_date,                                         -- Дата оплаты
 difference_last_month.account_id,                  -- ID аккаунта
 sum_in_rubles,                                     -- Сумма оплаты в рублях
 difference_last_month.subscription_id from difference_last_month
 left join `dwh-wazzup`.`dbt_nbespalov`.`int_payments_only_subscription_payments_real_money` real_money 
 on real_money.paid_date=difference_last_month.subscription_start
 and  real_money.subscription_id=difference_last_month.subscription_id
 where difference_subscription_start_and_previous_subscription_end>20 and real_money.action='renewal')
    -- Таблица, которая показывает сколько денег принесли клиенты, которые вернулись
 select * from returned_revenue