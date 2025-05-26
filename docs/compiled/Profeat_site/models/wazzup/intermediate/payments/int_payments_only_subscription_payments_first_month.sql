with subscripton_paid_with_real_money as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_payments_only_subscription_payments_real_money`
),

revenue_first_month as (
    select subscripton_paid_with_real_money.account_id, -- ID аккаунта
    sum(sum_in_rubles) as sum_in_rubles                 -- Сумма оплаты в рублях
    from subscripton_paid_with_real_money inner join `dwh-wazzup`.`dbt_nbespalov`.`stg_accounts` accounts 
    on accounts.account_id=subscripton_paid_with_real_money.account_id 
    where subscripton_paid_with_real_money.paid_date<=date_add(accounts.register_date, interval 1 month)
    and accounts.type='standart' and sum_in_rubles!=0
    group by 1
    )
    -- Таблица платежей за подписки в первый месяц после регистрации
select * from revenue_first_month