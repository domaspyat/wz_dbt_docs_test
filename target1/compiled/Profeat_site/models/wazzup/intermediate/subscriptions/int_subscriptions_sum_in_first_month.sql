with subscription_sum as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_with_sum_and_converted_currency`
),

subscription_info as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_subscription_updates__corect_date_and_filledna_tarif_period_quantity`
),

profile_info as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info`
)
 -- Таблица с суммой оплаты в первый месяц после регистрации. Длинные подписки делятся на свой период
SELECT subscription_sum.account_id,                                             -- ID аккаунта
sum(sum_in_rubles/subscription_info.period_new) as sum_in_rubles_by_period,     -- Сумма оплаты, разделенная на период подписки
sum(sum_in_rubles) as sum_in_rubles                                             -- Сумма оплаты
FROM subscription_sum inner join  subscription_info
on subscription_sum.guid=subscription_info.guid
inner join profile_info on profile_info.account_id=subscription_sum.account_id
where subscription_sum.start_date<=date_add(register_date, interval 1 month)
and period_new is not null
group by 1