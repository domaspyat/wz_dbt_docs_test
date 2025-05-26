with
    subscriptions as (
        select account_id,                                          -- ID аккаунта
        sum(full_tarif_sum_in_rubles) as full_tarif_sum_in_rubles   -- Полная цена тарифа в рублях
        from
            `dwh-wazzup`.`dbt_nbespalov`.`int_subscriptions_full_sum__free_and_paid_subscriptions_first_month`
        group by 1
    )
            -- Таблица, которая сравнивает прибыль от подписки с заплаченной суммой 
select
    coalesce(subscriptions.account_id, revenue.account_id) as account_id,   -- ID аккаунта
    coalesce(full_tarif_sum_in_rubles,0) as full_tarif_sum_in_rubles,       -- Полная цена тарифа в рублях
    coalesce(sum_in_rubles,0) as sum_in_rubles                              -- Оплаченная сумма в рублях
from
      `dwh-wazzup`.`dbt_nbespalov`.`int_payments_only_subscription_payments_first_month` revenue
full outer join subscriptions on revenue.account_id = subscriptions.account_id