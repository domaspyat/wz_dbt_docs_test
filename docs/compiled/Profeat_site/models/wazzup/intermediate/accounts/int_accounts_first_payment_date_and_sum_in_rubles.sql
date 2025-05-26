select account_id,                              -- ID аккаунта
        segment_type,                           -- Сегмент клиента
        min(paid_date) as first_payment_date,   -- Дата первой оплаты
        sum(sum_in_rubles) as sum_in_rubles     -- Сумма оплаты в рублях
from `dwh-wazzup`.`dbt_nbespalov`.`int_payments_revenue_with_waba`
where sum_in_rubles!=0
group by 1,2
    -- Таблица с датами первой оплаты и её суммой по аккаунтам