select account_id,      -- ID аккаунта
        segment_type,   -- Сегмент
        partner_id,     -- ID аккаунта партнера
        min(paid_date) as first_payment_date,   -- Дата первой оплаты
        sum(sum_in_rubles) as sum_in_rubles     -- Сумма оплаты в рублях
from `dwh-wazzup`.`dbt_nbespalov`.`int_payments_revenue_with_waba`
where sum_in_rubles!=0
group by 1,2,3
    -- Таблица, которая показывает первую оплату партнеров по сегменту и партнеру