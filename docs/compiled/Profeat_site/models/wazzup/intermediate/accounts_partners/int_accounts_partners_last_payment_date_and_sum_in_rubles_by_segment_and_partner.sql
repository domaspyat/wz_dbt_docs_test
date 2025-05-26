with last_payment_info as (
select account_id,      -- ID аккаунта
        segment_type,   -- Сегмент
        partner_id,     -- ID аккаунта партнера
        paid_date,      -- Дата последней оплаты
        row_number() over (partition by account_id,partner_id order by paid_date desc) rn,
        sum(sum_in_rubles) as sum_in_rubles     -- Сумма оплаты в рублях
from `dwh-wazzup`.`dbt_nbespalov`.`int_payments_revenue_with_waba`
where sum_in_rubles!=0
group by 1,2,3,4)
    -- Таблица, которая показывает последнюю оплату партнеров по сегменту и партнеру
select *except(rn)
from last_payment_info 
where rn = 1
--and account_id = 93135096