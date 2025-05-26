-- Таблица дохода с клиентов. Группировка по месяцу и сегменту
select date_trunc(paid_date, month) as date,    -- Месяц
segments_aggregated as segment,                 -- Сегмент клиента после группировки
sum(sum_in_rubles-waba_sum_in_rubles)  as revenue_without_waba_fact,    -- Доход без WABA
sum(waba_sum_in_rubles) as revenue_waba_fact                            -- Доход с WABA

from `dwh-wazzup`.`dbt_nbespalov`.`int_payments_revenue_with_waba`
where paid_date>='2024-01-01' and segments_aggregated!='unknown' and currency in ('RUR','KZT')
group by 1,2