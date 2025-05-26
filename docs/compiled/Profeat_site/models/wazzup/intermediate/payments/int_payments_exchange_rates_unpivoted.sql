with exchange_rates as (
    select *except(cor_rate),
     coalesce(cor_rate,first_value(cor_rate ignore nulls) over (partition by currency,nominal order by data ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) cor_rate
    from `dwh-wazzup`.`dbt_nbespalov`.`stg_exchangeRates`
)   -- Таблица с курсами обмена валют
select *
from exchange_rates
pivot
(avg(cor_rate) FOR nominal IN ('USD', 'RUR','EUR')

)