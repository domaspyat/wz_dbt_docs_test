with forecasted_data as (
  SELECT cast(date_trunc(forecast_timestamp,month) as date) as registration_month, 
  sum(cast(forecast_value as integer)) as registrations_by_month,
  sum(cast(prediction_interval_upper_bound as integer))  as registrations_by_month_upper_bound,
  sum(cast(prediction_interval_lower_bound as integer))  as registrations_by_month_lower_bound
   FROM ML.FORECAST(MODEL `dwh-wazzup.1066401_wazzup_plan_vs_fact_metrics.model_registrations_forecasting`,  STRUCT(31 AS horizon, 0.8 AS confidence_level))
group by 1
),

fact_data as (
  select date_trunc(registration_date,month), 
  sum(registration_by_day) as registrations_by_month,
  sum(registration_by_day) as registrations_by_month_upper_bound,
  sum(registration_by_day) as registrations_by_month_lower_bound
  
    from `dwh-wazzup.1066401_wazzup_plan_vs_fact_metrics.table_for_forecasting_registrations`
group by 1
),

registration_data_forecasted_and_fact_union as (

select * from forecasted_data 
union all 
select * from fact_data)
    -- Таблица c прогнозируемым и фактическим количеством регистраций помесячно
select registration_month,      -- Месяц регистрации
sum(registrations_by_month) as registration_by_month_forecasted,                            -- Прогнозируемое количество регистраций (фактическое после окончания месяца)
sum(registrations_by_month_upper_bound) as registrations_by_month_upper_bound_forecasted,   -- Прогнозируемое количество регистраций по верхней грани множества (фактическое после окончания месяца)
sum(registrations_by_month_lower_bound) as registrations_by_month_lower_bound_forecasted    -- Прогнозируемое количество регистраций по нижней грани множества (фактическое после окончания месяца)
from registration_data_forecasted_and_fact_union
group by 1