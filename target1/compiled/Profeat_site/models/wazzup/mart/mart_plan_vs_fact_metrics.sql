with plan_data as (
  SELECT (case when segment='Конечные клиенты' then 'final_client'
  when segment='Оф.партнеры' then 'of-partner'
  when segment='Тех.партнеры' then 'tech-partner'
  end) as segment,
  date,
  revenue,
  active_clients,
  registrations,
  waba_rur,
  waba_global
  FROM `dwh-wazzup.1066401_wazzup_plan_vs_fact_metrics.wazzup_plan_fact_metrics_from_google_sheet` 
),

forecasts_registrations_by_months as (
    select *,
    'final_client' as segment
     from `dwh-wazzup`.`dbt_nbespalov`.`int_forecasts_registrations_by_months`
     where registration_month<=date_trunc(current_date(), month)
),

forecast_payments_by_months as (
    select  *,
    'final_client' as segment
    from `dwh-wazzup.1066401_wazzup_plan_vs_fact_metrics.forecast_revenue_with_old_and_new_users_features`
)
    -- Метрики план vs факт

SELECT plan_data.segment,                                                           -- Сегмент
plan_data.date,                                                                     -- Рассматривемая дата
plan_data.revenue as revenue_without_waba_plan,                                     -- Выручка без WABA план
plan_data.active_clients as active_clients_plan,                                    -- Количество активных клиентов план
plan_data.registrations as registrations_plan,                                      -- Количество регистраций план
forecasts_registrations_by_months.registration_by_month_forecasted,                 -- Прогнозируемое количество регистраций
forecasts_registrations_by_months.registrations_by_month_lower_bound_forecasted,    -- Прогнозируемое количество регистраций по нижней границе
forecasts_registrations_by_months.registrations_by_month_upper_bound_forecasted,    -- Прогнозируемое количество регистраций по верхней границе
forecast_payments_by_months.sum_in_rubles_forecasted,                               -- Прогнозируемая выручка в рублях
revenue_without_waba_fact,                                                          -- Выручка без WABA факт
registration_by_month_fact,                                                         -- Количество регистраций факт
final_clients_by_month_fact as active_clients_by_month_fact,                        -- Количество активных клиентов факт
revenue_waba_fact,                                                                  -- Выручка за WABA факт
waba_rur,                                                                           -- Выручка за WABA с ру рынка
waba_global                                                                         -- Выручка за WABA с зарубежки
FROM plan_data
left join `dwh-wazzup`.`dbt_nbespalov`.`int_revenue_aggregated_by_month_and_segment` revenue_fact_data on plan_data.date=revenue_fact_data.date and plan_data.segment=revenue_fact_data.segment
left join dwh-wazzup.1066401_wazzup_plan_vs_fact_metrics.registration_aggregated_by_month_fact registrations_fact_data on registrations_fact_data.date=plan_data.date and registrations_fact_data.segment=plan_data.segment
left join  dwh-wazzup.1066401_wazzup_plan_vs_fact_metrics.1066401_active_accounts_by_month_final_clients active_accounts
on active_accounts.date=plan_data.date and active_accounts.segment=plan_data.segment
left join forecasts_registrations_by_months  on forecasts_registrations_by_months.registration_month=plan_data.date 
and forecasts_registrations_by_months.segment = plan_data.segment
left join forecast_payments_by_months 
on forecast_payments_by_months.segment=plan_data.segment 
and forecast_payments_by_months.paid_month=plan_data.date