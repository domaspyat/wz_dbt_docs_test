WITH QuarterlyRevenue AS (
  select 
      month_dynamics.partner_id as partner_id,
      date_trunc(month,quarter) quarter,
      sum(value) revenue
    from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_partners_merics_dynamics` month_dynamics
    join `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info` profile_info 
    on month_dynamics.partner_id = profile_info.account_Id 
    where metrics = 'sum_in_rubles_partner_paid'
         and profile_info.type = 'partner'
         and profile_info.currency in ('RUR','KZT')
    group by month_dynamics.partner_id ,2
),

-- Добавляем квартал n-1 как предыдущий квартал и объединяем эти значения
PreviousQuarterRevenue AS (
  SELECT
    a.partner_id,
    a.quarter AS current_quarter,
    b.revenue AS previous_quarter_revenue
  FROM 
    QuarterlyRevenue a
  LEFT JOIN 
    QuarterlyRevenue b
  ON 
    a.partner_id = b.partner_id 
    AND
          DATE_SUB(a.quarter, INTERVAL 1 QUARTER) = b.quarter
)
,calculating_top as (
    SELECT 
        current_quarter, 
        partner_id, 
        previous_quarter_revenue
  FROM 
    PreviousQuarterRevenue
)       -- Партнеры, которые принесли более 200к выручки за прошлый квартал
SELECT 
  current_quarter,                          -- Рассматриваемый квартал
  partner_id as account_id,                 -- ID аккаунта партнера
  round(previous_quarter_revenue,0) revenue -- Выручка
FROM calculating_top
WHERE 
  round(previous_quarter_revenue,0) >= 200000
  and current_quarter <= date_trunc(current_date(),quarter)