with mart_partners_metrics_by_month_dynamics as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_partners_merics_dynamics`
),
top_100 as (
    select 
          account_id,
          current_quarter
    from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_partners_top_100_ru_of_partners`
   -- where date_trunc(current_date(),quarter) = current_quarter
)   -- Динамика партнерских метрик по месяцам
select mart_partners_metrics_by_month_dynamics.*,
        russian_country_name,                                                           -- Название страны на русском языке
        account_currency_by_country,                                                    -- Валюта аккаунта по стране
        region_international,                                                           -- Регион
        partner_discount,                                                               -- Скидка партнера
        partner_register_date,                                                          -- Дата выдачи партнерки
        case when top_100.account_id is not null then TRUE else FALSE end as is_top_100 -- Партнер входит в топ100 по выручке?
from mart_partners_metrics_by_month_dynamics
left join top_100 on mart_partners_metrics_by_month_dynamics.partner_id = top_100.account_id 
                                            and date_trunc(mart_partners_metrics_by_month_dynamics.month,quarter) = top_100.current_quarter
left join `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info` profile_info on mart_partners_metrics_by_month_dynamics.partner_id = profile_info.account_Id