with metrics as (
select 'daughters_count' as metric
      union all
select      'c2'
      union all
select      'discount_sum_in_rubles'
      union all
select      'paid_channels_quantity'
      union all
select      'paid_channels_wa_quantity'
      union all
select      'paid_channels_instagram_quantity'

      union all
select      'paid_channels_vk_quantity'
      union all
select      'paid_channels_wa_sum'
      union all
select      'paid_channels_instagram_sum'

      union all
select      'left_daughters_count'
      union all
select      'paid_channels_tgapi_quantity'
      union all
select      'paid_channels_waba_quantity'
      union all
select      'paid_channels_tgapi_sum'
      union all
select      'paid_channels_waba_sum'
      union all
select      'paid_channels_vk_sum'
      union all
select      'c1'
      union all
select      'reg_daughters_count'
      union all
select      'paid_channels_avito_quantity'
      union all
select      'paid_channels_telegram_quantity'
      union all
select      'paid_channels_avito_sum'
      union all
select      'paid_channels_telegram_sum'
      union all
select      'earned_sum_referals'
          
)
    ,generated_months as (
select distinct active_periods.partner_id,month ,metric,currency,account_language                       
from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_partners_metrics_for_partners_active_users_for_whom_partner_paid_count` active_periods
join `dwh-wazzup`.`dbt_nbespalov`.`stg_months` months on 1=1
join `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info` profile_info on active_periods.partner_id = profile_info.account_id and is_employee is False and profile_info.type = 'partner'
join metrics on 1 = 1
where month >= '2024-03-01'
        and month <= '2024-12-01')
 , unpivot_monthly_plus_lost_data as (
select unpivot_monthly.*,
        coalesce(lost_data.lost_revenue_value,lost_data_transports.lost_revenue_value) as lost_revenue,
        lost_sum_referals
from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_partners_metrics_for_partners_all_metrics_together_unpivot_period_type` unpivot_monthly
left join `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_partners_metrics_for_partners_lost_revenue_due_to_churn` lost_data
                                            on unpivot_monthly.partner_id = lost_data.partner_id
                                                and unpivot_monthly.type = lost_data.type
                                                and unpivot_monthly.date = lost_data.churn_month
                                                and unpivot_monthly.metric = 'discount_sum_in_rubles'
                                                and lost_data.lost_revenue_transport = 'lost_revenue'

left join `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_partners_metrics_for_partners_lost_revenue_due_to_churn` lost_data_transports
                                            on unpivot_monthly.partner_id = lost_data_transports.partner_id
                                                and unpivot_monthly.type = lost_data_transports.type
                                                and unpivot_monthly.date = lost_data_transports.churn_month
                                                and replace(metric,'paid_channels_','') = replace(lost_data_transports.lost_revenue_transport,'lost_','')
left join `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_partners_metrics_for_partners_referal_earnings` referal_earning 
                                                on unpivot_monthly.partner_id = referal_earning.partner_id
                                                and unpivot_monthly.type = referal_earning.type
                                                and unpivot_monthly.date = referal_earning.paid_month
                                                and unpivot_monthly.metric = 'earned_sum_referals'




),unpivot_all as (
    select * 
    from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_partners_metrics_for_partners_all_metrics_together_unpivot_all_type`
)   -- Таблица с ключевыми метриками партнеров. Задача - https://wazzup.planfix.ru/task/1136620
select months.partner_id,                       -- ID аккаунта партнера
        currency,                               -- Валюта
        account_language,                       -- язык ЛК пользователя, указанный на текущий момент
        month as date,                          -- Дата начала месяца, в случае ежемесячных данных, current_date в случае данных за весь период
        months.metric,                          -- Название метрики. С общим списком можно ознакомиться тут https://www.notion.so/1136620_-373abd994fbd4dafbbfc7f39a3332846
        unpivot_monthly_plus_lost_data.value,   -- Значение метрики в рассматриваемый месяц (date)
        coalesce(unpivot_monthly_plus_lost_data.type,unpivot_all.type) type,    -- monthly, в случае ежемесячных данных, all - в случае данных за весь период, null - когда у партнера было нулевое значение value для данной metric
        unpivot_all.value as all_value,         -- Значение метрики за весь период
        lost_revenue,                           -- Потерянная выручка. Сколько партнер потерял денег из-за отвала (сменился партнер, дочка стала сама платить, аккаунт просто перестал быть активным) клиента. Подробное описание - https://www.notion.so/1136620_-373abd994fbd4dafbbfc7f39a3332846
        lost_sum_referals                       -- Сумма выручки, если бы партнер этих сам вел клиентов с учетом его текущего процента скидки. Подробное описание - https://www.notion.so/1136620_-373abd994fbd4dafbbfc7f39a3332846
from generated_months months
left join unpivot_monthly_plus_lost_data 
                       on months.partner_id = unpivot_monthly_plus_lost_data.partner_id 
                          and months.month = unpivot_monthly_plus_lost_data.date
                          and months.metric = unpivot_monthly_plus_lost_data.metric
left join unpivot_all on months.metric = unpivot_all.metric
                          and months.partner_id = unpivot_all.partner_id