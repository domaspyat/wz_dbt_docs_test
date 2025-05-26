with active_period_generation as (
  select partner_id,
        months.month,
        count(distinct all_account_id) as all_account_id,
        count(distinct active_account_id) as active_account_id
  from `dwh-wazzup`.`dbt_nbespalov`.`stg_months` months
  left join `dwh-wazzup`.`dbt_nbespalov`.`mart_partners_metrics_by_month`  partners on months.month >= partners.month
  group by partner_id,
            months.month
), all_metrics as (
SELECT 
  cast(active_period_generation.active_account_id as float64) active_account_id, --select * from dwh-wazzup.dbt_prod.mart_partners_metrics_by_month mart_partners_metrics_by_month
  cast(active_period_generation.all_account_id as float64) all_account_id,
  cast(count(distinct mart_partners_metrics_by_month.active_account_id) as float64) active_in_month,
  mart_partners_metrics_by_month.month AS month,
  mart_partners_metrics_by_month.partner_id AS partner_id,
  mart_partners_metrics_by_month.sum_in_rubles AS sum_in_rubles,
  mart_partners_metrics_by_month.sum_in_rubles_partner_paid AS sum_in_rubles_partner_paid,

  cast(paid_channels_quantity as float64) paid_channels_quantity,
  cast(paid_channels_waba_quantity as float64)  paid_channels_waba_quantity,
  cast(paid_channels_tgapi_quantity as float64)  paid_channels_tgapi_quantity,
  cast(paid_channels_wa_quantity as float64)  paid_channels_wa_quantity,
  cast(paid_channels_telegram_quantity as float64)  paid_channels_telegram_quantity,
  cast(paid_channels_instagram_quantity as float64)  paid_channels_instagram_quantity,
  cast(paid_channels_avito_quantity as float64) paid_channels_avito_quantity,
  cast(paid_channels_vk_quantity as float64) paid_channels_vk_quantity,
  cast(paid_channels_viber_quantity as float64) paid_channels_viber_quantity,

  coalesce(mart_partners_metrics_by_month.sum_in_rubles,0)  - coalesce(mart_partners_metrics_by_month.sum_in_rubles_partner_paid,0) as client_paid,
  coalesce(cast(active_period_generation.all_account_id as float64),0) - coalesce(cast(active_period_generation.active_account_id as float64),0) as never_paid,
  coalesce(cast(active_period_generation.active_account_id as float64),0) - coalesce(cast(count(distinct mart_partners_metrics_by_month.active_account_id) as float64),0) as stopped_paying,
  SAFE_DIVIDE(cast(count(distinct mart_partners_metrics_by_month.active_account_id) as float64),cast(active_period_generation.all_account_id as float64)) as working_percent,
  SAFE_DIVIDE(coalesce(cast(active_period_generation.active_account_id as float64),0) - coalesce(cast(count(distinct mart_partners_metrics_by_month.active_account_id) as float64),0),cast(active_period_generation.active_account_id as float64)) left_percent
FROM `dwh-wazzup`.`dbt_nbespalov`.`mart_partners_metrics_by_month` mart_partners_metrics_by_month
left join active_period_generation on mart_partners_metrics_by_month.partner_id = active_period_generation.partner_id
                                    and mart_partners_metrics_by_month.month = active_period_generation.month
left join `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_partners_metrics_channels_sold` channels on mart_partners_metrics_by_month.partner_id = channels.partner_id 
                                                                                        and mart_partners_metrics_by_month.month = channels.paid_month and channels.type = 'monthly'
group by 
         active_period_generation.all_account_id,
         active_period_generation.active_account_id,
         mart_partners_metrics_by_month.month,
         mart_partners_metrics_by_month.partner_id,
         mart_partners_metrics_by_month.sum_in_rubles,
         mart_partners_metrics_by_month.sum_in_rubles_partner_paid,
         cast(paid_channels_quantity as float64),
         cast(paid_channels_waba_quantity as float64),
         cast(paid_channels_tgapi_quantity as float64),
         cast(paid_channels_wa_quantity as float64),
         cast(paid_channels_telegram_quantity as float64),
         cast(paid_channels_instagram_quantity as float64),
         cast(paid_channels_avito_quantity as float64),
         cast(paid_channels_vk_quantity as float64),
         cast(paid_channels_viber_quantity as float64)
), metrics_names as (
 select 
           'active_account_id' as metric_name
           union all
 select          'all_account_id'
          union all
 select          'active_in_month'
         union all
 select          'sum_in_rubles'
         union all
 select          'sum_in_rubles_partner_paid'
         union all
 select          'never_paid'
        union all
 select          'stopped_paying'
         union all
 select          'working_percent'
         union all
 select          'left_percent'
          union all
 select          'client_paid'
          union all

 select      'paid_channels_quantity'
      union all
select      'paid_channels_wa_quantity'
      union all
select      'paid_channels_instagram_quantity'
      union all
select      'paid_channels_vk_quantity'
      union all
select      'paid_channels_tgapi_quantity'
      union all
select      'paid_channels_waba_quantity'
      union all
select      'paid_channels_avito_quantity'
      union all
select      'paid_channels_telegram_quantity'
      union all
select      'paid_channels_viber_quantity'

),  
    metric_names_and_partners as (
    select distinct partner_id,
                    type as partner_type,
                    metric_name,
                    month
                    
    from `dwh-wazzup`.`dbt_nbespalov`.`mart_partners_metrics_by_month` mart_partners_metrics_by_month
    --left join top_100 on mart_partners_metrics_by_month.partner_id = top_100.account_id
    join metrics_names on 1=1
),unpivot_data as (
select *
from all_metrics
unpivot (value for metrics in (active_account_id,
                                all_account_id,
                                active_in_month,
                                sum_in_rubles,
                                sum_in_rubles_partner_paid,
                                never_paid,
                                stopped_paying,
                                working_percent,
                                left_percent,
                                client_paid,
                                paid_channels_quantity,
                                paid_channels_waba_quantity,
                                paid_channels_tgapi_quantity,
                                paid_channels_wa_quantity,
                                paid_channels_telegram_quantity,
                                paid_channels_instagram_quantity,
                                paid_channels_avito_quantity,
                                paid_channels_vk_quantity,
                                paid_channels_viber_quantity
                                ) )),

mart_partners_metrics_by_month_dynamics as (                      
select metric_names_and_partners.* except(metric_name),
            metric_name as metrics,     -- Метрика
                value                   -- Число к метрике
from metric_names_and_partners
left join unpivot_data on metric_names_and_partners.partner_id = unpivot_data.partner_id
                            and metric_name = metrics
                            and metric_names_and_partners.month = unpivot_data.month)
select * from mart_partners_metrics_by_month_dynamics
    -- Таблица, которая собирает метрики для партнеров