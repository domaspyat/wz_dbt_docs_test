with left_data_pre as (
select distinct
        mart.currency,
        mart.account_id,
        first_value(live_month) over (partition by mart.account_id order by live_month) first_active_month,
        live_month,
        case when account_segment_type = 'Конечный клиент' then 'final_client'
        when account_segment_type = 'Оф. партнер' then 'of-partner'
        when account_segment_type = 'Тех. партнер' then 'tech-partner'
        end as segments_aggregated,
        case when first_value(live_month) over (partition by mart.account_id order by live_month) = date_trunc(register_date,month) then 'new' else 'old' end as client_type,
        cum_sum_up_to_live_month,
        registration_source_agg,
        LT
from `dwh-wazzup`.`dbt_nbespalov`.`mart_left_and_returned_accounts_with_partner_and_account_type_with_dates_and_ltv_info` mart
join `dwh-wazzup`.`dbt_nbespalov`.`stg_accounts`  on mart.account_id = stg_accounts.account_id
join `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_attribution_data` sources on stg_accounts.account_id = sources.account_id
where account_segment_type !='all'
),
left_data_rn as (
    select *,
        date_add(left_data_pre.first_active_month,interval 11 month) month_after_eleven_months,
        date_add(left_data_pre.first_active_month,interval 12 month) month_after_year,        
        row_number() over (partition by account_id,month order by live_month desc) rn
from left_data_pre
join `dwh-wazzup`.`dbt_nbespalov`.`stg_months` months on left_data_pre.first_active_month <= months.month
                        and months.month <= date_add(left_data_pre.first_active_month,interval 11 month)
                        and month >= live_month
),
left_data as (
    select *,
    from left_data_rn
    where rn = 1
            and month = month_after_eleven_months

),

 left_active_all as (
select  case when grouping(segments_aggregated) =1 then 'all' else segments_aggregated end segments_aggregated_ ,
        month,
        case when grouping(registration_source_agg) = 1 then 'all' else registration_source_agg end registration_source_agg_,
        case when grouping(client_type) = 1 then 'all' else client_type end client_type_,
        avg(cum_sum_up_to_live_month) LTV,
        sum(cum_sum_up_to_live_month) LTV_sum,
        count(distinct account_id) LTV_users_count,
      'all' as market_type
from left_data
group by GROUPING SETS 
(
  (segments_aggregated,month),
        (client_type,month),
        (segments_aggregated,month,client_type),
        (month),

  (segments_aggregated,month,registration_source_agg),
        (client_type,month,registration_source_agg),
        (segments_aggregated,month,client_type,registration_source_agg),
        (month,registration_source_agg)

)
)
,left_active_eur_usd_rur_kzt as (
select  case when grouping(segments_aggregated) =1 then 'all' else segments_aggregated end segments_aggregated_ ,
        month,
                case when grouping(registration_source_agg) = 1 then 'all' else registration_source_agg end registration_source_agg_,
        case when grouping(client_type) = 1 then 'all' else client_type end client_type_,
        --count(distinct case when return_or_left_status in ('left','came_back_after_leaving_period') then account_id end )/count(distinct case when return_or_left_status in ('active','returned') then account_id end) left_share,
        avg(cum_sum_up_to_live_month) LTV,
        sum(cum_sum_up_to_live_month) LTV_sum,
        count(distinct account_id) LTV_users_count,
      case when currency in ('USD','EUR') then  'usd_eur' else 'ru_kzt' end  as market_type
from left_data
where currency in ('RUR','KZT','EUR','USD')
group by GROUPING SETS 
(
  (segments_aggregated,month,market_type),
        (client_type,month,market_type),
        (segments_aggregated,month,client_type,market_type),
        (month,market_type),

  (segments_aggregated,month,registration_source_agg,market_type),
        (client_type,month,registration_source_agg,market_type),
        (segments_aggregated,month,client_type,registration_source_agg,market_type),
        (month,registration_source_agg,market_type)

)
)

,left_active_rur as (
select  case when grouping(segments_aggregated) =1 then 'all' else segments_aggregated end segments_aggregated_ ,   -- Сегмент после группировки
        month,                                          -- Рассматриваемый месяц
                case when grouping(registration_source_agg) = 1 then 'all' else registration_source_agg end registration_source_agg_,   -- агрегированный источник регистрации при регистрации
        case when grouping(client_type) = 1 then 'all' else client_type end client_type_,   -- Тип клиента
        avg(cum_sum_up_to_live_month) LTV,              -- lifetimevalue клиентов
        sum(cum_sum_up_to_live_month) LTV_sum,          -- Сумма LTV клиентов
        count(distinct account_id) LTV_users_count,     -- Количество юзеров за LTV
      currency as market_type                           -- Рынок
from left_data
where currency in ('RUR','KZT','EUR','USD')
group by GROUPING SETS 
(
  (segments_aggregated,month,market_type),
        (client_type,month,market_type),
        (segments_aggregated,month,client_type,market_type),
        (month,market_type),

  (segments_aggregated,month,registration_source_agg,market_type),
        (client_type,month,registration_source_agg,market_type),
        (segments_aggregated,month,client_type,registration_source_agg,market_type),
        (month,registration_source_agg,market_type)
)
)
    -- Таблица ушедших и вернувшихся клиентов с партнером, датами и LTV после группировки
select * from left_active_all
union all
select * from left_active_rur
union all
select * from left_active_eur_usd_rur_kzt