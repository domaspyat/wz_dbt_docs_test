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
        case when date_trunc(date,month) = date_trunc(register_date,month) then 'new' else 'old' end as client_type,
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
        row_number() over (partition by account_id,month order by live_month desc) rn
from left_data_pre
join `dwh-wazzup`.`dbt_nbespalov`.`stg_months` months on left_data_pre.first_active_month <= months.month
                        and month >= live_month
),
left_data as (
    select *,
    from left_data_rn
    where rn = 1

),
left_data_share as (
select 
        mart.currency,
        mart.account_id,
        return_or_left_status,
        date_trunc(date,month) month,
        case when account_segment_type = 'Конечный клиент' then 'final_client'
              when account_segment_type = 'Оф. партнер' then 'of-partner'
              when account_segment_type = 'Тех. партнер' then 'tech-partner'
              end as segments_aggregated,
        case when date_trunc(date,month) = date_trunc(register_date,month) then 'new' else 'old' end as client_type,
        cum_sum_up_to_live_month,
        registration_source_agg,
        LT
from `dwh-wazzup`.`dbt_nbespalov`.`mart_left_and_returned_accounts_with_partner_and_account_type_with_dates_and_ltv_info` mart
join `dwh-wazzup`.`dbt_nbespalov`.`stg_accounts`  on mart.account_id = stg_accounts.account_id
join `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_attribution_data` sources on stg_accounts.account_id = sources.account_id
where account_segment_type !='all'
)

,left_active_all_share as (
    select  case when grouping(segments_aggregated) =1 then 'all' else segments_aggregated end segments_aggregated_ ,
        month,
        case when grouping(registration_source_agg) = 1 then 'all' else registration_source_agg end registration_source_agg_,
        case when grouping(client_type) = 1 then 'all' else client_type end client_type_,
        count(distinct case when return_or_left_status in ('left','came_back_after_leaving_period') then account_id end ) left_users_count,
        count(distinct case when return_or_left_status in ('active','returned') then account_id end) active_users_count_for_left_share,
        count(distinct case when return_or_left_status in ('left','came_back_after_leaving_period') then account_id end )/Case when count(distinct case when return_or_left_status in ('active','returned') then account_id end) = 0 then 1 else count(distinct case when return_or_left_status in ('active','returned') then account_id end) end left_share,
      'all' as market_type
from left_data_share
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
),
 left_active_all as (
select  case when grouping(segments_aggregated) =1 then 'all' else segments_aggregated end segments_aggregated_ ,
        month,
        case when grouping(registration_source_agg) = 1 then 'all' else registration_source_agg end registration_source_agg_,
        case when grouping(client_type) = 1 then 'all' else client_type end client_type_,
        avg(LT) LT,
        sum(LT) sum_LT,
        count(distinct account_id) as accounts_for_lt,
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
),
left_active_all_with_share as (
        select left_active_all.*,
        left_share,
        left_users_count,
        active_users_count_for_left_share
        from left_active_all
        left join left_active_all_share on left_active_all.segments_aggregated_ = left_active_all_share.segments_aggregated_
                                            and left_active_all.month = left_active_all_share.month
                                            and left_active_all.client_type_ = left_active_all_share.client_type_
                                            and left_active_all.market_type = left_active_all_share.market_type
                                            and left_active_all.registration_source_agg_ = left_active_all_share.registration_source_agg_
)

,left_active_eur_usd_rur_kzt as (
select  case when grouping(segments_aggregated) =1 then 'all' else segments_aggregated end segments_aggregated_ ,
        month,
        case when grouping(registration_source_agg) = 1 then 'all' else registration_source_agg end registration_source_agg_,
        case when grouping(client_type) = 1 then 'all' else client_type end client_type_,
        --count(distinct case when return_or_left_status in ('left','came_back_after_leaving_period') then account_id end )/count(distinct case when return_or_left_status in ('active','returned') then account_id end) left_share,
        avg(LT) LT,
        sum(LT) sum_LT,
        count(distinct account_id) as accounts_for_lt,
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

,left_active_eur_usd_rur_kzt_share as (
select  case when grouping(segments_aggregated) =1 then 'all' else segments_aggregated end segments_aggregated_ ,
        month,
        case when grouping(registration_source_agg) = 1 then 'all' else registration_source_agg end registration_source_agg_,
        case when grouping(client_type) = 1 then 'all' else client_type end client_type_,
        count(distinct case when return_or_left_status in ('left','came_back_after_leaving_period') then account_id end ) left_users_count,
        count(distinct case when return_or_left_status in ('active','returned') then account_id end) active_users_count_for_left_share,
        count(distinct case when return_or_left_status in ('left','came_back_after_leaving_period') then account_id end )/Case when count(distinct case when return_or_left_status in ('active','returned') then account_id end) = 0 then 1 else count(distinct case when return_or_left_status in ('active','returned') then account_id end) end left_share,
      case when currency in ('USD','EUR') then  'usd_eur' else 'ru_kzt' end  as market_type
from left_data_share
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
),left_active_eur_usd_rur_kzt_all as (

       select left_active_eur_usd_rur_kzt.*,
        left_share,
        left_users_count,
        active_users_count_for_left_share
        from left_active_eur_usd_rur_kzt
        left join left_active_eur_usd_rur_kzt_share on left_active_eur_usd_rur_kzt.segments_aggregated_ = left_active_eur_usd_rur_kzt_share.segments_aggregated_
                                            and left_active_eur_usd_rur_kzt.month = left_active_eur_usd_rur_kzt_share.month
                                            and left_active_eur_usd_rur_kzt.client_type_ = left_active_eur_usd_rur_kzt_share.client_type_
                                            and left_active_eur_usd_rur_kzt.market_type = left_active_eur_usd_rur_kzt_share.market_type
)


,left_active_rur as (
select  case when grouping(segments_aggregated) =1 then 'all' else segments_aggregated end segments_aggregated_ ,               -- Сегмент клиента после группировки
        month,                                          -- Рассматриваемый месяц
        case when grouping(registration_source_agg) = 1 then 'all' else registration_source_agg end registration_source_agg_,   -- агрегированный источник регистрации при регистрации
        case when grouping(client_type) = 1 then 'all' else client_type end client_type_,   -- Тип клиента
        avg(LT) LT,                                     -- lifetime клиента
        sum(LT) sum_LT,                                 -- Сумма LT клиентов
        count(distinct account_id) as accounts_for_lt,  -- Количество аккаунтов за LT
      currency as market_type
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
),left_active_rur_share as (
select  case when grouping(segments_aggregated) =1 then 'all' else segments_aggregated end segments_aggregated_ ,
        month,
        case when grouping(registration_source_agg) = 1 then 'all' else registration_source_agg end registration_source_agg_,
        case when grouping(client_type) = 1 then 'all' else client_type end client_type_,
        count(distinct case when return_or_left_status in ('left','came_back_after_leaving_period') then account_id end ) left_users_count,
        count(distinct case when return_or_left_status in ('active','returned') then account_id end) active_users_count_for_left_share,
        count(distinct case when return_or_left_status in ('left','came_back_after_leaving_period') then account_id end )/Case when count(distinct case when return_or_left_status in ('active','returned') then account_id end) = 0 then 1 else count(distinct case when return_or_left_status in ('active','returned') then account_id end) end left_share,
      currency as market_type   -- Рынок
from left_data_share
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
),left_active_rur_all as (
       select left_active_rur.*,
                            left_share,     -- Доля ушедших клиентов
        left_users_count,                   -- Количество ушедших клиентов
        active_users_count_for_left_share   -- Количество активных юзеров
        from left_active_rur
        left join left_active_rur_share on left_active_rur.segments_aggregated_ = left_active_rur_share.segments_aggregated_
                                                        and left_active_rur.month = left_active_rur_share.month
                                                        and left_active_rur.client_type_ = left_active_rur_share.client_type_
                                                        and left_active_rur.market_type = left_active_rur_share.market_type
                                                        and left_active_rur.registration_source_agg_ = left_active_rur_share.registration_source_agg_


)
    -- Таблица ушедших и вернувшихся клиентов с партнером, датами и LT после группировки
select * from left_active_all_with_share
union all
select * from left_active_rur_all
union all
select * from left_active_eur_usd_rur_kzt_all