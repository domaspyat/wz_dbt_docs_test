with registration_data as (
select case when registration_source_agg in ('partner_code','manual_registration') then 'of-partner'
            when registration_source_agg in ('tech_partner_code') then 'tech-partner'
        else 'final_client' end as segments_aggregated,
              *,
        date_trunc(registration_date,month) as registration_month
from `dwh-wazzup`.`dbt_nbespalov`.`mart_accounts_registration_sources`
)
, registrations_all as (

select  case when grouping(segments_aggregated) =1 then 'all' else segments_aggregated end segments_aggregated_,
        registration_month,
        case when grouping(registration_source_agg) = 1 then 'all' else registration_source_agg end registration_source_agg_,
        count(distinct account_id) registration_count,
      'all' as market_type
from registration_data
   group by GROUPING SETS 
(
  (segments_aggregated,registration_month),
        (registration_source_agg,registration_month),
        (segments_aggregated,registration_month,registration_source_agg),
        (registration_month)
)


),registrations_eur_usd_rur_kzt as (

select case when grouping(segments_aggregated) =1 then 'all' else segments_aggregated end segments_aggregated_,
        registration_month,
        case when grouping(registration_source_agg) = 1 then 'all' else registration_source_agg end registration_source_agg_,
        count(distinct account_id) registration_count,
      case when currency in ('USD','EUR') then  'usd_eur' else 'ru_kzt' end as market_type
from registration_data
where currency in ('RUR','USD','KZT','EUR')
   group by GROUPING SETS 
(
(segments_aggregated,registration_month,market_type),
        (registration_source_agg,registration_month,market_type),
        (segments_aggregated,registration_month,registration_source_agg,market_type),
        (registration_month,market_type)
)

),

registrations_rur as (

select  case when grouping(segments_aggregated) =1 then 'all' else segments_aggregated end segments_aggregated_,                -- Сегмент после группировки
        registration_month,                                 -- Месяц регистрации
        case when grouping(registration_source_agg) = 1 then 'all' else registration_source_agg end registration_source_agg_,   -- Источник регистрации после группировки
        count(distinct account_id) registration_count,      -- Количество регистраций
      currency as market_type                               -- Рынок
from registration_data
where currency in ('RUR','KZT','EUR','USD')
   group by GROUPING SETS 
(
(segments_aggregated,registration_month,market_type),
        (registration_source_agg,registration_month,market_type),
        (segments_aggregated,registration_month,registration_source_agg,market_type),
        (registration_month,market_type)
)

    -- Таблица с указанием текущего и исходного источника регистрации и типа аккаунта после группировки

)
select 'new' as client_type_,* from registrations_all
union all
select 'new' as client_type_,* from registrations_eur_usd_rur_kzt
union all
select 'new' as client_type_,* from registrations_rur

union all

select 'all' as client_type_,* from registrations_all
union all
select 'all' as client_type_,* from registrations_eur_usd_rur_kzt
union all
select 'all' as client_type_,* from registrations_rur



      



--manual reg - руки
--partner code - ссылка