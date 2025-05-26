with registration_data as (
  select distinct case when account_registration_type_current in ('referal_code','standart') then 'final_client'
                      when account_registration_type_current = 'partner_code' then 'of-partner'
                      when account_registration_type_current = 'tech_partner_code' then 'tech-partner'
            end as segments_aggregated,
            date_trunc(registration_date,month) registration_month,
            account_id,
            case when min_subscription_date is not null then account_id end as paid_account_ids,
            currency,
            case when is_renewal then account_id end as returned_account_ids,
            registration_source_agg
from `dwh-wazzup`.`dbt_nbespalov`.`mart_key_metrics`
where account_registration_type_current is distinct from 'manual_registration'
)
, c_one_all as (

select  case when grouping(segments_aggregated) =1 then 'all' else segments_aggregated end segments_aggregated_,
        registration_month,
        case when grouping(registration_source_agg) = 1 then 'all' else registration_source_agg end registration_source_agg_,
        count(distinct account_id) all_users_for_c1,
        count(distinct paid_account_ids) paid_accounts,
        count(distinct returned_account_ids) returned_accounts,
        count(distinct paid_account_ids)/count(distinct account_id) c1_conv,
        count(distinct returned_account_ids)/IF(count(distinct paid_account_ids)=0,1,count(distinct paid_account_ids)) as c2_conv,
      'all' as market_type
from registration_data
   group by GROUPING SETS 
(
  (segments_aggregated,registration_month),
        (registration_source_agg,registration_month),
        (segments_aggregated,registration_month,registration_source_agg),
        (registration_month)
)

),c_one_eur_usd_kzt_rur as (

select  case when grouping(segments_aggregated) =1 then 'all' else segments_aggregated end segments_aggregated_,
        registration_month,
        case when grouping(registration_source_agg) = 1 then 'all' else registration_source_agg end registration_source_agg_,
        count(distinct account_id) all_users_for_c1,
        count(distinct paid_account_ids) paid_accounts,
        count(distinct returned_account_ids) returned_accounts,
        count(distinct paid_account_ids)/count(distinct account_id) c1_conv,
        count(distinct returned_account_ids)/IF(count(distinct paid_account_ids)=0,1,count(distinct paid_account_ids)) as c2_conv,
      case when currency in ('USD','EUR') then  'usd_eur' else 'ru_kzt' end  as market_type
from registration_data
where currency in ('USD','EUR','RUR','KZT')
   group by GROUPING SETS 
(
    (segments_aggregated,registration_month,market_type),
        (registration_source_agg,registration_month,market_type),
        (segments_aggregated,registration_month,registration_source_agg,market_type),
        (registration_month,market_type)
)       
)
,c_one_rur as (

select   case when grouping(segments_aggregated) =1 then 'all' else segments_aggregated end segments_aggregated_,                   -- Сегмент после группировки
        registration_month,                                                     -- Месяц регистрации
        case when grouping(registration_source_agg) = 1 then 'all' else registration_source_agg end registration_source_agg_,       -- Источник регистрации после группировки
        count(distinct account_id) all_users_for_c1,                            -- Количество клиентов, которые прошли конверсию 1го уровня
        count(distinct paid_account_ids) paid_accounts,                         -- Количество платящих клиентов
        count(distinct returned_account_ids) returned_accounts,                 -- Количество вернувшихся
        count(distinct paid_account_ids)/count(distinct account_id) c1_conv,    -- Конверсия 1го уровня
        count(distinct returned_account_ids)/IF(count(distinct paid_account_ids)=0,1,count(distinct paid_account_ids)) as c2_conv,  -- Конверсия 2го уровня
      currency as market_type                                                   -- Рынок
from registration_data
where currency in ('RUR','USD','KZT','EUR')
   group by GROUPING SETS 
(
    (segments_aggregated,registration_month,market_type),
        (registration_source_agg,registration_month,market_type),
        (segments_aggregated,registration_month,registration_source_agg,market_type),
        (registration_month,market_type)
)       
     
)   -- Таблица, которая показывает конверсию c1 и c2
select 'new' as client_type_,*  from c_one_all
union all
select 'new' as client_type_,* from c_one_rur
union all
select 'new' as  client_type_,* from c_one_eur_usd_kzt_rur

union all
 
select 'all' as  client_type_,*  from c_one_all
union all
select 'all' as  client_type_,* from c_one_rur
union all
select 'all' as  client_type_,* from c_one_eur_usd_kzt_rur