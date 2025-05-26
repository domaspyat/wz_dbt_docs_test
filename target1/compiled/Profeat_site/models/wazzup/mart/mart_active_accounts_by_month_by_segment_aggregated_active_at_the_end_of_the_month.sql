with accounts_data_pre as (
select mart_active_accounts_by_month_by_segment.account_id,
       mart_active_accounts_by_month_by_segment.currency,
       --case when date_trunc(subscription_end_fixed,month) = month and last_day(month,month) > subscription_end_fixed then False else True end is_active_in_month,
        date_trunc(register_date,month) as registration_month,
        case when month = date_trunc(register_date,month) then 'new' else 'old' end as client_type,
        
         case when segment in ('of_partner_child__of_partner_paid','partner') then 'of-partner'
              when segment in ('tech_partner_child__child_paid','tech_partner_child__tech_partner_paid','tech-partner') then 'tech-partner'
              when segment in ('standart_without_partner','of_partner_child_child_paid') then 'final_client'
           end segments_aggregated,
        registration_source_agg,
           month
from `dwh-wazzup`.`dbt_nbespalov`.`mart_active_accounts_by_month_by_segment`
join `dwh-wazzup`.`dbt_nbespalov`.`stg_accounts` accs on mart_active_accounts_by_month_by_segment.account_id = accs.account_id
join `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_attribution_data` sources on accs.account_id = sources.account_id

where segment in ('of_partner_child__of_partner_paid','partner','tech_partner_child__child_paid','tech_partner_child__tech_partner_paid','tech-partner','standart_without_partner','of_partner_child_child_paid'))
, accounts_data as (
    select *
    from accounts_data_pre
    --where is_active_in_month
)
, accounts_active_all as (


select  case when grouping(segments_aggregated) =1 then 'all' else segments_aggregated end segments_aggregated_,
        month,
        case when grouping(registration_source_agg) = 1 then 'all' else registration_source_agg end registration_source_agg_,
        case when grouping(client_type) = 1 then 'all' else client_type end client_type_,
        count(distinct account_Id) as active_accounts_count,
      'all' as market_type
from accounts_data
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



),accounts_active_eur_usd_kzt_rur as (

select  case when grouping(segments_aggregated) =1 then 'all' else segments_aggregated end segments_aggregated_,
        month,
        case when grouping(registration_source_agg) = 1 then 'all' else registration_source_agg end registration_source_agg_,
        case when grouping(client_type) = 1 then 'all' else client_type end client_type_,
        count(distinct account_Id) as active_accounts_count,
      case when currency in ('USD','EUR') then  'usd_eur' else 'ru_kzt' end  as market_type
from accounts_data
where currency in ('RUR','USD','KZT','EUR')
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


),accounts_active_rur as (

select  case when grouping(segments_aggregated) =1 then 'all' else segments_aggregated end segments_aggregated_,                -- Сегмент после группировки
        month,                                                  -- Рассматриваемый месяц
        case when grouping(registration_source_agg) = 1 then 'all' else registration_source_agg end registration_source_agg_,   -- Источник регистрации после группировки
        case when grouping(client_type) = 1 then 'all' else client_type end client_type_,   -- Тип клиента
        count(distinct account_Id) as active_accounts_count,    -- Количество активных аккаунтов
      currency as market_type                                   -- Рынок
from accounts_data
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
    -- Активность пользователей по месяцаам с активностью в конце месяца

)
select * from accounts_active_all
union all
select * from accounts_active_rur
union all
select * from accounts_active_eur_usd_kzt_rur