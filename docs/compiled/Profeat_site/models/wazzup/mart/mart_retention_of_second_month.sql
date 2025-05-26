with all_data as (
select accounts.account_Id,
        case when accounts.account_registration_type_current in ('referal_code','standart') then 'final_client'
                      when accounts.account_registration_type_current = 'partner_code' then 'of-partner'
                      when accounts.account_registration_type_current = 'tech_partner_code' then 'tech-partner'
            end as segments_aggregated,
        accounts.account_currency as currency,
        accounts.month,
        case when registration_month = month then 'new' else 'old' end as client_type,
        case when is_retained_second_month then accounts.account_id end retained_users,
        sources.registration_source_agg
from `dwh-wazzup`.`dbt_nbespalov`.`mart_feature_usage` accounts
join `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_attribution_data` sources on accounts.account_id = sources.account_id
where  first_subscription_start_month = month
and accounts.account_registration_type_current is not null
and accounts.account_registration_type_current in ('referal_code','standart','partner_code','tech_partner_code')

), retention_all as (
select  case when grouping(segments_aggregated) =1 then 'all' else segments_aggregated end segments_aggregated_ ,
        month,
        case when grouping(registration_source_agg) = 1 then 'all' else registration_source_agg end registration_source_agg_,
        case when grouping(client_type) = 1 then 'all' else client_type end client_type_,
        count(distinct retained_users) retained_users_for_second_month_retention,
        count(distinct account_id) all_users_for_second_month_retention,
        count(distinct retained_users)/IF(count(distinct account_id) = 0,1,count(distinct account_id)) retention_of_second_month,
      'all' as market_type
from all_data
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


),retention_eur_usd_rur_kzt as (
select  case when grouping(segments_aggregated) =1 then 'all' else segments_aggregated end segments_aggregated_ ,
        month,
        case when grouping(registration_source_agg) = 1 then 'all' else registration_source_agg end registration_source_agg_,
        case when grouping(client_type) = 1 then 'all' else client_type end client_type_,
        count(distinct retained_users) retained_users_for_second_month_retention,
        count(distinct account_id) all_users_for_second_month_retention,
        count(distinct retained_users)/IF(count(distinct account_id) = 0,1,count(distinct account_id)) retention_of_second_month,

      case when currency in ('USD','EUR') then  'usd_eur' else 'ru_kzt' end  as market_type
from all_data
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
,retention_rur as (
select  case when grouping(segments_aggregated) =1 then 'all' else segments_aggregated end segments_aggregated_ ,               -- Сегмент после группировки
        month,                                                                              -- Рассматриваемый месяц
        case when grouping(registration_source_agg) = 1 then 'all' else registration_source_agg end registration_source_agg_,   -- Аггрегированный источник регистрации
        case when grouping(client_type) = 1 then 'all' else client_type end client_type_,   -- Тип клиента
        count(distinct retained_users) retained_users_for_second_month_retention,           -- Количество сохраненных на 2 месяц клиентов
        count(distinct account_id) all_users_for_second_month_retention,                    -- Все юзеры, которые рассматривались на сохранение в 2 месяца
        count(distinct retained_users)/IF(count(distinct account_id) = 0,1,count(distinct account_id)) retention_of_second_month,   -- Доля сохраненных на 2 месяца
      currency as market_type       -- Рынок
from all_data
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
    -- Таблица, которая показывает сохранение на 2 месяца
)
select * from retention_all
union all
select * from retention_rur
union all
select * from retention_eur_usd_rur_kzt