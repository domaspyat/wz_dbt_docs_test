with channels_data as (
select accs.account_Id,
        month,
        currency,
        segments_aggregated,
        case when month = registration_month then 'new' else 'old' end as client_type,
        sum(paid_channels_quantity) paid_channels_quantity,
        sum(paid_channels_waba_quantity) paid_channels_waba_quantity,
        count(distinct subscription_type) distinct_transports_count,
        sources.registration_source_agg
from `dwh-wazzup`.`dbt_nbespalov`.`int_channels_active_paid_monthly` accs
join `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_attribution_data` sources on accs.account_id = sources.account_id
group by all

)
, channels_active_all as (

select  case when grouping(segments_aggregated) =1 then 'all' else segments_aggregated end segments_aggregated_,
        month,
        case when grouping(registration_source_agg) = 1 then 'all' else registration_source_agg end registration_source_agg_,
        case when grouping(client_type) = 1 then 'all' else client_type end client_type_,
        sum(paid_channels_quantity) paid_channels_quantity,
        sum(paid_channels_waba_quantity) paid_channels_waba_quantity,
        sum(distinct_transports_count) sum_distinct_transports_count_per_user,
        count(distinct account_id) as accounts_for_distinct_transport,
        avg(distinct_transports_count) avg_distinct_transports_count_per_user,
      'all' as market_type
from channels_data
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


),channels_active_eur_usd_rur_kzt as (

select  case when grouping(segments_aggregated) =1 then 'all' else segments_aggregated end segments_aggregated_,
        month,
        case when grouping(registration_source_agg) = 1 then 'all' else registration_source_agg end registration_source_agg_,
        case when grouping(client_type) = 1 then 'all' else client_type end client_type_,
        sum(paid_channels_quantity) paid_channels_quantity,
        sum(paid_channels_waba_quantity) paid_channels_waba_quantity,
        sum(distinct_transports_count) sum_distinct_transports_count_per_user,
        count(distinct account_id) as accounts_for_distinct_transport,
        avg(distinct_transports_count) avg_distinct_transports_count_per_user,
      case when currency in ('USD','EUR') then  'usd_eur' else 'ru_kzt' end  as market_type
from channels_data
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


),channels_active_rur as (

select  case when grouping(segments_aggregated) =1 then 'all' else segments_aggregated end segments_aggregated_,    -- Сегмент клиента после группировки
        month,                  -- Месяц между началом подписки и её окончанием
        case when grouping(registration_source_agg) = 1 then 'all' else registration_source_agg end registration_source_agg_,   -- Источник регистрации
        case when grouping(client_type) = 1 then 'all' else client_type end client_type_,   -- Тип клиента
        sum(paid_channels_quantity) paid_channels_quantity,                     -- Количество оплаченных каналов
        sum(paid_channels_waba_quantity) paid_channels_waba_quantity,           -- Количество оплаченных каналов с тарифом WABA
        sum(distinct_transports_count) sum_distinct_transports_count_per_user,  -- Сумма уникальных транспортов на юзера
        count(distinct account_id) as accounts_for_distinct_transport,          -- Сумма уникальных аккантов на транспорт
        avg(distinct_transports_count) avg_distinct_transports_count_per_user,  -- Среднее количество уникальных транспортов на юзера
      currency as market_type   -- Рынок
from channels_data
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

    -- Таблица c активными и оплаченными каналами после группировки
)
select *  from channels_active_all
union all
select * from channels_active_rur
union all
select * from channels_active_eur_usd_rur_kzt