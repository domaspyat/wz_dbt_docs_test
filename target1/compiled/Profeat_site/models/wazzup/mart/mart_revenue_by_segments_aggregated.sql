with revenue_data as (

select int_payments_revenue_with_waba.*,
      date_trunc(stg_accounts.register_date,month) register_month,
      case when paid_month = date_trunc(stg_accounts.register_date,month) then 'new' else 'old' end as client_type,
      registration_source_agg
  from `dwh-wazzup`.`dbt_nbespalov`.`int_payments_revenue_with_waba`
  join `dwh-wazzup`.`dbt_nbespalov`.`stg_accounts` on int_payments_revenue_with_waba.account_id = stg_accounts.account_id
  join `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_attribution_data` sources on stg_accounts.account_id = sources.account_id

),
  revenue_data_all as (
  
  select case when grouping(segments_aggregated) =1 then 'all' else segments_aggregated end segments_aggregated_,
          paid_month,
          case when grouping(registration_source_agg) = 1 then 'all' else registration_source_agg end registration_source_agg_,
          case when grouping(client_type) = 1 then 'all' else client_type end client_type_, 
          'all' as market_type,
          count(distinct account_id) as paying_clients,
          count(distinct case when waba_sum_in_rubles >0 then account_id end) paying_clients_waba,
          count(distinct case when waba_sum_in_rubles =0 then account_id end) paying_clients_without_waba,
          
          sum(sum_in_rubles) as revenue_amount,
          sum(waba_sum_in_rubles) as revenue_amount_waba,
          sum(sum_in_rubles) -sum(waba_sum_in_rubles)  as revenue_amount_without_waba,

          sum(sum_in_rubles)/IF(count(distinct account_id)=0,1,count(distinct account_id)) arpu,
          sum(waba_sum_in_rubles)/IF(count(distinct case when waba_sum_in_rubles >0 then account_id end) = 0,1,count(distinct case when waba_sum_in_rubles >0 then account_id end)) arpu_waba,
          (sum(sum_in_rubles) -sum(waba_sum_in_rubles))/IF(count(distinct case when waba_sum_in_rubles =0 then account_id end)=0,1,count(distinct case when waba_sum_in_rubles =0 then account_id end)) arpu_without_waba
          
  from revenue_data

  group by GROUPING SETS 
(
  (segments_aggregated,paid_month),
        (client_type,paid_month),
        (segments_aggregated,paid_month,client_type),
        (paid_month),


  (segments_aggregated,paid_month,registration_source_agg),
        (client_type,paid_month,registration_source_agg),
        (segments_aggregated,paid_month,client_type,registration_source_agg),
        (paid_month,registration_source_agg)

)


), revenue_data_ru_kzt as (

select case when grouping(segments_aggregated) =1 then 'all' else segments_aggregated end segments_aggregated_,
          paid_month,
          case when grouping(registration_source_agg) = 1 then 'all' else registration_source_agg end registration_source_agg_,
          case when grouping(client_type) = 1 then 'all' else client_type end client_type_, 
          case when currency in ('RUR','KZT') then 'ru_kzt' else 'usd_eur' end as market_type,
          count(distinct account_id) as paying_clients,
          count(distinct case when waba_sum_in_rubles >0 then account_id end) paying_clients_waba,
          count(distinct case when waba_sum_in_rubles =0 then account_id end) paying_clients_without_waba,
          
          sum(sum_in_rubles) as revenue_amount,
          sum(waba_sum_in_rubles) as revenue_amount_waba,
          sum(sum_in_rubles) -sum(waba_sum_in_rubles)  as revenue_amount_without_waba,

          sum(sum_in_rubles)/IF(count(distinct account_id)=0,1,count(distinct account_id)) arpu,
          sum(waba_sum_in_rubles)/IF(count(distinct case when waba_sum_in_rubles >0 then account_id end) = 0,1,count(distinct case when waba_sum_in_rubles >0 then account_id end)) arpu_waba,
          (sum(sum_in_rubles) -sum(waba_sum_in_rubles))/IF(count(distinct case when waba_sum_in_rubles =0 then account_id end)=0,1,count(distinct case when waba_sum_in_rubles =0 then account_id end)) arpu_without_waba
          

  from revenue_data
  where currency in ('RUR','KZT','USD','EUR')
   group by GROUPING SETS 
(
  (segments_aggregated,paid_month,market_type),
        (client_type,paid_month,market_type),
        (segments_aggregated,paid_month,client_type,market_type),
        (paid_month,market_type),


  (segments_aggregated,paid_month,registration_source_agg,market_type),
        (client_type,paid_month,registration_source_agg,market_type),
        (segments_aggregated,paid_month,client_type,registration_source_agg,market_type),
        (paid_month,registration_source_agg,market_type)
)


), revenue_data_ru as (

  select case when grouping(segments_aggregated) =1 then 'all' else segments_aggregated end segments_aggregated_,   -- Сегмент после группировки
          paid_month,                                                                               -- Месяц оплаты
          case when grouping(registration_source_agg) = 1 then 'all' else registration_source_agg end registration_source_agg_, -- Аггрегированный источник регистрации
          case when grouping(client_type) = 1 then 'all' else client_type end client_type_,         -- Тип клиента
          currency as market_type,                                                                  -- Рынок
          count(distinct account_id) as paying_clients,                                             -- Количество платящих клиентов
          count(distinct case when waba_sum_in_rubles >0 then account_id end) paying_clients_waba,          -- Количество клиентов, платящих за WABA
          count(distinct case when waba_sum_in_rubles =0 then account_id end) paying_clients_without_waba,  -- Количество платящих клиентов без WABA
          
          sum(sum_in_rubles) as revenue_amount,                                                     -- Сумма выручки
          sum(waba_sum_in_rubles) as revenue_amount_waba,                                           -- Сумма выручки за WABA
          sum(sum_in_rubles) -sum(waba_sum_in_rubles)  as revenue_amount_without_waba,              -- Сумма выручки без WABA

          sum(sum_in_rubles)/IF(count(distinct account_id)=0,1,count(distinct account_id)) arpu,    -- Средняя выручка с пользователя
          sum(waba_sum_in_rubles)/IF(count(distinct case when waba_sum_in_rubles >0 then account_id end) = 0,1,count(distinct case when waba_sum_in_rubles >0 then account_id end)) arpu_waba,                              -- Средняя выручка с пользователя WABA
          (sum(sum_in_rubles) -sum(waba_sum_in_rubles))/IF(count(distinct case when waba_sum_in_rubles =0 then account_id end)=0,1,count(distinct case when waba_sum_in_rubles =0 then account_id end)) arpu_without_waba   -- Средняя выручка с пользователя без WABA
          

  from revenue_data
  where currency in ('RUR','KZT','USD','EUR')

   group by GROUPING SETS 
(
  (segments_aggregated,paid_month,market_type),
        (client_type,paid_month,market_type),
        (segments_aggregated,paid_month,client_type,market_type),
        (paid_month,market_type),


  (segments_aggregated,paid_month,registration_source_agg,market_type),
        (client_type,paid_month,registration_source_agg,market_type),
        (segments_aggregated,paid_month,client_type,registration_source_agg,market_type),
        (paid_month,registration_source_agg,market_type)
)
    -- Выручка по сегментам после группировки
  )
select * from revenue_data_all
union all
select * from revenue_data_ru_kzt
union all
select * from revenue_data_ru
/*
left join revenue_data_ru_kzt on revenue_data_all.segments_aggregated = revenue_data_ru_kzt.segments_aggregated
left join revenue_data_ru     on  revenue_data_all.segments_aggregated =revenue_data_ru.segments_aggregated  
left join revenue_data_kzt    on revenue_data_all.segments_aggregated = revenue_data_kzt.segments_aggregated 
left join revenue_data_usd    on revenue_data_all.segments_aggregated = revenue_data_usd.segments_aggregated 
left join revenue_data_eur    on revenue_data_all.segments_aggregated = revenue_data_eur.segments_aggregated 
left join revenue_data_eur_usd on revenue_data_all.segments_aggregated = revenue_data_eur_usd.segments_aggregated 
*/



/*
 select *
 from dbt_prod.mart_revenue_by_segments*/