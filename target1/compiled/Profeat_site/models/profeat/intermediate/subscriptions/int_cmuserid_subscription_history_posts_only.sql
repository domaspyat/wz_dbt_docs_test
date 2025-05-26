with payments_posts as  (select  distinct
                              datetime as event_time,
                              event as  event_name,
                    cmuserid,
                    cast(datetime as date) event_date
                   from `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid_conversions_to_post`
                   where cast(datetime as date) >= registration_date
                   and event like '%promo_tariff%'
                   order by cmuserid

) ,events as (select 
                    distinct
                    date_trunc(cast(datetime as date),month) dmonth,
                    all_data.cmuserid,
                    all_data.event,
                    all_data.datetime
from `dwh-wazzup`.`dbt_nbespalov`.`stg_all_data_filtered_from_test` all_data
where event like '%payment.success%')
    ,unclear_payments as ( --определяем товарищей со слишком большим количеством оплат в месяц, чтобы в последующих запросах их исключить (пока не выяснено, что это за оплаты)
select dmonth,
      cmuserid,
      count(*)
from events
group by dmonth,cmuserid
having count(*)>2
)
, payments_info as (
select subscription_all.cmuserid,
      event_name,
      event_time,
      event_date,
      lead(event_name) over (partition by subscription_all.cmuserid order by event_time) as next_event,
      lead(event_date) over (partition by subscription_all.cmuserid order by event_time) as next_event_date,
      event_date as subscription_start,
      case when event_name = 'promo_tariff' then date_add(event_date,interval 1 year)
      end as subscription_end,
from payments_posts subscription_all     
left join unclear_payments on subscription_all.cmuserid = unclear_payments.cmuserid
where unclear_payments.cmuserid is null
), defining_previous_due as (
  select *,   lag(
              case  when event_name = 'promo_tariff' then date_add(event_date,interval 1 year)
          end) over (partition by cmuserid order by event_time) previous_due_date
  from payments_info
)
,types_of_payments as (
          select cmuserid,
                subscription_end,
                row_number() over (partition by cmuserid,subscription_end) rn,
                event_name,
                previous_due_date,
                case  when event_name = 'promo_tariff' and next_event like '%payment%' then 'paid_having_promo'
                      when subscription_start < previous_due_date then 'bought_while_active'
                end status,
                event_date,
                next_event,
                next_event_date
          from defining_previous_due
),subscriptions_info as (
  select *,
  case 
       when status = 'bought_while_active' then previous_due_date
       else event_date
       end as subscription_start
from types_of_payments)
, subscription_all as (
    select cmuserid,
            subscription_start as start_date,
            case when event_name = 'promo_tariff'   then date_add(subscription_start, interval 1 year)
                 end as end_date,
            event_date,
            status,
            event_name,
            next_event,
            next_event_date
    from subscriptions_info si
    join `dwh-wazzup`.`analytics_tech`.`days_with_month_intervals`  month_intervals on event_date = month_intervals.date and month_intervals.rn =2 
)
select *
from subscription_all