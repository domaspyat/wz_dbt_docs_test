with payments_with_times as (
  select datetime,cmuserid,event
  from `dwh-wazzup`.`dbt_nbespalov`.`stg_payments_success`
  where cast(datetime as date) >= '2023-10-18' --релиз задачи с необходимыми событиями
), events_that_can_cause_payment as (
   select event 
   from profeat.942313_events_lead_to_payments needed_events
),events_all as (
    select * 
    from `dwh-wazzup`.`dbt_nbespalov`.`stg_all_data_filtered_from_test`
    where  datetime >= '2023-10-18' --релиз задачи с необходимыми событиями
),
defining_what_caused as (
select events_all.cmuserid,
        max(events_all.datetime) over (partition by events_all.cmuserid,payments_with_times.event,payments_with_times.datetime) max_time_before_payment,
        events_all.event,
        events_all.datetime as event_time,
        payments_with_times.event as paid_event,
        payments_with_times.datetime as payment_time
from events_all
join payments_with_times on events_all.cmuserid = payments_with_times.cmuserid and events_all.datetime < payments_with_times.datetime
join events_that_can_cause_payment on events_all.event = events_that_can_cause_payment.event

)
select *
from defining_what_caused


--(events_all.event like '%payment.success%'and events_all.event not like '%payment.success.recurring%') or