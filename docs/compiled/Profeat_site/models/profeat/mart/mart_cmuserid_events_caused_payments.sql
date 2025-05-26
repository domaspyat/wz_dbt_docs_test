with data_used_as_filters as(
    select * 
    from `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid_all_info_that_used_as_filters`
),  defining_what_caused as (
    select * 
    from `dwh-wazzup`.`dbt_nbespalov`.`int_payments_events_close_to_payment`
 ),
defining_payment_type as (
select *,case when defining_what_caused.event like '%payment%' then false
            else true end as know_how_paid,
            date_diff(payment_time,event_time,minute) minutes_between_event_and_payment,
            percentile_cont(date_diff(payment_time,event_time,minute),0.75) over () seventy_five_percentile
from defining_what_caused 
where max_time_before_payment = event_time
), all_successful_payments as (
    select *
    from `dwh-wazzup`.`dbt_nbespalov`.`stg_payments_success`
)
,unknown_payments as (
    select distinct all_successful_payments.datetime,all_successful_payments.cmuserid,all_successful_payments.event
    from all_successful_payments
    left join defining_payment_type payment_types on all_successful_payments.cmuserid = payment_types.cmuserid 
                                                 and all_successful_payments.datetime = payment_types.payment_time
    where cast(all_successful_payments.datetime as date) >= '2023-10-18' and payment_types.cmuserid is null 
), all_payments_together as (
select defining_payment_type.*
from defining_payment_type
union all
select cmuserid,
        null,
        'unknown',
        null,
        null,
        datetime,
        false,
        0,
        (select max(seventy_five_percentile) from defining_payment_type)
from unknown_payments
)
select all_payments_together.*,
data_used_as_filters.* except(cmuserId)
from all_payments_together
join data_used_as_filters on all_payments_together.cmuserid = data_used_as_filters.cmuserid