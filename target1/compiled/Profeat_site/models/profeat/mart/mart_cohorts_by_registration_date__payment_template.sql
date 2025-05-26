with
    pre_events as (select * except(first_payment_datetime,payment_sum) from `dwh-wazzup`.`dbt_nbespalov`.`stg_payments_success_and_recurring`
                union all
                select localuserid, cmuserid,null as datetime, registration_date as date, 'registration' as event_name, null as event 
                from `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid_localuserid_registration_date`
                union all
                select localuserid, cmuserid,datetime, date(datetime) as date, 'visitkas' as event_name,'visitkas' as event
                from `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid_visitkas_visitors_with_visit_time` ),
    events as (select * from pre_events right join `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid_attribution_devices_phone_payment_template` us_data using (cmuserid))
select *
except(datetime,event)
from events