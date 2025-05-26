with registration_data as (
    select cmuserid,
            registration_datetime from  `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid_attribution_devices_phone_payment_template` 
), activations as (
    select distinct all_data.cmuserid,activation_datetime
    from  `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid_visitkas_visitors_with_visit_time_all_visits` all_data
    join registration_data on all_data.cmuserid = registration_data.cmuserid
    where all_data.datetime >= registration_datetime 
)
,activation_copies as (
select cmuserid,copied_datetime
 from `dwh-wazzup`.`dbt_nbespalov`.`stg_cmuserid_copied_template_link` 
union all
select *
from activations
)
select cmuserid,
      min(copied_datetime) as copied_datetime
from activation_copies
group by 1