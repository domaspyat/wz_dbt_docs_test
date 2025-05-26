with registration_data as (
    select cmuserid,
            registration_datetime from  `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid_attribution_devices_phone_payment_template` 
),
edits as (
select all_data.cmuserid,
       event,
       datetime,
       count(*) over (partition by all_data.cmuserid) edits_count,
       row_number() over (partition by all_data.cmuserid order by datetime) rn 
from `dwh-wazzup`.`dbt_nbespalov`.`stg_all_data_filtered_from_test` all_data
join registration_data on all_data.cmuserid = registration_data.cmuserid
where (event like '%delete.click' or event like '%save.click')
and all_data.datetime >= registration_datetime

)
select cmuserid,
        datetime as edits_datetime
from edits
where edits_count >= 1 
          and rn = 1