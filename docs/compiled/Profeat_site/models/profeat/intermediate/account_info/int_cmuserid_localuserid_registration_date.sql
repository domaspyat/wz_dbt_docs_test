select distinct
        cmuserid, 
       localuserid, 
       min(datetime) over (partition by cmuserid,localuserid) as registration_datetime,
       cast(min(datetime) over (partition by cmuserid,localuserid) as date) as registration_date
from `dwh-wazzup`.`dbt_nbespalov`.`stg_all_data_filtered_from_test` 
where event in ('register-confirm-code-success')
        and cmuserid not in('4e9dd753-87c9-4056-b460-87454ba0ec63','30c7c64f-0523-4cb4-82c3-5de1986242dc')