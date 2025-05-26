with business_sphere_events_data as (
select
    event,
    first_value(event) over (partition by cmuserid order by datetime) as eventgroup,
    row_number() over (partition by cmuserid order by datetime desc) rn,
    first_value(datetime) over (partition by cmuserid order by datetime desc) business_select_datetime,
    details.name,
    cmuserid
from `dwh-wazzup`.`dbt_nbespalov`.`stg_all_data_filtered_from_test` df
where event like '%business-select%'
        and cmuserid not in('4e9dd753-87c9-4056-b460-87454ba0ec63','30c7c64f-0523-4cb4-82c3-5de1986242dc')
)
select * from business_sphere_events_data