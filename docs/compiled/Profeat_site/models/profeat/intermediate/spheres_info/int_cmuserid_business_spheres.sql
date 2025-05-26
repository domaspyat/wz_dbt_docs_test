with business_select_events as(
                    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid_business_sphere_events_data`
), 
business_select_events_description as (
                    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid_business_sphere_event_description` 
),
business_select_events_description_by_users as (
                    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid_business_sphere__events_description_by_users`
)

select *
from business_select_events_description_by_users