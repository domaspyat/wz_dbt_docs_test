with
    profeat_site_funnel_first_stage as (
        select * from `dwh-wazzup`.`dbt_nbespalov`.`int_funnel_key_events__finding_all_users_stages`

    ),
    profeat_site_funnel_second_stage as (
        select * from `dwh-wazzup`.`dbt_nbespalov`.`int_funnel_key_events__counting_users_on_each_stage`
    ),
    profeat_site_funnel_third_stage as (
        select * from `dwh-wazzup`.`dbt_nbespalov`.`int_funnel_key_event__unpivot_events`
    )
select t1.*
from `dwh-wazzup`.`dbt_nbespalov`.`int_funnel_key_event__unpivot_events` t1