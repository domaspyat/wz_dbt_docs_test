with registration_data as (
    select cmuserid,
            registration_datetime 
    from `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid_attribution_devices_phone_payment_template` 
),
int_funnel_key_events__counting_users_on_each_stage as (
    select distinct
           cmuserid,
           registration_date,
           utm_source,
           utm_campaign,
           utm_medium,
           initRefferer,
           utm_traffic,
           abtest_name,
           abtest_group,
           abgroup_count_filter,
           devicetypes,
           template_link,
           business_spheres_filter,
           eventgroupname_description,
           business_spheres_filter_description
    from `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid_all_info_that_used_as_filters`
)
,
visitkas_visitors_with_visit_time as (
    select visits_time.*,
            first_value(datetime) over (partition by visits_time.cmuserid,template_link,localuserid order by datetime) first_time_value_over_localuserid 
    from `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid_visitkas_visitors_with_visit_time` visits_time
    join registration_data on visits_time.cmuserid = registration_data.cmuserid 
    where registration_datetime <= visits_time.datetime
    ),
visitkas_visitors_with_visit_time_deduplicated as 
    (select *except(date),
            date as month_trunc
    from `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid__active_users_monthly`
    ),
defining_countable_groups as (
    select  *,
            lead(month_trunc) over (partition by cmuserid order by month_trunc) next_active_month,
            date_diff(lead(month_trunc) over (partition by cmuserid order by month_trunc),month_trunc,month) diff_between_current_and_next_active_month,
            case when lead(month_trunc) over (partition by cmuserid order by month_trunc) is null or  date_diff(lead(month_trunc) over (partition by cmuserid order by month_trunc),month_trunc,month) >1 then
                True else False end as is_countable,
            first_value(month_trunc) over (partition by cmuserid order by month_trunc) first_active_month
    from visitkas_visitors_with_visit_time_deduplicated),

visitkas_visitors_with_visit_time_deduplicated_weekly as 
    (   select *
        from `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid__active_users_weekly`
    ),
defining_countable_groups_weekly as (
    select  *,
            lead(week_trunc) over (partition by cmuserid order by week_trunc) next_active_week,
            date_diff(lead(week_trunc) over (partition by cmuserid order by week_trunc),week_trunc,week) diff_between_current_and_next_active_week,
            case when lead(week_trunc) over (partition by cmuserid order by week_trunc) is null or  date_diff(lead(week_trunc) over (partition by cmuserid order by week_trunc),week_trunc,week) >1 then
                True else False end as is_countable_weekly,
            first_value(week_trunc) over (partition by cmuserid order by week_trunc) first_active_week
    from visitkas_visitors_with_visit_time_deduplicated_weekly)
select 
    distinct 
    groupss.* except(first_active_month,visitkas_users),
    groupps_weekly.* except(cmuserid,visitkas_users),
    registration_date,
    utm_source,
    utm_campaign,
    utm_medium,
    abtest_name,
    abtest_group,
    abgroup_count_filter,
    initrefferer,
    utm_traffic,
    devicetypes,
    template_link,
    business_spheres_filter,
    eventgroupname_description,
    business_spheres_filter_description,
from defining_countable_groups groupss
join int_funnel_key_events__counting_users_on_each_stage registration_data on registration_data.cmuserid = groupss.cmuserid
left join defining_countable_groups_weekly groupps_weekly on groupss.cmuserid = groupps_weekly.cmuserid