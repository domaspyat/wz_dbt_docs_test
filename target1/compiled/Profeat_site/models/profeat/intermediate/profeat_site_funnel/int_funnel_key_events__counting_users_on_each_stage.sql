with int_funnel_key_events__finding_all_users_stages as  (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_funnel_key_events__finding_all_users_stages`   
    ),

int_funnel_key_events__counting_users_on_each_stage as (
    select
    registration_date,
    utm_source,
    utm_campaign,
    utm_medium,
    initrefferer,
    utm_traffic,
    abtest_name,
    abtest_group,
    abgroup_count_filter,
    case
        when devicetypes = 'tablet' or os = 'Android'
        then 'mobile'
        when devicetypes is not null
        then devicetypes
        when os is null or os = 'Other'
        then 'other'
        when
            os = 'Linux'
            or os = 'Windows'
            or os = 'Ubuntu'
            or os = 'Mac OS'
            or os = 'Mac OS X'
        then 'desktop'
    end as devicetypes,
    template_link,
    business_spheres_filter,
    eventgroupname_description,
    business_spheres_filter_description,
    first_paid as first_paid,
    repeat_paid as repeat_paid,
    succreg as SuccReg,
    templateusage as TemplateUsage,
    edits as Edits,
    activation as Activation,
    copies as Copies,
    posted as Posted,
    paid as Paid
    from int_funnel_key_events__finding_all_users_stages
    )

select * from  int_funnel_key_events__counting_users_on_each_stage