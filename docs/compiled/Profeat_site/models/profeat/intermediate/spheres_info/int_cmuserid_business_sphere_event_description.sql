with business_sphere_events_data as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid_business_sphere_events_data`
),
    business_spheres_source as (
        select * from `dwh-wazzup`.`analytics_tech`.`business_spheres`
        ),

    business_sphere_event_description as (
        select
    business_sphere_events_data.event,
    business_sphere_events_data.eventgroup,
    name,
    case
        when
            business_spheres_source.description
            in ('Конверсионный лендинг', 'Другой тип страницы', 'Мультиссылка')
        then 'Не указал сферу бизнеса'
        else business_spheres_source.description
    end as business_spheres_filter,
    business_spheres_source.eventgroupname_description,
    coalesce(business_sphere_events_data.name, business_spheres_source.description) as business_spheres_filter_description,
    cmuserid,
    case
        when event = 'business-select.custom' and length(name) = 0
        then 'no'
        when
            (
                event in (
                    'business-select.main-site',
                    'business-select.main-personal',
                    'business-select.main-company'
                )
                and event = eventgroup
            )
        then 'no'
        else 'yes'
    end as includeinmetrics,
    business_select_datetime
    from business_sphere_events_data left join business_spheres_source
    on business_sphere_events_data.event = business_spheres_source.eventname
    and business_sphere_events_data.eventgroup = business_spheres_source.eventgroupname
where rn = 1
    )

select * from business_sphere_event_description