with int_cmuserid_business_sphere_event_description as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid_business_sphere_event_description`
    ),
    business_spheres_source as (
    select * from `dwh-wazzup`.`analytics_tech`.`business_spheres` 
    ),
    business_sphere__events_description_by_users as (
        select distinct
        business_select_datetime,
        event,
        eventgroup,
        includeinmetrics,
        coalesce(
            int_cmuserid_business_sphere_event_description.business_spheres_filter,
            case
                when
                    business_spheres_source.description
                    in ('Конверсионный лендинг', 'Другой тип страницы', 'Мультиссылка')
                then 'Не указал сферу бизнеса'
                else business_spheres_source.description
            end
        ) as business_spheres_filter,
        coalesce(
            int_cmuserid_business_sphere_event_description.eventgroupname_description, business_spheres_source.eventgroupname_description
        ) as eventgroupname_description,
        coalesce(name, business_spheres_source.description) as business_spheres_filter_description,
        cmuserid
        from  int_cmuserid_business_sphere_event_description left join business_spheres_source
        on int_cmuserid_business_sphere_event_description.event = business_spheres_source.eventname
    )
select * from business_sphere__events_description_by_users