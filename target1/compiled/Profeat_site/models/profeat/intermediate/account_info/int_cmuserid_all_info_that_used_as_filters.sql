

with registration_data as (
    select * from  `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid_attribution_devices_phone_payment_template` 
),

int_cmuserid_business_spheres as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid_business_spheres` 

),abtests_all as  (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid_abtest_group`
 ),

int_all_filters as (
    select distinct
    registration_data.cmuserid,
    registration_data.registration_date,
    registration_data.utm_source,
    registration_data.utm_campaign,
    registration_data.utm_medium,
    abtests.abtest_name,
    abtests.abtest_group,
    case 
        when abtests.cmuserid is not null
            and abtests.abgroup_count = 1 then 'normal_client'
        when abtests.cmuserid is not null 
            and abtests.abgroup_count > 1 then 'plural_groups'
    end as abgroup_count_filter,
    registration_data.initreferrer as initrefferer,
    utm_traffic,
    includeinmetrics,
    os,
    registration_data.device as devicetypes,
    registration_data.template_link,
    business_spheres_filter,
    eventgroupname_description,
    business_spheres_filter_description
    from registration_data
    left join abtests_all abtests on registration_data.cmuserId = abtests.cmuserid
    left join int_cmuserid_business_spheres on registration_data.cmuserid = int_cmuserid_business_spheres.cmuserid
    )
select * from int_all_filters