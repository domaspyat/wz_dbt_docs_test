with int_cmuserid_business_spheres as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid_business_spheres`
    ),
    
    int_cmuserid_attribution_devices_phone_payment_template as (
        select * from `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid_attribution_devices_phone_payment_template`
        ),
    int_cmuserid_business_spheres_only_other as (
        select distinct
        int_cmuserid_business_spheres.cmuserid,
        eventgroupname_description,
        business_spheres_filter,
        business_spheres_filter_description,
        int_cmuserid_attribution_devices_phone_payment_template.utm_source,
        int_cmuserid_attribution_devices_phone_payment_template.utm_campaign,
        int_cmuserid_attribution_devices_phone_payment_template.utm_medium,
        int_cmuserid_attribution_devices_phone_payment_template.template_link,
        int_cmuserid_attribution_devices_phone_payment_template.initreferrer as initrefferer,
        int_cmuserid_attribution_devices_phone_payment_template.device as devicetypes,
        int_cmuserid_attribution_devices_phone_payment_template.utm_traffic,
        int_cmuserid_attribution_devices_phone_payment_template.registration_date
        from  int_cmuserid_business_spheres
        left join  int_cmuserid_attribution_devices_phone_payment_template on int_cmuserid_business_spheres.cmuserid = int_cmuserid_attribution_devices_phone_payment_template.cmuserid
    )    

select * from int_cmuserid_business_spheres_only_other