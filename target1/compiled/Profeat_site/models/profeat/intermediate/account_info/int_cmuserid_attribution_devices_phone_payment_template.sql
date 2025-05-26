

with registration_data as (
    select * from
    `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid_localuserid_registration_date`
),

registration_sources as (
    select * from
    `dwh-wazzup`.`dbt_nbespalov`.`int_localuserid_registration_attribution_devices`
),

first_payment_date as 
   (
        select cmuserid, 
                min(date(datetime)) as first_payment_date,
                min(datetime) as first_payment_datetime
        from `dwh-wazzup`.`dbt_nbespalov`.`stg_all_data_filtered_from_test` 
        where event = 'payment.success'
        group by 1
    ),

last_payment_date as 
    (
         select cmuserid, max(date(datetime)) as last_payment_date
        from `dwh-wazzup`.`dbt_nbespalov`.`stg_all_data_filtered_from_test` 
        where
            event in ('payment.success', 'payment.success.recurring')
        group by 1
    ),
int_cmuserid_localuserid_template_link_groupped as 
 (
    select cmuserid,
    template_link from
    `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid_localuserid_template_link_groupped`
    group by 1,2),

user_mobiles as (
select * from  `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid_phone__last_value`
)
    
select distinct
    registration_data.cmuserid,
    registration_data.registration_date as  registration_date,
    registration_data.registration_datetime as  registration_datetime,
    registration_sources.utm_campaign,
    registration_sources.utm_source,
    registration_sources.utm_medium,
    first_payment_date.first_payment_date,
    first_payment_date.first_payment_datetime,
    last_payment_date.last_payment_date,
    template_link,
    phone,
    case
        when device = 'tablet' or os = 'Android'
        then 'mobile'
        when device is not null
        then device
        when os is null or os = 'Other'
        then 'other'
        when
            os = 'Linux'
            or os = 'Windows'
            or os = 'Ubuntu'
            or os = 'Mac OS'
            or os = 'Mac OS X'
        then 'desktop'
    end as device,
    os,
    (
        case
            when url like '%?r=%'
            then 'реферальный'
            when
                utm_source is null
                and (
                    replace(
                        regexp_extract(initreferrer, r'(?:\?|&)((?:[^=]+)=(?:[^&]*))'),
                        'utm_source=',
                        ''
                    )
                    like '%gclid%'
                    or replace(
                        regexp_extract(initreferrer, r'(?:\?|&)((?:[^=]+)=(?:[^&]*))'),
                        'utm_source=',
                        ''
                    )
                    like '%gbraid%'
                )
            then 'unknow_google_ads'
            when utm_source is null
            then regexp_extract(initreferrer, r':\/\/([a-zA-Z.0-9-]*)\/?')
        end
    ) as utm_traffic,
    initreferrer
from
    registration_data 
left join registration_sources 
    on registration_sources.localuserid = registration_data.localuserid and (registration_sources.rn =1 or registration_sources.rn is null)
left join
     first_payment_date on registration_data.cmuserid = first_payment_date.cmuserid
left join last_payment_date on registration_data.cmuserid = last_payment_date.cmuserid

left join int_cmuserid_localuserid_template_link_groupped on registration_data.cmuserId = int_cmuserid_localuserid_template_link_groupped.cmuserId
left join user_mobiles on registration_data.cmuserid = user_mobiles.cmuserId 
where (rn = 1 or rn is null) and registration_data.cmuserid is not null