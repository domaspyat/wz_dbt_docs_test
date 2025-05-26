
    
    

with all_values as (

    select
        eventgroup as value_field,
        count(*) as n_records

    from `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid_business_sphere_events_data`
    group by eventgroup

)

select *
from all_values
where value_field not in (
    'business-select.main-other','business-select.other','business-select.site-other','business-select.undefined','business-select.site-crypto','business-select.main-site','business-select.site-rieltor','business-select.site-law','business-select.company-crypto','business-select.custom','business-select.main-link','business-select.main-link-1','business-select.main-link-2','business-select.main-link-3','business-select.main-default','business-select.main-company','business-select.company-shop','business-select.company-cafe','business-select.company-medical','business-select.company-hotel','business-select.company-auto','business-select.company-other','business-select.beauty','business-select.internet-marketing','business-select.education','business-select.store','business-select.mlm','business-select.health','business-select.smm','business-select.needlework','business-select.cooking','business-select.main-landing','business-select.main-personal','business-select.personal-beauty','business-select.personal-coach','business-select.personal-fitness','business-select.personal-teacher','business-select.personal-magician','business-select.personal-other'
)


