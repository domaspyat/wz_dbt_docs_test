
    
    

with child as (
    select cmuserid as from_field
    from `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid_attribution_devices_phone_payment_template`
    where cmuserid is not null
),

parent as (
    select cmuserid as to_field
    from `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid_localuserid_registration_date`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


