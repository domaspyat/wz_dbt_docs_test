with registration_date as (select * from `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid_attribution_devices_phone_payment_template`)

select
    rc.cmuserid,
    registration_date,
    user_mobiles.phone,
    (
        case
            when block_2.cmuserid is not null and copy.cmuserid is not null
            then true
            else false
        end
    ) as add_2_blocks_and_copied_link,
    business_spheres_filter_description,
    eventgroupname_description,
    business_spheres_filter,
    includeinmetrics
from registration_date rc
left join
    `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid_edited_2_blocks` block_2 on rc.cmuserid = block_2.cmuserid
left join `dwh-wazzup`.`dbt_nbespalov`.`stg_cmuserid_copied_template_link` copy on rc.cmuserid = copy.cmuserid
left join
    `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid_business_spheres` business_spheres
    on business_spheres.cmuserid = rc.cmuserid
inner join `dwh-wazzup`.`dbt_nbespalov`.`stg_cmuserid_phone` user_mobiles on rc.cmuserid = user_mobiles.cmuserid
order by registration_date desc