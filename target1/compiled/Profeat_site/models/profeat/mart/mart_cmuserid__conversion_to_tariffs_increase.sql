with posts_payments  as (
           select  registration_date,
                    first_value(datetime) over (partition by cmuserid order by datetime) as event_time,
                    event event_name,
                    cmuserid
            from `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid_conversions_to_post`
),
registration_data as (
            select *
            from `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid_all_info_that_used_as_filters` 
)
select distinct
                filtrs.*,
                event_time
from registration_data filtrs
left join posts_payments on filtrs.cmuserid = posts_payments.cmuserid