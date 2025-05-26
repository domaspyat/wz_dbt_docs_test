with registration_data as (
    select cmuserid,
            registration_datetime from  `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid_attribution_devices_phone_payment_template` 
), 
visitkas_visitors_with_visit_time as 
    (select visits_time.*,
    first_value(datetime) over (partition by visits_time.cmuserid,template_link,localuserid order by datetime) first_time_value_over_localuserid 
    from `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid_visitkas_visitors_with_visit_time` visits_time
        join registration_data on visits_time.cmuserid = registration_data.cmuserid 
        where registration_datetime <= visits_time.datetime
    ),

    visitkas_visitors_with_visit_time_deduplicated as 
    (select cmuserid, 
            template_link,
            first_time_value_over_localuserid,
            dense_rank() over (partition by cmuserid,template_link order by first_time_value_over_localuserid,localuserid) rank,
            localuserid,
            count(distinct localUserId) over (partition by template_link,cmuserid) as visitkas_users 
            from visitkas_visitors_with_visit_time
    )
select distinct cmuserid,template_link,first_time_value_over_localuserid as activation_datetime
from visitkas_visitors_with_visit_time_deduplicated
where rank = 5
and cmuserid is not null