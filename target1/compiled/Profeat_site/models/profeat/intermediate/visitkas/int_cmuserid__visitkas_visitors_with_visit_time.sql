with registration_data as (
    select cmuserid,
            registration_datetime from  `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid_attribution_devices_phone_payment_template` 
)select visits_time.*,
    first_value(datetime) over (partition by visits_time.cmuserid,template_link,localuserid order by datetime) first_time_value_over_localuserid 
    from `dwh-wazzup`.`dbt_nbespalov`.`int_cmuserid_visitkas_visitors_with_visit_time` visits_time
        join registration_data on visits_time.cmuserid = registration_data.cmuserid 
        where registration_datetime <= visits_time.datetime