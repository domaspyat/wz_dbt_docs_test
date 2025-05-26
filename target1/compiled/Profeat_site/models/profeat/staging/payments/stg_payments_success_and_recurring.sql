select
    localuserid, 
    cmuserid,
    first_value(datetime) over (partition by cmuserid order by datetime) as first_payment_datetime,
    datetime,
    date(datetime) as date,
    'payment.success' as event_name,
    event,
    details.name as payment_sum
from `dwh-wazzup`.`dbt_nbespalov`.`stg_all_data_filtered_from_test`
where event like '%payment.success%'