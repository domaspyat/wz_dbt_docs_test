select
    cmuserid,
    if(e1.event = 'payment.attempt', 'payment.attempt.count', null) as event,
    datetime,
    date,
    null as payment_success_count,
    cast(null as timestamp) as next_date
from `dwh-wazzup`.`dbt_nbespalov`.`stg_all_data_filtered_from_test` e1
where event = 'payment.attempt'