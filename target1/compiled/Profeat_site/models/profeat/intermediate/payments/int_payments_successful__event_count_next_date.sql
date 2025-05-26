with stg_payments_success_and_recurring as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`stg_payments_success_and_recurring` 
),

int_payments_successful__event_count_next_date as  (
    select
    cmuserid,
    'payment.success.all' as event,
    cast(datetime as timestamp) as datetime,
    datetime as date,
    count(event) over (
        partition by cmuserid order by datetime
    ) as payment_success_count,
    lag(datetime) over (partition by cmuserid order by datetime) as next_date
from stg_payments_success_and_recurring
where event = 'payment.success' 
)

select * from int_payments_successful__event_count_next_date