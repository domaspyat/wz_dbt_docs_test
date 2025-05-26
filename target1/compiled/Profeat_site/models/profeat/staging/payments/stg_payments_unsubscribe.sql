select cmuserid, datetime
from `dwh-wazzup`.`dbt_nbespalov`.`stg_all_data_filtered_from_test`
where event = 'payment.unsubscribe'