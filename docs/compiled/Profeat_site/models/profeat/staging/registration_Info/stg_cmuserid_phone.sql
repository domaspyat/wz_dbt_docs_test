select cmuserid,
    dateTime,
    phone
from `dwh-wazzup`.`dbt_nbespalov`.`stg_all_data_filtered_from_test`
where usermobile is not null