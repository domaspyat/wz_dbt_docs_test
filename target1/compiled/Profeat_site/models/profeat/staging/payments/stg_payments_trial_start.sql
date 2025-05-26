select cmuserid, 1 as try_start
from `dwh-wazzup`.`dbt_nbespalov`.`stg_all_data_filtered_from_test`
where event = 'trial.start' and date >= '2022-04-10'