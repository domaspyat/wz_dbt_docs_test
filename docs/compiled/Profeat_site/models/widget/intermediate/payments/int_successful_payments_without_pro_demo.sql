select *
from `dwh-wazzup`.`dbt_nbespalov`.`int_successful_payments`
where sum is distinct from 1