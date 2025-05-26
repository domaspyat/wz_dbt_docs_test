select 
distinct 
guid, 
account_id 
from `dwh-wazzup`.`dbt_nbespalov`.`stg_billingAffiliate`
where object='subscription'