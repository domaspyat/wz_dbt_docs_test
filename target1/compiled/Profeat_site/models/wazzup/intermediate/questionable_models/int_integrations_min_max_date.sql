select MIN(CAST(created_date AS DATE)) as min_date,
       MAX(CAST(created_date AS DATE)) as max_date
from `dwh-wazzup`.`dbt_nbespalov`.`stg_integrations`