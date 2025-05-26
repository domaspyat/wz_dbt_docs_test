select account_id,
       register_at,
       country,
       region_type,
       currency,
       type,
       regEmail,
       demo_account
from `dwh-wazzup`.`dbt_nbespalov`.`stg_accounts`
where type in ('partner','tech-partner')