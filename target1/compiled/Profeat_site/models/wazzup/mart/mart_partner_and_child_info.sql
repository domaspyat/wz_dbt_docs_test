with profile_info as (
    select account_id
    from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info`
    where is_employee is true
)   -- Информация о партнерах и их дочках
select *,
from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_partners_partners_and_children_info` partners_and_children_info
where child_id not in (
    select account_id
    from profile_info
)