with test_accounts_based_on_seed as (
    select distinct account_id
    from `dwh-wazzup`.`dbt_nbespalov`.`wazzup_test_accounts`),
accounts_with_types as (
    select *
    from `dwh-wazzup`.`dbt_nbespalov`.`stg_accounts`
),

test_accounts_defined_by_other_criterias as (
    select distinct account_id
    from accounts_with_types
    where regemail like '%@wazzup.online%' or lower(account_name) like '%стажер%' or lower(account_name) like '%стажёр%')
,all_test_accounts as (
        select account_id       -- ID аккаунта
        from test_accounts_based_on_seed
        union distinct
        select account_id
        from test_accounts_defined_by_other_criterias)
select *    -- Таблица тестовых аккаунтов или аккаунтов сотрудников
from all_test_accounts