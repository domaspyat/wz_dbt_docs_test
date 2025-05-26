with profile_info as (
    select account_id
    from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info`
    where is_employee
)   -- Таблица с указанием текущего и исходного источника регистрации и типа аккаунта
select attribution_data.*, 
        accounts.currency   -- Валюта пользователя на сегодняшний день
from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_attribution_data` attribution_data
left join `dwh-wazzup`.`dbt_nbespalov`.`stg_accounts` accounts
                        on attribution_data.account_id=accounts.account_id
where account_type='standart'
and not exists (
    select account_id
    from profile_info
    where profile_info.account_id = attribution_data.account_id
)