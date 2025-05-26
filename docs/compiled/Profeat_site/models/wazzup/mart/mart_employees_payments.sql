with profile_info as (
    select account_id
    from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts_profile_info`
    where is_employee
)   -- Таблица со всеми оплатами наших сотрудников
select paid_date,                       -- Дата оплаты
        all_payments_union.account_id,  -- ID аккаунта
        all_payments_union.currency,    -- Валюта
        original_sum,                   -- Сумма в исходной валюте. Пользователи могут платить на в долларах, тенге, евро или рублях. В этом поле хранится сумма в исходной валюте. Например, если пользователь внес 10 долларов, то тут будет цифра 10
        data_source                     -- Источник оплаты
from `dwh-wazzup`.`dbt_nbespalov`.`int_payments_with_account_and_partner_type` all_payments_union
inner join `dwh-wazzup`.`dbt_nbespalov`.`stg_accounts` accounts on all_payments_union.account_id = accounts.account_id
where original_sum !=0
and (account_type in ('employee','partner-demo') or partner_type='employee' or all_payments_union.account_id in (select account_id from profile_info))
and all_payments_union.data_source != 'bank_pay'