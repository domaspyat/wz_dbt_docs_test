-- Таблица интеграций с информацией о сотрудниках, их активности и ролях
SELECT account_id,  -- ID аккаунта
count(distinct employee_user_id) as users_in_integration,   -- Количество сотрудников в интеграции
count(distinct case when has_role then employee_user_id end) as users_with_roles, -- Количество сотрудников с ролями в интеграции
count(distinct case when activated_at is not null then employee_user_id end) as users_activated -- Количество сотрудников с введенным номером телефона для МП
 FROM `dwh-wazzup`.`dbt_nbespalov`.`stg_crmEmployees`
group by 1