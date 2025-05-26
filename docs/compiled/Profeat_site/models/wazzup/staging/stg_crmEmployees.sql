select                         -- Таблица с информацией о сотрудниках в интеграции
    accountId as account_id,            -- Идентификатор аккаунта
    hasRole as has_role,                -- Есть ли роль у сотрудника?
    activatedAt as activated_at,        -- Дата и время выдачи доступа к МП
    userId as employee_user_id          -- ID сотрудника в интеграции
from `dwh-wazzup`.`wazzup`.`crmEmployees`