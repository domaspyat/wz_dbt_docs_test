select          -- Таблица с информацией по маркетплейсным интеграциям
    crmName as crm_name,            -- Название кастомной CRM
    crmCode as crm_code,            --
    status,                         -- Статус интеграции
    accountId as account_id         -- Идентификатор аккаунта, соответствует id из таблицы stg_accounts
from `dwh-wazzup`.`wazzup`.`crmMarketplace`