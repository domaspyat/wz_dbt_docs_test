select          -- В таблице хранятся все партнерские отношения и рефералы. Информация на текущий момент.
    childId as child_id,                    -- Идентфиикатор аккаунта дочки, соответсвует account_id из stg_accounts.
    refParentId as refparent_id,            -- Реферальный папа - записывается один раз и никогда не меняется. Не обязательно партнер, это могут быть обычные аккаунты. Соответсвует account_id из stg_accounts.
    partnerId as partner_id,                -- Идентфиикатор аккаунта партнера, соответсвует account_id из stg_accounts. Может быть изменен.
    refLinkCode as refLink_code,            -- Ссылка, по которой зарегестрировался пользователь
    createdAt as created_at,                -- Дата и время создания регистрации дочки
    name                                    -- Наименование аккаунта
from `dwh-wazzup`.`wazzup`.`affiliates`