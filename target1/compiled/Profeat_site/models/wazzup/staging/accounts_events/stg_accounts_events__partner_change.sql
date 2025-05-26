select          -- Таблица с изменениями по партнерам аккаунтов
    accountId as account_id,        -- Идентификатор аккаунта, соответствует id из таблицы stg_accounts. id аккаунта, которого прикрепили к партнеру/рефералу (ребенок)
    first_value(occured_at) over  (partition by accountId  order by occured_at) as first_value_occured_at,              -- Дата и время первого изменения партнера у аккаунта
    first_value(oldPartnerId) over (partition by accountId order by occured_at) as first_value_partner_id,              -- Первый аккаунт партнера, который был у аккаунта. Если null, то партнера не было
    first_value(oldRefParentId) over (partition by accountId order by occured_at) as first_value_refparent_id,          -- Первый аккаунт реф.папы, который был у аккаунта. Если null, то реф.папы не было
    datetime(occured_at) as start_occured_at,                      -- Дата и время изменения партнера
    coalesce(lag(datetime(occured_at),1 ) over (partition by accountId order by occured_at desc), datetime(current_timestamp,'Europe/Moscow')) as end_occured_at,    -- Дата и время, до которого был текущий партнер
    newPartnerId as partner_id,                                    -- Идентификатор аккаунта нового партнера, соответствует id из таблицы stg_accounts
    newRefParentId as refparent_id                                 -- Идентификатор аккаунта реферального папы, соответствует id из таблицы stg_accounts
from `dwh-wazzup`.`wazzup`.`analytic_events`
where event_type=1