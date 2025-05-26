select          -- Таблица изменений подписок
    guid,               -- Идентификатор изменения.Генерируется Postgress при создании записи
    subscriptionId as subscription_id,                          -- Идентификатор подписки.Соответствует полю guid из таблицы stg_billingPackages
    activationReasonId as activation_reason_id,                 -- Идентификатор аккаунта партнера, к которому на данный момент привязана дочка. null - если это аккаунт без партнера
    activationObject as activation_object,                      -- id/guid записи, на основании которого было применено(оплачено) изменение. Подробнее в wazzup_staging.yml 
    balanceToWithdraw as balance_to_withdraw,                   -- В случае частичной оплаты кол-во бонусов клиента используемых для оплаты подписки
    wapi_transactions as wapi_transactions,                     -- Cумма на которую пополняем ВАБА баланс
    datetime(createdAt,'Europe/Moscow') as created_at,          -- Дата и время создания изменения в базе данных
    datetime(updatedAt,'Europe/Moscow') as updated_at,          -- Дата и время обновления изменения (не работает, тут значение createdAt в большинстве случаев)
    cast(updatedAt as DATE) as updated_date,                    -- Дата обновления изменения (не работает, тут значение createdAt в большинстве случаев)
    sum as sum,                                                 -- Сумма изменений без учета скидок
    _ibk as created_date,                                       -- Дата создания изменения в базе данных
    currency,                                                   -- Валюта владельца подписки,
    state,                                                      -- Состояние изменения
    json_value(data,'$.action') as action,                      -- Действие, для которого создано изменение
    cast(json_value(logs,'$.promotionType') as INTEGER) as promotion_type,      -- Id акции, в случае, если изменение оплачивали по акции
    json_value(logs,'$.period') as old_period,                  -- Период подписки на момент создания изменения
    json_value(logs,'$.quantity') as old_quantity,              -- Кол-во каналов на момент создания изменения
    json_value(logs,'$.tariff') as old_tariff,                  -- Тариф подписки на момент создания изменения
    cast(json_value(data,'$.forAutoRenewal') as bool) as for_auto_renewal,      -- Признак показывающий включенное автопродление подписки 
    coalesce(json_value(logs,'$.newPeriod'),json_value(logs,'$.period')) as period,             -- Новый период подписки
    coalesce(json_value(logs,'$.newQuantity'),json_value(logs,'$.quantity')) as quantity,       -- Новое кол-во каналов
    coalesce(json_value(logs,'$.newTariff'),json_value(logs,'$.tariff')) as tariff,             -- Новый тариф подписки
    cast(json_value(logs,'$.untilExpiredDays') as INTEGER) as until_expired_days,               -- Количество дней до окончания на момент создания изменения
    cast(json_value(logs,'$.newUntilExpiredDays') as INTEGER) as new_until_expired_days,        -- Количество дней до окончания на момент применения изменения
    cast(json_value(logs,'$.partnerDiscount') as numeric) as partner_discount                   -- % партнерской скидки на момент создания изменения
from `dwh-wazzup`.`wazzup`.`subscriptionUpdates`