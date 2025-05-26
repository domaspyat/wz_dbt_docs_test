select          -- Таблица биллинга. На данный момент не используется. Нужна для исторических данных
    accountId as account_id,                            -- Идентификатор аккаунта, соответствует id из таблицы stg_accounts
    dateTime(dateTime,'Europe/Moscow') as paid_at,      -- Дата и время оплаты
    _ibk as paid_date,                                  -- Дата оплаты
    currency as currency,                               -- Валюта
    sumInRubles as sum_in_rubles,
    sum,
    object,
    method,
    json_value(details,'$.provider') as provider, 
    details,    
    guid,
    --две кошмарные строчки ниже обусловены тем, что BigQuery не может нормально парсить строчки с None. Отказывается парсить json дальше после None
    datetime(cast(json_value(replace(replace(replace(details, "'name': None, ",''),"'isFree': None,",''),"'changedAt': None,",'') ,'$.paidAt') as timestamp), 'Europe/Moscow') as start_at,
    datetime(cast(json_value(replace(replace(replace(details, "'name': None, ",''),"'isFree': None,",''),"'changedAt': None,",'') ,'$.expiresAt') as timestamp), 'Europe/Moscow') as end_at
from `dwh-wazzup`.`wazzup`.`billing`