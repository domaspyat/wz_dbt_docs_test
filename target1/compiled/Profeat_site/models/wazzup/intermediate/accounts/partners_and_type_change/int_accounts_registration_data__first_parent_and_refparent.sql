-- Таблица дат регистраций, связями с партнером/реф.папой и датой окончания первых связей   

with first_partner_and_refparentid_groupped as (            -- Тянем все данные из таблицы с первыми партнерами, реф.папами и датой окончания связи с ними
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_accounts__first_partner_and_refparent_groupped`
),

affiliates as (         -- Тянем все данные из таблицы, в которой хранятся все партнерские отношения и рефералы. Информация на текущий момент.
    select * from `dwh-wazzup`.`dbt_nbespalov`.`stg_affiliates`
),

accounts as (           -- Тянем данные из таблицы аккаунтов
    select * from `dwh-wazzup`.`dbt_nbespalov`.`stg_accounts` 
),

registration_data_with_first_parent_and_refparent as (          -- Таблица дат регистраций, связями с партнером/реф.папой и датой окончания первых связей   

select 
    accounts.account_id,                                                                                        -- Номер аккаунта
    register_at as start_occured_at,                                                                            -- Начало действия первой связи с партнером и реф.папой (Дата и время регистрации аккаунта)
    coalesce(datetime(end_occured_at),datetime(current_timestamp(),'Europe/Moscow')) as end_occured_at,         -- Дата и время первого изменения партнера у аккаунта, то есть завершение действия первого партнера/реф.папы. Если Null, то текущее время МСК
    coalesce(first_partner_and_refparentid.partner_id,affiliates.partner_id) as partner_id,                     -- Первый аккаунт партнера, который был у аккаунта. Если партнера не было (null), то присваеваем текущее значение из affilates
    coalesce(first_partner_and_refparentid.refparent_id, affiliates.refparent_id) as refparent_id               -- Первый аккаунт реф.папы, который был у аккаунта. Если партнера не было (null), то присваеваем текущее значение из affilates

from accounts
left join first_partner_and_refparentid_groupped first_partner_and_refparentid
    on accounts.account_id=first_partner_and_refparentid.account_id
left join affiliates
    on affiliates.child_id=accounts.account_id
)


select * from registration_data_with_first_parent_and_refparent