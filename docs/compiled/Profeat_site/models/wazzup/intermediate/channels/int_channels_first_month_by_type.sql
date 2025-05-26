with accounts as (
    select account_id, register_date from `dwh-wazzup`.`dbt_nbespalov`.`stg_accounts`
),

channels as (
    select account_id, guid from `dwh-wazzup`.`dbt_nbespalov`.`stg_channels`
),


active_channels as (
     select * from  `dwh-wazzup`.`dbt_nbespalov`.`int_channels_active_channels`
),

unique_transport_paid_channels as (
    SELECT 
    accounts.account_id,                        -- ID аккаунта
    transport,                                  -- Транспорт канала
    count(distinct channel_id) as channel_count -- Количество каналов
    FROM  accounts
    inner join active_channels on active_channels.account_id=accounts.account_id 
    where active_channels.date<=date_add(accounts.register_date, interval 1 month) 
    and subscription_id is not null and is_free is distinct from True
    group by 1,2
)
    -- Таблица c каналами в первый месяц после регистрации
select * from unique_transport_paid_channels
pivot(sum(channel_count) for transport in ('tgapi','whatsapp','instagram','telegram','waba','vk','avito'))