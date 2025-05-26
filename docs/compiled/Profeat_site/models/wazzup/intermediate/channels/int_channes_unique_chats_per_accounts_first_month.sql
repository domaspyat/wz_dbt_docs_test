with accounts as (
    select account_id, register_date from `dwh-wazzup`.`dbt_nbespalov`.`stg_accounts`
),

channels as (
    select account_id, guid from `dwh-wazzup`.`dbt_nbespalov`.`stg_channels`
),

unique_chats_data as (
    select * from  `dwh-wazzup`.`dbt_nbespalov`.`stg_unique_chats`
),

active_channels as (
     select * from  `dwh-wazzup`.`dbt_nbespalov`.`int_channels_active_channels`
),

unique_dialogs_paid_channels as (
    SELECT distinct accounts.account_id, 
    unique_chats_data.date, 
    unique_chats as unique_dialogs
    FROM  accounts
    left join channels on accounts.account_id=channels.account_id 
    left join unique_chats_data on  unique_chats_data.channel_id=channels.guid
    left join active_channels on active_channels.channel_id=unique_chats_data.channel_id 
    where unique_chats_data.date<=date_add(accounts.register_date, interval 1 month)
    and active_channels.date<=date_add(accounts.register_date, interval 1 month) 
    and subscription_id is not null and is_free is distinct from True
),

unique_dialogs_paid_channels_aggregated as (
    SELECT account_id,                      -- ID аккаунта
    sum(unique_dialogs) as unique_dialogs   -- Количество уникальных диалогов
    FROM  unique_dialogs_paid_channels
    group by 1
)
    -- Таблица c количеством уникальных диалогов по каналам в первый месяц после регистрации
select * from unique_dialogs_paid_channels_aggregated