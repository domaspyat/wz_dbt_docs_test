
with channels as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_channels_groupped_by_accounts_transport`
),
 channels_for_filter as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_channels_groupped_by_accounts_transport`
    where transport not like '%Не%'
),
active_channels as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_channels_active_groupped_by_accounts_transport`
    where transport not like '%Нет%'
),

active_channels_with_active_subscription as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_channels_active_with_subscription_groupped_by_accounts_transport`
    where transport not like '%Нет%'
),

does_not_have_channels as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_channels_groupped_by_accounts_transport`
    where transport like '%Не%'
),

does_not_have_active_channels as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_channels_active_groupped_by_accounts_transport`
    where transport like '%Нет%'
),

does_not_have_active_channels_with_active_subscription as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_channels_active_with_subscription_groupped_by_accounts_transport`
    where transport like '%Нет%'
),



all_channels as (
select channels.account_Id, -- ID аккаунта

        IFNULL(coalesce(channels_for_filter.transport,
                LAST_VALUE(channels_for_filter.transport IGNORE NULLS) OVER (PARTITION BY channels.account_id ORDER BY channels.account_id)),'Нет ни одного неудаленного канала') transport_channel,    -- Какой канал был у клиента?
        IFNULL(coalesce(active_channels.transport,
                LAST_VALUE(active_channels.transport IGNORE NULLS) OVER (PARTITION BY channels.account_id ORDER BY channels.account_id)),'Нет ни одного активного канала') transport_active_channel,    -- Какой активный канал был у клиента?
        IFNULL(coalesce(active_channels_with_active_subscription.transport,
                LAST_VALUE(active_channels_with_active_subscription.transport IGNORE NULLS) OVER (PARTITION BY channels.account_id ORDER BY channels.account_id)),'Нет ни одного активного канала с активной подпиской') transport_active_with_active_subscription, -- Какой активный канал с активной подпиской был у клиента?
        
        IFNULL(coalesce(does_not_have_channels.transport,
                LAST_VALUE(does_not_have_channels.transport IGNORE NULLS) OVER (PARTITION BY channels.account_id ORDER BY channels.account_id)),'У пользователя были все каналы') does_not_have_transport_channel,  -- Какого канала не было у клиента?
        coalesce(does_not_have_active_channels.transport,
                LAST_VALUE(does_not_have_active_channels.transport IGNORE NULLS) OVER (PARTITION BY channels.account_id ORDER BY channels.account_id)) does_not_have_transport_active_channel,  -- Какого активного канала не было у клиента?
        coalesce(does_not_have_active_channels_with_active_subscription.transport,
                LAST_VALUE(does_not_have_active_channels_with_active_subscription.transport IGNORE NULLS) OVER (PARTITION BY channels.account_id ORDER BY channels.account_id)) does_not_have_transport_active_with_active_subscription -- Какого активного канала с активной подпиской не было у клиента?

from channels

left join channels_for_filter
        on channels.account_Id = channels_for_filter.account_Id 
        and channels.transport_order_number = channels_for_filter.transport_order_number
left join active_channels 
        on channels.account_Id = active_channels.account_Id 
        and channels.transport_order_number = active_channels.transport_order_number
left join active_channels_with_active_subscription 
        on channels.account_Id = active_channels_with_active_subscription.account_Id 
        and channels.transport_order_number = active_channels_with_active_subscription.transport_order_number

left join does_not_have_channels 
        on channels.account_Id = does_not_have_channels.account_Id 
        and channels.transport_order_number = does_not_have_channels.transport_order_number
left join does_not_have_active_channels
        on channels.account_Id = does_not_have_active_channels.account_Id 
        and channels.transport_order_number = does_not_have_active_channels.transport_order_number
left join does_not_have_active_channels_with_active_subscription 
        on channels.account_Id = does_not_have_active_channels_with_active_subscription.account_Id 
        and channels.transport_order_number = does_not_have_active_channels_with_active_subscription.transport_order_number               
        )   -- Таблица с каналами после группировки по аккаунту и транспорту
select * 
from all_channels
order by account_Id