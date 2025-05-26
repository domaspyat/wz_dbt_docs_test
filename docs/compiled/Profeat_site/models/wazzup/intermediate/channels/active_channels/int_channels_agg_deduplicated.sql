

with channel_agg as (
    select * from `dwh-wazzup`.`dbt_nbespalov`.`int_channels_agg`
),


channel_agg_deduplicated as (
    select package_id,
    partition_no,
    ca.account_id,
    channel_id, 
    transport,
    (case 
    when min(DATETIME(occured_at, 'Europe/Moscow')) is null then min(DATETIME(created_At, 'Europe/Moscow')) 
    else min(DATETIME(occured_at, 'Europe/Moscow')) end) as min_datetime,
    max(DATETIME(occured_at_next, 'Europe/Moscow')) as max_datetime 
    from channel_agg ca
    where channel_current_state='active'
    group by 1,2,3,4,5
),
chanenels_with_waba_subscription as (
select  -- Таблица c активными каналами без дублей после группировки
(case when partition_no is null and channels_deduplicated.transport='waba' then channels.package_id else channels_deduplicated.package_id end) as package_id,   -- ID подписки
channels_deduplicated.partition_no, -- Порядковый номер изменения
channels_deduplicated.account_id,   -- ID аккаунта
channels_deduplicated.channel_id,   -- ID канала
channels_deduplicated.transport,    -- Транспорт канала
channels_deduplicated.min_datetime, -- Минимальная дата изменения
channels_deduplicated.max_datetime  -- Максимальная дата изменения
from channel_agg_deduplicated channels_deduplicated
left join `dwh-wazzup`.`dbt_nbespalov`.`stg_channels` channels
on channels.guid=channels_deduplicated.channel_id)
select * from chanenels_with_waba_subscription