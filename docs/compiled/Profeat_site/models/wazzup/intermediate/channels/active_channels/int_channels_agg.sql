with
    channels as (
        select
            account_id,
            (
                case
                    when transport in ('waba', 'wapi')
                    then 'waba'
                    when transport = 'vk'
                    then 'vk'
                    else transport
                end
            ) as transport,
            guid,
            deleted,
            state,
            created_at
        from `dwh-wazzup`.`dbt_nbespalov`.`stg_channels`
        where temporary = false
    ),

    channel_history_with_active_partition as (
        select
            *,
            sum(title_changed) over (
                partition by channel_id order by id
            ) as partition_no
        from `dwh-wazzup`.`dbt_nbespalov`.`int_channels_channelHistory_with_active_partition`
    ),

    channel_agg as (
        select
            channels.state as channel_state,
            transport,
            coalesce(guid, channel_id) as channel_id,
            occured_at,
            (
                case
                    when
                        lead(occured_at, 1) over (partition by channel_id order by id)
                        is not null
                    then lead(occured_at, 1) over (partition by channel_id order by id)
                    when channels.state = 'active' and deleted = false
                    then current_timestamp
                    else occured_at
                end
            ) as occured_at_next,
            partition_no,
            package_id,
            coalesce(
                channel_history_with_active_partition.state, channels.state
            ) as channel_current_state,
            account_id,
            created_at
        from channels
        left join
            channel_history_with_active_partition
            on channels.guid = channel_history_with_active_partition.channel_id
    ),

    min_occured_at as (
        select channel_id, min(occured_at) as occured_at_next
        from channel_agg
        where transport in ('telegram', 'vk')
        group by 1
    ),

    channel_start_telegram_vk as (
        select
            'active' as channel_state,          -- Состояние канала
            transport,                          -- Транспорт канала
            min_occured_at.channel_id,          -- Дата и время создания канала
            created_at as occured_at,           -- Дата и время события
            min_occured_at.occured_at_next,     -- Дата и время следующего события
            0 as partition_no,                  -- Порядковый номер изменения
            cast(null as string) as package_id, -- ID подписки
            'active' as channel_current_state,  -- Текущее состояние канала
            account_id, -- ID аккаунта
            created_at  -- Дата и время создания канала
        from min_occured_at
        left join channel_agg on channel_agg.channel_id = min_occured_at.channel_id
        where transport in ('telegram', 'vk')
        group by 1,2,3,4,5,6,7,8,9,10
    ),

    channel_agg_with_created_date as (
        select *
        from channel_agg
        union all
        select *
        from channel_start_telegram_vk
    )
    -- Таблица c активными каналами
select *
from channel_agg_with_created_date