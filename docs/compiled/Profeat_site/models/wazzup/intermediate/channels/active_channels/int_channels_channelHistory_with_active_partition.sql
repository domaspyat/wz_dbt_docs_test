with channel_history_with_state_group as (
    select ch.occured_at,  -- Дата и время события
    (case when ch.deleted=True then 'deleted' 
    else ch.state
    end) as 
    state,                 -- Состояние канала
    ch.channel_id,         -- ID канала
    (case when ch.state='active' then 1
    else 0 end) as state_group, -- Группа состояния. 1, если active, 0 в других случаях
    ch.package_id,         -- ID подписки
    ch.id                  -- ID записи
    from `dwh-wazzup`.`dbt_nbespalov`.`stg_channelHistory` ch
    JOIN `dwh-wazzup`.`dbt_nbespalov`.`stg_channels` c on ch.channel_id = c.guid
    WHERE cast(datetime(ch.occured_at,'Europe/Moscow') as timestamp) >= c.created_At
)
    -- Таблица, которая показывает были ли изменения по каналу
select *, 
    (case WHEN (state = lag(state, 1) OVER (partition by channel_id ORDER BY occured_at)) 
    and (package_Id= lag(package_id,1) OVER (partition by channel_id ORDER BY occured_at)) THEN 0 ELSE 1 END) AS title_changed  -- Были ли изменения. 1, если да. 0, если нет.
    from  channel_history_with_state_group